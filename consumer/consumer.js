const { Kafka } = require('kafkajs');
const log4js = require('log4js');

log4js.configure({
  appenders: { out: { type: 'console' } },
  categories: { default: { appenders: ['out'], level: 'info' } }
});
const logger = log4js.getLogger();

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const kafka = new Kafka({ clientId: 'tidb-consumer', brokers: [kafkaBroker] });
const consumer = kafka.consumer({ groupId: 'tidb-change-consumer' });

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForKafkaAndTopic(topicName, { retries = 30, delay = 2000 } = {}) {
  const admin = kafka.admin();
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await admin.connect();
      const topics = await admin.listTopics();
      if (topics.includes(topicName)) {
        await admin.disconnect();
        return true;
      }
      await admin.disconnect();
    } catch (err) {
      // ignore and retry
    }
    logger.info(`waiting for topic '${topicName}' (attempt ${attempt}/${retries})`);
    await sleep(delay);
  }
  return false;
}

async function start() {
  const topicName = 'tidb_changes';

  // Wait for Kafka/topic to be available to avoid UNKNOWN_TOPIC_OR_PARTITION errors
  const ok = await waitForKafkaAndTopic(topicName, { retries: 30, delay: 2000 });
  if (!ok) {
    logger.warn(`topic '${topicName}' not found after waiting — attempting to create it`);
    const admin = kafka.admin();
    try {
      await admin.connect();
      const created = await admin.createTopics({
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true
      });
      logger.info(`admin.createTopics result: ${created}`);
    } catch (err) {
      logger.error('failed to create topic via admin:', err && err.message);
    } finally {
      try { await admin.disconnect(); } catch (_) {}
    }

    // Wait again briefly for metadata to settle
    const ok2 = await waitForKafkaAndTopic(topicName, { retries: 15, delay: 2000 });
    if (!ok2) logger.warn(`topic '${topicName}' still not visible after create attempt; will continue and rely on broker auto-creation`);
  }

  // Ensure consumer connects with retries (broker may not have group coordinator yet)
  let connected = false;
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      await consumer.connect();
      connected = true;
      break;
    } catch (err) {
      logger.error(`consumer connect attempt ${attempt} failed: ${err && err.message}`);
      await sleep(2000 * attempt);
    }
  }
  if (!connected) {
    throw new Error('Unable to connect consumer to Kafka after multiple attempts');
  }

  // Subscribe with retries
  let subscribed = false;
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      await consumer.subscribe({ topic: topicName, fromBeginning: true});
      subscribed = true;
      break;
    } catch (err) {
      logger.error(`subscribe attempt ${attempt} failed: ${err && err.message}`);
      // try to refresh metadata by disconnecting and reconnecting
      try { await consumer.disconnect(); } catch (_) {}
      await sleep(1000 * attempt);
      try { await consumer.connect(); } catch (e) {}
    }
  }
  if (!subscribed) {
    throw new Error(`Unable to subscribe to topic '${topicName}' after multiple attempts`);
  }
  console.log('consumer connected to', kafkaBroker, 'and subscribed to', topicName);

  // Poll topic metadata to see partition counts and offsets (diagnostic)
  const admin = kafka.admin();
  try {
    await admin.connect();
    const partitions = await admin.fetchTopicMetadata({ topics: [topicName] });
    if (partitions.topics && partitions.topics.length > 0) {
      const topicMeta = partitions.topics[0];
      logger.info(`Topic '${topicName}' metadata: ${JSON.stringify({
        name: topicMeta.name,
        partitions: topicMeta.partitions && topicMeta.partitions.length,
        brokers: partitions.brokers && partitions.brokers.length
      })}`);
    }
  } catch (err) {
    logger.warn('failed to fetch topic metadata:', err && err.message);
  } finally {
    try { await admin.disconnect(); } catch (_) {}
  }

  // Run the consumer in a resilient loop: if run throws, attempt to restart it
  for (let runAttempt = 1; ; runAttempt++) {
    try {
      logger.info(`consumer.run() attempt ${runAttempt}`);
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const raw = message.value.toString();
          let parsed = null;
          try {
            parsed = JSON.parse(raw);
          } catch (e) {
            // not JSON, fallback to raw
          }

          // Build structured entry consistent with existing structuredLog style
          const entry = {
            timestamp: new Date().toISOString(),
            action: 'db_change',
            source: { topic, partition, offset: message.offset },
            change: null,
          };

          if (parsed && typeof parsed === 'object') {
            entry.change = {
              type: parsed.type || parsed.event || null,
              database: parsed.database || parsed.schema || parsed.source || null,
              table: parsed.table || null,
              commitTs: parsed.commitTs || parsed.ts || parsed.timestamp || null,
              data: parsed.data || parsed.rows || parsed.value || null,
              old: parsed.old || null,
              raw: parsed
            };
          } else {
            entry.change = { raw: raw };
          }

          logger.info(JSON.stringify(entry));
        }
      });
      // If run resolves, break (normally run doesn't resolve while consumer is active)
      break;
    } catch (err) {
      logger.error(`consumer run attempt ${runAttempt} failed: ${err && err.message}`);
      // Attempt graceful reconnect and retry
      try { await consumer.disconnect(); } catch (_) {}
      await sleep(Math.min(5000 * runAttempt, 30000));
      try { await consumer.connect(); } catch (e) {}
      // re-subscribe before next run attempt
      try { await consumer.subscribe({ topic: topicName, fromBeginning: false }); } catch (e) {}
    }
  }
}

start().catch(err => {
  console.error('consumer error', err);
  process.exit(1);
});
