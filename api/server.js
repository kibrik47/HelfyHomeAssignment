const express = require('express');
const bodyParser = require('body-parser');
const pool = require('./db');
const { v4: uuidv4 } = require('uuid');
const log4js = require('log4js');

log4js.configure({
  appenders: { out: { type: 'console' } },
  categories: { default: { appenders: ['out'], level: 'info' } }
});
const logger = log4js.getLogger();

const app = express();
app.use(bodyParser.json());

// helper log
function structuredLog({ userId = null, action, ip }) {
  const entry = {
    timestamp: new Date().toISOString(),
    userId,
    action,
    ip
  };
  logger.info(JSON.stringify(entry));
}

// Basic login route
app.post('/login', async (req, res) => {
  try {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const { usernameOrEmail, password } = req.body || {};
    if (!usernameOrEmail || !password) {
      return res.status(400).json({ message: 'usernameOrEmail and password required' });
    }

    const [rows] = await pool.query('SELECT * FROM users WHERE username = ? OR email = ? LIMIT 1', [usernameOrEmail, usernameOrEmail]);
    const user = rows[0];
    if (!user || user.password !== password) {
      structuredLog({ userId: user ? user.id : null, action: 'login_failed', ip });
      return res.status(401).json({ message: 'invalid credentials' });
    }

    // create token and store in DB
    const token = uuidv4();
    await pool.query('UPDATE users SET token = ? WHERE id = ?', [token, user.id]);

    structuredLog({ userId: user.id, action: 'login_success', ip });

    // return token in header and body (client should send token as header on future requests)
    res.set('x-auth-token', token);
    return res.json({ message: 'ok', token });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: 'server error' });
  }
});

// middleware to protect routes
async function authMiddleware(req, res, next) {
  const token = req.headers['x-auth-token'];
  if (!token) return res.status(401).json({ message: 'missing token' });
  const [rows] = await pool.query('SELECT id, username FROM users WHERE token = ? LIMIT 1', [token]);
  const user = rows[0];
  if (!user) return res.status(401).json({ message: 'invalid token' });
  req.user = user;
  next();
}

// protected route
app.get('/profile', authMiddleware, async (req, res) => {
  // example: read user data
  structuredLog({ userId: req.user.id, action: 'fetch_profile', ip: req.headers['x-forwarded-for'] || req.socket.remoteAddress });
  res.json({ id: req.user.id, username: req.user.username });
});

const port = 3000;
app.listen(port, () => {
  console.log(`api listening on ${port}`);
});
