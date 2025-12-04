# HelfyHomeAssignment
Test Assignment – Junior SRE Developer

**Overview**
Build a complete full-stack application with authentication, database integration, and monitoring
capabilities.


## Services Overview

| Service        | Purpose                                                                 |
|----------------|-------------------------------------------------------------------------|
| `tidb`         | MySQL-compatible database                                               |
| `tikv`         | Key-value storage for TiDB                                              |
| `pd`           | Placement Driver, coordinates TiDB cluster                              |
| `ticdc-init`   | Initializes TiCDC changefeeds                                           |
| `kafka`        | Message broker for CDC events                                           |
| `consumer`     | Node.js consumer that logs database changes                             |
| `api`          | Node.js API exposing `/login` and `/profile` endpoints                  |
| `client`       | Simple HTML/JS frontend     


## Setup Instructions

1. **Clone the repository**
git clone <repo url>

2. **Start the services using Docker Compose**
first time running: docker-compose up --build
Onwards: docker-compose up

3. **Access frontend**
http://localhost:8080/

## Project Overview
Once you access the frontend, you will be able to fill in the credentials with a username/email + password. If filled wrong, the authentication process will fail and you will not recieve a token. If filled correctly with the pre-configured credentials (username:admin, email:admin@example.com, password:password), the reverse proxy passes the request to the api that will validate the credentials in TiDB, generate a token and return it. With that token, one will be able to call for the /profile call successfully which requires a valid token. 
With the insertion / update / deletion of any credentials on the database, the TiCDC gets triggered. The change events get published to Kafka under the topic tidb_changes. The consumer reads these events in real-time and logs them in a structured JSON format on the console. 
