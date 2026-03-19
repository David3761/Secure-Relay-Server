# Secure Relay Server

A high-performance, lightweight WebSocket relay server built in Go. This server acts as a middleman for end-to-end encrypted (E2EE) chat applications. It routes messages in real-time between online clients and securely queues offline messages in a PostgreSQL database until the recipient comes back online.

## Features

* **Zero-Knowledge Routing:** The server only sees sender/recipient public keys and an `encrypted_blob`. It cannot read the plaintext contents of the messages.
* **Real-Time WebSockets:** Utilizes `gorilla/websocket` for fast, persistent, and low-latency bidirectional communication.
* **Offline Message Queuing:** If a recipient is offline, messages are safely stored in PostgreSQL. Upon reconnection, the server instantly delivers and clears the queued messages.
* **Delivery Acknowledgments (ACK):** Senders receive automatic `ack` events when a message is successfully routed to the recipient or queued in the database.
* **Authorized Connections:** WebSocket upgrades are protected by an `X-App-Secret` header, preventing unauthorized clients from connecting to the relay.
* **Automatic Schema Management:** The server automatically verifies and initializes the required PostgreSQL tables (`users` and `offline_messages`) on startup using `pgxpool`.

---

## Prerequisites

* **Go** (v1.26.1 or higher)
* **PostgreSQL** database
* Git (for version control)
