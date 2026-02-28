# Kafka Demo

A demonstration project showcasing Apache Kafka with Avro serialization, Schema Registry, and PostgreSQL integration using Python.

## Overview

This project implements a simple event-driven architecture:

1. **Producer** generates fake user data and publishes it to a Kafka topic
2. **Kafka** handles message streaming with Avro schema validation
3. **Consumer** reads from Kafka and persists data to PostgreSQL

## Tech Stack

- **Python 3.11+**
- **Apache Kafka 7.7.7** (via Confluent Platform)
- **Confluent Schema Registry**
- **Avro** for message serialization
- **PostgreSQL 16**
- **Docker Compose** for infrastructure

## Project Structure

```
kafka-demo/
├── producer.py          # Produces fake user data to Kafka
├── consumer.py          # Consumes from Kafka and writes to PostgreSQL
├── user.avsc            # Avro schema definition
├── compose.yml          # Docker Compose services
├── init-db/
│   └── init.sql         # Database initialization
└── pyproject.toml       # Python dependencies
```

## Getting Started

### Prerequisites

- uv (Python package manager)
- Docker and Docker Compose

### Setup

1. Install dependencies:

```bash
uv sync
```

2. Start the infrastructure services:

```bash
docker compose up -d
```

Wait a few seconds for Kafka to be fully ready.

### Run the Producer

```bash
uv run producer.py
```

The producer generates 5 fake users with the following attributes:

- `id` (UUID)
- `name`
- `email`
- `country`
- `created_at`

Each message is serialized using Avro and registered with the Schema Registry.

### Run the Consumer

```bash
uv run consumer.py
```

The consumer subscribes to the `users` topic, deserializes Avro messages, and inserts them into the `users` table in PostgreSQL.

### Verify Data

Connect to PostgreSQL:

```bash
docker exec -it kafka_db psql -h localhost -U postgres -d user_db
```

Query the users table:

```sql
SELECT * FROM users;
```

## Services

| Service         | Port | Description            |
| --------------- | ---- | ---------------------- |
| Kafka           | 9092 | Kafka broker           |
| Schema Registry | 8081 | Avro schema management |
| Control Center  | 9021 | Kafka management UI    |
| PostgreSQL      | 5432 | Database               |

## Cleanup

```bash
docker compose down -v
```

This removes all containers and volumes.
