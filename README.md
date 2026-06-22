# JetStream Fanout Examples

Demonstrates NATS JetStream fanout with Python — multiple independent consumers reading the same stream, plus replay from a specific sequence.

## Prerequisites

- Docker
- Python 3.10+

## Setup

**1. Start NATS with JetStream enabled:**

```bash
docker run -d --name nats-js -p 4222:4222 nats:latest -js
```

**2. Create a virtual environment and install dependencies:**

```bash
python3 -m venv venv
source venv/bin/activate
pip install nats-py
```

## Run

```bash
source venv/bin/activate
python3 jetstream_fanout.py
```

## What it does

1. Connects to NATS on `localhost:4222`
2. Creates (or reuses) a `logs` stream on subject `logs.>`
3. Publishes 10 messages with id, timestamp, message, and log level
4. Creates 3 durable consumers, each starting from the beginning of the stream
5. Runs all 3 consumers concurrently — each receives all 10 messages independently (fanout, not load-balanced)
6. Prints consumer state (delivered, pending) after processing
7. Demonstrates replay: creates a new consumer starting from a specific stream sequence and replays messages from that point
8. Prints final stats for all consumers

## Key concepts

| Concept | Description |
|---|---|
| **Stream** | Persistent log of messages on `logs.>` |
| **Durable consumer** | Named consumer with its own position in the stream — survives reconnects |
| **Fanout** | Each consumer gets every message (vs. queue group which load-balances) |
| **Replay** | Consumer created with `deliver_policy="by_start_sequence"` re-reads from any past sequence |
| **Pull consumer** | Consumer explicitly fetches messages (`fetch()`) rather than receiving a push |

## Stop NATS

```bash
docker stop nats-js && docker rm nats-js
```
