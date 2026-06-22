# JetStream Examples

NATS JetStream examples in Python — fanout, pull consumers, replay, stream inspection, and a serial-to-NATS bridge.

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

For the serial bridge only, also install:

```bash
pip install pyserial-asyncio
```

---

## Scripts

### `jetstream_fanout.py` — Fanout demo

Shows 3 independent consumers each receiving all 10 messages, plus replay from a specific sequence.

```bash
source venv/bin/activate
python3 jetstream_fanout.py
```

**What it does:**
1. Creates a `logs` stream on subject `logs.>`
2. Publishes 10 messages (id, timestamp, level, text)
3. Creates 3 durable consumers, each starting from the beginning
4. Runs all 3 concurrently — each receives all 10 messages independently (fanout)
5. Prints consumer state (delivered, pending)
6. Demonstrates replay: new consumer starts from a specific stream sequence

---

### `jetstream_examples.py` — Annotated walkthrough

Six self-contained examples that build up from basics to fanout and inspection.

```bash
source venv/bin/activate
python3 jetstream_examples.py
```

| Example | What it shows |
|---|---|
| 1 | Create a stream and publish messages |
| 2 | Pull consumer — fetch one message at a time |
| 3 | Fanout — 3 independent consumers running in parallel (runs ~10 sec) |
| 4 | Push consumer — server delivers messages to subscriber (commented out by default) |
| 5 | Replay — consumer starts from a specific sequence number |
| 6 | Inspect — stream state and per-consumer pending/ack counts |

---

### `serial_jetstream_bridge.py` — Serial ↔ NATS bridge

Bidirectional bridge: reads lines from a serial port and publishes them to NATS JetStream, while simultaneously writing inbound NATS messages back to the serial port.

```
serial port  ──►  device.tx  (stream: serial-bridge)
serial port  ◄──  device.rx  (stream: serial-bridge)
```

```bash
source venv/bin/activate
python3 serial_jetstream_bridge.py /dev/ttyUSB0
python3 serial_jetstream_bridge.py /dev/ttyUSB0 --baudrate 9600
python3 serial_jetstream_bridge.py COM3 --nats nats://myserver:4222
```

**Options:**

| Flag | Default | Description |
|---|---|---|
| `port` | (required) | Serial port, e.g. `/dev/ttyUSB0` or `COM3` |
| `--baudrate` | `115200` | Baud rate |
| `--nats` | `nats://localhost:4222` | NATS server URL |
| `--tx-subject` | `device.tx` | Subject for serial → NATS |
| `--rx-subject` | `device.rx` | Subject for NATS → serial |
| `--stream-name` | `serial-bridge` | JetStream stream name |

Messages are line-buffered: each `\n`-terminated line becomes one NATS message.

---

## Key concepts

| Concept | Description |
|---|---|
| **Stream** | Persistent, ordered log of messages on one or more subjects |
| **Durable consumer** | Named consumer with its own position in the stream — survives reconnects |
| **Fanout** | Each durable consumer gets every message independently (vs. queue group which load-balances) |
| **Replay** | Consumer created with `deliver_policy="by_start_sequence"` re-reads from any past sequence |
| **Pull consumer** | Consumer explicitly fetches messages (`fetch()`) rather than receiving a push |
| **Push consumer** | Server actively delivers messages to a subscriber callback |

## Stop NATS

```bash
docker stop nats-js && docker rm nats-js
```
