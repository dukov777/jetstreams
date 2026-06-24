# JetStream Examples

NATS JetStream examples in Python — fanout, pull consumers, replay, stream inspection, and a serial-to-NATS bridge.

## Prerequisites

- Docker
- [uv](https://docs.astral.sh/uv/) (Python 3.10+ is provisioned automatically)

## Setup

**1. Start NATS with JetStream enabled:**

```bash
docker run -d --name nats-js -p 4222:4222 nats:latest -js
```

**2. Install dependencies:**

```bash
uv sync
```

This creates a `.venv` and installs all dependencies (`nats-py`, `pyserial-asyncio`) from `pyproject.toml`.

---

## Scripts

### `jetstream_fanout.py` — Fanout demo

Shows 3 independent consumers each receiving all 10 messages, plus replay from a specific sequence.

```bash
uv run jetstream_fanout.py
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
uv run jetstream_examples.py
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
uv run serial_jetstream_bridge.py /dev/ttyUSB0
uv run serial_jetstream_bridge.py /dev/ttyUSB0 --baudrate 9600
uv run serial_jetstream_bridge.py COM3 --nats nats://myserver:4222
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

**Stream retention:** the bridge creates the `serial-bridge` stream with `max_msgs=1_000_000` and **no time limit** — messages are kept until the stream reaches a million, then the oldest are evicted. Reading messages never removes them; only these limits do.

> ⚠️ `add_stream` only applies limits when the stream is first created. If the stream already exists with older settings, update it in place rather than expecting the bridge to change it:
> ```bash
> nats stream edit serial-bridge --max-msgs=1000000 --max-age=0
> ```

**Per-message timestamp:** each published message carries a `Ts` NATS header holding the bridge's capture time (epoch seconds, `time.time()`). The payload body stays the raw line — consumers that only care about the data can ignore the header, while `serial_listener.py` uses it to show when a line was read.

---

### `serial_listener.py` — Bridge listener

Subscribes to the bridge's TX subject (`device.tx`) and dumps every message to the console. Pairs with `serial_jetstream_bridge.py` to watch what a device is sending.

```bash
uv run serial_listener.py                       # follow live (raw payload only)
uv run serial_listener.py --all                 # replay everything in the stream first
uv run serial_listener.py --fullformat          # prefix each line with a timestamp
uv run serial_listener.py --log-file out.log    # also tee to a file (overwrite)
uv run serial_listener.py --log-file out.log --append   # tee, appending
uv run serial_listener.py --subject device.tx --nats nats://myserver:4222
```

By default the listener prints **only the raw payload** and starts from now (it drops the existing backlog). Pass `--all` to replay everything still retained in the stream first, then follow live.

With `--fullformat`, each line is prefixed with the capture timestamp taken from the message's `Ts` header (falling back to JetStream's store time for older messages without the header):

```
[11:47:02.136] line-one
```

The listener uses an ephemeral push consumer, so each run is independent and leaves no durable state behind.

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--nats` | `nats://localhost:4222` | NATS server URL |
| `--subject` | `device.tx` | Subject to listen on |
| `--stream-name` | `serial-bridge` | JetStream stream name |
| `--all` | (off) | Replay all retained messages first (default: only new messages from now on) |
| `--fullformat` | (off) | Prefix each line with the capture timestamp (default: raw payload only) |
| `--log-file` | (none) | Also write every message to this file (tees to stdout + file) |
| `--append` | (off) | Append to `--log-file` instead of overwriting it on start |

---

## Running the bridge on Windows from WSL

Serial COM ports are owned by Windows — WSL2 can't see `COM*` devices directly. The simplest reliable workflow is to keep the repo on the Windows filesystem and drive the Windows `uv` from a WSL shell:

```powershell
# In Windows, from the repo checkout, drop into WSL:
PS C:\Users\you\development\jetstreams> wsl
```

```bash
# WSL now sits in the same dir via /mnt/c; run the Windows uv through powershell.exe:
powershell.exe -Command "uv run .\serial_jetstream_bridge.py COM234"
```

```
2026-06-24 10:38:18 - __main__ - INFO - Connected to serial port COM234 @ 115200 baud
2026-06-24 10:38:18 - __main__ - INFO - Connected to NATS at nats://localhost:4222
2026-06-24 10:38:18 - __main__ - INFO - Bridge started. Press Ctrl+C to stop.
```

**Why this layout:** a `.venv` is not portable across operating systems (Linux uses `.venv/bin/`, Windows uses `.venv\Scripts\`). Running both Linux `uv` (inside WSL) and Windows `uv` against one shared folder makes them fight over the same `.venv` — over a `\\wsl.localhost\...` share this fails with `os error 145` (directory not empty). Keeping the checkout on the Windows drive and using `powershell.exe -Command "uv run ..."` avoids the cross-OS venv conflict entirely.

> If NATS runs inside WSL while the bridge runs on Windows, `nats://localhost:4222` usually works (WSL2 mirrors localhost). If it can't connect, pass the WSL IP from `hostname -I`, e.g. `--nats nats://172.x.x.x:4222`.

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
