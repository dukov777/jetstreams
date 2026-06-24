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

Bidirectional bridge: reads raw byte chunks from a serial port and publishes them to NATS JetStream, while simultaneously writing inbound NATS messages back to the serial port. It does **no** line framing — that is the listener's job (see below).

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
| `-v`, `--verbose` | (off) | Enable DEBUG logging — shows every byte read from / written to serial |
| `--log-file` | (none) | Also write log output to this file (tees to console + file) |

**Chunk forwarding (not line-framed):** serial data does not always arrive on line boundaries, so the reader reads **up to 100 bytes with a 0.1s poll** (`read(100)`) and publishes whatever is available — 1 byte, 100 bytes, whatever the port has — as one NATS message. `read()` returns as soon as *any* bytes are present and only blocks when the buffer is empty, so the 0.1s timeout is a heartbeat (to stay responsive to shutdown), not a data deadline; no bytes are ever dropped. A single device line can therefore land in the stream split across several messages — reassembling it back into lines is `serial_listener.py`'s job.

**Byte-for-byte in both directions:** the reader publishes each chunk **verbatim** (no framing, no stripping) — the stream is an exact copy of the device output. The writer is the mirror: it writes RX payloads to the port **verbatim and appends nothing**, so the publisher owns the exact bytes (terminator included). See `serial_sender.py` below.

> Run with `-v` to confirm traffic: you'll see `Published chunk (N bytes)…` and `Wrote N bytes to serial` for each message. The `repr` in the write log tells you whether a terminator is real: `b'#restart\r\n'` (single backslash) is a real CR LF, while `b'#restart\\r\\n'` (double backslash) is the literal text `\r\n`.

**Stream retention:** the bridge creates the `serial-bridge` stream with `max_msgs=1_000_000` and **no time limit** — messages are kept until the stream reaches a million, then the oldest are evicted. Reading messages never removes them; only these limits do.

> ⚠️ `add_stream` only applies limits when the stream is first created. If the stream already exists with older settings, update it in place rather than expecting the bridge to change it:
> ```bash
> nats stream edit serial-bridge --max-msgs=1000000 --max-age=0
> ```

**Per-message timestamp:** each published chunk carries a `Ts` NATS header holding the bridge's capture time (epoch seconds, `time.time()`). The payload body stays the raw bytes — consumers that only care about the data can ignore the header, while `serial_listener.py` uses it to timestamp each reassembled line (the chunk that carried the line's terminator wins).

---

### `serial_listener.py` — Bridge listener

Subscribes to the bridge's TX subject (`device.tx`) and reassembles the raw chunks back into lines. Pairs with `serial_jetstream_bridge.py` to watch what a device is sending.

```bash
uv run serial_listener.py                       # follow live (raw payload only)
uv run serial_listener.py --all                 # replay everything in the stream first
uv run serial_listener.py --fullformat          # one timestamped line per record
uv run serial_listener.py --log-file out.log    # also tee to a file (overwrite)
uv run serial_listener.py --log-file out.log --append   # tee, appending
uv run serial_listener.py --subject device.tx --nats nats://myserver:4222
```

By default the listener prints **only the raw payload** and starts from now (it drops the existing backlog). Pass `--all` to replay everything still retained in the stream first, then follow live.

**Line reassembly:** because the bridge forwards raw chunks (a line may be split across several messages, or several lines may share one message), the listener buffers incoming bytes and emits a line when it sees a terminator. The rules:

- A line is complete on `\n`, `\r`, or `\r\n` (the terminator stays attached). A bare `\r` counts — some devices end lines with just a carriage return.
- Each completed line is tagged with the capture timestamp of the **chunk that carried the terminating newline** (taken from that message's `Ts` header, falling back to JetStream's store time).
- A partial line that sits with **no terminator for 2 seconds** is flushed anyway (with the latest chunk's timestamp) and the buffer cleared, so unterminated output is still shown instead of waiting forever. The bytes are emitted once — not duplicated when the rest of the line later arrives.

With `--fullformat`, each record is printed as one clean line — the device terminator is stripped and replaced with a real `\n`, so a bare-`\r` device (whose lines would otherwise overwrite each other on the terminal) still shows one timestamped line per record:

```
[12:04:13.363] 860693080371397
[12:04:16.988] 860693080371397
```

In the default (raw) mode the payload is emitted **verbatim**, terminator included, for a byte-for-byte capture.

The listener uses an ephemeral push consumer, so each run is independent and leaves no durable state behind.

When `--log-file ./logs/out.log` is given, the listener writes to **two** files: the path as passed, plus a per-run timestamped sibling in the same directory (`./logs/20260624_105059-out.log`). Payloads are written through without an added newline and with newline translation disabled, so the file is a byte-for-byte copy of the device output.

**Options:**

| Flag | Default | Description |
|---|---|---|
| `--nats` | `nats://localhost:4222` | NATS server URL |
| `--subject` | `device.tx` | Subject to listen on |
| `--stream-name` | `serial-bridge` | JetStream stream name |
| `--all` | (off) | Replay all retained messages first (default: only new messages from now on) |
| `--fullformat` | (off) | Prefix each line with the capture timestamp (default: raw payload only) |
| `--log-file` | (none) | Tee every message to this file **and** a timestamped per-run copy beside it |
| `--append` | (off) | Append to the named `--log-file` instead of overwriting it on start (the timestamped copy is always fresh) |

---

### `serial_sender.py` — Bridge sender

The opposite direction of `serial_listener.py`: publishes a single message to the bridge's RX subject (`device.rx`), which the bridge then writes out to the serial port. This is how you send a command *to* the device.

```bash
uv run serial_sender.py "#restart"                 # send literal text
uv run serial_sender.py -e "#restart\r\n"          # interpret \r \n \t as real control bytes
uv run serial_sender.py "hello" --subject device.rx
uv run serial_sender.py "reboot" --nats nats://myserver:4222
```

The bridge writes RX payloads **verbatim and appends no terminator**, so you own the exact bytes. If the device needs a `\r\n` line ending, include it yourself with `-e`:

- without `-e` → `"#restart\r\n"` sends the **literal 12-byte** text `#restart\r\n` (backslashes and letters)
- with `-e` → `-e "#restart\r\n"` sends **10 bytes** ending in a real CR LF
- your shell can also embed a real newline directly with `$'#restart\r\n'` (bash/zsh)

It publishes once and exits. Because the bridge's RX consumer is durable, a message is retained and delivered even if the bridge isn't running yet — it gets written out once the bridge connects.

**Options:**

| Flag | Default | Description |
|---|---|---|
| `message` | (required) | The string to send (published to the RX subject) |
| `--nats` | `nats://localhost:4222` | NATS server URL |
| `--subject` | `device.rx` | Subject to publish on |
| `-e`, `--interpret-escapes` | (off) | Interpret backslash escapes (`\n`, `\r`, `\t`, …) as real characters |

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
