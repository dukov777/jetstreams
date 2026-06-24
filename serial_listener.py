#!/usr/bin/env python3
"""
Serial Bridge Listener

Subscribes to the TX subject published by serial_jetstream_bridge.py
(serial → NATS). The bridge forwards raw serial chunks that may not align to
line boundaries, so this listener reassembles them into lines: a line is
complete on '\\n' ('\\r\\n' kept attached) and is tagged with the capture
timestamp of the chunk that carried the newline. A partial line with no
terminator is flushed after 2s so unterminated output is still shown.

Usage:
    python serial_listener.py
    python serial_listener.py --subject device.tx --nats nats://localhost:4222
    python serial_listener.py --all           # replay everything already in the stream
    python serial_listener.py --fullformat    # prefix each line with its timestamp
    python serial_listener.py --log-file out.log          # tee to file (overwrite)
    python serial_listener.py --log-file out.log --append # tee to file (append)
"""

import asyncio
import argparse
import logging
import os
from datetime import datetime
from typing import Optional, TextIO

import nats
from nats.js.api import DeliverPolicy


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _format_capture_time(msg) -> str:
    """
    Prefer the bridge's capture time from the 'Ts' header (epoch seconds, set
    when the serial line was read). Fall back to JetStream's store time if the
    header is missing (e.g. messages published before headers were added).
    """
    header_ts = msg.headers.get("Ts") if msg.headers else None
    if header_ts is not None:
        try:
            # fromtimestamp() with no tz returns local time.
            dt = datetime.fromtimestamp(float(header_ts))
            return dt.strftime("%H:%M:%S.%f")[:-3]
        except (ValueError, OverflowError):
            pass
    # Fallback: server-side store time stamped by JetStream (UTC) -> local.
    return msg.metadata.timestamp.astimezone().strftime("%H:%M:%S.%f")[:-3]


async def listen(
    nats_url: str,
    subject: str,
    stream_name: str,
    replay_all: bool,
    raw: bool,
    log_file: Optional[str] = None,
    append: bool = False,
) -> None:
    """Connect to NATS and print every message on `subject` until interrupted."""
    log_fps: list[TextIO] = []
    if log_file:
        # 1) The file exactly as passed (respects --append).
        # newline="" disables newline translation so the original terminator
        # (\n, \r\n, or \r) is written through verbatim — the log stays a
        # byte-for-byte copy of what the device emitted.
        mode = "a" if append else "w"
        log_fps.append(open(log_file, mode, encoding="utf-8", newline=""))
        logger.info(
            f"Logging to '{log_file}' (mode={'append' if append else 'overwrite'})"
        )

        # 2) A per-run, timestamped sibling in the same directory:
        #    ./logs/logfile.log -> ./logs/20260624_105059-logfile.log
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        directory, name = os.path.split(log_file)
        stamped_path = os.path.join(directory, f"{stamp}-{name}")
        log_fps.append(open(stamped_path, "w", encoding="utf-8", newline=""))
        logger.info(f"Logging this run to '{stamped_path}'")

    def emit(line: str) -> None:
        # `line` already carries its own terminator (or none, for a flush),
        # so don't add one — keeps the log a byte-for-byte copy.
        print(line, end="", flush=True)
        for fp in log_fps:
            fp.write(line)
            fp.flush()

    def emit_line(line_bytes: bytes, ts: str) -> None:
        text = line_bytes.decode(errors="replace")
        if raw:
            emit(text)
        else:
            emit(f"[{ts}] {text}")

    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    loop = asyncio.get_event_loop()
    logger.info(f"Connected to NATS at {nats_url}")

    deliver_policy = DeliverPolicy.ALL if replay_all else DeliverPolicy.NEW
    logger.info(
        f"Listening on '{subject}' (stream '{stream_name}', "
        f"deliver_policy={deliver_policy.value})"
    )

    # The bridge now publishes raw serial chunks that do not respect line
    # boundaries, so this listener reassembles lines. A line is complete when
    # a '\n' is seen ('\r\n' stays attached); it is tagged with the capture
    # timestamp of the chunk that carried that newline.
    FLUSH_TIMEOUT = 2.0  # seconds; dump a partial line if no newline arrives
    state = {"buf": b"", "ts": None, "last": None}

    async def on_message(msg):
        ts = _format_capture_time(msg)
        state["buf"] += msg.data
        state["ts"] = ts            # newline-bearing / latest chunk wins
        state["last"] = loop.time()

        # Drain every complete line currently in the buffer; each uses this
        # message's timestamp (the buffer held no '\n' before this chunk).
        buf = state["buf"]
        start = 0
        nl = buf.find(b"\n")
        while nl != -1:
            emit_line(buf[start:nl + 1], ts)
            start = nl + 1
            nl = buf.find(b"\n", start)

        state["buf"] = buf[start:]  # remainder is a partial line (no '\n')
        if not state["buf"]:
            state["ts"] = None
            state["last"] = None
        await msg.ack()

    # Ephemeral push consumer: server delivers messages straight to the callback.
    sub = await js.subscribe(
        subject,
        stream=stream_name,
        cb=on_message,
        deliver_policy=deliver_policy,
    )

    try:
        # Idle loop also flushes a partial line that has sat without a
        # terminator for longer than FLUSH_TIMEOUT, so unterminated output is
        # still shown (with its timestamp) instead of waiting forever.
        while True:
            await asyncio.sleep(0.5)
            if state["buf"] and state["last"] is not None:
                if loop.time() - state["last"] > FLUSH_TIMEOUT:
                    emit_line(state["buf"], state["ts"])
                    state["buf"] = b""
                    state["ts"] = None
                    state["last"] = None
    except asyncio.CancelledError:
        pass
    finally:
        await sub.unsubscribe()
        await nc.close()
        for fp in log_fps:
            fp.close()
        logger.info("Listener stopped")


async def main():
    parser = argparse.ArgumentParser(
        description="Dump messages from the serial bridge's TX subject to the console"
    )
    parser.add_argument(
        "--nats",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)"
    )
    parser.add_argument(
        "--subject",
        default="device.tx",
        help="Subject to listen on (default: device.tx)"
    )
    parser.add_argument(
        "--stream-name",
        default="serial-bridge",
        help="JetStream name (default: serial-bridge)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Replay all messages already in the stream "
             "(default: only messages published from now on)"
    )
    parser.add_argument(
        "--fullformat",
        action="store_true",
        default=False,
        help="Print full format with timestamp and sequence (default: False)"
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Also write messages to this file (default: stdout only)"
    )
    parser.add_argument(
        "--append",
        action="store_true",
        default=False,
        help="Append to --log-file instead of overwriting it (default: overwrite)"
    )

    args = parser.parse_args()

    try:
        await listen(
            nats_url=args.nats,
            subject=args.subject,
            stream_name=args.stream_name,
            replay_all=args.all,
            raw=not args.fullformat,
            log_file=args.log_file,
            append=args.append,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
