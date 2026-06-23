#!/usr/bin/env python3
"""
Serial Bridge Listener

Subscribes to the TX subject published by serial_jetstream_bridge.py
(serial → NATS) and dumps every message to the console.

Usage:
    python serial_listener.py
    python serial_listener.py --subject device.tx --nats nats://localhost:4222
    python serial_listener.py --all          # replay everything already in the stream
    python serial_listener.py --raw           # no timestamp/seq prefix
    python serial_listener.py --log-file out.log          # tee to file (overwrite)
    python serial_listener.py --log-file out.log --append # tee to file (append)
"""

import asyncio
import argparse
import logging
from datetime import datetime, timezone
from typing import Optional, TextIO

import nats
from nats.js.api import DeliverPolicy


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
    log_fp: Optional[TextIO] = None
    if log_file:
        mode = "a" if append else "w"
        log_fp = open(log_file, mode, encoding="utf-8")
        logger.info(
            f"Logging to '{log_file}' (mode={'append' if append else 'overwrite'})"
        )

    def emit(line: str) -> None:
        print(line, flush=True)
        if log_fp is not None:
            log_fp.write(line + "\n")
            log_fp.flush()

    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    logger.info(f"Connected to NATS at {nats_url}")

    deliver_policy = DeliverPolicy.ALL if replay_all else DeliverPolicy.NEW
    logger.info(
        f"Listening on '{subject}' (stream '{stream_name}', "
        f"deliver_policy={deliver_policy.value})"
    )

    async def on_message(msg):
        text = msg.data.decode(errors="replace")
        if raw:
            emit(text)
        else:
            seq = msg.metadata.sequence.stream
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            emit(f"[{ts}] seq={seq} <{msg.subject}> {text}")
        await msg.ack()

    # Ephemeral push consumer: server delivers messages straight to the callback.
    sub = await js.subscribe(
        subject,
        stream=stream_name,
        cb=on_message,
        deliver_policy=deliver_policy,
    )

    try:
        # Idle here while the callback handles messages.
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await sub.unsubscribe()
        await nc.close()
        if log_fp is not None:
            log_fp.close()
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
