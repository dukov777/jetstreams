#!/usr/bin/env python3
"""
Serial Bridge Listener

Subscribes to the TX subject published by serial_jetstream_bridge.py
(serial → NATS) and dumps every message to the console.

Usage:
    python serial_listener.py
    python serial_listener.py --subject device.tx --nats nats://localhost:4222
    python serial_listener.py --new          # only messages from now on
    python serial_listener.py --raw           # no timestamp/seq prefix
"""

import asyncio
import argparse
import logging
from datetime import datetime, timezone

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
    from_new: bool,
    raw: bool,
) -> None:
    """Connect to NATS and print every message on `subject` until interrupted."""
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    logger.info(f"Connected to NATS at {nats_url}")

    deliver_policy = DeliverPolicy.NEW if from_new else DeliverPolicy.ALL
    logger.info(
        f"Listening on '{subject}' (stream '{stream_name}', "
        f"deliver_policy={deliver_policy.value})"
    )

    async def on_message(msg):
        text = msg.data.decode(errors="replace")
        if raw:
            print(text, flush=True)
        else:
            seq = msg.metadata.sequence.stream
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            print(f"[{ts}] seq={seq} <{msg.subject}> {text}", flush=True)
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
        "--new",
        action="store_true",
        help="Only show messages published from now on (default: replay all)"
    )
    parser.add_argument(
        "--fullformat",
        action="store_true",
        default=False,
        help="Print full format with timestamp and sequence (default: False)"
    )

    args = parser.parse_args()

    try:
        await listen(
            nats_url=args.nats,
            subject=args.subject,
            stream_name=args.stream_name,
            from_new=args.new,
            raw=not args.fullformat,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
