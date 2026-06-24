#!/usr/bin/env python3
"""
Serial Bridge Sender

The opposite direction of serial_listener.py: publishes a message to the
bridge's RX subject (device.rx). serial_jetstream_bridge.py subscribes to that
subject and writes each message out to the serial port verbatim, so this is
how you send data *to* the device. The bridge no longer appends a terminator
— include any '\\r'/'\\n' you need in the message yourself (e.g. with -e).

Usage:
    python serial_sender.py "AT+CSQ"
    python serial_sender.py "hello device" --subject device.rx
    python serial_sender.py "reboot" --nats nats://myserver:4222
    python serial_sender.py -e "line1\\nline2"   # interpret \\n, \\r, \\t as real chars

Note: your shell can also embed real newlines directly with $'a\\nb' (bash/zsh).
"""

import asyncio
import argparse
import logging

import nats


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def send(nats_url: str, subject: str, message: str) -> None:
    """Connect to NATS and publish a single message on `subject`."""
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    logger.info(f"Connected to NATS at {nats_url}")

    try:
        # The bridge appends the line terminator when writing to serial,
        # so publish the raw string bytes without one here.
        ack = await js.publish(subject, message.encode())
        logger.info(
            f"Published {len(message)} chars to '{subject}' (seq: {ack.seq})"
        )
    finally:
        await nc.close()


async def main():
    parser = argparse.ArgumentParser(
        description="Publish a message to the serial bridge's RX subject"
    )
    parser.add_argument(
        "message",
        help="The string to send to the device (published to the RX subject)"
    )
    parser.add_argument(
        "--nats",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)"
    )
    parser.add_argument(
        "--subject",
        default="device.rx",
        help="Subject to publish on (default: device.rx)"
    )
    parser.add_argument(
        "-e", "--interpret-escapes",
        action="store_true",
        default=False,
        help=r"Interpret backslash escapes in the message (\n, \r, \t, ...) "
             r"as real characters (default: send the string literally)"
    )

    args = parser.parse_args()

    message = args.message
    if args.interpret_escapes:
        # Turn literal "\n", "\t", etc. into the real control characters.
        message = message.encode("utf-8").decode("unicode_escape")

    try:
        await send(
            nats_url=args.nats,
            subject=args.subject,
            message=message,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
