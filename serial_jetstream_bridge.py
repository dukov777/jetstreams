#!/usr/bin/env python3
"""
Serial-to-NATS JetStreams Bridge

Exposes a serial port (UART) to NATS JetStreams as an internal data tunnel.
Bidirectional: reads from serial → publishes to 'device.tx' stream
              subscribes to 'device.rx' stream → writes to serial

Usage:
    python serial_jetstream_bridge.py /dev/ttyUSB0 115200
    python serial_jetstream_bridge.py COM3 9600 --nats nats://localhost:4222
"""

import asyncio
import argparse
import sys
import time
import logging
from typing import Optional

import serial
import serial_asyncio
import nats
from nats.errors import TimeoutError as NatsTimeoutError


LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def configure_logging(verbose: bool, log_file: Optional[str]) -> None:
    """Set the log level and optionally tee logs to a file."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)

    if log_file:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root.addHandler(handler)
        logger.info(f"Also logging to '{log_file}'")


class SerialJetStreamBridge:
    """Bridges a serial port to NATS JetStreams for bidirectional tunneling."""
    
    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        nats_url: str = "nats://localhost:4222",
        tx_subject: str = "device.tx",  # serial → NATS
        rx_subject: str = "device.rx",  # NATS → serial
        stream_name: str = "serial-bridge",
    ):
        self.port = port
        self.baudrate = baudrate
        self.nats_url = nats_url
        self.tx_subject = tx_subject
        self.rx_subject = rx_subject
        self.stream_name = stream_name
        
        self.serial_reader: Optional[asyncio.StreamReader] = None
        self.serial_writer: Optional[asyncio.StreamWriter] = None
        self.nc: Optional[nats.NATS] = None
        self.js = None
        self._running = False

        # Set True only while the serial port is open and healthy. The reader
        # task owns reconnection; the writer checks this flag before touching
        # the port so it never writes into a half-dead transport.
        self._serial_connected = False
        # Backoff ceiling for serial (re)connect attempts, in seconds.
        self._reconnect_max_delay = 10.0
        
    async def connect_serial(self) -> None:
        """
        Open the serial port for async I/O, retrying until it succeeds.

        On Windows a USB serial adapter that is unplugged/reset throws
        SerialException (e.g. 'ClearCommError failed'); the same retry loop
        that handles the initial open also covers the case where the device
        is not plugged in yet at startup.
        """
        delay = 1.0
        while self._running:
            try:
                self.serial_reader, self.serial_writer = await serial_asyncio.open_serial_connection(
                    url=self.port,
                    baudrate=self.baudrate,
                    timeout=1.0
                )
                self._serial_connected = True
                logger.info(f"Connected to serial port {self.port} @ {self.baudrate} baud")
                return
            except (serial.SerialException, OSError) as e:
                logger.warning(
                    f"Failed to open serial port {self.port}: {e}; "
                    f"retrying in {delay:.0f}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._reconnect_max_delay)

    async def _close_serial(self) -> None:
        """
        Close the current serial transport, swallowing errors raised by an
        already-dead transport (the offending in_waiting/ClearCommError poll
        fires again during close on Windows).
        """
        self._serial_connected = False
        if self.serial_writer is not None:
            try:
                self.serial_writer.close()
                await self.serial_writer.wait_closed()
            except (serial.SerialException, OSError) as e:
                logger.debug(f"Ignoring error while closing dead serial port: {e}")
            finally:
                self.serial_writer = None
                self.serial_reader = None

    async def _reconnect_serial(self) -> None:
        """Tear down the broken serial connection and re-open it."""
        logger.warning(f"Serial port {self.port} lost; attempting to reconnect")
        await self._close_serial()
        await self.connect_serial()
        logger.info(f"Reconnected to serial port {self.port}")
    
    async def connect_nats(self) -> None:
        """Connect to NATS and set up JetStream streams."""
        try:
            self.nc = await nats.connect(self.nats_url)
            self.js = self.nc.jetstream()
            logger.info(f"Connected to NATS at {self.nats_url}")
            
            # Create stream if it doesn't exist, configured for both TX and RX subjects
            try:
                await self.js.add_stream(
                    name=self.stream_name,
                    subjects=[self.tx_subject, self.rx_subject],
                    max_msgs=1_000_000,  # cap by count; oldest evicted past this
                    # no max_age -> 0 means messages never expire by time
                )
                logger.info(f"Created JetStream '{self.stream_name}' with subjects: {self.tx_subject}, {self.rx_subject}")
            except nats.js.errors.BadRequestError as e:
                # Stream already exists
                if "stream already exists" in str(e) or "in use" in str(e):
                    logger.info(f"JetStream '{self.stream_name}' already exists")
                else:
                    raise
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise
    
    async def serial_reader_task(self) -> None:
        """
        Continuously read one line at a time from serial port and publish
        each line as a separate NATS message on the TX subject.

        Line-buffered, not chunk-buffered: a line may arrive across multiple
        underlying reads, but is only published once a full '\\n' is seen.
        The trailing terminator is preserved (not stripped) so the published
        bytes are an exact copy of what the device sent.
        """
        logger.info(f"Starting serial reader (publishing to '{self.tx_subject}')")
        try:
            while self._running:
                try:
                    # readline() blocks (internally) until '\n' or EOF;
                    # wrap in wait_for so we can still respond to shutdown.
                    line = await asyncio.wait_for(
                        self.serial_reader.readline(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    # No complete line yet, just continue
                    continue
                except (serial.SerialException, OSError) as e:
                    # Device unplugged/reset (e.g. Windows ClearCommError).
                    # Reconnect instead of crashing the whole bridge.
                    logger.error(f"Serial read error: {e}")
                    await self._reconnect_serial()
                    continue

                if not line:
                    # EOF on serial (port closed/unplugged) — treat as a
                    # dropped connection and try to bring it back. readline()
                    # only returns empty on EOF; a blank line is b"\n" (truthy).
                    logger.warning("Serial reader got EOF; reconnecting")
                    await self._reconnect_serial()
                    continue

                # Publish the line exactly as read, INCLUDING its trailing
                # terminator (\n, \r\n, or \r). Nothing is stripped and blank
                # lines are kept, so the stream — and any listener log — is a
                # byte-for-byte record of what the device emitted.
                ack = await self.js.publish(
                    self.tx_subject,
                    line,
                    headers={"Ts": str(time.time())},  # capture time, body stays raw
                )
                logger.debug(f"Published line ({len(line)} bytes) to {self.tx_subject} (seq: {ack.seq})")
        except asyncio.CancelledError:
            logger.info("Serial reader task cancelled")
        except Exception as e:
            logger.error(f"Serial reader error: {e}", exc_info=True)
    
    async def serial_writer_task(self) -> None:
        """
        Subscribe to NATS RX subject and write each message to the serial
        port verbatim. The bridge does NOT append a line terminator — the
        publisher owns the exact bytes (including any '\\r'/'\\n'), so what
        reaches the device is byte-for-byte what was published.
        """
        logger.info(f"Starting serial writer (subscribing to '{self.rx_subject}')")
        try:
            async def process_message(msg):
                # Don't write while the port is down/reconnecting. Leaving the
                # message unacked makes JetStream redeliver it once serial is
                # healthy again, so nothing destined for the device is lost.
                if not self._serial_connected or self.serial_writer is None:
                    logger.warning("Serial port down; deferring write (will redeliver)")
                    await msg.nak()
                    return
                try:
                    # Write the payload verbatim; the publisher owns the
                    # terminator, so the device gets exactly what was sent.
                    self.serial_writer.write(msg.data)
                    await self.serial_writer.drain()
                    logger.debug(f"Wrote {len(msg.data)} bytes to serial")
                    await msg.ack()
                except (serial.SerialException, OSError) as e:
                    # Connection just died; let the reader task reconnect and
                    # let JetStream redeliver this message.
                    logger.error(f"Error writing to serial: {e}")
                    await msg.nak()

            sub = await self.js.subscribe(
                self.rx_subject,
                cb=process_message,
                durable=f"{self.stream_name}-writer",  # Durable consumer survives restarts
            )
            
            # Keep this task alive
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Serial writer task cancelled")
        except Exception as e:
            logger.error(f"Serial writer error: {e}", exc_info=True)
    
    async def run(self) -> None:
        """Start the bridge: connect all components and run reader/writer tasks."""
        try:
            self._running = True

            # Connect both serial and NATS
            await self.connect_serial()
            await self.connect_nats()

            logger.info("Bridge started. Press Ctrl+C to stop.")
            
            # Run reader and writer concurrently
            await asyncio.gather(
                self.serial_reader_task(),
                self.serial_writer_task(),
                return_exceptions=True
            )
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Bridge error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self) -> None:
        """Clean shutdown: close serial and NATS connections."""
        self._running = False
        logger.info("Shutting down...")

        # _close_serial swallows the ClearCommError/in_waiting poll that a
        # dead Windows transport raises during close.
        await self._close_serial()

        if self.nc and self.nc.is_connected:
            await self.nc.close()
        
        logger.info("Bridge stopped")


async def main():
    parser = argparse.ArgumentParser(
        description="Expose a serial port to NATS JetStreams"
    )
    parser.add_argument("port", help="Serial port (e.g., /dev/ttyUSB0 or COM3)")
    parser.add_argument(
        "--baudrate",
        type=int,
        default=115200,
        help="Serial baud rate (default: 115200)"
    )
    parser.add_argument(
        "--nats",
        default="nats://localhost:4222",
        help="NATS server URL (default: nats://localhost:4222)"
    )
    parser.add_argument(
        "--tx-subject",
        default="device.tx",
        help="Subject for serial→NATS (default: device.tx)"
    )
    parser.add_argument(
        "--rx-subject",
        default="device.rx",
        help="Subject for NATS→serial (default: device.rx)"
    )
    parser.add_argument(
        "--stream-name",
        default="serial-bridge",
        help="JetStream name (default: serial-bridge)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Enable DEBUG logging (shows every byte read from / written to serial)"
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Also write log output to this file (default: console only)"
    )

    args = parser.parse_args()

    configure_logging(verbose=args.verbose, log_file=args.log_file)

    bridge = SerialJetStreamBridge(
        port=args.port,
        baudrate=args.baudrate,
        nats_url=args.nats,
        tx_subject=args.tx_subject,
        rx_subject=args.rx_subject,
        stream_name=args.stream_name,
    )
    
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
