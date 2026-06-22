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
import logging
from typing import Optional

import serial_asyncio
import nats
from nats.errors import TimeoutError as NatsTimeoutError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        
    async def connect_serial(self) -> None:
        """Open serial port for async I/O."""
        try:
            self.serial_reader, self.serial_writer = await serial_asyncio.open_serial_connection(
                url=self.port,
                baudrate=self.baudrate,
                timeout=1.0
            )
            logger.info(f"Connected to serial port {self.port} @ {self.baudrate} baud")
        except Exception as e:
            logger.error(f"Failed to open serial port {self.port}: {e}")
            raise
    
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
                    max_age=3600 * 1_000_000_000,  # 1 hour retention
                    max_msgs=100_000,
                )
                logger.info(f"Created JetStream '{self.stream_name}' with subjects: {self.tx_subject}, {self.rx_subject}")
            except nats.js.errors.BadRequest as e:
                # Stream already exists
                if "stream already exists" in str(e):
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
        This matches NDJSON / line-oriented UART protocols where each
        message is exactly one line.
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

                if not line:
                    # EOF on serial (port closed/unplugged)
                    logger.warning("Serial reader got EOF, stopping")
                    break

                # readline() includes the trailing '\n' (if present);
                # strip it before publishing, the wire framing is NATS messages now.
                line = line.rstrip(b"\n").rstrip(b"\r")
                if not line:
                    continue  # skip blank lines

                ack = await self.js.publish(self.tx_subject, line)
                logger.debug(f"Published line ({len(line)} bytes) to {self.tx_subject} (seq: {ack.seq})")
        except asyncio.CancelledError:
            logger.info("Serial reader task cancelled")
        except Exception as e:
            logger.error(f"Serial reader error: {e}", exc_info=True)
    
    async def serial_writer_task(self) -> None:
        """
        Subscribe to NATS RX subject and write each message to the serial
        port as one line, terminated with '\\n' (matching what the reader
        strips on the TX side).
        """
        logger.info(f"Starting serial writer (subscribing to '{self.rx_subject}')")
        try:
            async def process_message(msg):
                try:
                    self.serial_writer.write(msg.data + b"\n")
                    await self.serial_writer.drain()
                    logger.debug(f"Wrote line ({len(msg.data)} bytes) to serial")
                    await msg.ack()
                except Exception as e:
                    logger.error(f"Error writing to serial: {e}")

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
            # Connect both serial and NATS
            await self.connect_serial()
            await self.connect_nats()
            
            self._running = True
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
        
        if self.serial_writer:
            self.serial_writer.close()
            await self.serial_writer.wait_closed()
        
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
    
    args = parser.parse_args()
    
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
