"""
JetStream fanout examples.

Setup:
  pip install nats-py
  nats server -js  (or: docker run -p 4222:4222 nats:latest -js)
"""

import asyncio
import json
from datetime import datetime, timezone
import nats
from nats.errors import Error as NatsError
from nats.js.api import ConsumerConfig, DeliverPolicy


# ============================================================================
# Example 1: Create a stream and publish messages
# ============================================================================
async def example_1_create_stream_and_publish():
    """
    Create a JetStream stream and publish some test messages.
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    # Create a stream named "logs" with retention policy
    try:
        await js.add_stream(
            name="logs",
            subjects=["logs.>"],  # Match logs.* and logs.a.b, etc.
            max_age=3600 * 24,    # Keep for 1 day
        )
    except nats.errors.Error as e:
        # Stream already exists
        print(f"Stream exists: {e}")
    
    # Publish some messages
    for i in range(5):
        msg = {
            "id": i,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "info",
            "message": f"Log entry {i}",
        }
        ack = await js.publish("logs.app", json.dumps(msg).encode())
        print(f"Published: seq={ack.seq}")
    
    await nc.close()


# ============================================================================
# Example 2: Consumer-based pull (one-at-a-time)
# ============================================================================
async def example_2_pull_consumer():
    """
    Create a consumer and pull messages one at a time.
    Good for batch processing, rate-limited consumption.
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create (or bind to) a durable pull consumer on the stream
    consumer = "pull_consumer_1"
    sub = await js.pull_subscribe("logs.app", durable=consumer)

    # Pull up to 10 messages with a 1-second timeout
    try:
        messages = await sub.fetch(batch=10, timeout=1)
    except nats.errors.TimeoutError:
        messages = []

    for msg in messages:
        print(f"Pulled: {msg.metadata.sequence.stream} - {msg.data.decode()}")
        await msg.ack()  # Acknowledge, mark as processed

    await nc.close()


# ============================================================================
# Example 3: Multiple independent consumers (fanout)
# ============================================================================
async def consumer_worker(name: str, consumer_name: str):
    """
    A consumer that runs independently, reading from its own cursor.
    Multiple instances of this can run in parallel.
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create consumer (durable name ensures cursor is persisted)
    sub = await js.pull_subscribe("logs.app", durable=consumer_name)

    # Fetch and process indefinitely (like a daemon)
    while True:
        try:
            messages = await sub.fetch(batch=5, timeout=2)
            for msg in messages:
                data = json.loads(msg.data.decode())
                print(f"[{name}] Processing: {data['id']} - {data['message']}")
                await msg.ack()
        except nats.errors.TimeoutError:
            print(f"[{name}] Timeout (no messages), continuing...")
            continue


async def example_3_fanout():
    """
    Start 3 independent consumers, all reading from the same stream.
    Each maintains its own position.
    """
    # First, publish some fresh messages
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    for i in range(10):
        msg = {"id": i, "message": f"Fanout test {i}"}
        await js.publish("logs.app", json.dumps(msg).encode())
    
    await nc.close()
    
    # Now start 3 consumers in parallel
    tasks = [
        consumer_worker("Consumer-A", "fanout_a"),
        consumer_worker("Consumer-B", "fanout_b"),
        consumer_worker("Consumer-C", "fanout_c"),
    ]
    
    # Run for a few seconds to see fanout
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
    except asyncio.TimeoutError:
        print("\nFanout demo completed (timeout)")


# ============================================================================
# Example 4: Push consumer (server pushes to subscriber)
# ============================================================================
async def example_4_push_consumer():
    """
    Create a push consumer: server actively delivers messages.
    Good for real-time subscribers.
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    # Subscribe with push delivery
    async def message_handler(msg):
        data = json.loads(msg.data.decode())
        print(f"Pushed: {data['message']}")
        await msg.ack()
    
    # Create push consumer
    sub = await js.subscribe(
        "logs.app",
        durable_name="push_consumer",
        cb=message_handler,
    )
    
    # Keep listening for 5 seconds
    await asyncio.sleep(5)
    await sub.unsubscribe()
    await nc.close()


# ============================================================================
# Example 5: Replay from a specific point
# ============================================================================
async def example_5_replay():
    """
    Create a consumer that starts reading from a specific sequence.
    Useful for "give me all logs since message #50".
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    # Create consumer starting from sequence 3
    consumer = "replay_consumer"
    config = ConsumerConfig(
        deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
        opt_start_seq=3,  # Start from message 3
    )
    sub = await js.pull_subscribe("logs.app", durable=consumer, config=config)

    try:
        messages = await sub.fetch(batch=10, timeout=2)
    except nats.errors.TimeoutError:
        messages = []
    print(f"\nReplaying from sequence 3:")
    for msg in messages:
        seq = msg.metadata.sequence.stream
        data = json.loads(msg.data.decode())
        print(f"  {seq}: {data['message']}")
        await msg.ack()
    
    await nc.close()


# ============================================================================
# Example 6: List stream info and consumer info
# ============================================================================
async def example_6_inspect():
    """
    Inspect stream and consumer state.
    """
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    
    # Get stream info
    stream_info = await js.stream_info("logs")
    print(f"\nStream 'logs':")
    print(f"  State: {stream_info.state}")
    print(f"  Messages: {stream_info.state.messages}")
    print(f"  Bytes: {stream_info.state.bytes}")
    
    # List all consumers for this stream
    consumers = await js.consumers_info("logs")
    print(f"\n  Consumers: {[c.name for c in consumers]}")

    # Report per-consumer state
    for consumer_info in consumers:
        pending = consumer_info.num_pending
        acked = consumer_info.num_ack_pending
        print(f"    {consumer_info.name}: pending={pending}, ack_pending={acked}")
    
    await nc.close()


# ============================================================================
# Main
# ============================================================================
async def main():
    print("JetStream Fanout Examples\n")
    
    print("1. Create stream and publish")
    await example_1_create_stream_and_publish()
    await asyncio.sleep(1)
    
    print("\n2. Pull consumer (one-at-a-time)")
    await example_2_pull_consumer()
    await asyncio.sleep(1)
    
    print("\n3. Multiple fanout consumers (runs for 10 sec)")
    await example_3_fanout()
    await asyncio.sleep(1)
    
    # print("\n4. Push consumer")
    # await example_4_push_consumer()
    
    print("\n5. Replay from specific sequence")
    await example_5_replay()
    await asyncio.sleep(1)
    
    print("\n6. Inspect stream and consumers")
    await example_6_inspect()


if __name__ == "__main__":
    asyncio.run(main())
