#!/usr/bin/env python3
import asyncio
import json
from datetime import datetime
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()

    # Connect to NATS
    await nc.connect("nats://localhost:4222")
    print("✓ Connected to NATS server\n")

    js = nc.jetstream()

    # Create a stream if it doesn't exist
    stream_name = "logs"
    try:
        stream_info = await js.stream_info(stream_name)
        print(f"✓ Using existing stream: {stream_name}")
        await js.purge_stream(stream_name)
        print(f"  Purged stream\n")
    except:
        print(f"✓ Creating new stream: {stream_name}")
        await js.add_stream(
            name=stream_name,
            subjects=[f"{stream_name}.>"]
        )
        print()

    # Publish 10 messages
    print("📤 Publishing 10 messages to 'logs' stream:")
    published_seqs = []
    for i in range(1, 11):
        msg_data = {
            "id": i,
            "timestamp": datetime.now().isoformat(),
            "message": f"Log message #{i}",
            "level": ["INFO", "DEBUG", "WARN", "ERROR"][i % 4]
        }
        subject = f"logs.application"
        ack = await js.publish(subject, json.dumps(msg_data).encode())
        published_seqs.append(ack.seq)
        print(f"  [{i}] Published to {subject} (seq: {ack.seq})")

    print()

    # Show stream state
    stream_info = await js.stream_info(stream_name)
    print(f"📊 Stream State ({stream_name}):")
    print(f"  Messages: {stream_info.state.messages}")
    print(f"  Bytes: {stream_info.state.bytes}")
    print(f"  First seq: {stream_info.state.first_seq}")
    print(f"  Last seq: {stream_info.state.last_seq}\n")

    # Create 3 independent consumers
    consumers = []
    for consumer_id in range(1, 4):
        durable_name = f"consumer-{consumer_id}"

        try:
            await js.delete_consumer(stream_name, durable_name)
        except:
            pass

        # Each consumer starts from the beginning
        await js.add_consumer(
            stream_name,
            durable_name=durable_name,
            deliver_policy="all"
        )

        consumer = {
            "id": consumer_id,
            "name": f"Consumer-{consumer_id}",
            "durable": durable_name,
        }
        consumers.append(consumer)
        print(f"✓ Created consumer: Consumer-{consumer_id} ({durable_name})")

    print()

    # Each consumer reads messages independently using pull consumer pattern
    print("👥 Each consumer reading messages independently:\n")

    async def read_consumer(consumer_info):
        """Read messages for a specific consumer"""
        messages_read = []

        # Use pull consumer
        psub = await js.pull_subscribe(
            f"{stream_name}.>",
            durable=consumer_info["durable"]
        )

        # Read messages using fetch (pull consumer API)
        try:
            while len(messages_read) < 10:
                try:
                    msgs = await psub.fetch(1, timeout=1.0)
                    for msg in msgs:
                        data = json.loads(msg.data.decode())
                        messages_read.append(data)
                        await msg.ack()
                        print(f"  {consumer_info['name']} received msg #{data['id']}: {data['message']}")
                except Exception:
                    break
        finally:
            await psub.unsubscribe()

        return messages_read

    # Run all consumers concurrently
    results = await asyncio.gather(
        read_consumer(consumers[0]),
        read_consumer(consumers[1]),
        read_consumer(consumers[2]),
    )

    print()

    # Show consumer state
    print("📊 Consumer State:")
    for i, consumer_info in enumerate(consumers):
        consumer_state = await js.consumer_info(stream_name, consumer_info["durable"])
        print(f"  {consumer_info['name']}:")
        print(f"    Delivered: {consumer_state.delivered.consumer_seq}")
        print(f"    Pending: {consumer_state.num_pending}")
        print(f"    Messages processed: {len(results[i])}")

    print()

    # Demonstrate replay: create a new consumer that reads from sequence 5
    print("🔄 Demonstrating Replay:")
    replay_durable = "replay-consumer"

    try:
        await js.delete_consumer(stream_name, replay_durable)
    except:
        pass

    # Replay from the 6th published message. Stream sequence numbers are
    # monotonic and survive purges, so we use the actual sequence captured
    # at publish time rather than a hardcoded value.
    replay_seq = published_seqs[5]
    await js.add_consumer(
        stream_name,
        durable_name=replay_durable,
        deliver_policy="by_start_sequence",
        opt_start_seq=replay_seq  # Start from the 6th message
    )
    print(f"✓ Created replay consumer starting from sequence {replay_seq}\n")

    print(f"  Replaying from sequence {replay_seq}:")
    psub = await js.pull_subscribe(
        f"{stream_name}.>",
        durable=replay_durable
    )

    replay_count = 0
    try:
        while replay_count < 10:
            try:
                msgs = await psub.fetch(1, timeout=1.0)
                for msg in msgs:
                    data = json.loads(msg.data.decode())
                    print(f"    [{data['id']}] {data['message']} ({data['level']})")
                    await msg.ack()
                    replay_count += 1
            except Exception:
                break
    finally:
        await psub.unsubscribe()

    print(f"\n  Replayed {replay_count} messages\n")

    # Show final statistics
    print("📈 Final Statistics:")
    stream_info = await js.stream_info(stream_name)
    print(f"  Stream messages: {stream_info.state.messages}")

    all_durables = [c["durable"] for c in consumers] + [replay_durable]
    print(f"  Total consumers: {len(all_durables)}")

    for durable_name in all_durables:
        consumer_state = await js.consumer_info(stream_name, durable_name)
        pending = consumer_state.num_pending
        delivered = consumer_state.delivered.consumer_seq
        print(f"    - {durable_name}: delivered={delivered}, pending={pending}")

    print()
    print("✓ Example completed successfully!")

    # Cleanup
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
