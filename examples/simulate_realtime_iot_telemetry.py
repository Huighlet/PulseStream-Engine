"""
Enterprise Simulation: Real-Time IoT Telemetry Stream & Consumer Group Lag Invariant.
Simulates 100 edge devices publishing telemetry into a 4-partition PulseStream broker.
A 3-node consumer group processes the partitioned stream, commits offsets, and verifies 0 consumer lag.
"""

from __future__ import annotations

import json
import random
import time
from starlette.testclient import TestClient
from pulsestream.server import app
from pulsestream.client import PulseStreamClient


def run_iot_stream_simulation():
    print("=" * 75)
    print("PulseStream-Engine Enterprise Simulation: High-Throughput IoT Stream")
    print("=" * 75)

    test_http = TestClient(app)
    client = PulseStreamClient(
        base_url="http://localhost:9092",
        api_key="pulse_enterprise_mesh_key",
        http_client=test_http,
    )

    topic_name = "iot.factory.telemetry"
    num_partitions = 4
    total_events = 500

    print(f"\n[+] Provisioning partitioned topic '{topic_name}' ({num_partitions} partitions)...")
    try:
        client.create_topic(name=topic_name, num_partitions=num_partitions)
    except Exception:
        pass  # Topic might already exist in memory

    # 1. High-throughput ingestion across 100 edge devices
    devices = [f"sensor-node-{i:03d}" for i in range(100)]
    print(f"[+] Streaming {total_events} telemetry payloads from 100 edge sensors...")

    start_time = time.perf_counter()
    partition_counts = {p: 0 for p in range(num_partitions)}

    for i in range(total_events):
        device_id = random.choice(devices)
        payload = {
            "device_id": device_id,
            "reading_id": i + 1,
            "temperature_c": round(20.0 + random.uniform(-5.0, 15.0), 2),
            "vibration_rms": round(random.uniform(0.1, 4.2), 3),
            "voltage_mv": random.randint(3100, 3300),
            "status": "NORMAL" if random.random() > 0.05 else "WARNING",
            "timestamp_ns": time.time_ns(),
        }
        res = client.publish(topic=topic_name, value=payload, key=device_id)
        assigned_p = res["partition"]
        partition_counts[assigned_p] += 1

    elapsed_pub = time.perf_counter() - start_time
    pub_rate = total_events / elapsed_pub if elapsed_pub > 0 else 0

    print(f"[OK] Ingested {total_events} events in {elapsed_pub*1000:.2f} ms ({pub_rate:,.0f} ops/sec)")
    print(f"     Partition Distribution: {partition_counts}")

    # 2. Durable Point-in-Time Checkpoint
    print(f"\n[+] Capturing Point-in-Time Topic State Checkpoint...")
    snapshot = client.snapshot_topic(topic_name)
    print(f"     Snapshot ID: {snapshot['snapshot_id']}")
    print(f"     Total Records Archived: {snapshot['total_records']}")
    print(f"     Total Bytes Stored: {snapshot['total_bytes']:,} bytes")
    print(f"     Checkpoint Status: {snapshot['status']}")

    # 3. Consumer Group Coordination & Parallel Consumption
    group_id = "iot-realtime-analytics"
    print(f"\n[+] Initializing Consumer Group '{group_id}' with 3 worker instances...")

    workers = ["worker-alpha", "worker-beta", "worker-gamma"]
    assignments = {}
    for w in workers:
        join_res = client.join_group(group_id=group_id, member_id=w, subscribed_topics=[topic_name])
        assigned = [item["partition"] for item in join_res["assigned_partitions"] if item["topic"] == topic_name]
        assignments[w] = assigned
        print(f"     Worker '{w}' assigned partitions: {assigned}")

    # 4. Check Initial Consumer Lag
    pre_lag = client.get_topic_lag(topic_name, group_id=group_id)
    print(f"\n[+] Pre-Consumption Group Lag: {pre_lag['total_lag']} pending records (Status: {pre_lag['status']})")

    # 5. Process and Commit Offsets per Worker
    print(f"\n[+] Consuming and acknowledging event batches per assigned partition...")
    records_processed = 0

    for worker, parts in assignments.items():
        for p in parts:
            high_watermark = snapshot["partitions"][p]["latest_offset"]
            if high_watermark < 0:
                continue

            batch = client.consume(topic=topic_name, partition=p, offset=0, max_records=500)
            rec_count = batch["count"]
            records_processed += rec_count

            # Commit the last read offset
            last_offset = batch["records"][-1]["offset"]
            client.commit_offset(group_id=group_id, topic=topic_name, partition=p, offset=last_offset)
            print(f"     [{worker}] Partition {p}: Processed {rec_count} records -> Committed Offset {last_offset}")

    # 6. Verify Zero Consumer Lag Invariant
    post_lag = client.get_topic_lag(topic_name, group_id=group_id)
    print(f"\n[+] Post-Consumption Group Lag: {post_lag['total_lag']} pending records (Status: {post_lag['status']})")
    assert post_lag["total_lag"] == 0, f"Expected 0 lag, got {post_lag['total_lag']}"

    # 7. Prometheus Metrics Telemetry Verification
    metrics = client.get_metrics()
    assert "pulsestream_records_appended_total" in metrics
    print(f"[OK] Prometheus Metrics Verified (/metrics live and reporting)")

    print("\n" + "=" * 75)
    print("[SUCCESS] Real-Time IoT Telemetry Stream Simulation Passed with 0 Lag!")
    print("=" * 75)


if __name__ == "__main__":
    run_iot_stream_simulation()
