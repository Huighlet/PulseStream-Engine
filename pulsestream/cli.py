"""
PulseStream-Engine: Command Line Interface & Microsecond Performance Benchmark Suite.
"""

from __future__ import annotations

import argparse
import sys
import time

from pulsestream.broker import PulseStreamBroker
from pulsestream.coordinator import AssignmentStrategy, ConsumerGroupCoordinator
from pulsestream.partition import PartitionLog
from pulsestream.processor import StreamPipeline
from pulsestream.types import Record, WindowType
from pulsestream.window import StreamWindowAggregator


def run_benchmark(iterations: int = 100000) -> None:
    """Run comprehensive streaming benchmarks across all core components."""
    print("=" * 76)
    print("PulseStream-Engine: Microsecond Streaming Throughput Benchmark Suite")
    print(f"Iterations: {iterations:,} operations per test")
    print("=" * 76)

    # 1. PartitionLog Raw In-Memory Append Throughput
    part = PartitionLog(topic="bench-raw", partition_id=0)
    sample_payload = b'{"event":"telemetry","sensor_id":"gh-acc-01","reading":42.5}'
    sample_key = b"sensor-gh-01"

    t0 = time.perf_counter()
    for _ in range(iterations):
        part.append(value=sample_payload, key=sample_key)
    t1 = time.perf_counter()
    dur_append = t1 - t0
    ops_append = iterations / dur_append
    lat_append_us = (dur_append / iterations) * 1_000_000

    print(f"[1] PartitionLog Sequential In-Memory Append:")
    print(f"    Throughput: {ops_append:12,.1f} records/sec")
    print(f"    Latency:    {lat_append_us:12.3f} us/record\n")

    # 2. PartitionLog Scan & Read Throughput
    t0 = time.perf_counter()
    offset = 0
    read_count = 0
    while offset < iterations:
        chunk = part.read(start_offset=offset, max_records=500, verify_crc=True)
        if not chunk:
            break
        read_count += len(chunk)
        offset += len(chunk)
    t1 = time.perf_counter()
    dur_read = t1 - t0
    ops_read = read_count / dur_read
    lat_read_us = (dur_read / read_count) * 1_000_000

    print(f"[2] PartitionLog Monotonic Read with CRC32 Checksums (batch=500):")
    print(f"    Throughput: {ops_read:12,.1f} records/sec")
    print(f"    Latency:    {lat_read_us:12.3f} us/record\n")

    # 3. Multi-Partition Broker Routed Ingestion
    broker = PulseStreamBroker()
    broker.create_topic("payments", num_partitions=8)

    t0 = time.perf_counter()
    for i in range(iterations):
        k = f"account-{i % 1000}".encode("ascii")
        broker.produce("payments", value=sample_payload, key=k)
    t1 = time.perf_counter()
    dur_broker = t1 - t0
    ops_broker = iterations / dur_broker
    lat_broker_us = (dur_broker / iterations) * 1_000_000

    print(f"[3] Multi-Partition Broker Ingestion (8 Partitions, CRC32 Key Routing):")
    print(f"    Throughput: {ops_broker:12,.1f} records/sec")
    print(f"    Latency:    {lat_broker_us:12.3f} us/record\n")

    # 4. Consumer Group Offset Commit Store
    coordinator = ConsumerGroupCoordinator(broker, strategy=AssignmentStrategy.ROUND_ROBIN)
    coordinator.join_group("group-analytics", "worker-0", ["payments"])
    coordinator.join_group("group-analytics", "worker-1", ["payments"])
    coordinator.join_group("group-analytics", "worker-2", ["payments"])

    t0 = time.perf_counter()
    for i in range(iterations):
        coordinator.commit_offset("group-analytics", "payments", partition=i % 8, offset=i)
    t1 = time.perf_counter()
    dur_commit = t1 - t0
    ops_commit = iterations / dur_commit
    lat_commit_us = (dur_commit / iterations) * 1_000_000

    print(f"[4] Consumer Group Offset Commits:")
    print(f"    Throughput: {ops_commit:12,.1f} commits/sec")
    print(f"    Latency:    {lat_commit_us:12.3f} us/commit\n")

    # 5. Tumbling Window Stateful Stream Aggregation
    aggregator = StreamWindowAggregator(
        window_size_ms=1000,
        window_type=WindowType.TUMBLING,
        value_extractor=lambda r: 1.0,
        key_extractor=lambda r: r.key.decode("ascii") if r.key else "default",
    )
    base_ts = 1_000_000_000_000  # arbitrary start
    sample_records = [
        Record.create(
            topic="bench",
            partition=0,
            offset=i,
            value=b"1",
            key=f"k{i % 10}".encode("ascii"),
            timestamp_ns=base_ts + (i * 10_000_000),  # advance 10ms per record
        )
        for i in range(min(iterations, 50000))
    ]

    t0 = time.perf_counter()
    emitted_count = 0
    for r in sample_records:
        emitted = aggregator.add_record(r)
        emitted_count += len(emitted)
    t1 = time.perf_counter()
    dur_win = t1 - t0
    ops_win = len(sample_records) / dur_win
    lat_win_us = (dur_win / len(sample_records)) * 1_000_000

    print(f"[5] Stateful Tumbling Window Aggregation:")
    print(f"    Throughput: {ops_win:12,.1f} records/sec")
    print(f"    Latency:    {lat_win_us:12.3f} us/record")
    print(f"    Windows Emitted: {emitted_count:,}\n")

    # 6. End-to-End Stream Pipeline (Filter -> Map -> Sink)
    pipeline_broker = PulseStreamBroker()
    pipeline_broker.create_topic("ingress", num_partitions=4)
    pipeline = (
        StreamPipeline(pipeline_broker, "ingress")
        .filter(lambda r: len(r.value) > 5)
        .map(lambda r: r.value.upper())
        .to_topic("egress")
    )

    t0 = time.perf_counter()
    for i in range(min(iterations, 25000)):
        rec = Record.create("ingress", 0, i, b"test-stream-data-payload")
        pipeline.process_record(rec)
    t1 = time.perf_counter()
    dur_pipe = t1 - t0
    ops_pipe = min(iterations, 25000) / dur_pipe
    lat_pipe_us = (dur_pipe / min(iterations, 25000)) * 1_000_000

    print(f"[6] End-to-End Stream Pipeline (Filter -> Map -> Sink Topic):")
    print(f"    Throughput: {ops_pipe:12,.1f} records/sec")
    print(f"    Latency:    {lat_pipe_us:12.3f} us/record")
    print("=" * 76)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pulsestream",
        description="PulseStream-Engine: High-Throughput Event Streaming Broker & Partition Log",
    )
    subparsers = parser.add_subparsers(dest="command")

    bench_parser = subparsers.add_parser("bench", help="Execute streaming performance benchmarks")
    bench_parser.add_argument("-n", "--iterations", type=int, default=100000, help="Number of iterations")

    args = parser.parse_args()
    if args.command == "bench":
        run_benchmark(iterations=args.iterations)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
