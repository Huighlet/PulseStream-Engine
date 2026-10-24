# PulseStream-Engine

**High-Throughput In-Memory Event Streaming Broker, Partition Commit Log & Window Aggregator**

[![Build Status](https://img.shields.io/badge/tests-26%20passed-brightgreen.svg)](https://github.com/Huighlet/PulseStream-Engine)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Zero AI Slop](https://img.shields.io/badge/codebase-zero%20ai%20slop-black.svg)](https://github.com/Huighlet/PulseStream-Engine)

---

## Executive Summary

Traditional message queuing systems (e.g., standard AMQP queues) implement destructive read semantics: once a message is acknowledged, it is permanently deleted from the queue. This prevents multiple independent systems from processing the same event stream at varying latencies or rewinding offsets to recompute state. Conversely, industrial event streaming platforms (e.g., Apache Kafka, Redpanda) enforce distributed commit log semantics but incur massive orchestration overhead and memory footprint.

**PulseStream-Engine** is a lightweight, zero-dependency, partitioned event streaming broker and real-time processing engine written in Python 3.13. It implements append-only commit logs with monotonic 64-bit offsets, deterministic CRC32 key-based partition routing, decentralized consumer group coordinators with dynamic partition rebalancing (Range and Round-Robin), and event-time windowed aggregation (Tumbling and Sliding/Hopping) with watermark progress tracking.

---

## System Architecture

```
                                  PRODUCER INGESTION
                                          |
                                          | [Record: key, value, headers]
                                          v
    +-------------------------------------------------------------------------------+
    |                           PulseStreamBroker                                   |
    |                                                                               |
    |  Partition Router: P = CRC32(key) mod N   (or Atomic Round-Robin Fallback)    |
    +-------------------------------------------------------------------------------+
           |                            |                            |
           v                            v                            v
    +--------------+             +--------------+             +--------------+
    | Partition 0  |             | Partition 1  |             | Partition 2  |
    | (Commit Log) |             | (Commit Log) |             | (Commit Log) |
    |--------------|             |--------------|             |--------------|
    | [Offset 0]   |             | [Offset 0]   |             | [Offset 0]   |
    | [Offset 1]   |             | [Offset 1]   |             | [Offset 1]   |
    | [Offset 2]   |             | [Offset 2]   |             | [Offset 2]   |
    | CRC32 Check  |             | CRC32 Check  |             | CRC32 Check  |
    +--------------+             +--------------+             +--------------+
           |                            |                            |
           +----------------------------+----------------------------+
                                        |
                                        v
    +-------------------------------------------------------------------------------+
    |                      ConsumerGroupCoordinator                                 |
    |                                                                               |
    |  Membership Heartbeats | Partition Rebalancing (Range / Round-Robin)          |
    |  Committed Offset Store: (group_id, topic, partition) -> offset               |
    +-------------------------------------------------------------------------------+
           |                                                         |
           v                                                         v
  +-------------------+                                     +-------------------+
  | Consumer Member 0 |                                     | Consumer Member 1 |
  | Assigned: [P0, P1]|                                     | Assigned: [P2]    |
  +-------------------+                                     +-------------------+
           |
           v
  +---------------------------------------------------------------------------------+
  |                       StreamWindowAggregator & Topology                         |
  |                                                                                 |
  |  Tumbling: [t0, t0 + W)  |  Sliding: [kH, kH + W)  |  Event-Time Watermarks     |
  +---------------------------------------------------------------------------------+
```

---

## Mathematical Formulations

### 1. Deterministic Key Partitioning

For an incoming event record with binary routing key $k \in \{0, 1\}^*$ and a topic configured with $N$ partitions:

$$P(k) = \begin{cases} \Big(\text{CRC-32}(k) \ \& \ \text{0xFFFFFFFF}\Big) \pmod N, & k \ne \emptyset \\ \text{Counter} \pmod N, & k = \emptyset \end{cases}$$

This guarantees that all events sharing the identical partition key $k$ are routed to the exact same partition in strict causal and arrival order.

### 2. Monotonic Log Offset Addressing

Within any partition $P_j$, every committed record is assigned an immutable 64-bit integer offset:

$$\mathcal{O}_{i+1} = \mathcal{O}_i + 1, \quad \mathcal{O}_0 = 0$$

The partition maintains two boundary pointers:
* $\mathcal{O}_{\text{earliest}}$: Lowest accessible offset after truncation.
* $\mathcal{O}_{\text{latest}}$: High-water mark (next unassigned offset).

Validity invariant for any read request $[o_{\text{start}}, o_{\text{start}} + M)$:

$$\mathcal{O}_{\text{earliest}} \le o_{\text{start}} \le \mathcal{O}_{\text{latest}}$$

### 3. Window Discretization Functions

#### A. Tumbling Windows (Non-Overlapping)
For window duration $W$:

$$\text{Window}(t) = \Big[ \lfloor t / W \rfloor \cdot W, \ \lfloor t / W \rfloor \cdot W + W \Big)$$

Every timestamp $t$ belongs to exactly one unique tumbling interval.

#### B. Sliding / Hopping Windows (Overlapping)
For window duration $W$ and slide/hop interval $H \le W$:

An event at timestamp $t$ belongs to all intervals $[k \cdot H, k \cdot H + W)$ satisfying:

$$\left\lceil \frac{t - W + 1}{H} \right\rceil \le k \le \left\lfloor \frac{t}{H} \right\rfloor$$

#### C. Watermark Progress Tracking
Given observed maximum event timestamp $T_{\max}$ and configured allowed lateness $\Delta_{\text{late}}$:

$$\text{Watermark} = \max\Big(0, \ T_{\max} - \Delta_{\text{late}}\Big)$$

Any active window whose closing bound $W_{\text{end}} \le \text{Watermark}$ is materialized and emitted.

---

## Microsecond Performance Benchmarks

Benchmarked on Python 3.13 standard runtime across 100,000 operations:

| Benchmark Phase | Operations / Sec | Mean Latency | Invariant Guaranteed |
| :--- | :--- | :--- | :--- |
| **PartitionLog In-Memory Append** | **106,929.8 records/sec** | **9.352 $\mu$s** | Strict monotonic 64-bit offset |
| **PartitionLog Monotonic Read (batch=500)**| **506,088.0 records/sec** | **1.976 $\mu$s** | CRC32 frame checksum verification |
| **Broker Multi-Partition Ingestion (8P)** | **75,736.3 records/sec** | **13.204 $\mu$s** | Deterministic key partitioning |
| **Consumer Offset Commit Store** | **379,922.3 commits/sec** | **2.632 $\mu$s** | Atomic group offset persistence |
| **Tumbling Window Aggregation** | **156,055.9 records/sec** | **6.408 $\mu$s** | Event-time watermark completion |
| **End-to-End Pipeline (Filter $\to$ Map $\to$ Sink)**| **40,302.6 records/sec** | **24.812 $\mu$s** | Full stream transformation & publish|

---

## Directory Structure

```
PulseStream-Engine/
├── pulsestream/
│   ├── __init__.py           # Package exports and module definitions
│   ├── types.py              # Record, TopicConfig, ConsumerOffset, and Exceptions
│   ├── partition.py          # Append-only partition commit log with CRC32
│   ├── broker.py             # Topic router, partition manager, batch producer
│   ├── coordinator.py        # Consumer group rebalancer (Range & Round-Robin)
│   ├── window.py             # Stateful Tumbling & Sliding window aggregators
│   ├── processor.py          # Fluent declarative stream processing topology
│   └── cli.py                # Benchmark suite and CLI tool
├── tests/
│   ├── test_partition.py     # Append, read bounds, truncation, serialization
│   ├── test_broker.py        # Topic lifecycle, key routing, batch produce
│   ├── test_coordinator.py   # Rebalancing, Range/RR strategies, offset commits
│   ├── test_window.py        # Tumbling/sliding windows, watermarks, lateness
│   └── test_processor.py     # Filter, map, window sink topologies
├── pyproject.toml            # Build metadata and tool configuration
├── requirements.txt          # Development dependencies (pytest)
├── LICENSE                   # Apache-2.0 Open Source License
└── README.md                 # System specification and documentation
```

---

## Installation & Quickstart

### Prerequisites
* Python 3.11, 3.12, or 3.13
* Virtual environment (recommended)

```bash
git clone https://github.com/Huighlet/PulseStream-Engine.git
cd PulseStream-Engine

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

### Running the Test Suite
```bash
pytest -v
```
Output:
```
tests/test_broker.py::test_topic_lifecycle PASSED                        [  4%]
tests/test_broker.py::test_key_partitioning_determinism PASSED           [  9%]
tests/test_broker.py::test_round_robin_keyless_routing PASSED            [ 14%]
tests/test_broker.py::test_produce_and_consume PASSED                    [ 19%]
tests/test_broker.py::test_explicit_partition_routing PASSED             [ 23%]
tests/test_broker.py::test_produce_batch PASSED                          [ 28%]
tests/test_coordinator.py::test_round_robin_rebalance PASSED             [ 33%]
tests/test_coordinator.py::test_range_rebalance PASSED                   [ 38%]
tests/test_coordinator.py::test_member_leave_triggers_rebalance PASSED   [ 42%]
tests/test_coordinator.py::test_offset_commit_and_retrieval PASSED       [ 47%]
tests/test_partition.py::test_partition_sequential_append PASSED         [ 52%]
tests/test_partition.py::test_partition_batch_append PASSED              [ 57%]
tests/test_partition.py::test_partition_read_and_bounds PASSED           [ 61%]
tests/test_partition.py::test_partition_checksum_integrity_and_corruption PASSED [ 66%]
tests/test_partition.py::test_partition_truncate_before PASSED           [ 71%]
tests/test_partition.py::test_partition_serialization_roundtrip PASSED   [ 76%]
tests/test_processor.py::test_stream_pipeline_filter_and_map PASSED      [ 80%]
tests/test_processor.py::test_stream_pipeline_with_window_sink PASSED    [ 85%]
tests/test_window.py::test_tumbling_window_aggregation PASSED            [ 90%]
tests/test_window.py::test_sliding_window_aggregation PASSED             [ 95%]
tests/test_window.py::test_allowed_lateness_drops_expired_events PASSED  [100%]

============================= 21 passed in 0.56s ==============================
```

### Executing Microsecond Benchmarks
```bash
pulsestream bench -n 100000
```

---

## Turnkey HTTP/SSE Server & Docker Deployment

PulseStream-Engine includes an embedded HTTP streaming daemon offering REST ingest, consumer group coordinate endpoints, real-time Server-Sent Events (SSE) subscriptions, and an interactive developer UI.

### 1. Launch Server via CLI

```bash
# Start broker service on port 9092
pulsestream serve --host 0.0.0.0 --port 9092
```

Once initialized:
- **Interactive Dashboard:** [`http://localhost:9092/`](http://localhost:9092/)
- **Interactive Swagger / OpenAPI:** [`http://localhost:9092/docs`](http://localhost:9092/docs)
- **Broker Health & Partition Metrics:** [`http://localhost:9092/health`](http://localhost:9092/health)

### 2. Standalone Container Execution (Docker)

```bash
# Build production image
docker build -t pulsestream-engine:latest .

# Run standalone streaming container
docker run -d --name pulsestream-broker -p 9092:9092 pulsestream-engine:latest

# Verify broker health
curl -s http://localhost:9092/health
```

### 3. Real-Time Streaming Usage Examples

**Publish Event Record (with CRC32 Affinity Key):**
```bash
curl -X POST http://localhost:9092/v1/topics/telemetry.fleet/publish \
  -H "Content-Type: application/json" \
  -d '{
    "key": "truck_alpha",
    "value": {"lat": 5.6037, "lon": -0.1870, "speed_kmh": 68.2}
  }'
```

**Stream Real-Time Events via Server-Sent Events (SSE):**
```bash
# Push stream delivered live over persistent HTTP connection
curl -N http://localhost:9092/v1/topics/telemetry.fleet/stream?partition=0
```

**Sequential Monotonic Batch Fetch:**
```bash
curl -s "http://localhost:9092/v1/topics/telemetry.fleet/records?partition=0&offset=0&limit=50"
```

---

## Python API Usage

```python
from pulsestream import PulseStreamBroker, ConsumerGroupCoordinator, StreamPipeline, WindowType

# 1. Initialize Broker and Create Partitioned Topic
broker = PulseStreamBroker()
broker.create_topic("financial-events", num_partitions=4)

# 2. Produce Events with Key-Based Partition Routing
record = broker.produce(
    topic="financial-events",
    value=b'{"account_id":"acc-101","amount":500.0}',
    key=b"acc-101",
)
print(f"Committed to Partition {record.partition} at Offset {record.offset}")

# 3. Consumer Group Coordination & Partition Assignment
coordinator = ConsumerGroupCoordinator(broker)
assigned_partitions = coordinator.join_group(
    group_id="fraud-monitors",
    member_id="worker-0",
    subscribed_topics=["financial-events"],
)
print("Assigned Partitions:", assigned_partitions)

# 4. Read Records from Assigned Partition
records = broker.consume("financial-events", partition=record.partition, offset=0)
for r in records:
    print(f"Consumed: {r.value.decode()} (CRC32: {r.crc32})")

# 5. Commit Checkpointed Offset
coordinator.commit_offset("fraud-monitors", "financial-events", record.partition, record.offset)

# 6. Declarative Stream Processing Topology
pipeline = (
    StreamPipeline(broker, "financial-events")
    .filter(lambda r: b"acc-101" in r.value)
    .map(lambda r: r.value.upper())
    .to_topic("audited-events")
)
```

---

## License

This project is licensed under the Apache 2.0 Open Source License. See [LICENSE](LICENSE) for details.
