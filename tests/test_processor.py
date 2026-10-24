"""
Tests for StreamPipeline declarative stream topology.
"""

import pytest
from pulsestream.broker import PulseStreamBroker
from pulsestream.processor import StreamPipeline
from pulsestream.types import Record


@pytest.fixture
def broker() -> PulseStreamBroker:
    b = PulseStreamBroker()
    b.create_topic("input-stream", num_partitions=2)
    return b


def test_stream_pipeline_filter_and_map(broker):
    pipeline = (
        StreamPipeline(broker, "input-stream")
        .filter(lambda r: r.key == b"keep")
        .map(lambda r: r.value.upper())
        .to_topic("output-stream")
    )

    r_drop = Record.create("input-stream", 0, 0, b"drop-me", key=b"ignore")
    r_keep = Record.create("input-stream", 0, 1, b"process-me", key=b"keep")

    res_drop = pipeline.process_record(r_drop)
    assert res_drop == []

    res_keep = pipeline.process_record(r_keep)
    assert len(res_keep) == 1
    committed_sink = res_keep[0]
    assert committed_sink.topic == "output-stream"
    assert committed_sink.value == b"PROCESS-ME"
    assert committed_sink.key == b"keep"


def test_stream_pipeline_with_window_sink(broker):
    pipeline = (
        StreamPipeline(broker, "metrics")
        .window(
            window_size_ms=1000,
            value_extractor=lambda r: float(r.value.decode()),
            key_extractor=lambda r: r.key.decode() if r.key else "global",
        )
        .to_topic("metrics-aggregated")
    )

    base = 1_000_000_000_000
    r1 = Record.create("metrics", 0, 0, b"10.0", key=b"k1", timestamp_ns=base + 100_000_000)
    r2 = Record.create("metrics", 0, 1, b"20.0", key=b"k1", timestamp_ns=base + 500_000_000)

    pipeline.process_record(r1)
    pipeline.process_record(r2)

    # Flush windows
    flushed = pipeline.flush_windows()
    assert len(flushed) == 1
    assert flushed[0].value == 30.0

    # Ensure result was published to output sink topic
    sink_records = broker.consume("metrics-aggregated", partition=0, offset=0, max_records=10)
    # The record could land on any partition depending on CRC32(k1)
    # Check all partitions
    total_sink = sum(
        len(broker.consume("metrics-aggregated", partition=p, offset=0, max_records=10))
        for p in range(4)
    )
    assert total_sink == 1
