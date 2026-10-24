"""
Tests for StreamWindowAggregator Tumbling & Sliding windows with event-time watermarking.
"""

import pytest
from pulsestream.types import Record, WindowType
from pulsestream.window import StreamWindowAggregator


def test_tumbling_window_aggregation():
    # 1000ms window
    agg = StreamWindowAggregator(
        window_size_ms=1000,
        window_type=WindowType.TUMBLING,
        value_extractor=lambda r: float(r.value.decode()),
        key_extractor=lambda r: r.key.decode() if r.key else "default",
    )

    base = 1_000_000_000_000  # 1000 seconds in ns

    # Add 3 records in [0..1000ms)
    r1 = Record.create("t", 0, 0, b"10.0", key=b"k1", timestamp_ns=base + 100_000_000)
    r2 = Record.create("t", 0, 1, b"25.0", key=b"k1", timestamp_ns=base + 400_000_000)
    r3 = Record.create("t", 0, 2, b"15.0", key=b"k1", timestamp_ns=base + 800_000_000)

    agg.add_record(r1)
    agg.add_record(r2)
    agg.add_record(r3)

    # Window has not closed yet because watermark has not advanced past base + 1000ms
    # Advance time into the next window [1000ms..2000ms)
    r4 = Record.create("t", 0, 3, b"5.0", key=b"k1", timestamp_ns=base + 1_500_000_000)
    emitted = agg.add_record(r4)

    assert len(emitted) == 1
    res = emitted[0]
    assert res.key == "k1"
    assert res.record_count == 3
    assert res.value == 50.0  # 10 + 25 + 15
    assert res.window_start_ns == base
    assert res.window_end_ns == base + 1_000_000_000


def test_sliding_window_aggregation():
    # 1000ms window with 500ms hop
    agg = StreamWindowAggregator(
        window_size_ms=1000,
        window_type=WindowType.SLIDING,
        hop_size_ms=500,
        value_extractor=lambda r: 1.0,
        key_extractor=lambda r: "all",
    )

    base = 1_000_000_000_000
    # Record at 600ms falls into [0..1000ms) and [500..1500ms)
    r = Record.create("t", 0, 0, b"event", timestamp_ns=base + 600_000_000)
    agg.add_record(r)

    # Flush all active windows
    emitted = agg.flush()
    assert len(emitted) == 2
    windows = sorted(emitted, key=lambda w: w.window_start_ns)
    assert windows[0].window_start_ns == base
    assert windows[0].window_end_ns == base + 1_000_000_000
    assert windows[1].window_start_ns == base + 500_000_000
    assert windows[1].window_end_ns == base + 1_500_000_000


def test_allowed_lateness_drops_expired_events():
    agg = StreamWindowAggregator(
        window_size_ms=1000,
        window_type=WindowType.TUMBLING,
        allowed_lateness_ms=500,
    )
    base = 1_000_000_000_000

    # Advance clock to base + 5000ms
    future = Record.create("t", 0, 0, b"future", timestamp_ns=base + 5_000_000_000)
    agg.add_record(future)

    # Send historical record that is way past lateness
    stale = Record.create("t", 0, 1, b"stale", timestamp_ns=base)
    emitted = agg.add_record(stale)
    assert emitted == []
