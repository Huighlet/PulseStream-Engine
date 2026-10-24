"""
Tests for PartitionLog append-only operations, offset indexing, and CRC32 verification.
"""

import pytest
from pulsestream.partition import PartitionLog
from pulsestream.types import ChecksumMismatchError, OffsetOutOfRangeError, Record


def test_partition_sequential_append():
    part = PartitionLog(topic="orders", partition_id=0)
    assert part.earliest_offset == 0
    assert part.latest_offset == 0
    assert part.record_count == 0

    r0 = part.append(value=b"order-1", key=b"k1")
    assert r0.offset == 0
    assert r0.topic == "orders"
    assert r0.partition == 0
    assert r0.verify_checksum() is True

    r1 = part.append(value=b"order-2", key=b"k2")
    assert r1.offset == 1
    assert part.latest_offset == 2
    assert part.record_count == 2


def test_partition_batch_append():
    part = PartitionLog(topic="events", partition_id=1)
    batch = [(f"val-{i}".encode(), f"k-{i}".encode(), None) for i in range(10)]
    recs = part.append_batch(batch)

    assert len(recs) == 10
    assert recs[0].offset == 0
    assert recs[-1].offset == 9
    assert part.latest_offset == 10


def test_partition_read_and_bounds():
    part = PartitionLog(topic="metrics", partition_id=0)
    for i in range(5):
        part.append(value=f"metric-{i}".encode())

    # Read all
    recs = part.read(start_offset=0, max_records=10)
    assert len(recs) == 5
    assert [r.offset for r in recs] == [0, 1, 2, 3, 4]

    # Partial read
    slice_recs = part.read(start_offset=2, max_records=2)
    assert len(slice_recs) == 2
    assert [r.offset for r in slice_recs] == [2, 3]

    # Reading at high watermark returns empty list
    assert part.read(start_offset=5) == []

    # Out of bounds
    with pytest.raises(OffsetOutOfRangeError):
        part.read(start_offset=6)
    with pytest.raises(OffsetOutOfRangeError):
        part.read(start_offset=-1)


def test_partition_checksum_integrity_and_corruption():
    part = PartitionLog(topic="audit", partition_id=0)
    part.append(value=b"secure-audit-trail")

    # Manually tamper with internal record's value
    original = part._records[0]
    tampered = Record(
        topic=original.topic,
        partition=original.partition,
        offset=original.offset,
        timestamp_ns=original.timestamp_ns,
        key=original.key,
        value=b"tampered-audit-trail",
        headers=original.headers,
        crc32=original.crc32,
    )
    part._records[0] = tampered

    with pytest.raises(ChecksumMismatchError):
        part.read(start_offset=0, max_records=1, verify_crc=True)


def test_partition_truncate_before():
    part = PartitionLog(topic="cleanup", partition_id=0)
    for i in range(10):
        part.append(value=f"data-{i}".encode())

    deleted = part.truncate_before(5)
    assert deleted == 5
    assert part.earliest_offset == 5
    assert part.latest_offset == 10
    assert part.record_count == 5

    # Reading before 5 should now raise OffsetOutOfRangeError
    with pytest.raises(OffsetOutOfRangeError):
        part.read(start_offset=0)

    recs = part.read(start_offset=5)
    assert len(recs) == 5
    assert recs[0].offset == 5


def test_partition_serialization_roundtrip():
    part = PartitionLog(topic="binary-test", partition_id=0)
    rec = Record.create("binary-test", 0, 42, b"payload-bytes", key=b"test-key")

    serialized = part.serialize_record(rec)
    deserialized = part.deserialize_record(serialized)

    assert deserialized.offset == 42
    assert deserialized.key == b"test-key"
    assert deserialized.value == b"payload-bytes"
    assert deserialized.crc32 == rec.crc32
