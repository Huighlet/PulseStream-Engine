"""
Tests for PulseStreamBroker topic management, partitioning, and message routing.
"""

import pytest
from pulsestream.broker import PulseStreamBroker
from pulsestream.types import (
    PartitionOutOfBoundsError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
)


@pytest.fixture
def broker() -> PulseStreamBroker:
    b = PulseStreamBroker()
    b.create_topic("orders", num_partitions=4)
    b.create_topic("payments", num_partitions=2)
    return b


def test_topic_lifecycle(broker):
    assert broker.has_topic("orders") is True
    assert broker.has_topic("nonexistent") is False
    assert broker.list_topics() == ["orders", "payments"]

    with pytest.raises(TopicAlreadyExistsError):
        broker.create_topic("orders", num_partitions=4)

    broker.delete_topic("payments")
    assert broker.has_topic("payments") is False
    assert broker.list_topics() == ["orders"]

    with pytest.raises(TopicNotFoundError):
        broker.delete_topic("payments")


def test_key_partitioning_determinism(broker):
    # Same key must consistently map to the exact same partition
    k = b"customer-uuid-12345"
    p1 = broker.route_partition("orders", key=k)
    p2 = broker.route_partition("orders", key=k)
    p3 = broker.route_partition("orders", key=k)
    assert p1 == p2 == p3
    assert 0 <= p1 < 4


def test_round_robin_keyless_routing(broker):
    # Successive keyless messages should cycle through partitions
    parts = [broker.route_partition("orders", key=None) for _ in range(8)]
    assert parts == [0, 1, 2, 3, 0, 1, 2, 3]


def test_produce_and_consume(broker):
    r0 = broker.produce("orders", value=b"item-a", key=b"k1")
    assert r0.topic == "orders"
    assert r0.offset == 0

    records = broker.consume("orders", partition=r0.partition, offset=0)
    assert len(records) == 1
    assert records[0].value == b"item-a"


def test_explicit_partition_routing(broker):
    r = broker.produce("orders", value=b"targeted", partition=3)
    assert r.partition == 3

    with pytest.raises(PartitionOutOfBoundsError):
        broker.produce("orders", value=b"fail", partition=99)


def test_produce_batch(broker):
    batch = [(f"val-{i}".encode(), f"key-{i}".encode()) for i in range(20)]
    results = broker.produce_batch("orders", batch)
    assert len(results) == 20
    for r in results:
        assert r.topic == "orders"
        assert 0 <= r.partition < 4
