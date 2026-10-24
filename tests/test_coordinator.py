"""
Tests for ConsumerGroupCoordinator partition rebalancing and offset commits.
"""

import pytest
from pulsestream.broker import PulseStreamBroker
from pulsestream.coordinator import AssignmentStrategy, ConsumerGroupCoordinator


@pytest.fixture
def broker() -> PulseStreamBroker:
    b = PulseStreamBroker()
    b.create_topic("events", num_partitions=6)
    return b


def test_round_robin_rebalance(broker):
    coord = ConsumerGroupCoordinator(broker, strategy=AssignmentStrategy.ROUND_ROBIN)

    # 1 member -> gets all 6 partitions
    m1_parts = coord.join_group("cg-1", "member-1", ["events"])
    assert len(m1_parts) == 6
    assert [p for _, p in m1_parts] == [0, 1, 2, 3, 4, 5]

    # 2 members -> each gets 3 partitions
    coord.join_group("cg-1", "member-2", ["events"])
    m1_parts = coord.get_assignment("cg-1", "member-1")
    m2_parts = coord.get_assignment("cg-1", "member-2")
    assert len(m1_parts) == 3
    assert len(m2_parts) == 3
    # Union covers all 6 partitions without duplicate
    all_assigned = sorted([p for _, p in m1_parts] + [p for _, p in m2_parts])
    assert all_assigned == [0, 1, 2, 3, 4, 5]


def test_range_rebalance(broker):
    coord = ConsumerGroupCoordinator(broker, strategy=AssignmentStrategy.RANGE)
    coord.join_group("cg-range", "member-a", ["events"])
    coord.join_group("cg-range", "member-b", ["events"])

    ma_parts = coord.get_assignment("cg-range", "member-a")
    mb_parts = coord.get_assignment("cg-range", "member-b")
    # Range should allocate partitions [0, 1, 2] to member-a and [3, 4, 5] to member-b
    assert [p for _, p in ma_parts] == [0, 1, 2]
    assert [p for _, p in mb_parts] == [3, 4, 5]


def test_member_leave_triggers_rebalance(broker):
    coord = ConsumerGroupCoordinator(broker, strategy=AssignmentStrategy.ROUND_ROBIN)
    coord.join_group("cg-leave", "m1", ["events"])
    coord.join_group("cg-leave", "m2", ["events"])

    assert len(coord.get_assignment("cg-leave", "m1")) == 3
    assert len(coord.get_assignment("cg-leave", "m2")) == 3

    coord.leave_group("cg-leave", "m2")
    # m1 should now hold all 6
    assert len(coord.get_assignment("cg-leave", "m1")) == 6


def test_offset_commit_and_retrieval(broker):
    coord = ConsumerGroupCoordinator(broker)
    assert coord.get_committed_offset("cg-off", "events", 0) is None

    coord.commit_offset("cg-off", "events", 0, offset=42)
    assert coord.get_committed_offset("cg-off", "events", 0) == 42

    coord.commit_offset("cg-off", "events", 0, offset=99)
    assert coord.get_committed_offset("cg-off", "events", 0) == 99
