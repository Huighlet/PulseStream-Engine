"""
PulseStream-Engine: Consumer Group Coordinator & Partition Rebalancer.
Manages distributed group membership, Range and Round-Robin partition assignment, and offset commits.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from pulsestream.broker import PulseStreamBroker
from pulsestream.types import ConsumerOffset, ConsumerRebalanceError


class AssignmentStrategy(enum.StrEnum):
    """Partition assignment rebalancing algorithm."""
    RANGE = "RANGE"
    ROUND_ROBIN = "ROUND_ROBIN"


@dataclasses.dataclass(slots=True)
class GroupMember:
    """Registered consumer member in a group."""
    member_id: str
    subscribed_topics: Set[str]
    last_heartbeat: float
    assigned_partitions: List[Tuple[str, int]] = dataclasses.field(default_factory=list)


class ConsumerGroupCoordinator:
    """
    Coordinates partition assignments and committed offsets for consumer groups.
    Implements deterministic Range and Round-Robin assignment algorithms.
    """

    def __init__(
        self,
        broker: PulseStreamBroker,
        strategy: AssignmentStrategy = AssignmentStrategy.ROUND_ROBIN,
        session_timeout_sec: float = 30.0,
    ) -> None:
        self.broker = broker
        self.strategy = strategy
        self.session_timeout_sec = session_timeout_sec
        self._lock = threading.RLock()

        # group_id -> {member_id -> GroupMember}
        self._groups: Dict[str, Dict[str, GroupMember]] = collections.defaultdict(dict)
        # (group_id, topic, partition) -> ConsumerOffset
        self._committed_offsets: Dict[Tuple[str, str, int], ConsumerOffset] = {}

    def join_group(
        self,
        group_id: str,
        member_id: str,
        subscribed_topics: List[str],
    ) -> List[Tuple[str, int]]:
        """
        Register a consumer into a group, trigger rebalance, and return assigned partitions.
        """
        with self._lock:
            now = time.time()
            self._groups[group_id][member_id] = GroupMember(
                member_id=member_id,
                subscribed_topics=set(subscribed_topics),
                last_heartbeat=now,
            )
            self._rebalance_group(group_id)
            return list(self._groups[group_id][member_id].assigned_partitions)

    def leave_group(self, group_id: str, member_id: str) -> None:
        """Remove a member from a consumer group and rebalance remaining members."""
        with self._lock:
            if group_id in self._groups and member_id in self._groups[group_id]:
                del self._groups[group_id][member_id]
                if self._groups[group_id]:
                    self._rebalance_group(group_id)
                else:
                    del self._groups[group_id]

    def heartbeat(self, group_id: str, member_id: str) -> None:
        """Update member liveness timestamp."""
        with self._lock:
            if group_id in self._groups and member_id in self._groups[group_id]:
                self._groups[group_id][member_id].last_heartbeat = time.time()

    def get_assignment(self, group_id: str, member_id: str) -> List[Tuple[str, int]]:
        """Get the current partition assignment for a member."""
        with self._lock:
            if group_id not in self._groups or member_id not in self._groups[group_id]:
                return []
            return list(self._groups[group_id][member_id].assigned_partitions)

    def commit_offset(
        self,
        group_id: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> ConsumerOffset:
        """Commit processed partition offset for a consumer group."""
        with self._lock:
            now_ns = time.time_ns()
            c_offset = ConsumerOffset(
                group_id=group_id,
                topic=topic,
                partition=partition,
                offset=offset,
                committed_at_ns=now_ns,
            )
            self._committed_offsets[(group_id, topic, partition)] = c_offset
            return c_offset

    def get_committed_offset(
        self,
        group_id: str,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """Retrieve committed offset or None if no offset committed yet."""
        with self._lock:
            key = (group_id, topic, partition)
            if key in self._committed_offsets:
                return self._committed_offsets[key].offset
            return None

    def _rebalance_group(self, group_id: str) -> None:
        """
        Rebalance partition ownership across active group members.
        Enforces Range or Round-Robin strategy.
        """
        members = self._groups.get(group_id, {})
        if not members:
            return

        sorted_member_ids = sorted(list(members.keys()))
        for m in members.values():
            m.assigned_partitions.clear()

        # Gather all unique subscribed topics across all group members
        all_topics = set()
        for m in members.values():
            all_topics.update(m.subscribed_topics)

        if self.strategy == AssignmentStrategy.ROUND_ROBIN:
            self._assign_round_robin(group_id, sorted_member_ids, all_topics)
        else:
            self._assign_range(group_id, sorted_member_ids, all_topics)

    def _assign_round_robin(
        self,
        group_id: str,
        member_ids: List[str],
        topics: Set[str],
    ) -> None:
        """
        Round-robin assign all (topic, partition) pairs to subscribed members.
        """
        # Collect all valid (topic, partition) pairs in deterministic order
        all_partitions: List[Tuple[str, int]] = []
        for t in sorted(list(topics)):
            if not self.broker.has_topic(t):
                continue
            cfg = self.broker.get_topic_config(t)
            for p in range(cfg.num_partitions):
                all_partitions.append((t, p))

        # Distribute round-robin to members subscribed to the topic
        mem_idx = 0
        num_members = len(member_ids)
        for t, p in all_partitions:
            # Find next member subscribed to topic t
            assigned = False
            for _ in range(num_members):
                cand_id = member_ids[mem_idx % num_members]
                mem_idx += 1
                if t in self._groups[group_id][cand_id].subscribed_topics:
                    self._groups[group_id][cand_id].assigned_partitions.append((t, p))
                    assigned = True
                    break

    def _assign_range(
        self,
        group_id: str,
        member_ids: List[str],
        topics: Set[str],
    ) -> None:
        """
        Range assign partitions for each topic independently.
        Contiguous slices of partitions are allocated to each member.
        """
        for t in sorted(list(topics)):
            if not self.broker.has_topic(t):
                continue
            cfg = self.broker.get_topic_config(t)
            num_parts = cfg.num_partitions

            # Members subscribed to this specific topic
            subscribed = [m_id for m_id in member_ids if t in self._groups[group_id][m_id].subscribed_topics]
            if not subscribed:
                continue

            num_subs = len(subscribed)
            parts_per_member = num_parts // num_subs
            remainder = num_parts % num_subs

            current_part = 0
            for i, m_id in enumerate(subscribed):
                # Extra partition for first 'remainder' members
                count = parts_per_member + (1 if i < remainder else 0)
                assigned_range = [(t, p) for p in range(current_part, current_part + count)]
                self._groups[group_id][m_id].assigned_partitions.extend(assigned_range)
                current_part += count
