"""
PulseStream-Engine: High-Throughput In-Memory Streaming Broker.
Manages topic topologies, deterministic partition routing, and sequential commit logs.
"""

from __future__ import annotations

import itertools
import threading
import zlib
from typing import Dict, List, Optional, Tuple

from pulsestream.partition import PartitionLog
from pulsestream.types import (
    PartitionOutOfBoundsError,
    Record,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicNotFoundError,
)


class PulseStreamBroker:
    """
    Central partitioned streaming broker.
    Provides deterministic key partitioning, round-robin fallback, and multi-partition coordination.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._topics: Dict[str, TopicConfig] = {}
        self._partitions: Dict[str, List[PartitionLog]] = {}
        # Round-robin counter per topic for keyless routing
        self._rr_counters: Dict[str, itertools.count] = {}

    def create_topic(
        self,
        name: str,
        num_partitions: int = 4,
        retention_bytes: Optional[int] = None,
        retention_ms: Optional[int] = None,
    ) -> TopicConfig:
        """Create a new partitioned topic."""
        with self._lock:
            if name in self._topics:
                raise TopicAlreadyExistsError(f"Topic '{name}' already exists.")

            config = TopicConfig(
                name=name,
                num_partitions=num_partitions,
                retention_bytes=retention_bytes,
                retention_ms=retention_ms,
            )
            self._topics[name] = config
            self._partitions[name] = [
                PartitionLog(topic=name, partition_id=p) for p in range(num_partitions)
            ]
            self._rr_counters[name] = itertools.count()
            return config

    def delete_topic(self, name: str) -> None:
        """Delete an existing topic and discard all partition data."""
        with self._lock:
            if name not in self._topics:
                raise TopicNotFoundError(f"Topic '{name}' does not exist.")
            del self._topics[name]
            del self._partitions[name]
            del self._rr_counters[name]

    def has_topic(self, name: str) -> bool:
        """Check if topic exists."""
        with self._lock:
            return name in self._topics

    def list_topics(self) -> List[str]:
        """List all active topic names."""
        with self._lock:
            return sorted(list(self._topics.keys()))

    def get_topic_config(self, name: str) -> TopicConfig:
        """Retrieve topic configuration."""
        with self._lock:
            if name not in self._topics:
                raise TopicNotFoundError(f"Topic '{name}' does not exist.")
            return self._topics[name]

    def route_partition(self, topic: str, key: Optional[bytes] = None) -> int:
        """
        Determine destination partition index for an incoming record.
        Uses CRC32(key) % N if key provided, otherwise round-robin.
        """
        config = self.get_topic_config(topic)
        n = config.num_partitions
        if key is not None and len(key) > 0:
            return (zlib.crc32(key) & 0xFFFFFFFF) % n
        return next(self._rr_counters[topic]) % n

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        headers: Optional[dict] = None,
        timestamp_ns: Optional[int] = None,
    ) -> Record:
        """
        Publish a single event record to a topic partition.
        If partition is omitted, routes via key CRC32 or round-robin.
        """
        if not self.has_topic(topic):
            raise TopicNotFoundError(f"Topic '{topic}' does not exist.")

        config = self._topics[topic]
        if partition is not None:
            if partition < 0 or partition >= config.num_partitions:
                raise PartitionOutOfBoundsError(
                    f"Partition {partition} out of range for topic '{topic}' (0..{config.num_partitions - 1})"
                )
            target_p = partition
        else:
            target_p = self.route_partition(topic, key=key)

        part_log = self._partitions[topic][target_p]
        return part_log.append(value=value, key=key, headers=headers, timestamp_ns=timestamp_ns)

    def produce_batch(
        self,
        topic: str,
        records: List[Tuple[bytes, Optional[bytes]]],
    ) -> List[Record]:
        """
        Produce a batch of records, routing each to its target partition.
        Optimized by grouping writes per partition.
        """
        if not self.has_topic(topic):
            raise TopicNotFoundError(f"Topic '{topic}' does not exist.")

        # Group records by partition
        by_partition: Dict[int, List[Tuple[bytes, Optional[bytes], Optional[dict]]]] = {}
        for val, key in records:
            p = self.route_partition(topic, key=key)
            if p not in by_partition:
                by_partition[p] = []
            by_partition[p].append((val, key, None))

        results: List[Record] = []
        for p, batch in by_partition.items():
            part_log = self._partitions[topic][p]
            results.extend(part_log.append_batch(batch))
        return results

    def consume(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_records: int = 100,
        verify_crc: bool = True,
    ) -> List[Record]:
        """
        Read up to max_records from topic partition at offset.
        """
        if not self.has_topic(topic):
            raise TopicNotFoundError(f"Topic '{topic}' does not exist.")

        config = self._topics[topic]
        if partition < 0 or partition >= config.num_partitions:
            raise PartitionOutOfBoundsError(
                f"Partition {partition} out of bounds for topic '{topic}'"
            )

        return self._partitions[topic][partition].read(
            start_offset=offset, max_records=max_records, verify_crc=verify_crc
        )

    def get_watermarks(self, topic: str, partition: int) -> Tuple[int, int]:
        """Return (earliest_offset, latest_offset) for given partition."""
        if not self.has_topic(topic):
            raise TopicNotFoundError(f"Topic '{topic}' does not exist.")
        config = self._topics[topic]
        if partition < 0 or partition >= config.num_partitions:
            raise PartitionOutOfBoundsError(f"Partition {partition} out of bounds")
        part = self._partitions[topic][partition]
        return part.earliest_offset, part.latest_offset
