"""
PulseStream-Engine: Core Streaming Primitives, Event Records, and Configuration Types.
"""

from __future__ import annotations

import dataclasses
import enum
import time
import zlib
from typing import Any, Callable, Dict, Optional


class WindowType(enum.StrEnum):
    """Streaming window discretization strategy."""
    TUMBLING = "TUMBLING"
    SLIDING = "SLIDING"


@dataclasses.dataclass(slots=True, frozen=True)
class Record:
    """
    Immutable event record stored in an append-only partition log.
    Includes 64-bit nanosecond timestamp, binary payload, headers, and CRC32 verification.
    """
    topic: str
    partition: int
    offset: int
    timestamp_ns: int
    key: Optional[bytes]
    value: bytes
    headers: Dict[str, str] = dataclasses.field(default_factory=dict)
    crc32: int = 0

    @classmethod
    def create(
        cls,
        topic: str,
        partition: int,
        offset: int,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timestamp_ns: Optional[int] = None,
    ) -> Record:
        """Create a new Record and calculate its CRC32 checksum."""
        ts = timestamp_ns if timestamp_ns is not None else time.time_ns()
        hdr = headers if headers is not None else {}
        
        # Calculate checksum over key + value + timestamp
        hasher = zlib.crc32(value)
        if key:
            hasher = zlib.crc32(key, hasher)
        hasher = zlib.crc32(str(ts).encode("ascii"), hasher)
        checksum = hasher & 0xFFFFFFFF

        return cls(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp_ns=ts,
            key=key,
            value=value,
            headers=hdr,
            crc32=checksum,
        )

    def verify_checksum(self) -> bool:
        """Verify record CRC32 integrity."""
        hasher = zlib.crc32(self.value)
        if self.key:
            hasher = zlib.crc32(self.key, hasher)
        hasher = zlib.crc32(str(self.timestamp_ns).encode("ascii"), hasher)
        return (hasher & 0xFFFFFFFF) == self.crc32


@dataclasses.dataclass(slots=True, frozen=True)
class TopicConfig:
    """Configuration specification for a partitioned streaming topic."""
    name: str
    num_partitions: int = 4
    retention_bytes: Optional[int] = None
    retention_ms: Optional[int] = None

    def __post_init__(self) -> None:
        if self.num_partitions <= 0:
            raise ValueError(f"Partition count must be positive, got {self.num_partitions}")


@dataclasses.dataclass(slots=True)
class ConsumerOffset:
    """Tracked committed offset state for a consumer group."""
    group_id: str
    topic: str
    partition: int
    offset: int
    committed_at_ns: int


@dataclasses.dataclass(slots=True, frozen=True)
class WindowResult:
    """Result of a windowed stream aggregation."""
    window_start_ns: int
    window_end_ns: int
    window_type: WindowType
    key: Any
    value: Any
    record_count: int


# -------------------------------------------------------------------------
# Exceptions
# -------------------------------------------------------------------------

class PulseStreamError(Exception):
    """Base exception for all PulseStream-Engine errors."""


class TopicNotFoundError(PulseStreamError):
    """Raised when querying a non-existent topic."""


class TopicAlreadyExistsError(PulseStreamError):
    """Raised when attempting to create an already existing topic."""


class PartitionOutOfBoundsError(PulseStreamError):
    """Raised when accessing a partition index outside topic bounds."""


class OffsetOutOfRangeError(PulseStreamError):
    """Raised when reading or committing an invalid offset."""


class ChecksumMismatchError(PulseStreamError):
    """Raised when record CRC32 verification fails upon read."""


class ConsumerRebalanceError(PulseStreamError):
    """Raised during active group rebalance operations."""
