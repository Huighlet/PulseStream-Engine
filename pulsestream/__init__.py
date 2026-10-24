"""
PulseStream-Engine: High-Throughput In-Memory Event Streaming Broker & Partition Log.
"""

from pulsestream.broker import PulseStreamBroker
from pulsestream.coordinator import (
    AssignmentStrategy,
    ConsumerGroupCoordinator,
    GroupMember,
)
from pulsestream.partition import PartitionLog
from pulsestream.processor import StreamPipeline
from pulsestream.types import (
    ChecksumMismatchError,
    ConsumerOffset,
    ConsumerRebalanceError,
    OffsetOutOfRangeError,
    PartitionOutOfBoundsError,
    PulseStreamError,
    Record,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicNotFoundError,
    WindowResult,
    WindowType,
)
from pulsestream.window import StreamWindowAggregator

__version__ = "1.0.0"

__all__ = [
    "AssignmentStrategy",
    "ChecksumMismatchError",
    "ConsumerGroupCoordinator",
    "ConsumerOffset",
    "ConsumerRebalanceError",
    "GroupMember",
    "OffsetOutOfRangeError",
    "PartitionLog",
    "PartitionOutOfBoundsError",
    "PulseStreamBroker",
    "PulseStreamError",
    "Record",
    "StreamPipeline",
    "StreamWindowAggregator",
    "TopicAlreadyExistsError",
    "TopicConfig",
    "TopicNotFoundError",
    "WindowResult",
    "WindowType",
]
