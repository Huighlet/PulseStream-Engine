"""
PulseStream-Engine: Declarative Stream Processing Pipeline Topology.
Provides fluent map, filter, window, and sink operators over partitioned streams.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional

from pulsestream.broker import PulseStreamBroker
from pulsestream.types import Record, WindowResult, WindowType
from pulsestream.window import StreamWindowAggregator


class StreamPipeline:
    """
    Fluent streaming pipeline for continuous transformation, filtering, and aggregation.
    """

    def __init__(self, broker: PulseStreamBroker, source_topic: str) -> None:
        self.broker = broker
        self.source_topic = source_topic
        self._operations: List[Callable[[Any], Any]] = []
        self._sink_topic: Optional[str] = None
        self._aggregator: Optional[StreamWindowAggregator] = None

    def filter(self, predicate: Callable[[Record], bool]) -> StreamPipeline:
        """Filter incoming records based on a boolean predicate."""
        def op(item: Any) -> Optional[Any]:
            if isinstance(item, Record):
                return item if predicate(item) else None
            return item

        self._operations.append(op)
        return self

    def map(self, transform: Callable[[Record], Any]) -> StreamPipeline:
        """Transform records into mapped outputs."""
        def op(item: Any) -> Optional[Any]:
            if item is None:
                return None
            if isinstance(item, Record):
                return transform(item)
            return item

        self._operations.append(op)
        return self

    def window(
        self,
        window_size_ms: int,
        window_type: WindowType = WindowType.TUMBLING,
        hop_size_ms: Optional[int] = None,
        value_extractor: Optional[Callable[[Record], float]] = None,
        key_extractor: Optional[Callable[[Record], Any]] = None,
    ) -> StreamPipeline:
        """Attach a stateful window aggregator to the stream."""
        self._aggregator = StreamWindowAggregator(
            window_size_ms=window_size_ms,
            window_type=window_type,
            hop_size_ms=hop_size_ms,
            value_extractor=value_extractor,
            key_extractor=key_extractor,
        )
        return self

    def to_topic(self, sink_topic: str) -> StreamPipeline:
        """Route processed records or window results to a destination topic."""
        self._sink_topic = sink_topic
        if not self.broker.has_topic(sink_topic):
            self.broker.create_topic(sink_topic, num_partitions=4)
        return self

    def process_record(self, record: Record) -> List[Any]:
        """
        Execute the pipeline on a single incoming record.
        Returns any emitted results (including sink commits or window outputs).
        """
        current: Any = record
        for op in self._operations:
            current = op(current)
            if current is None:
                return []

        # If window aggregator configured, route into aggregator
        if self._aggregator:
            if isinstance(current, Record):
                emitted = self._aggregator.add_record(current)
                if self._sink_topic:
                    for res in emitted:
                        val = str(res.value).encode("utf-8")
                        k = str(res.key).encode("utf-8") if res.key else None
                        self.broker.produce(self._sink_topic, value=val, key=k)
                return emitted
            return []

        # Route to sink if specified
        if self._sink_topic:
            val_bytes = current if isinstance(current, bytes) else str(current).encode("utf-8")
            sink_rec = self.broker.produce(self._sink_topic, value=val_bytes, key=record.key)
            return [sink_rec]

        return [current]

    def flush_windows(self) -> List[WindowResult]:
        """Flush any pending window results to sink."""
        if not self._aggregator:
            return []
        emitted = self._aggregator.flush()
        if self._sink_topic:
            for res in emitted:
                val = str(res.value).encode("utf-8")
                k = str(res.key).encode("utf-8") if res.key else None
                self.broker.produce(self._sink_topic, value=val, key=k)
        return emitted
