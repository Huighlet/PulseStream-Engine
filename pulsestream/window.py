"""
PulseStream-Engine: Windowed Stream Aggregator.
Implements Tumbling and Sliding/Hopping time-window semantics with event-time watermarking.
"""

from __future__ import annotations

import collections
import dataclasses
from typing import Any, Callable, Dict, List, Optional, Tuple

from pulsestream.types import Record, WindowResult, WindowType


@dataclasses.dataclass(slots=True)
class _WindowState:
    """Internal accumulator state for an active window."""
    window_start_ns: int
    window_end_ns: int
    key: Any
    count: int = 0
    total_val: float = 0.0
    min_val: float = float("inf")
    max_val: float = float("-inf")
    custom_state: Any = None


class StreamWindowAggregator:
    """
    Stateful streaming window engine.
    Discretizes incoming event streams into Tumbling or Sliding windows,
    advances event-time watermarks, and emits finalized aggregates.
    """

    def __init__(
        self,
        window_size_ms: int,
        window_type: WindowType = WindowType.TUMBLING,
        hop_size_ms: Optional[int] = None,
        allowed_lateness_ms: int = 0,
        value_extractor: Optional[Callable[[Record], float]] = None,
        key_extractor: Optional[Callable[[Record], Any]] = None,
        custom_reducer: Optional[Callable[[Any, Record], Any]] = None,
    ) -> None:
        self.window_size_ns = window_size_ms * 1_000_000
        self.window_type = window_type
        if window_type == WindowType.SLIDING:
            if not hop_size_ms or hop_size_ms <= 0:
                raise ValueError("Sliding window requires positive hop_size_ms")
            self.hop_size_ns = hop_size_ms * 1_000_000
        else:
            self.hop_size_ns = self.window_size_ns

        self.allowed_lateness_ns = allowed_lateness_ms * 1_000_000
        self.value_extractor = (
            value_extractor
            if value_extractor is not None
            else (lambda r: float(r.value.decode("utf-8", errors="replace")) if r.value.isdigit() else 1.0)
        )
        self.key_extractor = (
            key_extractor
            if key_extractor is not None
            else (lambda r: r.key.decode("utf-8", errors="replace") if r.key else "_all_")
        )
        self.custom_reducer = custom_reducer

        # (window_start_ns, window_end_ns, key) -> _WindowState
        self._windows: Dict[Tuple[int, int, Any], _WindowState] = {}
        self._max_timestamp_ns: int = 0

    @property
    def current_watermark_ns(self) -> int:
        """Current progress watermark based on observed event time."""
        return max(0, self._max_timestamp_ns - self.allowed_lateness_ns)

    def _calculate_windows_for_timestamp(self, ts_ns: int) -> List[Tuple[int, int]]:
        """Calculate all (start_ns, end_ns) windows covering the timestamp."""
        if self.window_type == WindowType.TUMBLING:
            start = (ts_ns // self.window_size_ns) * self.window_size_ns
            return [(start, start + self.window_size_ns)]

        # Sliding window: find all k where ts_ns - W < k * H <= ts_ns
        results = []
        w = self.window_size_ns
        h = self.hop_size_ns
        # Lowest k where k * h + w > ts_ns => k * h > ts_ns - w
        # Highest k where k * h <= ts_ns
        min_start = ts_ns - w + 1
        first_hop = (min_start // h) * h
        if first_hop < min_start:
            first_hop += h

        cur_start = first_hop
        while cur_start <= ts_ns:
            results.append((cur_start, cur_start + w))
            cur_start += h

        return results

    def add_record(self, record: Record) -> List[WindowResult]:
        """
        Ingest a record into its matching window(s) and emit closed windows.
        Returns newly completed WindowResult records that crossed the watermark.
        """
        ts = record.timestamp_ns
        if ts > self._max_timestamp_ns:
            self._max_timestamp_ns = ts

        watermark = self.current_watermark_ns

        # Drop records older than allowed lateness
        if ts < watermark - self.window_size_ns:
            return []

        # Extract key and numeric value
        try:
            val = self.value_extractor(record)
        except Exception:
            val = 1.0
        k = self.key_extractor(record)

        # Update matching windows
        windows = self._calculate_windows_for_timestamp(ts)
        for w_start, w_end in windows:
            # Skip if window is already behind the watermark
            if w_end <= watermark:
                continue

            win_key = (w_start, w_end, k)
            if win_key not in self._windows:
                self._windows[win_key] = _WindowState(
                    window_start_ns=w_start,
                    window_end_ns=w_end,
                    key=k,
                )

            state = self._windows[win_key]
            state.count += 1
            state.total_val += val
            if val < state.min_val:
                state.min_val = val
            if val > state.max_val:
                state.max_val = val

            if self.custom_reducer:
                state.custom_state = self.custom_reducer(state.custom_state, record)

        # Check and emit closed windows
        return self._emit_closed_windows(watermark)

    def _emit_closed_windows(self, watermark: int) -> List[WindowResult]:
        """Find windows that have ended and emit results."""
        emitted: List[WindowResult] = []
        to_remove: List[Tuple[int, int, Any]] = []

        for win_key, state in self._windows.items():
            if state.window_end_ns <= watermark:
                res = WindowResult(
                    window_start_ns=state.window_start_ns,
                    window_end_ns=state.window_end_ns,
                    window_type=self.window_type,
                    key=state.key,
                    value=state.total_val,
                    record_count=state.count,
                )
                emitted.append(res)
                to_remove.append(win_key)

        for win_key in to_remove:
            del self._windows[win_key]

        return emitted

    def flush(self) -> List[WindowResult]:
        """Force-emit all active pending windows regardless of watermark."""
        emitted: List[WindowResult] = []
        for win_key, state in self._windows.items():
            res = WindowResult(
                window_start_ns=state.window_start_ns,
                window_end_ns=state.window_end_ns,
                window_type=self.window_type,
                key=state.key,
                value=state.total_val,
                record_count=state.count,
            )
            emitted.append(res)
        self._windows.clear()
        return emitted
