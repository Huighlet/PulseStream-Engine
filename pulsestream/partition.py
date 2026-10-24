"""
PulseStream-Engine: Append-Only Partition Commit Log.
Provides microsecond append throughput, sequential offset addressing, and CRC32 integrity checks.
"""

from __future__ import annotations

import io
import struct
import threading
from typing import List, Optional

from pulsestream.types import (
    ChecksumMismatchError,
    OffsetOutOfRangeError,
    Record,
)

# Binary frame format:
# offset (uint64), timestamp_ns (uint64), key_len (uint32), val_len (uint32), crc32 (uint32)
# Total fixed header size: 8 + 8 + 4 + 4 + 4 = 28 bytes
_HEADER_FORMAT = ">QQIII"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)


class PartitionLog:
    """
    Append-only commit log for a single topic partition.
    Maintains sequential monotonic offsets, in-memory index table, and CRC32 verification.
    """

    def __init__(self, topic: str, partition_id: int) -> None:
        self.topic = topic
        self.partition_id = partition_id
        self._lock = threading.RLock()
        
        # In-memory record log and offset index
        self._records: List[Record] = []
        self._base_offset: int = 0
        self._total_bytes: int = 0

    @property
    def earliest_offset(self) -> int:
        """First valid offset available in the partition."""
        with self._lock:
            return self._base_offset

    @property
    def latest_offset(self) -> int:
        """High-water mark offset (next offset to be assigned)."""
        with self._lock:
            return self._base_offset + len(self._records)

    @property
    def record_count(self) -> int:
        """Total active records currently in partition."""
        with self._lock:
            return len(self._records)

    @property
    def total_bytes(self) -> int:
        """Total cumulative payload bytes stored."""
        with self._lock:
            return self._total_bytes

    def append(
        self,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None,
        timestamp_ns: Optional[int] = None,
    ) -> Record:
        """
        Append a single record atomically to the partition log.
        Returns the committed Record with its assigned offset and CRC32.
        """
        with self._lock:
            assigned_offset = self.latest_offset
            record = Record.create(
                topic=self.topic,
                partition=self.partition_id,
                offset=assigned_offset,
                value=value,
                key=key,
                headers=headers,
                timestamp_ns=timestamp_ns,
            )
            self._records.append(record)
            self._total_bytes += len(value) + (len(key) if key else 0)
            return record

    def append_batch(
        self,
        records_data: List[tuple[bytes, Optional[bytes], Optional[dict]]],
    ) -> List[Record]:
        """
        Append a batch of records atomically with a single lock acquisition.
        """
        committed: List[Record] = []
        with self._lock:
            for val, k, hdr in records_data:
                assigned_offset = self.latest_offset
                record = Record.create(
                    topic=self.topic,
                    partition=self.partition_id,
                    offset=assigned_offset,
                    value=val,
                    key=k,
                    headers=hdr,
                )
                self._records.append(record)
                self._total_bytes += len(val) + (len(k) if k else 0)
                committed.append(record)
        return committed

    def read(
        self,
        start_offset: int,
        max_records: int = 100,
        verify_crc: bool = True,
    ) -> List[Record]:
        """
        Read up to max_records starting from start_offset.

        Raises:
            OffsetOutOfRangeError: If start_offset is outside [earliest_offset, latest_offset].
            ChecksumMismatchError: If any record fails CRC32 verification.
        """
        with self._lock:
            if start_offset < self._base_offset or start_offset > self.latest_offset:
                raise OffsetOutOfRangeError(
                    f"Offset {start_offset} out of bounds for {self.topic}[{self.partition_id}]. "
                    f"Valid range: [{self._base_offset}, {self.latest_offset}]"
                )

            if start_offset == self.latest_offset:
                return []

            rel_start = start_offset - self._base_offset
            rel_end = min(rel_start + max_records, len(self._records))
            slice_records = self._records[rel_start:rel_end]

        if verify_crc:
            for rec in slice_records:
                if not rec.verify_checksum():
                    raise ChecksumMismatchError(
                        f"CRC32 mismatch on record at {self.topic}[{self.partition_id}] offset {rec.offset}"
                    )

        return list(slice_records)

    def truncate_before(self, offset: int) -> int:
        """
        Truncate/expire records with offset strictly less than specified offset.
        Returns the number of deleted records.
        """
        with self._lock:
            if offset <= self._base_offset:
                return 0
            if offset > self.latest_offset:
                offset = self.latest_offset

            del_count = offset - self._base_offset
            self._records = self._records[del_count:]
            self._base_offset = offset
            return del_count

    def serialize_record(self, record: Record) -> bytes:
        """Serialize a record to standard binary format."""
        key_bytes = record.key if record.key is not None else b""
        key_len = len(key_bytes)
        val_len = len(record.value)

        header = struct.pack(
            _HEADER_FORMAT,
            record.offset,
            record.timestamp_ns,
            key_len,
            val_len,
            record.crc32,
        )
        return header + key_bytes + record.value

    def deserialize_record(self, raw_bytes: bytes) -> Record:
        """Deserialize a binary frame into a Record instance."""
        if len(raw_bytes) < _HEADER_SIZE:
            raise ValueError(f"Raw frame too short for header ({len(raw_bytes)} < {_HEADER_SIZE})")

        offset, ts, key_len, val_len, crc32 = struct.unpack(
            _HEADER_FORMAT, raw_bytes[:_HEADER_SIZE]
        )
        expected_len = _HEADER_SIZE + key_len + val_len
        if len(raw_bytes) < expected_len:
            raise ValueError(f"Payload truncated: expected {expected_len} bytes, got {len(raw_bytes)}")

        pos = _HEADER_SIZE
        key = raw_bytes[pos : pos + key_len] if key_len > 0 else None
        pos += key_len
        val = raw_bytes[pos : pos + val_len]

        rec = Record(
            topic=self.topic,
            partition=self.partition_id,
            offset=offset,
            timestamp_ns=ts,
            key=key,
            value=val,
            headers={},
            crc32=crc32,
        )
        if not rec.verify_checksum():
            raise ChecksumMismatchError(f"Checksum mismatch during deserialization at offset {offset}")
        return rec
