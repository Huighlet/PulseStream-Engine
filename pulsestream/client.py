"""
PulseStream Client SDK: Idiomatic high-performance client for the PulseStream-Engine Streaming Broker.
Supports high-throughput event publishing, consumer group coordination, partition consumption, and consumer lag tracking.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Optional, Union

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore


class PulseStreamClient:
    """
    Client for interacting with a live PulseStream-Engine streaming broker.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:9092",
        api_key: Optional[str] = None,
        timeout_seconds: float = 10.0,
        http_client: Optional[Any] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        if http_client is not None:
            self._client = http_client
        elif httpx is not None:
            self._client = httpx.Client(
                base_url=self.base_url,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        else:
            self._client = None

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> PulseStreamClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def create_topic(self, name: str, num_partitions: int = 4) -> Dict[str, Any]:
        """Provision a partitioned streaming topic."""
        payload = {"name": name, "num_partitions": num_partitions}
        resp = self._client.post("/v1/topics", json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_topics(self) -> List[str]:
        """List all active topics in the broker."""
        resp = self._client.get("/v1/topics")
        resp.raise_for_status()
        return resp.json()["topics"]

    def publish(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Publish record with deterministic CRC32 partition routing or explicit partition index."""
        payload = {
            "value": value,
            "key": key,
            "partition": partition,
            "headers": headers or {},
        }
        resp = self._client.post(f"/v1/topics/{topic}/publish", json=payload)
        resp.raise_for_status()
        return resp.json()

    def consume(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_records: int = 50,
    ) -> Dict[str, Any]:
        """Fetch sequential batch of records from a partition commit log."""
        params = {"partition": partition, "offset": offset, "max_records": max_records}
        resp = self._client.get(f"/v1/topics/{topic}/consume", params=params)
        resp.raise_for_status()
        return resp.json()

    def join_group(self, group_id: str, member_id: str, subscribed_topics: List[str]) -> Dict[str, Any]:
        """Register consumer with coordinator and receive assigned topic-partitions."""
        payload = {"member_id": member_id, "subscribed_topics": subscribed_topics}
        resp = self._client.post(f"/v1/groups/{group_id}/join", json=payload)
        resp.raise_for_status()
        return resp.json()

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int) -> Dict[str, Any]:
        """Commit processed watermark offset for a consumer group."""
        payload = {"topic": topic, "partition": partition, "offset": offset}
        resp = self._client.post(f"/v1/groups/{group_id}/commit", json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_topic_lag(self, topic: str, group_id: str) -> Dict[str, Any]:
        """Calculate consumer group lag across topic partitions."""
        params = {"group_id": group_id}
        resp = self._client.get(f"/v1/topics/{topic}/lag", params=params)
        resp.raise_for_status()
        return resp.json()

    def snapshot_topic(self, topic: str) -> Dict[str, Any]:
        """Create point-in-time topic checkpoint and partition metadata export."""
        resp = self._client.post(f"/v1/topics/{topic}/snapshot")
        resp.raise_for_status()
        return resp.json()

    def get_metrics(self) -> str:
        """Fetch Prometheus metrics."""
        resp = self._client.get("/metrics")
        resp.raise_for_status()
        return resp.text
