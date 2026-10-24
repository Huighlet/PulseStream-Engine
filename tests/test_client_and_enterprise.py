"""
Enterprise Platform Tests for PulseStream-Engine.
Tests Client SDK, Rate Limiting, Prometheus Metrics, Checkpointing Snapshots, and Consumer Lag.
"""

import pytest
from starlette.testclient import TestClient
from pulsestream.server import app, broker
from pulsestream.client import PulseStreamClient


@pytest.fixture
def test_client():
    return TestClient(app)


@pytest.fixture
def sdk_client(test_client):
    return PulseStreamClient(
        base_url="http://testserver",
        api_key="pulse_live_testkey_123",
        http_client=test_client,
    )


def test_prometheus_metrics_endpoint(test_client):
    resp = test_client.get("/metrics")
    assert resp.status_code == 200
    assert "pulsestream_topics_total" in resp.text
    assert "pulsestream_records_appended_total" in resp.text
    assert "pulsestream_bytes_stored_total" in resp.text


def test_rate_limiter_headers(test_client):
    resp = test_client.get("/health")
    assert resp.status_code == 200
    assert "X-RateLimit-Limit" in resp.headers
    assert "X-RateLimit-Remaining" in resp.headers


def test_topic_snapshot_checkpoint(sdk_client):
    topic = "durability.audit"
    sdk_client.create_topic(topic, num_partitions=3)

    # Publish records
    sdk_client.publish(topic, {"sensor": "temp", "val": 22.4}, key="device_001")
    sdk_client.publish(topic, {"sensor": "pressure", "val": 101.3}, key="device_002")

    snapshot = sdk_client.snapshot_topic(topic)
    assert snapshot["topic"] == topic
    assert snapshot["num_partitions"] == 3
    assert snapshot["total_records"] == 2
    assert snapshot["status"] == "DURABLE_CHECKPOINT_COMMITTED"
    assert len(snapshot["partitions"]) == 3


def test_consumer_group_lag_tracking(sdk_client):
    topic = "telemetry.lagtest"
    sdk_client.create_topic(topic, num_partitions=2)

    # Publish to partition 0
    sdk_client.publish(topic, {"msg": 1}, partition=0)
    sdk_client.publish(topic, {"msg": 2}, partition=0)
    sdk_client.publish(topic, {"msg": 3}, partition=0)

    # Initially before commit, lag should reflect 3 records on partition 0
    lag_report = sdk_client.get_topic_lag(topic, group_id="analytics_workers")
    assert lag_report["topic"] == topic
    assert lag_report["group_id"] == "analytics_workers"
    assert lag_report["total_lag"] >= 3

    # Commit offset 2 on partition 0
    sdk_client.commit_offset(group_id="analytics_workers", topic=topic, partition=0, offset=2)

    # Lag should now be 0 (offsets 0, 1, 2 processed, high watermark is 2)
    updated_lag = sdk_client.get_topic_lag(topic, group_id="analytics_workers")
    assert updated_lag["total_lag"] == 0
    assert updated_lag["status"] == "IN_SYNC"


def test_client_sdk_consume(sdk_client):
    topic = "events.ingest"
    sdk_client.create_topic(topic, num_partitions=1)
    sdk_client.publish(topic, {"event": "login", "user": "alice"}, partition=0)
    sdk_client.publish(topic, {"event": "click", "user": "alice"}, partition=0)

    consumed = sdk_client.consume(topic, partition=0, offset=0, max_records=10)
    assert consumed["count"] == 2
    assert consumed["records"][0]["value"]["event"] == "login"
    assert consumed["records"][1]["value"]["event"] == "click"
