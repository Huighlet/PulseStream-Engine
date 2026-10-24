"""
Unit & Integration Tests for PulseStream-Engine HTTP & SSE Broker Service.
"""

import pytest
from starlette.testclient import TestClient
from pulsestream.server import app, broker


@pytest.fixture
def client():
    return TestClient(app)


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert "topics_count" in data


def test_dashboard_ui(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "PulseStream-Engine Partitioned Streaming Broker" in response.text


def test_create_and_list_topic(client):
    topic_name = "orders.checkout"
    resp = client.post("/v1/topics", json={"name": topic_name, "num_partitions": 3})
    assert resp.status_code == 201
    assert resp.json()["name"] == topic_name

    # Duplicate topic creation should return 409 Conflict
    dup_resp = client.post("/v1/topics", json={"name": topic_name, "num_partitions": 3})
    assert dup_resp.status_code == 409

    # List topics
    list_resp = client.get("/v1/topics")
    assert list_resp.status_code == 200
    names = [t["name"] for t in list_resp.json()["topics"]]
    assert topic_name in names


def test_publish_and_read_records(client):
    topic_name = "sensors.iot"
    client.post("/v1/topics", json={"name": topic_name, "num_partitions": 2})

    # 1. Publish with explicit partition
    pub_resp = client.post(f"/v1/topics/{topic_name}/publish", json={
        "partition": 0,
        "key": "sensor-alpha",
        "value": {"temperature": 36.6, "humidity": 55},
    })
    assert pub_resp.status_code == 200
    pub_data = pub_resp.json()
    assert pub_data["committed"] is True
    assert pub_data["partition"] == 0
    assert pub_data["offset"] == 0

    # 2. Publish with CRC32 key routing
    pub_resp2 = client.post(f"/v1/topics/{topic_name}/publish", json={
        "key": "sensor-beta",
        "value": {"temperature": 28.1},
    })
    assert pub_resp2.status_code == 200
    assert pub_resp2.json()["committed"] is True

    # 3. Read back from partition 0
    read_resp = client.get(f"/v1/topics/{topic_name}/records?partition=0&offset=0&limit=10")
    assert read_resp.status_code == 200
    read_data = read_resp.json()
    assert read_data["count"] >= 1
    rec0 = read_data["records"][0]
    assert rec0["key"] == "sensor-alpha"
    assert rec0["value"]["temperature"] == 36.6


def test_consumer_group_coordination(client):
    topic_name = "payments.events"
    client.post("/v1/topics", json={"name": topic_name, "num_partitions": 4})

    group_id = "analytics-group"
    join_resp = client.post(f"/v1/groups/{group_id}/join", json={
        "member_id": "worker-1",
        "subscribed_topics": [topic_name],
    })
    assert join_resp.status_code == 200
    assert len(join_resp.json()["assigned_partitions"]) == 4

    # Commit offset
    commit_resp = client.post(f"/v1/groups/{group_id}/commit", json={
        "topic": topic_name,
        "partition": 0,
        "offset": 42,
    })
    assert commit_resp.status_code == 200
    assert commit_resp.json()["committed_offset"] == 42

    # Query offset
    offset_resp = client.get(f"/v1/groups/{group_id}/offsets?topic={topic_name}&partition=0")
    assert offset_resp.status_code == 200
    assert offset_resp.json()["committed_offset"] == 42
