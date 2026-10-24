"""
PulseStream-Engine: Production In-Memory Streaming Broker Server.
Provides high-throughput HTTP REST ingest, real-time Server-Sent Events (SSE) subscriptions,
consumer group offset management, and an interactive streaming developer UI.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from pulsestream.broker import PulseStreamBroker
from pulsestream.coordinator import AssignmentStrategy, ConsumerGroupCoordinator
from pulsestream.types import (
    PartitionOutOfBoundsError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
)

# ---------------------------------------------------------
# Request / Response Schemas
# ---------------------------------------------------------

class CreateTopicRequest(BaseModel):
    name: str = Field(..., description="Unique topic name identifier")
    num_partitions: int = Field(4, ge=1, le=128, description="Number of partitions")


class PublishRecordRequest(BaseModel):
    value: Any = Field(..., description="Record payload (JSON object, string, or number)")
    key: Optional[str] = Field(None, description="Partition routing key for CRC32 affinity")
    partition: Optional[int] = Field(None, description="Explicit target partition index")
    headers: Optional[Dict[str, str]] = Field(None, description="Metadata key-value pairs")


class CommitOffsetRequest(BaseModel):
    topic: str
    partition: int
    offset: int
    metadata: Optional[str] = None


class JoinGroupRequest(BaseModel):
    member_id: str
    subscribed_topics: List[str]


# ---------------------------------------------------------
# Broker & Coordinator Instance
# ---------------------------------------------------------

app = FastAPI(
    title="PulseStream-Engine Streaming Broker",
    description="High-throughput partitioned event broker with deterministic CRC32 routing, sequential commit logs, and real-time SSE streaming.",
    version="1.0.0",
)

broker = PulseStreamBroker()
coordinator = ConsumerGroupCoordinator(broker, strategy=AssignmentStrategy.ROUND_ROBIN)

# Pre-seed a default demo topic for out-of-the-box streaming
if not broker.has_topic("telemetry.fleet"):
    broker.create_topic("telemetry.fleet", num_partitions=4)


# ---------------------------------------------------------
# Developer UI Dashboard
# ---------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def streaming_dashboard():
    """Live In-Browser Event Streaming Dashboard."""
    topics = broker.list_topics()
    topic_cards = []
    for t in topics:
        cfg = broker.get_topic_config(t)
        total_recs = sum(broker._partitions[t][p].record_count for p in range(cfg.num_partitions))
        total_bytes = sum(broker._partitions[t][p].total_bytes for p in range(cfg.num_partitions))
        topic_cards.append(f"""
        <div class="card">
            <div class="card-header">
                <strong>Topic:</strong> <span class="badge">{t}</span>
            </div>
            <div class="card-body">
                <div>Partitions: <strong>{cfg.num_partitions}</strong></div>
                <div>Records: <strong>{total_recs:,}</strong></div>
                <div>Volume: <strong>{total_bytes:,} B</strong></div>
            </div>
        </div>
        """)

    cards_html = "\n".join(topic_cards)

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>PulseStream-Engine Streaming Dashboard</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background: #0b0f19; color: #f3f4f6; margin: 0; padding: 32px; }}
            .container {{ max-width: 1000px; margin: 0 auto; background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 28px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }}
            h1 {{ color: #38bdf8; margin-top: 0; font-size: 24px; font-weight: 600; }}
            p {{ color: #9ca3af; font-size: 14px; line-height: 1.5; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; margin: 20px 0; }}
            .card {{ background: #1f2937; border: 1px solid #374151; border-radius: 6px; padding: 16px; }}
            .card-header {{ margin-bottom: 8px; font-size: 13px; }}
            .badge {{ background: #0284c7; color: #fff; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
            .card-body {{ font-size: 13px; color: #d1d5db; line-height: 1.8; }}
            .btn {{ display: inline-block; padding: 9px 16px; border-radius: 6px; background: #0284c7; color: #ffffff; text-decoration: none; font-size: 13px; font-weight: 600; margin-right: 8px; }}
            .btn:hover {{ background: #0369a1; }}
            pre {{ background: #030712; padding: 14px; border-radius: 6px; color: #38bdf8; font-size: 12px; overflow-x: auto; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>PulseStream-Engine Partitioned Streaming Broker</h1>
            <p>In-memory append-only streaming broker featuring monotonic offsets, CRC32 routing, and HTTP/SSE real-time streaming.</p>

            <h3>Active Topics</h3>
            <div class="grid">
                {cards_html}
            </div>

            <p>
                <a href="/docs" class="btn">Explore OpenAPI / Swagger &rarr;</a>
                <a href="/v1/topics" class="btn" style="background: #374151;">JSON Topic Topology</a>
            </p>

            <h3>Real-Time Live SSE Stream Consumption</h3>
            <pre># Connect using standard curl to stream live events pushed by the broker
curl -N http://localhost:9092/v1/topics/telemetry.fleet/stream?partition=0</pre>

            <h3>Publish Event via REST</h3>
            <pre>curl -X POST http://localhost:9092/v1/topics/telemetry.fleet/publish \\
  -H "Content-Type: application/json" \\
  -d '{{"key": "vehicle_042", "value": {{"speed_kmh": 84.5, "fuel": 72}}}}'</pre>
        </div>
    </body>
    </html>
    """


# ---------------------------------------------------------
# Health & Topic Management Endpoints
# ---------------------------------------------------------

@app.get("/health")
def health():
    """Broker liveness and partition summary."""
    topics = broker.list_topics()
    total_recs = 0
    total_bytes = 0
    for t in topics:
        cfg = broker.get_topic_config(t)
        total_recs += sum(broker._partitions[t][p].record_count for p in range(cfg.num_partitions))
        total_bytes += sum(broker._partitions[t][p].total_bytes for p in range(cfg.num_partitions))

    return {
        "status": "HEALTHY",
        "timestamp_ns": time.time_ns(),
        "topics_count": len(topics),
        "total_records": total_recs,
        "total_bytes": total_bytes,
    }


@app.get("/v1/topics")
def list_topics():
    """List all registered topics and their partition configurations."""
    results = []
    for t in broker.list_topics():
        cfg = broker.get_topic_config(t)
        results.append({
            "name": t,
            "num_partitions": cfg.num_partitions,
            "retention_bytes": cfg.retention_bytes,
            "retention_ms": cfg.retention_ms,
        })
    return {"topics": results}


@app.post("/v1/topics", status_code=status.HTTP_201_CREATED)
def create_topic(req: CreateTopicRequest):
    """Create a new partitioned topic."""
    try:
        cfg = broker.create_topic(req.name, num_partitions=req.num_partitions)
        return {
            "created": True,
            "name": cfg.name,
            "num_partitions": cfg.num_partitions,
        }
    except TopicAlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.get("/v1/topics/{topic}")
def get_topic_details(topic: str):
    """Inspect topic partitions, low/high watermarks, and record counts."""
    if not broker.has_topic(topic):
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' does not exist.")

    cfg = broker.get_topic_config(topic)
    partitions_info = []
    for p in range(cfg.num_partitions):
        part = broker._partitions[topic][p]
        earliest, latest = broker.get_watermarks(topic, p)
        partitions_info.append({
            "partition": p,
            "earliest_offset": earliest,
            "latest_offset": latest,
            "record_count": part.record_count,
            "total_bytes": part.total_bytes,
        })

    return {
        "topic": topic,
        "num_partitions": cfg.num_partitions,
        "partitions": partitions_info,
    }


# ---------------------------------------------------------
# Ingest & Read Records
# ---------------------------------------------------------

@app.post("/v1/topics/{topic}/publish")
def publish_record(topic: str, req: PublishRecordRequest):
    """
    Publish an event record to a topic.
    If 'key' is provided, routes deterministically via CRC32(key) % Partitions.
    If 'partition' is explicitly specified, writes directly to that partition.
    """
    if not broker.has_topic(topic):
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' does not exist.")

    val_bytes = req.value.encode("utf-8") if isinstance(req.value, str) else json.dumps(req.value).encode("utf-8")
    key_bytes = req.key.encode("utf-8") if req.key else None

    try:
        record = broker.produce(
            topic=topic,
            value=val_bytes,
            key=key_bytes,
            partition=req.partition,
            headers=req.headers,
        )
        return {
            "committed": True,
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "crc32": record.crc32,
            "timestamp_ns": record.timestamp_ns,
        }
    except PartitionOutOfBoundsError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/v1/topics/{topic}/records")
def read_records(
    topic: str,
    partition: int = Query(0, ge=0),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Read a batch of sequential records from a topic partition starting at offset."""
    if not broker.has_topic(topic):
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' does not exist.")

    try:
        records = broker.consume(
            topic=topic,
            partition=partition,
            offset=offset,
            max_records=limit,
            verify_crc=True,
        )
        output = []
        for r in records:
            # Decode string/json if utf-8
            try:
                decoded_val = json.loads(r.value.decode("utf-8"))
            except Exception:
                decoded_val = r.value.decode("utf-8", errors="replace")

            decoded_key = r.key.decode("utf-8", errors="replace") if r.key else None
            output.append({
                "topic": r.topic,
                "partition": r.partition,
                "offset": r.offset,
                "timestamp_ns": r.timestamp_ns,
                "key": decoded_key,
                "value": decoded_val,
                "crc32": r.crc32,
            })
        return {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "count": len(output),
            "records": output,
        }
    except PartitionOutOfBoundsError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------
# Real-Time Server-Sent Events (SSE) Streaming
# ---------------------------------------------------------

@app.get("/v1/topics/{topic}/stream")
async def stream_topic_sse(
    topic: str,
    partition: int = Query(0, ge=0),
    start_offset: Optional[int] = Query(None, ge=0),
):
    """
    Real-time Server-Sent Events (SSE) stream.
    Clients receive new records pushed dynamically as they are appended to the partition log.
    """
    if not broker.has_topic(topic):
        raise HTTPException(status_code=404, detail=f"Topic '{topic}' does not exist.")

    cfg = broker.get_topic_config(topic)
    if partition < 0 or partition >= cfg.num_partitions:
        raise HTTPException(status_code=400, detail=f"Partition {partition} out of range.")

    async def event_generator():
        current_offset = start_offset if start_offset is not None else broker.get_watermarks(topic, partition)[1]
        while True:
            records = broker.consume(
                topic=topic,
                partition=partition,
                offset=current_offset,
                max_records=50,
                verify_crc=True,
            )
            if records:
                for r in records:
                    try:
                        val = json.loads(r.value.decode("utf-8"))
                    except Exception:
                        val = r.value.decode("utf-8", errors="replace")

                    data = {
                        "topic": r.topic,
                        "partition": r.partition,
                        "offset": r.offset,
                        "key": r.key.decode("utf-8", errors="replace") if r.key else None,
                        "value": val,
                        "timestamp_ns": r.timestamp_ns,
                    }
                    yield f"data: {json.dumps(data)}\n\n"
                    current_offset = r.offset + 1
            else:
                # Non-blocking pause waiting for new commits
                await asyncio.sleep(0.05)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------
# Consumer Groups & Coordinator Endpoints
# ---------------------------------------------------------

@app.post("/v1/groups/{group_id}/join")
def join_consumer_group(group_id: str, req: JoinGroupRequest):
    """Register consumer instance with consumer group coordinator and receive partition assignments."""
    assigned = coordinator.join_group(
        group_id=group_id,
        member_id=req.member_id,
        subscribed_topics=req.subscribed_topics,
    )
    return {
        "group_id": group_id,
        "member_id": req.member_id,
        "assigned_partitions": [{"topic": t, "partition": p} for t, p in assigned],
    }


@app.post("/v1/groups/{group_id}/commit")
def commit_offset(group_id: str, req: CommitOffsetRequest):
    """Commit consumer processed offset for a topic partition."""
    offset = coordinator.commit_offset(
        group_id=group_id,
        topic=req.topic,
        partition=req.partition,
        offset=req.offset,
    )
    return {
        "committed": True,
        "group_id": group_id,
        "topic": offset.topic,
        "partition": offset.partition,
        "committed_offset": offset.offset,
    }


@app.get("/v1/groups/{group_id}/offsets")
def get_committed_offsets(group_id: str, topic: str, partition: int):
    """Retrieve last committed offset for a group and partition."""
    offset = coordinator.get_committed_offset(group_id, topic, partition)
    return {
        "group_id": group_id,
        "topic": topic,
        "partition": partition,
        "committed_offset": offset,
    }


def serve(host: str = "0.0.0.0", port: int = 9092):
    """Launch standalone broker server."""
    import uvicorn
    uvicorn.run("pulsestream.server:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    serve()
