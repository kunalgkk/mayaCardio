"""
Cloud Ingestion and Processing Service for MayaCardio.
Provides RESTful HTTP API for:
- Live streaming ingestion from device.
- Idempotent cloud storage.
- Real-time online window result computation (mode="online") within < 1.0s.
- Session export and status inspection.
- Free-tier compliant deployment (single lightweight ASGI process, SQLite WAL).
"""

import json
import os
import time
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

from mayacardio.storage import StorageEngine
from mayacardio.processor import process_window

app = FastAPI(
    title="MayaCardio Cloud Ingestion Service",
    description="Real-time ingestion, durable storage, and online processing service.",
    version="1.0.0",
)

# Cloud durable storage instance
CLOUD_DB_PATH = os.getenv("CLOUD_DB_PATH", "mayacardio_cloud.db")
storage = StorageEngine(CLOUD_DB_PATH)


class SampleItem(BaseModel):
    session_id: str
    sensor: int
    sample_index: int
    values: List[int]


class LossItem(BaseModel):
    session_id: str
    sensor: int
    from_index: int
    to_index: Optional[int] = None
    reason: str


class IngestionPayload(BaseModel):
    samples: List[SampleItem]


class LossPayload(BaseModel):
    losses: List[LossItem]


@app.get("/health")
def health_check():
    """Health probe used by device synchronizer for automatic mode switching."""
    return {"status": "ok", "mode": "cloud", "timestamp": time.time()}


@app.get("/api/status")
def get_cloud_status():
    """Returns overview of persisted samples and backlog status."""
    total = storage.get_total_saved_samples()
    return {
        "status": "online",
        "total_samples": total,
        "database": CLOUD_DB_PATH,
    }


def _process_eligible_windows_in_cloud(session_id: str, max_sample_idx: int) -> int:
    """
    Checks if newly ingested samples allow computing results for pending windows.
    Results are saved with mode='online'.
    """
    # Find latest window index that can be computed
    latest_eligible_w = (max_sample_idx - 1) // 400
    if latest_eligible_w < 0:
        return 0

    computed_count = 0
    # Process windows that have not been computed yet
    for w in range(0, latest_eligible_w + 1):
        # Window samples
        start_sample = 400 * w
        end_sample = 400 * w + 399
        s1_map, s2_map = storage.get_samples_for_window(session_id, start_sample, end_sample)

        # Retrieve lookback history for heart rate if w >= 4
        hr_history = None
        if w >= 4:
            hr_start = 400 * (w - 4)
            # Need up to end_sample + 1 for lookahead
            hr_map_s1, _ = storage.get_samples_for_window(session_id, hr_start, end_sample + 1)
            hr_history = {idx: vals[1] for idx, vals in hr_map_s1.items() if len(vals) > 1}

        res = process_window(w, s1_map, s2_map, mode="online", s1_ch2_history=hr_history)
        storage.save_result(session_id, w, "online", res)
        computed_count += 1

    return computed_count


@app.post("/api/samples")
def ingest_samples(payload: IngestionPayload):
    """
    Idempotent batch ingestion of samples.
    Saves to cloud durable storage and triggers online processing.
    """
    if not payload.samples:
        return {"saved": 0, "total": storage.get_total_saved_samples()}

    # Group by session_id and sensor
    grouped: Dict[tuple, List[tuple]] = {}
    max_idx_by_session: Dict[str, int] = {}

    for item in payload.samples:
        key = (item.session_id, item.sensor)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append((item.sample_index, item.values))
        max_idx_by_session[item.session_id] = max(
            max_idx_by_session.get(item.session_id, 0), item.sample_index
        )

    total_inserted = 0
    for (sess, sensor), items in grouped.items():
        items.sort(key=lambda x: x[0])
        # Save batches
        curr_batch: List[List[int]] = []
        batch_start_idx = items[0][0]
        expected_idx = batch_start_idx

        for idx, vals in items:
            if idx == expected_idx:
                curr_batch.append(vals)
                expected_idx += 1
            else:
                # Discontinuity, flush batch
                total_inserted += storage.save_samples_batch(sess, sensor, batch_start_idx, curr_batch)
                curr_batch = [vals]
                batch_start_idx = idx
                expected_idx = idx + 1

        if curr_batch:
            total_inserted += storage.save_samples_batch(sess, sensor, batch_start_idx, curr_batch)

    # Online processing for each affected session
    for sess, max_idx in max_idx_by_session.items():
        _process_eligible_windows_in_cloud(sess, max_idx)

    return {
        "status": "success",
        "inserted": total_inserted,
        "total_in_cloud": storage.get_total_saved_samples(),
    }


@app.post("/api/losses")
def ingest_losses(payload: LossPayload):
    """Records losses in the cloud loss ledger."""
    for loss in payload.losses:
        storage.record_loss(
            loss.session_id,
            loss.sensor,
            loss.from_index,
            loss.to_index,
            loss.reason,
        )
    return {"status": "success", "count": len(payload.losses)}


@app.get("/api/export")
def export_session(session_id: str = Query(..., description="Target session ID")):
    """Streams JSONL export of samples then losses directly from cloud storage."""
    lines = []
    for item in storage.export_samples_and_losses(session_id):
        lines.append(json.dumps(item))
    content = "\n".join(lines) + ("\n" if lines else "")
    return Response(content=content, media_type="application/x-ndjson")


@app.get("/api/results")
def export_results(session_id: str = Query(..., description="Target session ID")):
    """Streams JSONL window results computed in the cloud."""
    lines = []
    for item in storage.get_results(session_id):
        lines.append(json.dumps(item))
    content = "\n".join(lines) + ("\n" if lines else "")
    return Response(content=content, media_type="application/x-ndjson")
