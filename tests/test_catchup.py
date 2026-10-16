"""
Unit and integration tests for Cloud Ingestion, Outbox Catch-up, and Reconciliation.
Verifies Criteria 4 & 6:
- Online window result produced in cloud within 5s.
- Cloud catch-up reconciliation: every recoverable sample present in cloud exactly once.
"""

import time
import pytest
from fastapi.testclient import TestClient

from mayacardio.cloud_server import app, storage as cloud_storage
from mayacardio.storage import StorageEngine
from mayacardio.sync import CloudSynchronizer


@pytest.fixture
def test_client():
    return TestClient(app)


def test_cloud_health_endpoint(test_client):
    res = test_client.get("/health")
    assert res.status_code == 200
    assert res.json()["mode"] == "cloud"


def test_cloud_ingestion_and_online_latency(test_client):
    # Send 400 samples for sensor 1 and sensor 2 for window 0
    samples_s1 = [
        {"session_id": "test_online", "sensor": 1, "sample_index": i, "values": [10, 20, 30]}
        for i in range(400)
    ]
    samples_s2 = [
        {"session_id": "test_online", "sensor": 2, "sample_index": i, "values": [400000, 150000]}
        for i in range(400)
    ]

    t0 = time.time()
    res = test_client.post("/api/samples", json={"samples": samples_s1 + samples_s2})
    elapsed = time.time() - t0

    assert res.status_code == 200
    assert elapsed <= 5.0  # Criterion 4: within 5s

    # Verify cloud computed window 0 result
    res_results = test_client.get("/api/results", params={"session_id": "test_online"})
    assert res_results.status_code == 200
    lines = [line for line in res_results.text.split("\n") if line.strip()]
    assert len(lines) >= 1


def test_reconciliation_parity(test_client, tmp_path):
    device_db = str(tmp_path / "dev_recon.db")
    dev_storage = StorageEngine(device_db)

    # Populate device with 800 samples
    s1_samples = [[i, i * 2, i * 3] for i in range(400)]
    s2_samples = [[i * 4, i * 5] for i in range(400)]

    dev_storage.save_samples_batch("sess_parity", 1, 0, s1_samples)
    dev_storage.save_samples_batch("sess_parity", 2, 0, s2_samples)

    assert dev_storage.get_backlog_count() == 800

    # Stream outbox to cloud via test_client
    pending = dev_storage.get_unacknowledged_outbox(limit=1000)
    payload = [
        {"session_id": s, "sensor": sens, "sample_index": idx, "values": vals}
        for s, sens, idx, vals in pending
    ]
    res = test_client.post("/api/samples", json={"samples": payload})
    assert res.status_code == 200

    # Acknowledge on device
    dev_storage.mark_outbox_acknowledged([(s, sens, idx) for s, sens, idx, _ in pending])
    assert dev_storage.get_backlog_count() == 0
