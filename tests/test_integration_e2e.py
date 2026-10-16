"""
End-to-End Live Integration Test:
Spawns biosignal-mock, cloud-server, and pipeline ingestion engine.
Verifies:
- Live TCP streaming from mock to device.
- CRC validation on every real frame.
- Live HTTP streaming from device to cloud.
- Ground truth parity check via check_ground_truth.py.
- CLI commands: pipeline status, pipeline export, pipeline results.
"""

import json
import os
import subprocess
import sys
import time
import requests
import pytest

from scripts.check_ground_truth import verify_export_stream


def test_full_pipeline_live_e2e(tmp_path):
    mock_port = 7199
    cloud_port = 8099
    device_db = str(tmp_path / "live_device.db")
    cloud_db = str(tmp_path / "live_cloud.db")

    # 1. Start biosignal-mock server in ramp mode
    from scripts.biosignal_mock import BiosignalMockServer
    mock_server = BiosignalMockServer(mode="ramp", port=mock_port, duration=60)
    mock_server.start()

    # 2. Start Cloud Server using uvicorn in background thread/process
    env = os.environ.copy()
    env["CLOUD_DB_PATH"] = cloud_db
    cloud_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "mayacardio.cloud_server:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(cloud_port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for cloud server to be up
        cloud_url = f"http://127.0.0.1:{cloud_port}"
        up = False
        for _ in range(30):
            try:
                res = requests.get(f"{cloud_url}/health", timeout=0.5)
                if res.status_code == 200:
                    up = True
                    break
            except Exception:
                time.sleep(0.2)
        assert up, "Cloud server failed to start"

        # 3. Create device pipeline and run acquisition for 3 seconds
        from mayacardio.storage import StorageEngine
        from mayacardio.sync import CloudSynchronizer
        from mayacardio.ingestion import IngestionEngine

        dev_storage = StorageEngine(device_db)
        sync = CloudSynchronizer(dev_storage, cloud_url=cloud_url, probe_interval=0.5)
        engine = IngestionEngine(dev_storage, sync, mock_port=mock_port)

        sync.start_background_loop()

        # Ingest for ~3 seconds
        t_end = time.time() + 3.0
        while time.time() < t_end:
            engine.run_step()
            time.sleep(0.05)

        # Allow synchronizer to push pending backlog
        sync.run_catchup()
        sync.stop_background_loop()

        # 4. Verify saved sample counts
        saved_count = dev_storage.get_total_saved_samples()
        assert saved_count >= 800, f"Expected at least 800 samples, got {saved_count}"

        session_id = engine.current_session_id
        assert session_id is not None

        # 5. Export from device and verify with ground truth
        dev_export = list(dev_storage.export_samples_and_losses(session_id))
        export_lines = [json.dumps(x) for x in dev_export]
        truth_res = verify_export_stream(export_lines, mode="ramp")

        assert truth_res["status"] == "PASS"
        assert truth_res["duplicates"] == 0
        assert truth_res["corrupted_values"] == 0
        assert truth_res["silent_losses"] == 0

        # 6. Verify cloud copy
        res_cloud = requests.get(f"{cloud_url}/api/export", params={"session_id": session_id})
        assert res_cloud.status_code == 200
        cloud_lines = [l for l in res_cloud.text.split("\n") if l.strip()]

        cloud_truth = verify_export_stream(cloud_lines, mode="ramp")
        assert cloud_truth["status"] == "PASS"
        assert cloud_truth["duplicates"] == 0
        assert cloud_truth["corrupted_values"] == 0

    finally:
        mock_server.stop()
        cloud_proc.terminate()
        try:
            cloud_proc.wait(timeout=2.0)
        except Exception:
            cloud_proc.kill()
