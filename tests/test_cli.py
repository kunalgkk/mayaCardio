"""
Unit tests for the pipeline CLI interface.
Verifies Section 4 required commands:
1. pipeline status (prints exactly 1 JSON line)
2. pipeline export --session ID --from device|cloud
3. pipeline results --session ID
"""

import io
import json
import sys
from mayacardio.cli import cmd_status, cmd_export, cmd_results
from mayacardio.storage import StorageEngine


class Args:
    pass


def test_cli_status(monkeypatch, tmp_path):
    db_file = str(tmp_path / "cli_test.db")
    monkeypatch.setenv("DEVICE_DB_PATH", db_file)
    monkeypatch.setenv("CLOUD_URL", "http://127.0.0.1:58999")  # offline

    storage = StorageEngine(db_file)
    storage.save_samples_batch("sess_cli", 1, 0, [[1, 2, 3]])

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    cmd_status(Args())
    output = buf.getvalue().strip()

    # Must be exactly one single line
    lines = output.split("\n")
    assert len(lines) == 1

    parsed = json.loads(lines[0])
    assert "mode" in parsed
    assert "samples_saved" in parsed
    assert "samples_in_cloud" in parsed
    assert "backlog" in parsed
    assert parsed["samples_saved"] == 1


def test_cli_export(monkeypatch, tmp_path):
    db_file = str(tmp_path / "cli_export_test.db")
    monkeypatch.setenv("DEVICE_DB_PATH", db_file)

    storage = StorageEngine(db_file)
    storage.save_samples_batch("sess_exp", 1, 100, [[10, 20, 30]])
    storage.record_loss("sess_exp", 2, 4000, 4399, "buffer_overflow")

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    args = Args()
    args.session = "sess_exp"
    args.source = "device"

    cmd_export(args)
    lines = [line.strip() for line in buf.getvalue().split("\n") if line.strip()]
    assert len(lines) == 2

    sample_rec = json.loads(lines[0])
    assert sample_rec["sensor"] == 1
    assert sample_rec["index"] == 100
    assert sample_rec["values"] == [10, 20, 30]

    loss_rec = json.loads(lines[1])
    assert loss_rec["sensor"] == 2
    assert loss_rec["from_index"] == 4000
    assert loss_rec["to_index"] == 4399
    assert loss_rec["reason"] == "buffer_overflow"


def test_cli_results(monkeypatch, tmp_path):
    db_file = str(tmp_path / "cli_res_test.db")
    monkeypatch.setenv("DEVICE_DB_PATH", db_file)

    storage = StorageEngine(db_file)
    storage.save_result("sess_res", 0, "offline", {"window": 0, "mode": "offline", "complete": True})

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    args = Args()
    args.session = "sess_res"

    cmd_results(args)
    lines = [line.strip() for line in buf.getvalue().split("\n") if line.strip()]
    assert len(lines) == 1

    res = json.loads(lines[0])
    assert res["window"] == 0
    assert res["mode"] == "offline"
    assert res["complete"] is True
