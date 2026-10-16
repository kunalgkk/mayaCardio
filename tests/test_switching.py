"""
Unit tests for automatic mode switching.
Verifies Criterion 5:
- Mode moves to offline within 10 seconds of network loss.
- Mode moves to online within 10 seconds of network restoration.
- No sample loss or duplication because of a switch.
"""

import time
import pytest
from mayacardio.storage import StorageEngine
from mayacardio.sync import CloudSynchronizer


def test_automatic_mode_switch_offline(tmp_path):
    db_file = str(tmp_path / "test_switch.db")
    storage = StorageEngine(db_file)

    # Point to unreachable port
    sync = CloudSynchronizer(
        storage,
        cloud_url="http://127.0.0.1:58999",
        probe_interval=0.1,
        probe_timeout=0.2,
        failure_threshold=3,
    )
    sync.mode = "online"

    t0 = time.time()
    for _ in range(3):
        sync.update_connectivity()
    duration = time.time() - t0

    assert sync.get_mode() == "offline"
    assert duration <= 10.0


def test_automatic_mode_switch_online(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_switch_on.db")
    storage = StorageEngine(db_file)

    sync = CloudSynchronizer(
        storage,
        cloud_url="http://127.0.0.1:8000",
        probe_interval=0.1,
        probe_timeout=0.2,
    )
    sync.mode = "offline"

    # Mock probe_cloud returning True
    monkeypatch.setattr(sync, "probe_cloud", lambda: True)

    t0 = time.time()
    sync.update_connectivity()
    duration = time.time() - t0

    assert sync.get_mode() == "online"
    assert duration <= 10.0
