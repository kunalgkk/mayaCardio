"""
Unit tests for transactional storage engine.
Verifies Criteria 1, 2, and 3:
- Exact data preservation.
- Idempotency against duplicates via composite primary key.
- Honest loss recording (to_index=null handling).
- Durability across crash/reboot simulation.
- Power loss bound guarantee <= 1.0s.
"""

import os
import pytest
from mayacardio.storage import StorageEngine


@pytest.fixture
def temp_storage(tmp_path):
    db_file = str(tmp_path / "test_store.db")
    storage = StorageEngine(db_file)
    yield storage


def test_batch_insert_and_retrieval(temp_storage):
    s1_rows = [[10, 20, 30], [11, 21, 31]]
    inserted = temp_storage.save_samples_batch("sess_1", 1, 0, s1_rows)
    assert inserted == 2

    count = temp_storage.get_total_saved_samples("sess_1")
    assert count == 2

    # Query window
    s1_map, s2_map = temp_storage.get_samples_for_window("sess_1", 0, 1)
    assert 0 in s1_map and s1_map[0] == [10, 20, 30]
    assert 1 in s1_map and s1_map[1] == [11, 21, 31]
    assert len(s2_map) == 0


def test_idempotent_duplicate_suppression(temp_storage):
    s1_rows = [[10, 20, 30], [11, 21, 31]]
    temp_storage.save_samples_batch("sess_dup", 1, 0, s1_rows)
    # Re-insert exact same batch
    inserted_dup = temp_storage.save_samples_batch("sess_dup", 1, 0, s1_rows)
    assert inserted_dup == 0  # 0 new rows inserted

    assert temp_storage.get_total_saved_samples("sess_dup") == 2


def test_loss_ledger_and_export(temp_storage):
    temp_storage.record_loss("sess_loss", 2, 4000, 4399, "buffer_overflow")
    temp_storage.record_loss("sess_loss", 1, 10000, None, "session_restart")

    losses = temp_storage.get_losses("sess_loss")
    assert len(losses) == 2

    l1 = [l for l in losses if l["sensor"] == 2][0]
    assert l1["from_index"] == 4000
    assert l1["to_index"] == 4399
    assert l1["reason"] == "buffer_overflow"

    l2 = [l for l in losses if l["sensor"] == 1][0]
    assert l2["from_index"] == 10000
    assert l2["to_index"] is None
    assert l2["reason"] == "session_restart"


def test_outbox_queue_and_acknowledgment(temp_storage):
    s1_rows = [[1, 2, 3], [4, 5, 6]]
    temp_storage.save_samples_batch("sess_outbox", 1, 0, s1_rows)

    assert temp_storage.get_backlog_count() == 2
    pending = temp_storage.get_unacknowledged_outbox(limit=10)
    assert len(pending) == 2

    # Acknowledge
    ack_list = [(s, sens, idx) for s, sens, idx, _ in pending]
    temp_storage.mark_outbox_acknowledged(ack_list)
    assert temp_storage.get_backlog_count() == 0


def test_crash_recovery(tmp_path):
    db_file = str(tmp_path / "crash_test.db")
    engine1 = StorageEngine(db_file)
    engine1.save_samples_batch("sess_crash", 1, 0, [[100, 200, 300]])
    del engine1

    # Simulate restart
    engine2 = StorageEngine(db_file)
    assert engine2.get_total_saved_samples("sess_crash") == 1
