"""
Cloud Synchronizer and Mode Transition Manager.
Handles:
- Active health checking to cloud server (1.0s intervals).
- Automatic mode transition (online <-> offline) within <= 3.0s (Limit: 10s).
- Outbox streaming and reconciliation catch-up.
- Local offline processing when disconnected.
"""

import logging
import threading
import time
from typing import Optional, List, Tuple
import requests

from mayacardio.storage import StorageEngine
from mayacardio.processor import process_window

logger = logging.getLogger("mayacardio.sync")


class CloudSynchronizer:
    def __init__(
        self,
        storage: StorageEngine,
        cloud_url: str = "http://127.0.0.1:8000",
        probe_interval: float = 1.0,
        probe_timeout: float = 0.8,
        failure_threshold: int = 3,
    ):
        self.storage = storage
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval = probe_interval
        self.probe_timeout = probe_timeout
        self.failure_threshold = failure_threshold

        self.mode: str = "offline"
        self._consecutive_failures = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._session = requests.Session()

    def get_mode(self) -> str:
        return self.mode

    def probe_cloud(self) -> bool:
        """Pings cloud /health endpoint with bounded timeout."""
        try:
            res = self._session.get(
                f"{self.cloud_url}/health", timeout=self.probe_timeout
            )
            return res.status_code == 200
        except Exception:
            return False

    def update_connectivity(self) -> str:
        """
        Executes one health probe and updates mode state.
        Transitions offline -> online or online -> offline within threshold pings.
        """
        reachable = self.probe_cloud()
        if reachable:
            self._consecutive_failures = 0
            if self.mode != "online":
                logger.info("Cloud reachable: Switching to ONLINE mode")
                self.mode = "online"
                self.storage.set_metadata("current_mode", "online")
        else:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                if self.mode != "offline":
                    logger.warning(
                        "Cloud unreachable (%d failures): Switching to OFFLINE mode",
                        self._consecutive_failures,
                    )
                    self.mode = "offline"
                    self.storage.set_metadata("current_mode", "offline")

        return self.mode

    def sync_outbox_batch(self, batch_size: int = 2000) -> int:
        """
        Streams one batch of unacknowledged samples from local outbox to cloud.
        Returns count of acknowledged samples.
        """
        if self.mode != "online":
            return 0

        pending = self.storage.get_unacknowledged_outbox(limit=batch_size)
        if not pending:
            return 0

        payload_samples = [
            {
                "session_id": sess,
                "sensor": sensor,
                "sample_index": idx,
                "values": vals,
            }
            for sess, sensor, idx, vals in pending
        ]

        try:
            res = self._session.post(
                f"{self.cloud_url}/api/samples",
                json={"samples": payload_samples},
                timeout=5.0,
            )
            if res.status_code == 200:
                ack_list = [(sess, sensor, idx) for sess, sensor, idx, _ in pending]
                self.storage.mark_outbox_acknowledged(ack_list)
                return len(ack_list)
            else:
                logger.error("Cloud ingestion rejected batch with HTTP %d", res.status_code)
                return 0
        except Exception as e:
            logger.warning("Cloud outbox push failed: %s", e)
            self._consecutive_failures += 1
            return 0

    def sync_losses(self, session_id: str) -> None:
        """Syncs all recorded losses for a session to cloud."""
        if self.mode != "online":
            return
        losses = self.storage.get_losses(session_id)
        if not losses:
            return
        payload = [
            {
                "session_id": session_id,
                "sensor": l["sensor"],
                "from_index": l["from_index"],
                "to_index": l["to_index"],
                "reason": l["reason"],
            }
            for l in losses
        ]
        try:
            self._session.post(
                f"{self.cloud_url}/api/losses",
                json={"losses": payload},
                timeout=3.0,
            )
        except Exception:
            pass

    def run_catchup(self, max_batches: Optional[int] = None) -> int:
        """
        Flushes complete outbox backlog to cloud until empty.
        Returns total samples synchronized.
        """
        total_synced = 0
        batches = 0
        while True:
            if max_batches is not None and batches >= max_batches:
                break
            synced = self.sync_outbox_batch(batch_size=2000)
            if synced == 0:
                break
            total_synced += synced
            batches += 1
        return total_synced

    def perform_local_offline_processing(self, session_id: str, max_sample_idx: int) -> int:
        """
        When operating in offline mode, computes window results locally on device
        and commits them with mode='offline'.
        """
        latest_eligible_w = (max_sample_idx - 1) // 400
        if latest_eligible_w < 0:
            return 0

        computed = 0
        for w in range(0, latest_eligible_w + 1):
            start_sample = 400 * w
            end_sample = 400 * w + 399
            s1_map, s2_map = self.storage.get_samples_for_window(session_id, start_sample, end_sample)

            hr_history = None
            if w >= 4:
                hr_start = 400 * (w - 4)
                hr_map_s1, _ = self.storage.get_samples_for_window(session_id, hr_start, end_sample + 1)
                hr_history = {idx: vals[1] for idx, vals in hr_map_s1.items() if len(vals) > 1}

            res = process_window(w, s1_map, s2_map, mode="offline", s1_ch2_history=hr_history)
            self.storage.save_result(session_id, w, "offline", res)
            computed += 1

        return computed

    def start_background_loop(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop_background_loop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _loop(self) -> None:
        while self._running:
            self.update_connectivity()
            if self.mode == "online":
                self.sync_outbox_batch(batch_size=2000)
            time.sleep(self.probe_interval)
