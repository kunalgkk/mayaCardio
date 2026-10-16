"""
Device Ingestion Pipeline Engine for MayaCardio.
Manages continuous acquisition of Sensor 1 and Sensor 2 from biosignal-mock:
- Strict protocol communication and CRC-32 verification.
- Re-read on corruption via `from_index`.
- Loss accounting and mock restart handling (to_index=null on unknown session termination).
- Atomic local disk persistence with bounded in-flight buffering (<= 1.0s power loss bound).
"""

import logging
import socket
import time
from typing import Optional, Dict

from mayacardio.protocol import MockClient, CRCError, ProtocolError
from mayacardio.storage import StorageEngine
from mayacardio.sync import CloudSynchronizer

logger = logging.getLogger("mayacardio.ingestion")


class IngestionEngine:
    def __init__(
        self,
        storage: StorageEngine,
        synchronizer: CloudSynchronizer,
        mock_host: str = "127.0.0.1",
        mock_port: int = 7100,
        reconnect_delay: float = 0.5,
    ):
        self.storage = storage
        self.synchronizer = synchronizer
        self.mock_client = MockClient(host=mock_host, port=mock_port)
        self.reconnect_delay = reconnect_delay

        self.current_session_id: Optional[str] = None
        self.expected_index: Dict[int, int] = {1: 0, 2: 0}
        self.running = False

    def handle_session_transition(self, new_session_id: str, sensor: int, first_index: int) -> None:
        """
        Detects mock restart and records unread remainder of old session with to_index=None.
        """
        if self.current_session_id is not None and new_session_id != self.current_session_id:
            logger.warning(
                "Mock session restart detected: %s -> %s",
                self.current_session_id,
                new_session_id,
            )
            # Record unrecovered loss for previous session
            for s in (1, 2):
                old_exp = self.expected_index.get(s, 0)
                self.storage.record_loss(
                    session_id=self.current_session_id,
                    sensor=s,
                    from_index=old_exp,
                    to_index=None,
                    reason="session_restart",
                )
            self.expected_index = {1: 0, 2: 0}

        self.current_session_id = new_session_id

    def read_and_persist_sensor(self, sensor: int) -> bool:
        """
        Reads one frame for the specified sensor, validates CRC, and commits data.
        Returns True if samples were read, False otherwise.
        """
        req_from_index: Optional[int] = None
        exp = self.expected_index.get(sensor, 0)

        # First attempt
        reply = None
        try:
            reply = self.mock_client.read_sensor(sensor=sensor, from_index=None)
        except CRCError as e:
            logger.warning("CRC error reading sensor %d: %s. Initiating re-read...", sensor, e)
            try:
                # Re-read requested range
                reply = self.mock_client.read_sensor(sensor=sensor, from_index=exp)
            except Exception as re_err:
                logger.error("Re-read failed for sensor %d: %s", sensor, re_err)
                return False
        except (socket.timeout, ConnectionError, OSError) as net_err:
            logger.debug("Network error reading mock: %s", net_err)
            self.mock_client.close()
            return False

        if not reply:
            return False

        sess_id = reply["session_id"]
        first_idx = reply["first_index"]
        count = reply["count"]
        data_rows = reply["data"]
        lost_ranges = reply.get("lost", [])

        self.handle_session_transition(sess_id, sensor, first_idx)

        # Record explicit lost ranges reported by mock
        for item in lost_ranges:
            self.storage.record_loss(
                session_id=sess_id,
                sensor=sensor,
                from_index=item["from_index"],
                to_index=item["to_index"],
                reason=item.get("reason", "mock_reported_loss"),
            )

        # Detect unexpected forward index gaps
        current_exp = self.expected_index[sensor]
        if first_idx > current_exp:
            logger.warning(
                "Detected index gap on sensor %d: expected %d, got %d",
                sensor,
                current_exp,
                first_idx,
            )
            self.storage.record_loss(
                session_id=sess_id,
                sensor=sensor,
                from_index=current_exp,
                to_index=first_idx - 1,
                reason="buffer_overflow",
            )

        # Commit samples to storage
        if data_rows:
            self.storage.save_samples_batch(sess_id, sensor, first_idx, data_rows)
            self.expected_index[sensor] = first_idx + count

            # If operating offline, trigger local edge processing
            if self.synchronizer.get_mode() == "offline":
                self.synchronizer.perform_local_offline_processing(
                    sess_id, self.expected_index[sensor]
                )

        return True

    def run_step(self) -> None:
        """Performs one round of reading sensor 1 and sensor 2."""
        self.read_and_persist_sensor(1)
        self.read_and_persist_sensor(2)

    def run(self) -> None:
        """Main acquisition loop."""
        self.running = True
        self.synchronizer.start_background_loop()
        logger.info("Ingestion pipeline started")

        try:
            while self.running:
                try:
                    self.run_step()
                except Exception as e:
                    logger.error("Unhandled error in ingestion step: %s", e)
                    time.sleep(self.reconnect_delay)
                time.sleep(0.01)  # High-frequency poll (10ms)
        finally:
            self.mock_client.close()
            self.synchronizer.stop_background_loop()
            logger.info("Ingestion pipeline stopped")

    def stop(self) -> None:
        self.running = False
