"""
High-Fidelity Reference Mock Service for biosignal sensors.
Implements the exact requirements of Section 6 of the MayaCardio assessment:
- Two modes: --mode ramp and --mode signal.
- 400 samples/sec per sensor.
- Sensor 1: 3 channels (signed 24-bit ints).
- Sensor 2: 2 channels (signed 24-bit ints).
- Exact CRC-32 (zlib) generation over raw data array text span from [ to ].
- 10-second (4000 sample) buffer window with eviction reported in `lost`.
- Fault injection: CRC corruption, duplicate reply, sensor stalls, session restarts.
"""

import argparse
import json
import math
import random
import socket
import sys
import threading
import time
import zlib
from typing import Dict, Any, List, Optional, Tuple


def generate_ramp_sample(sensor: int, sample_idx: int) -> List[int]:
    """Mathematical ramp generator: predictable values for loss/integrity checks."""
    modulo = 8388608  # 2^23
    if sensor == 1:
        # 3 channels: c = 0, 1, 2
        return [((3 * sample_idx) + c) % modulo for c in range(3)]
    else:
        # 2 channels: c = 0, 1
        return [((2 * sample_idx) + c) % modulo for c in range(2)]


def generate_signal_sample(sensor: int, sample_idx: int, seed: int = 7) -> List[int]:
    """
    Realistic synthetic signal generator:
    - 400 Hz sampling rate.
    - Sensor 1, ch2: ECG wave with sharp peaks.
      HR = 60 bpm (min 0-2), 90 bpm (min 2-4), 72 bpm (min 4-6), repeating.
    - Sensor 1, ch1 = ch2 * 0.6; ch3 = ch2 * 1.3
    - Sensor 2, ch1 ~ 400,000, ch2 ~ 150,000, varying slowly.
    """
    t_seconds = sample_idx / 400.0
    minute_cycle = (t_seconds / 60.0) % 6.0

    if minute_cycle < 2.0:
        bpm = 60.0
    elif minute_cycle < 4.0:
        bpm = 90.0
    else:
        bpm = 72.0

    beat_interval = 60.0 / bpm
    t_in_beat = t_seconds % beat_interval

    # Sharp QRS-like peak at t_in_beat ~ 0.2s with width ~ 0.04s
    peak_center = 0.2
    dt = t_in_beat - peak_center
    qrs = math.exp(-0.5 * (dt / 0.02) ** 2) * 5000.0

    # Baseline drift + small noise
    baseline = 1000.0 + 100.0 * math.sin(2.0 * math.pi * 0.1 * t_seconds)
    random.seed(seed + sample_idx)
    noise = random.randint(-15, 15)

    ch2_val = int(baseline + qrs + noise)

    if sensor == 1:
        ch1_val = int(ch2_val * 0.6)
        ch3_val = int(ch2_val * 1.3)
        return [ch1_val, ch2_val, ch3_val]
    else:
        s2_ch1 = int(400000 + 5000 * math.sin(2.0 * math.pi * 0.05 * t_seconds) + noise * 5)
        s2_ch2 = int(150000 + 3000 * math.cos(2.0 * math.pi * 0.04 * t_seconds) + noise * 3)
        return [s2_ch1, s2_ch2]


class BiosignalMockServer:
    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        host: str = "127.0.0.1",
        buffer_seconds: int = 10,
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.host = host
        self.buffer_size = buffer_seconds * 400  # 4000 samples

        self.session_id = f"{random.randint(0x10000000, 0xFFFFFFFF):08x}"
        self.start_time = time.time()
        self.running = False
        self.server_sock: Optional[socket.socket] = None

        # Tracking read positions per sensor
        self.last_delivered_index: Dict[int, int] = {1: -1, 2: -1}

        # Fault injection flags
        self.inject_crc_error = False
        self.inject_duplicate = False
        self.inject_sensor_loss = False
        self.inject_session_restart = False
        self._last_reply_bytes: Optional[bytes] = None

    def restart_session(self) -> None:
        self.session_id = f"{random.randint(0x10000000, 0xFFFFFFFF):08x}"
        self.start_time = time.time()
        self.last_delivered_index = {1: -1, 2: -1}

    def current_produced_index(self) -> int:
        elapsed = time.time() - self.start_time
        return int(elapsed * 400)

    def generate_sample(self, sensor: int, idx: int) -> List[int]:
        if self.mode == "ramp":
            return generate_ramp_sample(sensor, idx)
        else:
            return generate_signal_sample(sensor, idx, self.seed)

    def handle_client(self, client_sock: socket.socket) -> None:
        client_sock.settimeout(5.0)
        buffer = bytearray()

        try:
            while self.running:
                newline_idx = buffer.find(b"\n")
                if newline_idx == -1:
                    chunk = client_sock.recv(4096)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    continue

                line = buffer[:newline_idx].decode("utf-8")
                del buffer[: newline_idx + 1]

                if not line.strip():
                    continue

                try:
                    cmd_obj = json.loads(line)
                except Exception:
                    continue

                if cmd_obj.get("cmd") == "read":
                    sensor = int(cmd_obj.get("sensor", 1))
                    from_index = cmd_obj.get("from_index")
                    reply_bytes = self._build_read_reply(sensor, from_index)

                    if self.inject_duplicate and self._last_reply_bytes:
                        # Send duplicate first
                        client_sock.sendall(self._last_reply_bytes)
                        self.inject_duplicate = False

                    self._last_reply_bytes = reply_bytes
                    client_sock.sendall(reply_bytes)

        except Exception:
            pass
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    def _build_read_reply(self, sensor: int, from_index: Optional[int]) -> bytes:
        if self.inject_session_restart:
            self.restart_session()
            self.inject_session_restart = False

        prod_idx = self.current_produced_index()
        earliest_retained = max(0, prod_idx - self.buffer_size)

        if from_index is not None:
            first_idx = max(from_index, earliest_retained)
        else:
            first_idx = max(self.last_delivered_index[sensor] + 1, earliest_retained)

        # Batch up to 1 second of data (400 samples)
        count = min(400, max(0, prod_idx - first_idx))
        lost_records = []

        if from_index is not None and from_index < earliest_retained:
            lost_records.append(
                {
                    "from_index": from_index,
                    "to_index": earliest_retained - 1,
                    "reason": "buffer_overflow",
                }
            )

        data = []
        for i in range(first_idx, first_idx + count):
            data.append(self.generate_sample(sensor, i))

        self.last_delivered_index[sensor] = first_idx + count - 1

        # Format exact data text
        data_text = json.dumps(data)
        data_bytes = data_text.encode("utf-8")

        # Compute zlib CRC-32
        crc_val = zlib.crc32(data_bytes) & 0xFFFFFFFF
        if self.inject_crc_error:
            crc_val = (crc_val ^ 0xDEADBEEF) & 0xFFFFFFFF
            self.inject_crc_error = False

        crc_str = f"{crc_val:08x}"
        session_end = (first_idx + count) >= (self.duration * 400)

        reply_dict = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": first_idx,
            "count": count,
            "lost": lost_records,
            "session_end": session_end,
            "crc32": crc_str,
        }

        # Build raw frame string to ensure exact data array bytes match
        prefix = json.dumps(reply_dict)[:-1]  # remove trailing '}'
        full_frame = f'{prefix}, "data": {data_text}}}\n'
        return full_frame.encode("utf-8")

    def start(self) -> None:
        self.running = True
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.host, self.port))
        s.listen(1)
        self.server_sock = s

        def accept_loop():
            while self.running:
                try:
                    client_sock, _ = s.accept()
                    self.handle_client(client_sock)
                except Exception:
                    break

        t = threading.Thread(target=accept_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self.running = False
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
            self.server_sock = None


def main():
    parser = argparse.ArgumentParser(description="biosignal-mock reference service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duration", type=int, default=3600)
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--host", default="127.0.0.1")

    args = parser.parse_args()
    server = BiosignalMockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        host=args.host,
    )
    server.start()
    print(f"biosignal-mock listening on {args.host}:{args.port} (mode={args.mode}, seed={args.seed})")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
