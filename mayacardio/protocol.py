"""
TCP Wire Protocol and CRC-32 Validation for biosignal-mock service.
Strictly adheres to Section 6.2 of the MayaCardio assessment:
- Newline-delimited JSON over TCP.
- Exact slice extraction of the `data` array text from opening `[` to matching `]`.
- zlib CRC-32 computation over original unparsed bytes.
"""

import json
import socket
import zlib
from typing import Dict, Any, Optional, Tuple


class CRCError(Exception):
    """Raised when the calculated CRC-32 does not match the received crc32 field."""
    pass


class ProtocolError(Exception):
    """Raised when protocol invariants or frame format are violated."""
    pass


def extract_raw_data_span(raw_line: bytes) -> Tuple[bytes, int, int]:
    """
    Locates the exact byte slice of the `data` array text starting from opening `[`
    to the corresponding closing `]`.
    Returns (data_bytes, start_pos, end_pos).
    """
    data_key_pos = raw_line.find(b'"data"')
    if data_key_pos == -1:
        raise ProtocolError("Missing 'data' key in raw reply frame")

    start_bracket_pos = raw_line.find(b'[', data_key_pos)
    if start_bracket_pos == -1:
        raise ProtocolError("Missing opening bracket for 'data' in raw reply frame")

    # Match brackets to find corresponding closing bracket
    depth = 0
    in_string = False
    escape = False
    end_bracket_pos = -1

    for idx in range(start_bracket_pos, len(raw_line)):
        b = raw_line[idx : idx + 1]
        if escape:
            escape = False
            continue
        if b == b'\\':
            escape = True
            continue
        if b == b'"':
            in_string = not in_string
            continue
        if not in_string:
            if b == b'[':
                depth += 1
            elif b == b']':
                depth -= 1
                if depth == 0:
                    end_bracket_pos = idx + 1
                    break

    if depth != 0 or end_bracket_pos == -1:
        raise ProtocolError("Unmatched brackets in 'data' array payload")

    data_span = raw_line[start_bracket_pos:end_bracket_pos]
    return data_span, start_bracket_pos, end_bracket_pos


def compute_crc32_hex(data_bytes: bytes) -> str:
    """Computes CRC-32 using zlib and returns 8-character lowercase hex string."""
    checksum = zlib.crc32(data_bytes) & 0xFFFFFFFF
    return f"{checksum:08x}"


def parse_and_validate_reply(raw_line: bytes) -> Dict[str, Any]:
    """
    Validates the exact raw line against its crc32 field before JSON decoding data.
    Raises CRCError if checksum fails.
    Returns parsed dictionary.
    """
    raw_clean = raw_line.strip()
    if not raw_clean:
        raise ProtocolError("Empty reply received")

    # First decode outer metadata
    try:
        parsed = json.loads(raw_clean.decode("utf-8"))
    except Exception as e:
        raise ProtocolError(f"Malformed JSON: {e}")

    for req_field in ("session_id", "sensor", "first_index", "count", "lost", "session_end", "crc32", "data"):
        if req_field not in parsed:
            raise ProtocolError(f"Missing required protocol field: {req_field}")

    # Extract exact byte slice for `data`
    data_span, _, _ = extract_raw_data_span(raw_clean)
    expected_crc = str(parsed["crc32"]).strip().lower()
    calculated_crc = compute_crc32_hex(data_span)

    if calculated_crc != expected_crc:
        raise CRCError(
            f"CRC-32 mismatch: calculated {calculated_crc} != expected {expected_crc} "
            f"for sensor {parsed.get('sensor')} index {parsed.get('first_index')}"
        )

    return parsed


class MockClient:
    """
    Robust TCP client connection to biosignal-mock service.
    Handles read requests, re-reads upon corruption, buffer line-splitting, and reconnection.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7100, timeout: float = 3.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self._buffer = bytearray()

    def connect(self) -> None:
        self.close()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect((self.host, self.port))
        self.sock = s
        self._buffer.clear()

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        self._buffer.clear()

    def is_connected(self) -> bool:
        return self.sock is not None

    def read_sensor(self, sensor: int, from_index: Optional[int] = None) -> Dict[str, Any]:
        """
        Sends read command and returns parsed and CRC-validated reply.
        Raises socket.timeout, ConnectionError, CRCError, or ProtocolError.
        """
        if not self.is_connected():
            self.connect()

        cmd_dict: Dict[str, Any] = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            cmd_dict["from_index"] = from_index

        payload = (json.dumps(cmd_dict) + "\n").encode("utf-8")
        assert self.sock is not None
        self.sock.sendall(payload)

        # Read until newline
        while True:
            newline_idx = self._buffer.find(b"\n")
            if newline_idx != -1:
                line_bytes = bytes(self._buffer[:newline_idx])
                del self._buffer[: newline_idx + 1]
                return parse_and_validate_reply(line_bytes)

            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed by biosignal-mock server")
            self._buffer.extend(chunk)
