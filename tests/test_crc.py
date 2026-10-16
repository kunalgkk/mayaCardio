"""
Unit tests for raw TCP frame parsing and CRC-32 (zlib) slice validation.
Verifies Section 6.2 requirement:
- Exact data array text extraction from opening [ to matching ].
- zlib CRC-32 checksum calculation.
- Rejection of corrupted frames.
"""

import json
import zlib
import pytest

from mayacardio.protocol import (
    extract_raw_data_span,
    compute_crc32_hex,
    parse_and_validate_reply,
    CRCError,
    ProtocolError,
)


def test_extract_raw_data_span():
    raw = b'{"session_id": "abc", "data": [[1, 2], [3, 4]], "crc32": "12345678"}'
    span, start, end = extract_raw_data_span(raw)
    assert span == b"[[1, 2], [3, 4]]"
    assert raw[start:end] == b"[[1, 2], [3, 4]]"


def test_crc32_valid_reply():
    data_bytes = b"[[720, 1200, 1560], [708, 1180, 1534]]"
    crc = compute_crc32_hex(data_bytes)

    frame = (
        f'{{"session_id": "test", "sensor": 1, "first_index": 0, "count": 2, '
        f'"lost": [], "session_end": false, "crc32": "{crc}", "data": {data_bytes.decode()}}}\n'
    ).encode("utf-8")

    parsed = parse_and_validate_reply(frame)
    assert parsed["session_id"] == "test"
    assert parsed["sensor"] == 1
    assert parsed["count"] == 2
    assert parsed["data"] == [[720, 1200, 1560], [708, 1180, 1534]]


def test_crc32_corrupt_reply_rejected():
    data_bytes = b"[[720, 1200, 1560], [708, 1180, 1534]]"
    wrong_crc = "deadbeef"

    frame = (
        f'{{"session_id": "test", "sensor": 1, "first_index": 0, "count": 2, '
        f'"lost": [], "session_end": false, "crc32": "{wrong_crc}", "data": {data_bytes.decode()}}}\n'
    ).encode("utf-8")

    with pytest.raises(CRCError) as exc_info:
        parse_and_validate_reply(frame)
    assert "CRC-32 mismatch" in str(exc_info.value)


def test_malformed_protocol_frame():
    # Missing required 'data'
    with pytest.raises(ProtocolError):
        parse_and_validate_reply(b'{"session_id": "test"}\n')

    # Empty frame
    with pytest.raises(ProtocolError):
        parse_and_validate_reply(b"\n")
