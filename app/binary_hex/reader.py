from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def read_binary_or_hex_input(file_path: str) -> dict[str, Any]:
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext == ".hex":
        text = path.read_text(encoding="utf-8", errors="replace")
        raw_bytes, decode_mode = _decode_hex_text(text)
    else:
        raw_bytes = path.read_bytes()
        decode_mode = "binary_bytes"

    head = raw_bytes[:64]

    return {
        "file_path": str(path),
        "extension": ext,
        "input_mode": decode_mode,
        "raw_bytes": raw_bytes,
        "byte_count": len(raw_bytes),
        "head_hex": head.hex(" "),
        "head_ascii": "".join(chr(b) if 32 <= b <= 126 else "." for b in head),
    }


def _decode_hex_text(text: str) -> tuple[bytes, str]:
    stripped = text.strip()

    # Intel HEX style
    if _looks_like_intel_hex(stripped):
        return _decode_intel_hex(stripped), "intel_hex_text"

    # Plain hex dump style
    return _decode_plain_hex_text(stripped), "plain_hex_text"


def _looks_like_intel_hex(text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return False
    colon_lines = sum(1 for line in lines if line.startswith(":"))
    return colon_lines >= max(1, len(lines) // 2)


def _decode_intel_hex(text: str) -> bytes:
    output = bytearray()

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not line.startswith(":"):
            raise ValueError("Invalid Intel HEX line: missing ':'")

        line = line[1:]
        if len(line) < 10 or len(line) % 2 != 0:
            raise ValueError("Invalid Intel HEX line length")

        byte_count = int(line[0:2], 16)
        _address = int(line[2:6], 16)
        record_type = int(line[6:8], 16)
        data_hex = line[8:8 + (byte_count * 2)]

        if len(data_hex) != byte_count * 2:
            raise ValueError("Intel HEX data length mismatch")

        if record_type == 0x00:
            output.extend(bytes.fromhex(data_hex))
        elif record_type == 0x01:
            break
        else:
            # Ignore non-data records for MVP
            continue

    return bytes(output)


def _decode_plain_hex_text(text: str) -> bytes:
    # Case 1: long compact hex string
    compact = re.sub(r"[^0-9A-Fa-f]", "", text)
    if len(compact) >= 16 and len(compact) % 2 == 0:
        try:
            return bytes.fromhex(compact)
        except Exception:
            pass

    # Case 2: spaced / dumped hex bytes
    tokens = re.findall(r"\b[0-9A-Fa-f]{2}\b", text)
    if len(tokens) < 8:
        raise ValueError("Not enough hex byte tokens to decode")
    return bytes(int(tok, 16) for tok in tokens)