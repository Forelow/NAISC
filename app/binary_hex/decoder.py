from __future__ import annotations

import struct
from typing import Any


MAGIC = b"SLOG"

TYPE_MAP = {
    1: "equipment_state",
    2: "process_parameter_recipe",
    3: "sensor_reading",
    4: "fault_event",
    5: "wafer_processing_sequence",
}


def is_known_mvp_binary(raw_bytes: bytes) -> bool:
    return len(raw_bytes) >= 8 and raw_bytes[:4] == MAGIC


def decode_mvp_binary_container(raw_bytes: bytes) -> dict[str, Any]:
    if not is_known_mvp_binary(raw_bytes):
        raise ValueError("Unknown MVP binary format")

    version = raw_bytes[4]
    flags = raw_bytes[5]
    record_count = struct.unpack_from("<H", raw_bytes, 6)[0]

    offset = 8
    records: list[dict[str, Any]] = []

    for idx in range(record_count):
        if offset + 3 > len(raw_bytes):
            break

        rec_type = raw_bytes[offset]
        payload_len = struct.unpack_from("<H", raw_bytes, offset + 1)[0]
        offset += 3

        if offset + payload_len > len(raw_bytes):
            break

        payload_bytes = raw_bytes[offset: offset + payload_len]
        offset += payload_len

        payload_text = payload_bytes.decode("utf-8", errors="replace")
        payload_data = _parse_kv_payload(payload_text)
        payload_data = _normalize_payload(TYPE_MAP.get(rec_type, "unknown_record"), payload_data)

        records.append(
            {
                "record_type": TYPE_MAP.get(rec_type, "unknown_record"),
                "source_reference": f"record:{idx + 1}",
                "confidence": 1.0,
                "evidence_text": payload_text,
                "data": payload_data,
            }
        )

    return {
        "header": {
            "magic_ascii": MAGIC.decode("ascii"),
            "version": version,
            "flags": flags,
            "declared_record_count": record_count,
            "parsed_record_count": len(records),
        },
        "records": records,
    }


def _parse_kv_payload(payload_text: str) -> dict[str, Any]:
    data: dict[str, Any] = {}

    for part in payload_text.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue

        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        data[key] = _coerce_value(value)

    return data


def _coerce_value(value: str) -> Any:
    lowered = value.lower()

    if lowered in {"true", "false"}:
        return lowered == "true"

    try:
        if "." in value:
            return float(value)
        return int(value)
    except Exception:
        return value


def _normalize_payload(record_type: str, data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)

    if record_type == "equipment_state":
        if "state" in out and "curr_state" not in out:
            out["curr_state"] = out["state"]
        if "new_state" in out and "curr_state" not in out:
            out["curr_state"] = out["new_state"]

    elif record_type == "sensor_reading":
        if "metric" in out and "parameter" not in out:
            out["parameter"] = out["metric"]

    elif record_type == "fault_event":
        if "code" in out and "fault_code" not in out:
            out["fault_code"] = out["code"]
        if "message" in out and "fault_summary" not in out:
            out["fault_summary"] = out["message"]

    elif record_type == "wafer_processing_sequence":
        if "event" in out and "event_name" not in out:
            out["event_name"] = out["event"]

    return out