from __future__ import annotations

from typing import Any

from free_form.schema import ALLOWED_COARSE_TYPES, ALLOWED_FINAL_TYPES, COARSE_TO_FINAL


def validate_extracted_records(chunk_result: dict[str, Any]) -> dict[str, Any]:
    chunk_id = chunk_result.get("chunk_id")
    raw_records = chunk_result.get("records", [])
    validated_records: list[dict[str, Any]] = []

    for record in raw_records:
        if not isinstance(record, dict):
            continue

        coarse_type = record.get("coarse_type")
        confidence = record.get("confidence")
        evidence_text = record.get("evidence_text")
        data = record.get("data")
        extra = record.get("extra", {})
        uncertain = bool(record.get("uncertain", False))
        subtype = record.get("subtype")

        if coarse_type not in ALLOWED_COARSE_TYPES:
            continue

        if not isinstance(confidence, (int, float)):
            continue
        confidence = float(confidence)
        if confidence < 0.0 or confidence > 1.0:
            continue

        if not isinstance(evidence_text, str) or not evidence_text.strip():
            continue
        if not isinstance(data, dict) or not data:
            continue
        if not isinstance(extra, dict):
            extra = {}

        final_type = COARSE_TO_FINAL.get(coarse_type)
        normalized_data = _normalize_data(data)

        if final_type is not None and not _passes_minimums(final_type, normalized_data):
            # downgrade weak mapped records instead of dropping everything
            final_type = None
            uncertain = True

        validated_records.append(
            {
                "coarse_type": coarse_type,
                "final_type": final_type,
                "subtype": subtype,
                "source_reference": f"chunk:{chunk_id}",
                "confidence": round(confidence, 3),
                "evidence_text": evidence_text.strip(),
                "data": normalized_data,
                "extra": extra,
                "uncertain": uncertain,
            }
        )

    return {
        "chunk_id": chunk_id,
        "records": validated_records,
        "debug": chunk_result.get("debug", {}),
    }


def _normalize_data(data: dict[str, Any]) -> dict[str, Any]:
    rename_map = {
        "wafer_id": "wafer",
        "lot_id": "lot",
        "process_step": "step",
        "equipment_id": "equipment",
        "state": "curr_state",
        "from_state": "prev_state",
        "to_state": "curr_state",
        "equipment_state": "curr_state",
        "event": "event_name",
        "measurement": "parameter",
        "sensor": "parameter",
    }

    out: dict[str, Any] = {}
    for key, value in data.items():
        new_key = rename_map.get(key, key)
        if new_key not in out:
            out[new_key] = value
    return out


def _passes_minimums(final_type: str, data: dict[str, Any]) -> bool:
    keys = set(data.keys())

    if final_type == "equipment_state":
        return bool({"curr_state", "prev_state"} & keys)

    if final_type == "process_parameter_recipe":
        return bool({
            "recipe", "recipe_id", "recipe_name", "step", "parameter",
            "target_value", "duration_s", "temperature_C", "energy", "dose"
        } & keys)

    if final_type == "sensor_reading":
        return bool({
            "parameter", "value", "unit", "acceptable_range",
            "target_value", "limit", "threshold"
        } & keys)

    if final_type == "fault_event":
        return bool({
            "fault", "fault_code", "fault_summary", "event_name",
            "cause", "reason", "component"
        } & keys)

    if final_type == "wafer_processing_sequence":
        return bool({
            "wafer", "lot", "step", "status", "action", "result",
            "wafer_count", "wafer_range_start"
        } & keys)

    return True