from __future__ import annotations

from typing import Any


def validate_records(normalized_batch: dict[str, Any]) -> dict[str, Any]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for record in normalized_batch.get("records", []):
        ok, reason = _validate_record(record)
        row = dict(record)
        row["validation_reason"] = reason

        if ok:
            accepted.append(row)
        else:
            rejected.append(row)

    return {
        "file_id": normalized_batch.get("file_id"),
        "filename": normalized_batch.get("filename"),
        "parser_route": normalized_batch.get("parser_route"),
        "parser_version": normalized_batch.get("parser_version"),
        "accepted_records": accepted,
        "accepted_count": len(accepted),
        "rejected_records": rejected,
        "rejected_count": len(rejected),
    }


def _validate_record(record: dict[str, Any]) -> tuple[bool, str]:
    record_type = record.get("record_type")
    confidence = float(record.get("confidence", 0.0))
    fields = dict(record.get("normalized_fields", {}) or {})

    if confidence < 0.5:
        return False, "confidence_below_threshold"

    if record_type == "equipment_state":
        if "curr_state" in fields:
            return True, "ok"
        return False, "missing_curr_state"

    if record_type == "process_parameter_recipe":
        if any(k in fields for k in ["recipe", "parameter", "steps", "energy_keV", "dose_cm-2", "dose_cm2", "target_value"]):
            return True, "ok"
        return False, "missing_recipe_or_parameter_content"

    if record_type == "sensor_reading":
        if ("parameter" in fields and "value" in fields) or "acceptable_range" in fields:
            return True, "ok"
        return False, "missing_parameter_or_value"

    if record_type == "fault_event":
        if any(k in fields for k in ["fault", "fault_code", "fault_summary", "issue", "threshold", "condition", "event_name"]):
            return True, "ok"
        return False, "missing_fault_signal"

    if record_type == "wafer_processing_sequence":
        if any(k in fields for k in ["wafer", "lot", "step", "action", "event_name", "status", "disposition", "wafer_count"]):
            return True, "ok"
        return False, "missing_sequence_signal"

    if record_type == "generic_operational_observation":
        # keep generic records for fallback/staging if rich enough
        if len(fields) >= 3:
            return True, "ok_generic"
        return False, "generic_too_thin"

    return False, "unknown_record_type"