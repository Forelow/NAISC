from __future__ import annotations

from typing import Any

ALLOWED_CANONICAL_TYPES = {
    "equipment_state",
    "process_parameter_recipe",
    "sensor_reading",
    "fault_event",
    "wafer_processing_sequence",
    "generic_operational_observation",
}

ALLOWED_CANONICAL_FIELDS = {
    "tool_id",
    "timestamp",
    "wafer",
    "lot",
    "slot",
    "recipe",
    "step",
    "parameter",
    "value",
    "unit",
    "sensor_id",
    "curr_state",
    "prev_state",
    "event_name",
    "fault_code",
    "fault_summary",
    "severity",
    "status",
    "action",
    "duration_s",
    "wafer_count",
    "expected_value",
    "measured_value",
    "component",
    "module_id",
    "recipe_step_id",
    "operator_id",
    "process_job_id",
    "control_job_id",
    "equipment_class",
    "to_state",
    "from_state",
    "record_id",
}


def validate_adapter_spec(raw: dict[str, Any]) -> dict[str, Any]:
    raw = raw or {}

    out: dict[str, Any] = {
        "schema_fingerprint": str(raw.get("schema_fingerprint", "") or ""),
        "schema_family": str(raw.get("schema_family", "") or ""),
        "record_type_mapping": {},
        "dispatch_rules": [],
        "field_aliases": {},
        "fallback_rules": [],
    }

    for src_type, dst_type in (raw.get("record_type_mapping") or {}).items():
        if isinstance(src_type, str) and isinstance(dst_type, str) and dst_type in ALLOWED_CANONICAL_TYPES:
            out["record_type_mapping"][src_type] = dst_type

    for rule in raw.get("dispatch_rules") or []:
        if not isinstance(rule, dict):
            continue

        source_record_type = str(rule.get("source_record_type", "") or "")
        field = str(rule.get("field", "") or "")
        mapping = rule.get("map") or {}

        if not source_record_type or not field or not isinstance(mapping, dict):
            continue

        clean_map: dict[str, str] = {}
        for src_value, dst_type in mapping.items():
            if isinstance(src_value, str) and isinstance(dst_type, str) and dst_type in ALLOWED_CANONICAL_TYPES:
                clean_map[src_value] = dst_type

        if clean_map:
            out["dispatch_rules"].append(
                {
                    "source_record_type": source_record_type,
                    "field": field,
                    "map": clean_map,
                }
            )

    for source_key, canonical_key in (raw.get("field_aliases") or {}).items():
        if (
            isinstance(source_key, str)
            and isinstance(canonical_key, str)
            and canonical_key in ALLOWED_CANONICAL_FIELDS
        ):
            out["field_aliases"][source_key] = canonical_key

    for rule in raw.get("fallback_rules") or []:
        if not isinstance(rule, dict):
            continue

        when_record_type = str(rule.get("when_record_type", "") or "")
        if_missing = str(rule.get("if_missing", "") or "")
        copy_from = str(rule.get("copy_from", "") or "")

        if (
            when_record_type in ALLOWED_CANONICAL_TYPES
            and if_missing in ALLOWED_CANONICAL_FIELDS
            and copy_from in ALLOWED_CANONICAL_FIELDS
        ):
            out["fallback_rules"].append(
                {
                    "when_record_type": when_record_type,
                    "if_missing": if_missing,
                    "copy_from": copy_from,
                }
            )

    return out