from __future__ import annotations

import json
from typing import Any


TABLE_MAP = {
    "equipment_state": "equipment_states",
    "process_parameter_recipe": "process_parameters_recipes",
    "sensor_reading": "sensor_readings",
    "fault_event": "fault_events",
    "wafer_processing_sequence": "wafer_processing_sequences",
    "generic_operational_observation": "generic_observations_staging",
}


def route_records(validated_batch: dict[str, Any]) -> dict[str, Any]:
    routed_rows: dict[str, list[dict[str, Any]]] = {table: [] for table in TABLE_MAP.values()}

    for record in validated_batch.get("accepted_records", []):
        table = TABLE_MAP.get(record.get("record_type"), "generic_observations_staging")
        row = _build_table_row(record)
        routed_rows.setdefault(table, []).append(row)

    return {
        "file_id": validated_batch.get("file_id"),
        "filename": validated_batch.get("filename"),
        "accepted_count": validated_batch.get("accepted_count", 0),
        "rejected_count": validated_batch.get("rejected_count", 0),
        "routed_rows": routed_rows,
        "rejected_records": validated_batch.get("rejected_records", []),
    }


def _build_table_row(record: dict[str, Any]) -> dict[str, Any]:
    normalized_fields = dict(record.get("normalized_fields", {}) or {})

    row = {
        "file_id": record.get("file_id"),
        "filename": record.get("filename"),
        "parser_route": record.get("parser_route"),
        "parser_version": record.get("parser_version"),
        "source_record_type": record.get("source_record_type"),
        "source_reference": record.get("source_reference"),
        "parser_confidence": record.get("confidence"),
        "evidence_text": record.get("evidence_text"),
        "raw_field_map": json.dumps(record.get("raw_fields", {}), ensure_ascii=False),
        "raw_to_standard_map": json.dumps(record.get("raw_to_standard_map", {}), ensure_ascii=False),
        "validation_reason": record.get("validation_reason"),
        "tool_id": _extract_tool_id(normalized_fields),
    }

    row.update(normalized_fields)
    return row


def _extract_tool_id(fields: dict[str, Any]) -> Any:
    for key in ["tool_id", "tool", "equipment", "machine"]:
        if key in fields:
            return fields[key]
    return None