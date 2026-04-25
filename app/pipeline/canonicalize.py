from __future__ import annotations

from typing import Any


PARSER_VERSION = "mvp_v1"

SOURCE_TO_CANONICAL_TYPE = {
    "process_job": "wafer_processing_sequence",
    "module_process_report": "wafer_processing_sequence",
    "sensor_measurement": "sensor_reading",
    "alarm_event": "fault_event",
    "error_event": "fault_event",
    "control_state_event": "equipment_state",
}

def _map_parser_record_to_canonical(parser_record_type: str, raw_fields: dict) -> str:
    if parser_record_type == "event":
        event_type = str(raw_fields.get("@type", "")).upper()

        if event_type == "STATE":
            return "equipment_state"
        if event_type == "STEP":
            return "wafer_processing_sequence"
        if event_type == "SENSOR":
            return "sensor_reading"
        if event_type == "ALARM":
            return "fault_event"

        return "generic_operational_observation"

    if parser_record_type == "csv_row":
        return "sensor_reading"

    return SOURCE_TO_CANONICAL_TYPE.get(parser_record_type, parser_record_type)

def canonicalize_result_payload(result_payload: dict[str, Any]) -> dict[str, Any]:
    ingested = result_payload.get("ingested", {}) or {}
    detection = result_payload.get("detection", {}) or {}
    routing = result_payload.get("routing", {}) or {}
    parsed_result = result_payload.get("parsed_result", {}) or {}
    structure_config = result_payload.get("structure_config", {}) or {}
    agent_debug = result_payload.get("agent_debug", {}) or {}

    records = parsed_result.get("records", []) or []

    file_id = ingested.get("file_id")
    filename = ingested.get("filename")
    ingestion_time = ingested.get("ingestion_time")
    source_format = detection.get("format_guess", "unknown")
    parser_route = routing.get("next_route") or structure_config.get("family") or "unknown_route"

    canonical_records: list[dict[str, Any]] = []

    for idx, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            continue

        raw_fields = dict(record.get("data", {}) or {})
        extra = dict(record.get("extra", {}) or {})

        confidence = record.get("confidence", 1.0)
        if not isinstance(confidence, (int, float)):
            confidence = 1.0

        parser_record_type = str(record.get("record_type") or "unknown_record")
        source_record_type = str(
            record.get("coarse_type")
            or record.get("source_record_type")
            or parser_record_type
        )
        record_type = _map_parser_record_to_canonical(parser_record_type, raw_fields)
        canonical_records.append(
            {
                "file_id": file_id,
                "filename": filename,
                "ingestion_time": ingestion_time,
                "source_format": source_format,
                "parser_route": parser_route,
                "parser_version": PARSER_VERSION,
                "record_type": record_type,
                "source_record_type": source_record_type,
                "source_reference": record.get("source_reference", f"record:{idx}"),
                "confidence": float(confidence),
                "evidence_text": str(record.get("evidence_text", "") or ""),
                "raw_fields": raw_fields,
                "candidate_fields": dict(raw_fields),
                "extra": extra,
                "unmapped_fields": {},
                "agent_debug_hint": agent_debug.get("final_source"),
            }
        )

    return {
        "file_id": file_id,
        "filename": filename,
        "source_format": source_format,
        "parser_route": parser_route,
        "parser_version": PARSER_VERSION,
        "record_count": len(canonical_records),
        "records": canonical_records,
    }