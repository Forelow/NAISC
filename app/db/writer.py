from __future__ import annotations

from typing import Any

from db.schema import init_db, get_connection

import json

def _db_safe_text(value):
    if value in (None, "", [], {}):
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, (int, float, bool)):
        return str(value)

    if isinstance(value, (list, tuple, set)):
        flat = [str(v) for v in value if v not in (None, "", [], {})]
        return ", ".join(flat) if flat else None

    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, default=str)

    return str(value)


def _db_safe_float(value):
    if value in (None, "", [], {}):
        return None

    if isinstance(value, (list, tuple, set, dict)):
        return None

    try:
        return float(value)
    except Exception:
        return None


def _db_safe_int(value):
    if value in (None, "", [], {}):
        return None

    if isinstance(value, (list, tuple, set, dict)):
        return None

    try:
        return int(value)
    except Exception:
        return None

def write_pipeline_output_to_db(result_payload: dict[str, Any]) -> dict[str, Any]:
    init_db()

    pipeline_output = result_payload.get("pipeline_output", {}) or {}
    routing_output = pipeline_output.get("routing_output", {}) or {}
    validated_output = pipeline_output.get("validated_output", {}) or {}
    canonical_output = pipeline_output.get("canonical_output", {}) or {}

    ingested = result_payload.get("ingested", {}) or {}
    detection = result_payload.get("detection", {}) or {}

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO files (
                file_id, filename, extension, sha256,
                source_format, parser_version, schema_fingerprint,
                accepted_count, rejected_count, ingestion_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                filename = VALUES(filename),
                extension = VALUES(extension),
                sha256 = VALUES(sha256),
                source_format = VALUES(source_format),
                parser_version = VALUES(parser_version),
                schema_fingerprint = VALUES(schema_fingerprint),
                accepted_count = VALUES(accepted_count),
                rejected_count = VALUES(rejected_count),
                ingestion_time = VALUES(ingestion_time)
            """,
            (
                ingested.get("file_id"),
                ingested.get("filename"),
                ingested.get("extension"),
                ingested.get("sha256"),
                detection.get("format_guess"),
                canonical_output.get("parser_version"),
                pipeline_output.get("schema_fingerprint"),
                validated_output.get("accepted_count", 0),
                validated_output.get("rejected_count", 0),
                ingested.get("ingestion_time"),
            ),
        )

        routed_rows = routing_output.get("routed_rows", {}) or {}

        inserted_counts = {
            "equipment_states": _insert_equipment_states(cur, routed_rows.get("equipment_states", [])),
            "process_parameters_recipes": _insert_process_parameters_recipes(cur, routed_rows.get("process_parameters_recipes", [])),
            "sensor_readings": _insert_sensor_readings(cur, routed_rows.get("sensor_readings", [])),
            "fault_events": _insert_fault_events(cur, routed_rows.get("fault_events", [])),
            "wafer_processing_sequences": _insert_wafer_processing_sequences(cur, routed_rows.get("wafer_processing_sequences", [])),
            "generic_observations_staging": _insert_generic_observations(cur, routed_rows.get("generic_observations_staging", [])),
            "rejected_records": _insert_rejected_records(cur, routing_output.get("rejected_records", [])),
        }

        conn.commit()

        return {
            "database": "semicon_parser",
            "file_id": ingested.get("file_id"),
            "inserted_counts": inserted_counts,
            "status": "ok",
        }
    finally:
        conn.close()


def _insert_equipment_states(cur, rows):
    sql = """
    INSERT INTO equipment_states (
        filename, tool_id, event_ts, curr_state, prev_state,
        lot, wafer, recipe, step, severity, event_name
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        _tool_id(row),
        _event_ts(row),
        row.get("curr_state"),
        row.get("prev_state"),
        row.get("lot"),
        row.get("wafer"),
        row.get("recipe"),
        row.get("step"),
        row.get("severity"),
        row.get("event_name"),
    ))


def _insert_process_parameters_recipes(cur, rows):
    sql = """
    INSERT INTO process_parameters_recipes (
        filename, tool_id, event_ts, recipe, step, parameter,
        value, unit, status, lot, wafer
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        _tool_id(row),
        _event_ts(row),
        row.get("recipe"),
        row.get("step"),
        row.get("parameter"),
        _float_or_none(row.get("value")),
        row.get("unit"),
        row.get("status"),
        row.get("lot"),
        row.get("wafer"),
    ))


def _insert_sensor_readings(cur, rows):
    sql = """
    INSERT INTO sensor_readings (
        filename, tool_id, event_ts, parameter, value, unit,
        lot, wafer, recipe, step, severity
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        _tool_id(row),
        _event_ts(row),
        row.get("parameter"),
        _float_or_none(row.get("value") if row.get("value") is not None else row.get("measured_value")),
        row.get("unit"),
        row.get("lot"),
        row.get("wafer"),
        row.get("recipe"),
        row.get("step"),
        row.get("severity"),
    ))


def _insert_fault_events(cur, rows):
    sql = """
    INSERT INTO fault_events (
        filename, tool_id, event_ts, fault_code, fault_summary,
        severity, lot, wafer, recipe, step, status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        _tool_id(row),
        _event_ts(row),
        row.get("fault_code"),
        row.get("fault_summary"),
        row.get("severity"),
        row.get("lot"),
        row.get("wafer"),
        row.get("recipe"),
        row.get("step"),
        row.get("status"),
    ))


def _insert_wafer_processing_sequences(cur, rows):
    sql = """
    INSERT INTO wafer_processing_sequences (
        filename, tool_id, event_ts, lot, wafer, slot, recipe, step,
        status, action, event_name, duration_s, wafer_count
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        _db_safe_text(row.get("filename")),
        _db_safe_text(_tool_id(row)),
        _db_safe_text(_event_ts(row)),
        _db_safe_text(row.get("lot")),
        _db_safe_text(row.get("wafer")),
        _db_safe_text(row.get("slot")),
        _db_safe_text(row.get("recipe")),
        _db_safe_text(row.get("step")),
        _db_safe_text(row.get("status")),
        _db_safe_text(row.get("action")),
        _db_safe_text(row.get("event_name")),
        _db_safe_float(row.get("duration_s")),
        _db_safe_int(row.get("wafer_count")),
    ))


def _insert_generic_observations(cur, rows):
    sql = """
    INSERT INTO generic_observations_staging (
        filename, tool_id, event_ts, record_type, note
    ) VALUES (%s, %s, %s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        _tool_id(row),
        _event_ts(row),
        row.get("record_type"),
        row.get("event_name") or row.get("fault_summary") or row.get("status") or "generic observation",
    ))


def _insert_rejected_records(cur, rows):
    sql = """
    INSERT INTO rejected_records (
        filename, record_type, validation_reason
    ) VALUES (%s, %s, %s)
    """
    return _insert_many(cur, sql, rows, lambda row: (
        row.get("filename"),
        row.get("record_type"),
        row.get("validation_reason"),
    ))


def _insert_many(cur, sql, rows, mapper):
    count = 0
    for row in rows:
        cur.execute(sql, mapper(row))
        count += 1
    return count


def _tool_id(row: dict[str, Any]) -> Any:
    for key in ("tool_id", "equipment_id", "equipment_name", "tool", "eqp"):
        if row.get(key) not in (None, "", [], {}):
            return row.get(key)
    return None


def _event_ts(row: dict[str, Any]) -> Any:
    for key in ("timestamp", "event_ts", "ts", "datetime", "time"):
        if row.get(key) not in (None, "", [], {}):
            return row.get(key)
    return None


def _float_or_none(value: Any):
    if value in (None, "", [], {}):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _int_or_none(value: Any):
    if value in (None, "", [], {}):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _text_or_none(value: Any):
    if value in (None, "", [], {}):
        return None
    return str(value)