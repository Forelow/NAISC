from __future__ import annotations

from typing import Any

from semi_structured.spec_builder import preview_parse_record


def parse_semi_structured(
    text_payload: dict[str, Any],
    family_info: dict[str, Any],
    parse_spec: dict[str, Any],
) -> dict[str, Any]:
    records = []

    boundary_type = (parse_spec.get("record_boundary") or {}).get("type", "per_line")

    if boundary_type == "timestamp_started_blocks":
        for block_no, block_text in _iter_timestamp_started_blocks(text_payload):
            data = preview_parse_record(block_text, parse_spec)
            if not data:
                continue
            record_type = _classify_record_type(data, parse_spec)
            records.append(
                {
                    "record_type": record_type,
                    "source_reference": f"block:{block_no}",
                    "data": data,
                }
            )
    elif boundary_type == "blank_line_blocks":
        for block_no, block_text in _iter_blank_line_blocks(text_payload):
            data = preview_parse_record(block_text, parse_spec)
            if not data:
                continue
            record_type = _classify_record_type(data, parse_spec)
            records.append(
                {
                    "record_type": record_type,
                    "source_reference": f"block:{block_no}",
                    "data": data,
                }
            )
    else:
        for item in text_payload.get("lines", []):
            raw = item.get("text", "").strip()
            if not raw:
                continue
            data = preview_parse_record(raw, parse_spec)
            if not data:
                continue
            record_type = _classify_record_type(data, parse_spec)
            records.append(
                {
                    "record_type": record_type,
                    "source_reference": f"line:{item['line_no']}",
                    "data": data,
                }
            )

    return {
        "schema_family": "semi_structured_log",
        "records": records,
        "record_count": len(records),
    }
def _iter_timestamp_started_blocks(text_payload: dict[str, Any]):
    import re

    iso_start_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    block_no = 0
    current: list[str] = []

    for item in text_payload.get("lines", []):
        text = item.get("text", "").rstrip()
        stripped = text.strip()

        if not stripped:
            continue

        # Ignore file metadata/comments when not already inside a record
        if stripped.startswith(";") and not current:
            continue

        if iso_start_re.match(stripped):
            if current:
                block_no += 1
                yield block_no, "\n".join(current).strip()
            current = [text]
        else:
            if current:
                current.append(text)

    if current:
        block_no += 1
        yield block_no, "\n".join(current).strip()

def _iter_blank_line_blocks(text_payload: dict[str, Any]):
    block_no = 0
    current: list[str] = []
    for item in text_payload.get("lines", []):
        text = item.get("text", "")
        if text.strip():
            current.append(text.rstrip())
        elif current:
            block_no += 1
            yield block_no, "\n".join(current).strip()
            current = []
    if current:
        block_no += 1
        yield block_no, "\n".join(current).strip()


def _classify_record_type(data: dict[str, Any], parse_spec: dict[str, Any]) -> str:
    keys = set(data.keys())
    event_code = str(data.get("event_code", "")).upper()
    event_type = str(data.get("event_type", "")).upper()
    severity = str(data.get("severity", "")).upper()
    event_prefix = str(data.get("event_prefix", "")).upper()

    classification_hints = parse_spec.get("classification_hints") or {}

    event_code_matches: list[str] = []
    for record_type, hint in classification_hints.items():
        if not isinstance(hint, dict):
            continue
        hint_codes = {str(x).upper() for x in hint.get("event_codes", []) if x}
        if event_code and event_code in hint_codes:
            event_code_matches.append(record_type)
        elif event_type and event_type in hint_codes:
            event_code_matches.append(record_type)

    priority = [
        "fault_event",
        "equipment_state",
        "process_parameter_recipe",
        "sensor_reading",
        "wafer_processing_sequence",
        "generic_text_record",
    ]
    for preferred in priority:
        if preferred in event_code_matches:
            return preferred

    field_matches: list[str] = []
    for record_type, hint in classification_hints.items():
        if not isinstance(hint, dict):
            continue
        required_any_fields = {
            field
            for field in hint.get("required_any_fields", [])
            if field not in {"severity", "message", "ts", "tool_id", "channel", "context"}
        }
        if required_any_fields and (required_any_fields & keys):
            field_matches.append(record_type)

    # Vendor C: event prefix helps
    if event_prefix.startswith("SENSOR"):
        return "sensor_reading"
    if event_prefix.startswith("SYSTEM WARNING") or event_prefix.startswith("SYSTEM ERROR"):
        if {"fault", "reason", "alarm_code", "code"} & keys or event_code in {"ALARM_CLEARED", "STEP_ABORT", "STATE_CHANGE"}:
            return "fault_event"

    # Strong explicit mappings
    if event_code in {"STATE", "EQUIP_STATE", "STATE_CHANGE", "SYS_BOOT", "STARTUP", "SHUTDOWN"}:
        if "fault" in keys:
            return "fault_event"
        return "equipment_state"

    if event_code in {"RECIPE", "RECIPE_DEFINE", "RECIPE_STEP", "RECIPE_LOADED", "RCP_LOAD", "RCP_PARAM"}:
        return "process_parameter_recipe"

    if event_code in {"SENSOR", "TRACE", "READING", "MEASURE", "PERIODIC"}:
        return "sensor_reading"

    if event_code in {"ALARM", "FAULT", "ALARM_CLR", "ALARM_CLEARED", "FAULT_CLEAR", "INTERLOCK"}:
        return "fault_event"
    
    if event_prefix.startswith("SYSTEM WARNING") or event_prefix.startswith("SYSTEM ERROR"):
        return "fault_event"

    if event_code in {"SRC_PWR_DROOP", "ALARM_CLEARED"}:
        return "fault_event"

    if event_code in {
        "WAFER", "WAFER_LOAD", "WAFER_RESULT", "WAFER_LOADED", "WAFER_UNLOADED",
        "STEP", "STEP_START", "STEP_END", "STEP_ABORT",
        "WFR_SEQ", "WF_MOVE", "ACTION",
        "LOAD_COMPLETE", "UNLOAD_COMPLETE",
        "LOT_IN", "LOT_OUT", "LOT_RESUME", "LOT_START",
        "MAINT"
    }:
        return "wafer_processing_sequence"

    for preferred in priority:
        if preferred in field_matches:
            return preferred

    if severity in {"ERROR", "ERR", "CRITICAL", "ALERT", "EMERG", "FATAL", "WRN", "WARN", "WARNING"} and ({"fault", "code", "reason", "alarm_code"} & keys):
        return "fault_event"

    if {"from", "to", "prev", "curr", "state", "from_state", "to_state"} & keys:
        if "fault" in keys:
            return "fault_event"
        return "equipment_state"

    if {"recipe", "recipe_id", "name", "rev", "parameter", "setpoint", "target_temp_C", "version"} & keys:
        return "process_parameter_recipe"

    if {"sensor", "sensor_id", "value", "reading", "temp_C", "pressure_mTorr", "base_pres_Pa", "proc_pres_Pa", "dep_rate_A_s", "src_pwr_W", "vac_Pa", "dose_actual_mJcm2"} & keys:
        return "sensor_reading"

    if {"wafer", "wafer_id", "wfr", "step", "step_name", "seq", "action", "slot", "lot"} & keys:
        return "wafer_processing_sequence"

    return "generic_text_record"