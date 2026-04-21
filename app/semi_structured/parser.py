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

    if boundary_type == "blank_line_blocks":
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
            if field not in {"severity", "message", "ts", "tool_id", "channel"}
        }
        if required_any_fields and (required_any_fields & keys):
            field_matches.append(record_type)

    for preferred in priority:
        if preferred in field_matches:
            return preferred

    if event_code in {"ALARM", "FAULT", "ALARM_CLR", "FAULT_CLEAR", "INTERLOCK"}:
        return "fault_event"
    if severity in {"ERROR", "ERR", "CRITICAL", "ALERT", "EMERG", "FATAL"} and ({"fault", "code", "reason"} & keys):
        return "fault_event"

    if event_code in {"STATE", "EQUIP_STATE", "SYS_BOOT", "STARTUP", "SHUTDOWN"} or {"from", "to", "prev", "curr", "state", "from_state", "to_state"} & keys:
        return "equipment_state"

    if event_code in {"RECIPE", "RECIPE_DEFINE", "RECIPE_STEP", "RCP_LOAD", "RCP_PARAM"} or {"recipe", "recipe_id", "name", "rev", "parameter", "setpoint", "target_temp_C"} & keys:
        return "process_parameter_recipe"

    if event_code in {"SENSOR", "TRACE", "READING", "MEASURE"} or {"sensor", "sensor_id", "value", "reading", "temp_C", "pressure_mTorr", "base_pres_Pa", "proc_pres_Pa", "dep_rate_A_s"} & keys:
        return "sensor_reading"

    if event_code in {
        "WAFER", "WAFER_LOAD", "WAFER_RESULT",
        "STEP", "STEP_START", "STEP_END", "STEP_ABORT",
        "WFR_SEQ", "WF_MOVE", "ACTION",
        "LOAD_COMPLETE", "UNLOAD_COMPLETE",
        "LOT_IN", "LOT_OUT", "LOT_RESUME", "LOT_START",
        "MAINT"
    } or {"wafer", "wafer_id", "wfr", "step", "step_name", "seq", "action", "slot", "lot"} & keys:
        return "wafer_processing_sequence"

    return "generic_text_record"