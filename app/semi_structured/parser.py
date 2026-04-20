from __future__ import annotations

import re
from typing import Any
import re


KV_PAIR_RE = re.compile(r'([\w.-]+)=(".*?"|[^\s]+)')

SYSLOG_LINE_RE = re.compile(
    r"""
    ^\s*
    (?P<ts>\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)
    \s+
    (?P<tool_id>[A-Za-z0-9_.-]+)
    \s+
    (?P<third>[A-Za-z0-9_.-]+)
    (?P<rest>.*)
    $
    """,
    re.VERBOSE,
)

UPPER_TOKEN_RE = re.compile(r"^[A-Z][A-Z0-9_.-]*$")


def parse_semi_structured(
    text_payload: dict[str, Any],
    family_info: dict[str, Any],
) -> dict[str, Any]:
    family = family_info.get("family")

    if family == "kv_line_log":
        return _parse_kv_line_log(text_payload)

    if family == "syslog_like_log":
        return _parse_syslog_like_log(text_payload)

    return {
        "schema_family": "semi_structured_log",
        "records": [],
        "record_count": 0,
        "status": f"family_not_implemented:{family}",
    }


def _parse_kv_line_log(text_payload: dict[str, Any]) -> dict[str, Any]:
    records = []

    for line in text_payload.get("lines", []):
        raw = line["text"].strip()
        if not raw:
            continue

        matches = KV_PAIR_RE.findall(raw)
        if not matches:
            continue

        parsed = {}
        for key, value in matches:
            parsed[key] = _coerce_value(value)

        record_type = _classify_record_type(parsed)

        records.append(
            {
                "record_type": record_type,
                "source_reference": f"line:{line['line_no']}",
                "data": parsed,
            }
        )

    return {
        "schema_family": "semi_structured_log",
        "records": records,
        "record_count": len(records),
    }


def _classify_record_type(data: dict[str, Any]) -> str:
    keys = set(data.keys())

    if {"from_state", "to_state"} & keys or "state" in keys:
        return "equipment_state"

    if {"sensor", "sensor_id", "value"} & keys and ("unit" in keys or "timestamp" in keys or "ts" in keys):
        return "sensor_reading"

    if {"fault", "fault_code", "alarm_code", "severity"} & keys:
        return "fault_event"

    if {"recipe", "recipe_id", "parameter", "setpoint"} & keys:
        return "process_parameter_recipe"

    if {"wafer_id", "step", "step_name", "seq", "action"} & keys:
        return "wafer_processing_sequence"

    return "generic_text_record"


def _coerce_value(value: str) -> Any:
    value = value.strip()

    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]

    if value == "":
        return None

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if "." in value or "e" in lowered:
            return float(value)
        return int(value)
    except ValueError:
        return value
    
def _parse_syslog_like_log(text_payload: dict[str, Any]) -> dict[str, Any]:
    records = []

    for line in text_payload.get("lines", []):
        raw = line["text"].strip()
        if not raw:
            continue

        match = SYSLOG_LINE_RE.match(raw)
        if not match:
            continue

        ts = match.group("ts")
        tool_id = match.group("tool_id")
        third = match.group("third")
        rest = (match.group("rest") or "").strip()

        data: dict[str, Any] = {
            "ts": ts,
            "tool_id": tool_id,
        }

        rest_kv = dict((k, _coerce_value(v)) for k, v in KV_PAIR_RE.findall(rest))
        rest_without_kv = KV_PAIR_RE.sub("", rest).strip()
        upper_rest_tokens = rest_without_kv.split()

        if third in {"INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}:
            data["severity"] = third

            if upper_rest_tokens and UPPER_TOKEN_RE.match(upper_rest_tokens[0]):
                data["fault_code"] = upper_rest_tokens[0]
                message = " ".join(upper_rest_tokens[1:]).strip()
            else:
                message = rest_without_kv

            if message:
                data["message"] = message

        elif third in {"STATE", "STEP", "SENSOR", "ALARM", "EVENT"}:
            data["event_type"] = third
            if rest_without_kv:
                data["message"] = rest_without_kv

        else:
            data["event_type"] = third
            if rest_without_kv:
                data["message"] = rest_without_kv

        data.update(rest_kv)

        record_type = _classify_syslog_record_type(data)

        records.append(
            {
                "record_type": record_type,
                "source_reference": f"line:{line['line_no']}",
                "data": data,
            }
        )

    return {
        "schema_family": "semi_structured_log",
        "records": records,
        "record_count": len(records),
    }

def _classify_syslog_record_type(data: dict[str, Any]) -> str:
    keys = set(data.keys())

    if "severity" in keys or "fault_code" in keys or data.get("event_type") == "ALARM":
        return "fault_event"

    if {"from_state", "to_state"} & keys or data.get("event_type") == "STATE" or "state" in keys:
        return "equipment_state"

    if (
        {"sensor", "sensor_id", "value"} & keys
        and ("unit" in keys or "ts" in keys)
    ) or data.get("event_type") == "SENSOR":
        return "sensor_reading"

    if (
        {"wafer_id", "step", "step_name", "seq", "action"} & keys
        or data.get("event_type") == "STEP"
    ):
        return "wafer_processing_sequence"

    if {"recipe", "recipe_id", "parameter", "setpoint"} & keys:
        return "process_parameter_recipe"

    return "generic_text_record"