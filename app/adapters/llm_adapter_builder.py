from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Any

from dotenv import load_dotenv

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

from adapters.fingerprint import build_schema_fingerprint
from adapters.spec_schema import (
    validate_adapter_spec,
    ALLOWED_CANONICAL_TYPES,
    ALLOWED_CANONICAL_FIELDS,
)

load_dotenv()


def build_adapter_spec(
    result_payload: dict[str, Any],
    validation_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schema_fingerprint = build_schema_fingerprint(result_payload)
    structure_summary = result_payload.get("structure_summary", {}) or {}
    structure_config = result_payload.get("structure_config", {}) or {}
    detection = result_payload.get("detection", {}) or {}
    parsed_result = result_payload.get("parsed_result", {}) or {}

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return enrich_adapter_spec(_fallback_spec(schema_fingerprint, structure_config), result_payload)

    sample_records = _sample_records_by_source_type(parsed_result.get("records", []) or [])
    validation_summary = _summarize_validation_feedback(validation_feedback)

    system_prompt = (
        "You are a schema adaptation planner for semiconductor tool-log parsing. "
        "Your job is NOT to parse full logs. "
        "Your job is to create a deterministic adapter specification that maps parser output into a fixed canonical model. "
        "Return JSON only. "
        "Allowed canonical record types are: "
        + ", ".join(sorted(ALLOWED_CANONICAL_TYPES))
        + ". "
        "Allowed field alias targets are: "
        + ", ".join(sorted(ALLOWED_CANONICAL_FIELDS))
        + ". "
        "Use direct record_type_mapping for simple parser-local type conversion. "
        "Use dispatch_rules when one source type must be dispatched based on a field such as EventCategory or @sev. "
        "Use field_aliases only to map source/vendor field names into the allowed canonical fields. "
        "Use fallback_rules only for copy-if-missing repairs, for example copy sensor_id into parameter if parameter is missing. "
        "Do not invent record types or alias targets outside the allowed sets."
    )

    user_payload = {
        "schema_fingerprint": schema_fingerprint,
        "schema_family": structure_config.get("schema_family"),
        "format_guess": detection.get("format_guess"),
        "structure_summary": structure_summary,
        "record_groups": structure_config.get("record_groups", []),
        "sample_records_by_source_type": sample_records,
        "validation_feedback": validation_summary,
        "allowed_canonical_record_types": sorted(ALLOWED_CANONICAL_TYPES),
        "allowed_field_alias_targets": sorted(ALLOWED_CANONICAL_FIELDS),
    }

    try:
        client = OpenAI(api_key=api_key)
        model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

        if hasattr(client, "responses"):
            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
                ],
            )
            output_text = getattr(response, "output_text", None)
            parsed = json.loads(output_text) if output_text else {}
        else:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
                ],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
            parsed = json.loads(content) if content else {}

        parsed["schema_fingerprint"] = schema_fingerprint
        if not parsed.get("schema_family"):
            parsed["schema_family"] = str(structure_config.get("schema_family", "") or "")

        return enrich_adapter_spec(parsed, result_payload)

    except Exception:
        return enrich_adapter_spec(_fallback_spec(schema_fingerprint, structure_config), result_payload)


def enrich_adapter_spec(adapter_spec: dict[str, Any], result_payload: dict[str, Any]) -> dict[str, Any]:
    structure_config = result_payload.get("structure_config", {}) or {}
    schema_fingerprint = build_schema_fingerprint(result_payload)

    spec = validate_adapter_spec(adapter_spec or {})
    spec["schema_fingerprint"] = spec.get("schema_fingerprint") or schema_fingerprint
    spec["schema_family"] = spec.get("schema_family") or str(structure_config.get("schema_family", "") or "")

    _apply_builtin_defaults(spec, result_payload)

    return validate_adapter_spec(spec)


def _apply_builtin_defaults(spec: dict[str, Any], result_payload: dict[str, Any]) -> None:
    structure_summary = result_payload.get("structure_summary", {}) or {}
    structure_config = result_payload.get("structure_config", {}) or {}

    columns = {str(c) for c in (structure_summary.get("columns", []) or [])}
    record_groups = structure_config.get("record_groups", []) or []
    source_types = {
        str(group.get("record_type"))
        for group in record_groups
        if isinstance(group, dict) and group.get("record_type")
    }

    # ---------- Stable JSON / structured defaults ----------
    _ensure_mapping(spec, "process_job", "wafer_processing_sequence")
    _ensure_mapping(spec, "sensor_measurement", "sensor_reading")
    _ensure_mapping(spec, "alarm_event", "fault_event")
    _ensure_mapping(spec, "error_event", "fault_event")
    _ensure_mapping(spec, "control_state_event", "equipment_state")

    for src, dst in [
        ("LotID", "lot"),
        ("WaferID", "wafer"),
        ("SlotID", "slot"),
        ("Recipe.RecipeID", "recipe"),
        ("RecipeName", "recipe"),
        ("RecipeStepName", "step"),
        ("DateTime", "timestamp"),
        ("Value", "value"),
        ("Unit", "unit"),
        ("Text", "event_name"),
        ("Name", "event_name"),
        ("SensorID", "sensor_id"),
        ("SensorName", "parameter"),
        ("AlarmID", "fault_code"),
        ("ErrorID", "fault_code"),
        ("Severity", "severity"),
        ("ControlJob.EquipmentID", "tool_id"),
        ("EquipmentID", "tool_id"),
        ("ToState", "to_state"),
        ("FromState", "from_state"),
    ]:
        _ensure_alias(spec, src, dst)

    _ensure_fallback(spec, "sensor_reading", "parameter", "sensor_id")
    _ensure_fallback(spec, "equipment_state", "curr_state", "event_name")

    # ---------- CSV defaults ----------
    if "csv_row" in source_types:
        # Event-style CSV
        if "EventCategory" in columns:
            _merge_dispatch_rule(
                spec,
                "csv_row",
                "EventCategory",
                {
                    "ALARM": "fault_event",
                    "FAULT": "fault_event",
                    "ERROR": "fault_event",
                    "WARNING": "fault_event",
                    "CRITICAL": "fault_event",
                    "EVENT": "generic_operational_observation",
                    "INFO": "generic_operational_observation",
                },
            )

            for src, dst in [
                ("EventDateTime", "timestamp"),
                ("EquipmentName", "tool_id"),
                ("AlarmCode", "fault_code"),
                ("AlarmLevel", "severity"),
                ("AlarmDescription", "fault_summary"),
                ("AffectedComponent", "component"),
                ("MeasuredValue", "value"),
                ("ExpectedValue", "expected_value"),
                ("MeasureUnit", "unit"),
                ("LotNumber", "lot"),
                ("WaferNumber", "wafer"),
                ("AutoResponse", "action"),
            ]:
                _ensure_alias(spec, src, dst)

        # Step / execution style CSV
        elif {"tool_id", "lot_id", "wafer_id", "step_name", "step_status"} <= columns:
            _ensure_mapping(spec, "csv_row", "wafer_processing_sequence")
            for src, dst in [
                ("tool_id", "tool_id"),
                ("lot_id", "lot"),
                ("wafer_id", "wafer"),
                ("slot_no", "slot"),
                ("recipe_name", "recipe"),
                ("step_name", "step"),
                ("step_status", "status"),
                ("duration_s", "duration_s"),
            ]:
                _ensure_alias(spec, src, dst)

    # ---------- XML defaults ----------
    _ensure_mapping(spec, "process_step", "wafer_processing_sequence")
    _ensure_mapping(spec, "wafer", "wafer_processing_sequence")
    _ensure_mapping(spec, "wafer_measurement", "sensor_reading")
    _ensure_mapping(spec, "recipe_param", "process_parameter_recipe")
    _ensure_mapping(spec, "setpoint", "process_parameter_recipe")
    _ensure_mapping(spec, "fault_summary", "fault_event")
    _ensure_mapping(spec, "equipment_state_transition", "equipment_state")

    if "wafer_event" in source_types:
        _merge_dispatch_rule(
            spec,
            "wafer_event",
            "@sev",
            {
                "E": "fault_event",
                "ERROR": "fault_event",
                "W": "fault_event",
                "WARN": "fault_event",
                "WARNING": "fault_event",
                "CRITICAL": "fault_event",
                "I": "generic_operational_observation",
                "INFO": "generic_operational_observation",
            },
        )

    for src, dst in [
        ("@sensor", "parameter"),
        ("@timestamp", "timestamp"),
        ("@unit", "unit"),
        ("@value", "value"),
        ("@name", "parameter"),
        ("@u", "unit"),
        ("@v", "value"),
        ("@stepName", "step"),
        ("StepOutcome", "status"),
        ("FabricationRecord.ProductionOrder.LotID", "lot"),
        ("FabricationRecord.Hdr.Lot", "lot"),
        ("FabricationRecord.Equipment.EquipmentID", "tool_id"),
        ("RcpExecLog.@eqp", "tool_id"),
        ("RcpExecLog.Hdr.Lot", "lot"),
        ("RcpExecLog.Hdr.Rcp", "recipe"),
        ("RcpExecLog.WfrLog.W[].@id", "wafer"),
        ("@id", "wafer"),
        ("@n", "slot"),
        ("@cd", "fault_code"),
        ("FaultCode", "fault_code"),
        ("Description", "fault_summary"),
        ("@msg", "fault_summary"),
        ("Timestamp", "timestamp"),
        ("@t", "timestamp"),
        ("Severity", "severity"),
        ("@sev", "severity"),
        ("ToState", "to_state"),
        ("FromState", "from_state"),
    ]:
        _ensure_alias(spec, src, dst)

    _ensure_fallback(spec, "sensor_reading", "parameter", "sensor_id")
    _ensure_fallback(spec, "equipment_state", "curr_state", "event_name")


def _ensure_mapping(spec: dict[str, Any], source_record_type: str, target_record_type: str) -> None:
    if source_record_type not in spec["record_type_mapping"]:
        spec["record_type_mapping"][source_record_type] = target_record_type


def _ensure_alias(spec: dict[str, Any], source_key: str, canonical_key: str) -> None:
    if canonical_key in ALLOWED_CANONICAL_FIELDS and source_key not in spec["field_aliases"]:
        spec["field_aliases"][source_key] = canonical_key


def _ensure_fallback(spec: dict[str, Any], when_record_type: str, if_missing: str, copy_from: str) -> None:
    wanted = {
        "when_record_type": when_record_type,
        "if_missing": if_missing,
        "copy_from": copy_from,
    }
    if wanted not in spec["fallback_rules"]:
        spec["fallback_rules"].append(wanted)


def _merge_dispatch_rule(
    spec: dict[str, Any],
    source_record_type: str,
    field: str,
    mapping: dict[str, str],
) -> None:
    for rule in spec["dispatch_rules"]:
        if rule.get("source_record_type") == source_record_type and rule.get("field") == field:
            current = rule.get("map", {}) or {}
            for k, v in mapping.items():
                if k not in current:
                    current[k] = v
            rule["map"] = current
            return

    spec["dispatch_rules"].append(
        {
            "source_record_type": source_record_type,
            "field": field,
            "map": dict(mapping),
        }
    )


def _sample_records_by_source_type(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for record in records:
        if not isinstance(record, dict):
            continue

        src_type = str(record.get("record_type", "") or "unknown")
        if len(by_type[src_type]) >= 3:
            continue

        data = dict(record.get("data", {}) or {})
        by_type[src_type].append(
            {
                "source_reference": record.get("source_reference"),
                "keys": sorted(list(data.keys()))[:40],
                "sample_data": data,
            }
        )

    return dict(by_type)


def _summarize_validation_feedback(validation_feedback: dict[str, Any] | None) -> dict[str, Any]:
    if not validation_feedback:
        return {}

    rejected = validation_feedback.get("rejected_records", []) or []
    reasons: dict[str, int] = {}

    for row in rejected:
        reason = str(row.get("validation_reason", "unknown") or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1

    return {
        "accepted_count": validation_feedback.get("accepted_count", 0),
        "rejected_count": validation_feedback.get("rejected_count", 0),
        "top_rejection_reasons": reasons,
    }


def _fallback_spec(schema_fingerprint: str, structure_config: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_fingerprint": schema_fingerprint,
        "schema_family": str(structure_config.get("schema_family", "") or ""),
        "record_type_mapping": {},
        "dispatch_rules": [],
        "field_aliases": {},
        "fallback_rules": [],
    }