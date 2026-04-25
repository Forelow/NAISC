from __future__ import annotations

from pathlib import Path
import csv
import json
import xml.etree.ElementTree as ET
from typing import Any

from app.transform.canonicalizer import (
    CanonicalParseResult,
    build_canonical_record,
)


class StructuredParserError(Exception):
    pass


def parse_structured_file(file_info: dict, format_guess: str) -> CanonicalParseResult:
    file_path = file_info["raw_path"]

    if format_guess == "json":
        records = _parse_json(file_path)
        return _build_result(
            file_info=file_info,
            source_format="json",
            parser_name="json_structured_parser",
            records=records,
        )

    if format_guess == "csv":
        records = _parse_csv(file_path)
        return _build_result(
            file_info=file_info,
            source_format="csv",
            parser_name="csv_structured_parser",
            records=records,
        )

    if format_guess == "xml":
        records = _parse_xml(file_path)
        return _build_result(
            file_info=file_info,
            source_format="xml",
            parser_name="xml_structured_parser",
            records=records,
        )

    raise StructuredParserError(
        f"Unsupported structured format for current parser: {format_guess}"
    )


def _build_result(
    file_info: dict,
    source_format: str,
    parser_name: str,
    records: list[dict[str, Any]],
) -> CanonicalParseResult:
    canonical_records = []

    for index, record in enumerate(records, start=1):
        raw_fields = record.get("raw_fields", {})
        record_type = record.get("record_type", "generic_structured_log")
        source_reference = record.get("source_reference", f"record_{index}")
        confidence = record.get("confidence", 1.0)
        warnings = record.get("warnings", [])

        canonical_records.append(
            build_canonical_record(
                raw_fields=raw_fields,
                source_reference=source_reference,
                record_type=record_type,
                confidence=confidence,
                warnings=warnings,
            )
        )

    return CanonicalParseResult(
        file_id=file_info["file_id"],
        filename=file_info["filename"],
        source_format=source_format,
        parser_name=parser_name,
        parse_status="parsed",
        record_count=len(canonical_records),
        records=canonical_records,
        warnings=[],
    )


# =========================
# HELPER FUNCTIONS
# =========================

def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _get_path(obj: Any, path: str, default: Any = None) -> Any:
    """
    Read nested dict paths like:
    - Recipe.RecipeID
    - Keys.SensorID
    """
    current = obj
    for part in path.split("."):
        if not isinstance(current, dict):
            return default
        if part not in current:
            return default
        current = current[part]
    return current


def _first_available(obj: dict, paths: list[str], default: Any = None) -> Any:
    for path in paths:
        value = _get_path(obj, path, default=None)
        if not _is_missing(value):
            return value
    return default


def _pick_preferred(*values: Any, default: Any = None) -> Any:
    for value in values:
        if not _is_missing(value):
            return value
    return default


# =========================
# JSON PARSING
# =========================

def _parse_json(file_path: str) -> list[dict[str, Any]]:
    path = Path(file_path)

    with path.open("r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    if isinstance(data, dict) and "ControlJob" in data and isinstance(data["ControlJob"], dict):
        return _extract_control_job_family_records(data["ControlJob"])

    return _extract_json_records_generic(data)


def _extract_json_records_generic(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        records = []
        for i, item in enumerate(data, start=1):
            if isinstance(item, dict):
                records.append(
                    {
                        "record_type": "generic_structured_log",
                        "raw_fields": item,
                        "source_reference": f"record_{i}",
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )
            else:
                records.append(
                    {
                        "record_type": "generic_structured_log",
                        "raw_fields": {"value": item},
                        "source_reference": f"record_{i}",
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )
        return records

    if isinstance(data, dict):
        list_of_dict_candidates = [
            value
            for value in data.values()
            if isinstance(value, list)
            and value
            and all(isinstance(item, dict) for item in value)
        ]

        if len(list_of_dict_candidates) == 1:
            records = []
            for i, item in enumerate(list_of_dict_candidates[0], start=1):
                records.append(
                    {
                        "record_type": "generic_structured_log",
                        "raw_fields": item,
                        "source_reference": f"record_{i}",
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )
            return records

        return [
            {
                "record_type": "generic_structured_log",
                "raw_fields": data,
                "source_reference": "record_1",
                "confidence": 1.0,
                "warnings": [],
            }
        ]

    return [
        {
            "record_type": "generic_structured_log",
            "raw_fields": {"value": data},
            "source_reference": "record_1",
            "confidence": 1.0,
            "warnings": [],
        }
    ]


def _extract_control_job_family_records(control_job: dict[str, Any]) -> list[dict[str, Any]]:
    extracted: list[dict[str, Any]] = []

    control_job_id = _first_available(control_job, ["ControlJobID"])
    equipment_id = _first_available(control_job, ["EquipmentID", "ToolID", "MachineID"])
    operator_id = _first_available(control_job, ["OperatorID"])
    start_time = _first_available(control_job, ["StartTime"])
    end_time = _first_available(control_job, ["EndTime"])

    # 1) Control job record
    extracted.append(
        {
            "record_type": "control_job",
            "raw_fields": {
                "control_job_id": control_job_id,
                "equipment_id": equipment_id,
                "operator_id": operator_id,
                "start_time": start_time,
                "end_time": end_time,
            },
            "source_reference": "ControlJob",
            "confidence": 1.0,
            "warnings": [],
        }
    )

    process_jobs = control_job.get("ProcessJobs", [])
    for pj_index, process_job in enumerate(process_jobs, start=1):
        prjob_id = _first_available(process_job, ["PRJobID", "ProcessJobID"])
        lot_id = _first_available(process_job, ["LotID"])
        wafer_id = _first_available(process_job, ["WaferID"])
        slot_id = _first_available(process_job, ["SlotID"])

        recipe_name = _first_available(
            process_job,
            ["RecipeName", "Recipe.RecipeID", "RecipeID"]
        )
        recipe_step_name = _first_available(
            process_job,
            ["RecipeStepName", "Recipe.Type"]
        )

        extracted.append(
            {
                "record_type": "process_job",
                "raw_fields": {
                    "control_job_id": control_job_id,
                    "equipment_id": equipment_id,
                    "process_job_id": prjob_id,
                    "lot_id": lot_id,
                    "wafer_id": wafer_id,
                    "slot_id": slot_id,
                    "recipe_name": recipe_name,
                    "recipe_step_name": recipe_step_name,
                },
                "source_reference": f"ControlJob.ProcessJobs[{pj_index}]",
                "confidence": 1.0,
                "warnings": [],
            }
        )

        module_reports = process_job.get("ModuleProcessReports", [])
        for mr_index, report in enumerate(module_reports, start=1):
            keys = report.get("Keys", {})
            attrs = report.get("Attributes", {})
            events = attrs.get("Events", {})

            module_id = _first_available(keys, ["ModuleID"])
            recipe_step_id = _first_available(keys, ["RecipeStepID"])
            key_wafer_id = _first_available(keys, ["WaferID"])

            effective_slot_id = _pick_preferred(
                _first_available(attrs, ["SlotID"]),
                slot_id
            )

            effective_recipe_name = _pick_preferred(
                _first_available(attrs, ["RecipeName", "Recipe.RecipeID"]),
                recipe_name
            )

            effective_recipe_step_name = _pick_preferred(
                _first_available(attrs, ["RecipeStepName"]),
                recipe_step_name,
                recipe_step_id
            )

            base_context = {
                "control_job_id": control_job_id,
                "equipment_id": equipment_id,
                "process_job_id": prjob_id,
                "lot_id": lot_id,
                "wafer_id": wafer_id,
                "slot_id": effective_slot_id,
                "recipe_name": effective_recipe_name,
                "recipe_step_name": effective_recipe_step_name,
                "module_id": module_id,
                "recipe_step_id": recipe_step_id,
                "key_wafer_id": key_wafer_id,
            }

            # 3) Control state events
            for ev_index, event in enumerate(events.get("ControlStateEvents", []), start=1):
                event_id = _first_available(event, ["EventID"])
                event_name = _first_available(event, ["Name"])
                message = _first_available(event, ["Text", "Message", "Name"])
                timestamp = _first_available(event, ["DateTime", "Timestamp", "TimeStamp"])

                extracted.append(
                    {
                        "record_type": "control_state_event",
                        "raw_fields": {
                            **base_context,
                            "event_id": event_id,
                            "event_name": event_name,
                            "timestamp": timestamp,
                            "message": message,
                        },
                        "source_reference": (
                            f"ControlJob.ProcessJobs[{pj_index}]."
                            f"ModuleProcessReports[{mr_index}]."
                            f"Attributes.Events.ControlStateEvents[{ev_index}]"
                        ),
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )

            # 4) Alarm events
            for al_index, alarm in enumerate(events.get("Alarms", []), start=1):
                extracted.append(
                    {
                        "record_type": "alarm_event",
                        "raw_fields": {
                            **base_context,
                            "alarm_id": _first_available(alarm, ["AlarmID"]),
                            "severity": _first_available(alarm, ["Severity"]),
                            "timestamp": _first_available(alarm, ["DateTime", "Timestamp"]),
                            "message": _first_available(alarm, ["Text", "Message", "Name"]),
                            "chamber": _first_available(alarm, ["Chamber"]),
                            "value": _first_available(alarm, ["Value"]),
                            "unit": _first_available(alarm, ["Unit"]),
                            "threshold": _first_available(alarm, ["Threshold"]),
                        },
                        "source_reference": (
                            f"ControlJob.ProcessJobs[{pj_index}]."
                            f"ModuleProcessReports[{mr_index}]."
                            f"Attributes.Events.Alarms[{al_index}]"
                        ),
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )

            # 5) Error events
            for er_index, error in enumerate(events.get("Errors", []), start=1):
                extracted.append(
                    {
                        "record_type": "error_event",
                        "raw_fields": {
                            **base_context,
                            "error_id": _first_available(error, ["ErrorID"]),
                            "severity": _first_available(error, ["Severity"]),
                            "timestamp": _first_available(error, ["DateTime", "Timestamp"]),
                            "message": _first_available(error, ["Text", "Message", "Name"]),
                            "expected_value": _first_available(error, ["ExpectedValue"]),
                            "actual_value": _first_available(error, ["ActualValue"]),
                            "unit": _first_available(error, ["Unit"]),
                        },
                        "source_reference": (
                            f"ControlJob.ProcessJobs[{pj_index}]."
                            f"ModuleProcessReports[{mr_index}]."
                            f"Attributes.Events.Errors[{er_index}]"
                        ),
                        "confidence": 1.0,
                        "warnings": [],
                    }
                )

            # 6) Sensor measurements
            sensor_data = report.get("SensorData", [])
            for sd_index, sensor in enumerate(sensor_data, start=1):
                sensor_id = _first_available(sensor, ["SensorID", "Keys.SensorID"])
                sensor_name = _first_available(sensor, ["SensorName"])
                sensor_unit = _first_available(sensor, ["Unit"])

                measurements = sensor.get("Measurements", [])
                for ms_index, measurement in enumerate(measurements, start=1):
                    extracted.append(
                        {
                            "record_type": "sensor_measurement",
                            "raw_fields": {
                                **base_context,
                                "sensor_id": sensor_id,
                                "sensor_name": sensor_name,
                                "unit": sensor_unit,
                                "timestamp": _first_available(
                                    measurement,
                                    ["DateTime", "Timestamp", "TimeStamp"]
                                ),
                                "value": _first_available(measurement, ["Value"]),
                            },
                            "source_reference": (
                                f"ControlJob.ProcessJobs[{pj_index}]."
                                f"ModuleProcessReports[{mr_index}]."
                                f"SensorData[{sd_index}].Measurements[{ms_index}]"
                            ),
                            "confidence": 1.0,
                            "warnings": [],
                        }
                    )

    return extracted


# =========================
# CSV PARSING
# =========================

def _parse_csv(file_path: str) -> list[dict[str, Any]]:
    path = Path(file_path)

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        return [
            {
                "record_type": "generic_structured_log",
                "raw_fields": dict(row),
                "source_reference": f"record_{i}",
                "confidence": 1.0,
                "warnings": [],
            }
            for i, row in enumerate(reader, start=1)
        ]


# =========================
# XML PARSING
# =========================

def _parse_xml(file_path: str) -> list[dict[str, Any]]:
    path = Path(file_path)
    tree = ET.parse(path)
    root = tree.getroot()

    candidate_nodes = _find_xml_record_nodes(root)
    return [
        {
            "record_type": "generic_structured_log",
            "raw_fields": _xml_element_to_dict(node),
            "source_reference": f"record_{i}",
            "confidence": 1.0,
            "warnings": [],
        }
        for i, node in enumerate(candidate_nodes, start=1)
    ]


def _find_xml_record_nodes(root: ET.Element) -> list[ET.Element]:
    children = list(root)

    if not children:
        return [root]

    child_tags = [child.tag for child in children]

    if len(set(child_tags)) == 1 and len(children) > 1:
        return children

    for child in children:
        grand_children = list(child)
        if not grand_children:
            continue
        grand_tags = [gc.tag for gc in grand_children]
        if len(set(grand_tags)) == 1 and len(grand_children) > 1:
            return grand_children

    return [root]


def _xml_element_to_dict(element: ET.Element) -> dict[str, Any]:
    result: dict[str, Any] = {}

    if element.attrib:
        for key, value in element.attrib.items():
            result[f"@{key}"] = value

    children = list(element)
    text = (element.text or "").strip()

    if not children:
        if result:
            if text:
                result["#text"] = text
            return result
        return {element.tag: text}

    grouped_children: dict[str, list[Any]] = {}

    for child in children:
        child_value = _xml_element_to_dict(child)
        grouped_children.setdefault(child.tag, []).append(child_value)

    for tag, values in grouped_children.items():
        if len(values) == 1:
            result[tag] = values[0]
        else:
            result[tag] = values

    if text:
        result["#text"] = text

    return result