from __future__ import annotations

from typing import Any


ALLOWED_RECORD_TYPES = {
    "equipment_state",
    "process_parameter_recipe",
    "sensor_reading",
    "fault_event",
    "wafer_processing_sequence",
}


def validate_extracted_records(chunk_result: dict[str, Any]) -> dict[str, Any]:
    chunk_id = chunk_result.get("chunk_id")
    raw_records = chunk_result.get("records", [])

    validated_records: list[dict[str, Any]] = []

    for record in raw_records:
        if not isinstance(record, dict):
            continue

        record_type = record.get("record_type")
        confidence = record.get("confidence")
        evidence_text = record.get("evidence_text")
        data = record.get("data")

        if record_type not in ALLOWED_RECORD_TYPES:
            continue

        if not isinstance(confidence, (int, float)):
            continue

        confidence = float(confidence)
        if confidence < 0.0 or confidence > 1.0:
            continue

        if not isinstance(evidence_text, str) or not evidence_text.strip():
            continue

        if not isinstance(data, dict) or not data:
            continue

        normalized = _normalize_record(record_type, data)
        if not normalized:
            continue

        if not _passes_type_minimums(record_type, normalized):
            continue

        validated_records.append(
            {
                "record_type": record_type,
                "source_reference": f"chunk:{chunk_id}",
                "confidence": round(confidence, 3),
                "evidence_text": evidence_text.strip(),
                "data": normalized,
            }
        )

    return {
        "chunk_id": chunk_id,
        "records": validated_records,
        "debug": chunk_result.get("debug", {}),
    }


def _normalize_record(record_type: str, data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(data)

    if record_type == "equipment_state":
        normalized = _rename_keys(
            normalized,
            {
                "state": "curr_state",
                "equipment_state": "curr_state",
                "from_state": "prev_state",
                "to_state": "curr_state",
                "previous_state": "prev_state",
                "resulting_state": "curr_state",
            },
        )

    elif record_type == "fault_event":
        normalized = _rename_keys(
            normalized,
            {
                "event": "fault_summary",
            },
        )

    elif record_type == "wafer_processing_sequence":
        normalized = _rename_keys(
            normalized,
            {
                "process_step": "step",
                "wafer_id": "wafer",
                "lot_id": "lot",
            },
        )

    elif record_type == "sensor_reading":
        normalized = _rename_keys(
            normalized,
            {
                "observed_value": "value",
                "parameter_name": "parameter",
            },
        )

    elif record_type == "process_parameter_recipe":
        normalized = _rename_keys(
            normalized,
            {
                "process_step": "step",
            },
        )

    # Drop empty/null-like values
    cleaned: dict[str, Any] = {}
    for k, v in normalized.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        cleaned[k] = v

    return cleaned


def _rename_keys(data: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in data.items():
        new_key = mapping.get(key, key)
        if new_key not in out:
            out[new_key] = value
    return out


def _passes_type_minimums(record_type: str, data: dict[str, Any]) -> bool:
    keys = set(data.keys())

    if record_type == "equipment_state":
        return bool(
            {"prev_state", "curr_state", "from_state", "to_state", "state"} & keys
        )

    if record_type == "process_parameter_recipe":
        strong_recipe_keys = {
            "recipe",
            "recipe_id",
            "step",
            "step_name",
            "parameter",
            "setpoint",
            "duration_s",
            "target_temp_C",
            "version",
            "rev",
        }
        # Require at least one real recipe/config signal, not just one execution number
        return bool(strong_recipe_keys & keys)

    if record_type == "sensor_reading":
        measurement_keys = {
            "parameter",
            "value",
            "unit",
            "observed_value",
            "temp_C",
            "pressure_mTorr",
            "base_pres_Pa",
            "proc_pres_Pa",
            "dep_rate_A_s",
            "src_pwr_W",
            "vac_Pa",
            "dose_actual_mJcm2",
            "scan_pos_mm",
            "threshold_value",
        }
        return bool(measurement_keys & keys)

    if record_type == "fault_event":
        fault_keys = {
            "fault",
            "fault_code",
            "reason",
            "alarm_code",
            "error_code",
            "code",
            "fault_summary",
            "event",
        }
        return bool(fault_keys & keys)

    if record_type == "wafer_processing_sequence":
        sequence_keys = {
            "step",
            "step_name",
            "wafer",
            "lot",
            "slot",
            "seq",
            "status",
            "result",
            "elapsed_time_seconds_approx",
            "run_status",
        }
        return bool(sequence_keys & keys)

    return True