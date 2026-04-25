from __future__ import annotations

import re
from typing import Any


FIELD_ALIASES = {
    "tool_id": {
        "tool_id", "tool", "machine", "equip_id", "equipment_id", "equipment", "toolname", "tool_name",
        "controljob.equipmentid", "keys.moduleid", "@tool", "eventlog.@toolid", "eqp_name"
    },
    "timestamp": {
        "timestamp", "time", "ts", "timestamp_hours", "time_h", "approx_time", "timestamp_approx", "event_time",
        "datetime", "@ts", "sample_ts"
    },
    "wafer": {
        "wafer", "wafer_id", "unit_id", "item_id", "material_id", "wfr",
        "controljob.processjobs[].waferid", "keys.waferid", "controljob.processjobs[].moduleprocessreports[].keys.waferid",
        "@wafer", "wfr_ref", "waferid", "keyswaferid"
    },
    "lot": {
        "lot", "lot_id", "controljob.processjobs[].lotid",
        "eventlog.@lotid", "lot_ref", "lotid"
    },
    "parameter": {
        "parameter", "measurement", "measurement_name", "metric", "sensor",
        "controljob.processjobs[].moduleprocessreports[].sensordata[].sensorname",
        "param"
    },
    "value": {"value", "measured_value", "reading", "eng_val"},
    "unit": {
        "unit", "units",
        "controljob.processjobs[].moduleprocessreports[].sensordata[].unit",
        "uom"
    },
    "curr_state": {
        "curr_state", "state", "new_state", "system_state", "equipment_state", "status",
        "@currst"
    },
    "prev_state": {
        "prev_state", "from_state", "previous_state",
        "@prevst"
    },
    "event_name": {
        "event_name", "event", "text",
        "@msg", "@action", "name"
    },
    "step": {
        "step", "process_step", "recipestepname", "attributes.recipestepname",
        "@step", "process_stage"
    },
    "fault_code": {"fault_code", "error_code", "alarm_code", "code", "alarmid", "errorid"},
    "fault_summary": {
        "fault_summary", "fault_description", "message", "issue", "text",
        "@msg"
    },
    "recipe": {
        "recipe", "recipename", "attributes.recipename",
        "eventlog.@recipeid", "recipeid", "recipe.recipeid"
    },
    "slot": {"slot", "slotid", "attributes.slotid"},
    "severity": {"severity", "qc_flag"},
    "sensor_id": {"sensorid", "keys.sensorid"},
}


_EMBEDDED_UNIT_SUFFIXES = {
    # temperature
    "c": "degC",
    "degc": "degC",
    "f": "degF",
    "degf": "degF",
    "k": "K",

    # pressure
    "pa": "Pa",
    "kpa": "kPa",
    "mpa": "MPa",
    "bar": "bar",
    "mbar": "mbar",
    "atm": "atm",
    "psi": "psi",
    "torr": "Torr",
    "mtorr": "mTorr",
    "mmhg": "mmHg",

    # time
    "ns": "ns",
    "us": "us",
    "ms": "ms",
    "s": "s",
    "sec": "s",
    "min": "min",
    "h": "h",
    "hr": "h",

    # rotational speed
    "rpm": "rpm",
    "rps": "rps",
    "rads": "rad/s",

    # force
    "n": "N",
    "kn": "kN",
    "lbf": "lbf",

    # gas / liquid flow
    "sccm": "sccm",
    "slm": "slm",
    "ccmin": "cc/min",
    "mlmin": "mL/min",
    "mls": "mL/s",
    "lmin": "L/min",
    "lh": "L/h",

    # power
    "w": "W",
    "kw": "kW",
    "mw": "mW",

    # frequency
    "hz": "Hz",
    "khz": "kHz",
    "mhz": "MHz",
    "ghz": "GHz",

    # length / thickness
    "m": "m",
    "cm": "cm",
    "mm": "mm",
    "um": "um",
    "nm": "nm",
    "a": "A",

    # mass
    "kg": "kg",
    "g": "g",
    "mg": "mg",
    "ug": "ug",

    # percent
    "pct": "%",
    "percent": "%",
    "fraction": "fraction",
}


def standardize_records(canonical_batch: dict) -> dict:
    out = dict(canonical_batch)

    expanded_records = []
    for record in canonical_batch.get("records", []):
        expanded_records.extend(_standardize_record(record))

    out["records"] = expanded_records
    out["record_count"] = len(expanded_records)
    return out


def _standardize_record(record: dict) -> list[dict]:
    candidate_fields = dict(record.get("candidate_fields", {}) or {})

    standardized: dict[str, object] = {}
    raw_to_standard_map: dict[str, str] = {}
    used_keys = set()

    # First: direct alias matching
    for standard_name in FIELD_ALIASES.keys():
        if standard_name in candidate_fields and candidate_fields[standard_name] not in (None, "", [], {}):
            standardized[standard_name] = candidate_fields[standard_name]
            raw_to_standard_map[standard_name] = standard_name
            used_keys.add(standard_name)

    norm_lookup = {_canonical_key(str(k)): k for k in candidate_fields.keys()}

    for standard_name, aliases in FIELD_ALIASES.items():
        if standard_name in standardized and standardized[standard_name] not in (None, "", [], {}):
            continue

        for alias in aliases:
            alias_norm = _canonical_key(alias)
            if alias_norm in norm_lookup:
                original_key = norm_lookup[alias_norm]
                value = candidate_fields[original_key]

                if value not in (None, "", [], {}):
                    standardized[standard_name] = value
                    raw_to_standard_map[original_key] = standard_name
                    used_keys.add(original_key)
                    break

    # Second: embedded measurement extraction from keys like:
    # temp_F=90 pressure_mTorr=750 power_kW=1.2
    embedded_measurements = _extract_all_embedded_measurements(candidate_fields)

    if embedded_measurements:
        # Keep only non-measurement context as common fields
        base_standardized = {
            k: v
            for k, v in standardized.items()
            if k not in {"parameter", "value", "unit"}
        }

        exploded_records: list[dict] = []

        for measurement in embedded_measurements:
            cloned = dict(record)

            # Force these expanded records into sensor_reading lane
            cloned["record_type"] = "sensor_reading"

            measurement_standardized = dict(base_standardized)
            measurement_standardized["parameter"] = measurement["parameter"]
            measurement_standardized["value"] = measurement["value"]
            measurement_standardized["unit"] = measurement["unit"]

            cloned["standardized_fields"] = measurement_standardized

            measurement_map = dict(raw_to_standard_map)
            measurement_map[measurement["source_key"]] = "parameter/value/unit"
            cloned["raw_to_standard_map"] = measurement_map

            used_for_this = set(used_keys)
            used_for_this.add(measurement["source_key"])

            cloned["unmapped_fields"] = {
                k: v for k, v in candidate_fields.items() if k not in used_for_this
            }

            exploded_records.append(cloned)

        return exploded_records

    # Fallback: original single-record behavior
    fallback_standardized = dict(standardized)
    for key, value in candidate_fields.items():
        if key not in used_keys:
            fallback_standardized[key] = value

    out = dict(record)
    out["standardized_fields"] = fallback_standardized
    out["raw_to_standard_map"] = raw_to_standard_map
    out["unmapped_fields"] = {
        k: v for k, v in candidate_fields.items() if k not in raw_to_standard_map
    }

    return [out]


def _extract_all_embedded_measurements(candidate_fields: dict[str, object]) -> list[dict[str, object]]:
    extracted: list[dict[str, object]] = []

    for raw_key, raw_value in candidate_fields.items():
        if not _is_scalar_measurement_value(raw_value):
            continue

        key_text = str(raw_key).strip()
        if not key_text:
            continue

        parts = re.split(r"[_\-\s/]+", key_text)
        parts = [p for p in parts if p]
        if len(parts) < 2:
            continue

        parts_lower = [p.lower() for p in parts]

        # Try 2-token suffix first, e.g. L_h, mL_s, cc_min, rad_s
        if len(parts_lower) >= 3:
            suffix2 = "".join(parts_lower[-2:])
            if suffix2 in _EMBEDDED_UNIT_SUFFIXES:
                param_name = _normalize_parameter_name_from_key("_".join(parts[:-2]))
                if param_name:
                    extracted.append({
                        "parameter": param_name,
                        "value": raw_value,
                        "unit": _EMBEDDED_UNIT_SUFFIXES[suffix2],
                        "source_key": raw_key,
                    })
                    continue

        # Then try 1-token suffix, e.g. F, kPa, rpm, Torr
        suffix1 = parts_lower[-1]
        if suffix1 in _EMBEDDED_UNIT_SUFFIXES:
            param_name = _normalize_parameter_name_from_key("_".join(parts[:-1]))
            if param_name:
                extracted.append({
                    "parameter": param_name,
                    "value": raw_value,
                    "unit": _EMBEDDED_UNIT_SUFFIXES[suffix1],
                    "source_key": raw_key,
                })
                continue

    return extracted


def _normalize_parameter_name_from_key(raw_name: str) -> str:
    text = raw_name.strip().lower()
    if not text:
        return ""

    replacements = {
        "temp": "temperature",
        "tmp": "temperature",
        "press": "pressure",
        "spd": "speed",
        "dur": "duration",
        "freq": "frequency",
    }

    parts = re.split(r"[_\-\s]+", text)
    parts = [replacements.get(part, part) for part in parts if part]
    return "_".join(parts)


def _is_scalar_measurement_value(value: object) -> bool:
    return value not in (None, "", [], {})


def _canonical_key(key: str) -> str:
    return "".join(ch.lower() for ch in str(key) if ch.isalnum())