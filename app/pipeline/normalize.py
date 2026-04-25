from __future__ import annotations

import math
import re
from typing import Any


def normalize_records(standardized_batch: dict[str, Any]) -> dict[str, Any]:
    out = dict(standardized_batch)
    out["records"] = [_normalize_record(record) for record in standardized_batch.get("records", [])]
    return out


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    standardized_fields = dict(record.get("standardized_fields", {}) or {})

    normalized = _normalize_value(standardized_fields)

    # Harmonize common fields first
    if "curr_state" not in normalized and "new_state" in normalized:
        normalized["curr_state"] = normalized["new_state"]

    if "parameter" not in normalized and "measurement_name" in normalized:
        normalized["parameter"] = normalized["measurement_name"]

    if "value" not in normalized and "measured_value" in normalized:
        normalized["value"] = normalized["measured_value"]

    if "unit" not in normalized and "units" in normalized:
        normalized["unit"] = normalized["units"]

    if "timestamp" in normalized:
        normalized["timestamp"] = _normalize_timestamp_string(normalized["timestamp"])

    if "curr_state" not in normalized and "to_state" in normalized:
        normalized["curr_state"] = normalized["to_state"]

    if "prev_state" not in normalized and "from_state" in normalized:
        normalized["prev_state"] = normalized["from_state"]

    if "curr_state" not in normalized and "event_name" in normalized:
        normalized["curr_state"] = normalized["event_name"]

    if "parameter" not in normalized and "sensor_id" in normalized:
        normalized["parameter"] = normalized["sensor_id"]

    # Normalize unit aliases first
    if "unit" in normalized:
        normalized["unit"] = _normalize_unit_symbol(normalized.get("unit"))

    if "units" in normalized:
        normalized["units"] = _normalize_unit_symbol(normalized.get("units"))

    if "expected_unit" in normalized:
        normalized["expected_unit"] = _normalize_unit_symbol(normalized.get("expected_unit"))

    # Apply real measurement/unit normalization
    normalized = _normalize_measurement_fields(normalized)

    # Keep your old dose-string handling
    for key in list(normalized.keys()):
        if key.endswith("cm-2") or key.endswith("cm2"):
            normalized[key] = _normalize_scalar(normalized[key])

    out["normalized_fields"] = normalized
    return out


def _normalize_measurement_fields(fields: dict[str, Any]) -> dict[str, Any]:
    out = dict(fields)

    parameter = out.get("parameter") or out.get("sensor_id") or out.get("event_name")

    # Main value + unit
    if "value" in out and "unit" in out:
        result = _normalize_measurement(
            value=out.get("value"),
            unit=out.get("unit"),
            parameter=parameter,
        )
        if result["value"] is not None:
            out["value"] = result["value"]
        if result["unit"] is not None:
            out["unit"] = result["unit"]
        if result["family"] is not None:
            out["measurement_family"] = result["family"]

    # measured_value + unit
    if "measured_value" in out:
        measured_unit = out.get("measured_unit") or out.get("unit")
        if measured_unit is not None:
            result = _normalize_measurement(
                value=out.get("measured_value"),
                unit=measured_unit,
                parameter=parameter,
            )
            if result["value"] is not None:
                out["measured_value"] = result["value"]
            if result["unit"] is not None:
                out["measured_unit"] = result["unit"]
            if "unit" not in out and result["unit"] is not None:
                out["unit"] = result["unit"]
            if result["family"] is not None:
                out["measurement_family"] = result["family"]

    # expected_value + expected_unit
    if "expected_value" in out:
        expected_unit = out.get("expected_unit") or out.get("unit")
        if expected_unit is not None:
            result = _normalize_measurement(
                value=out.get("expected_value"),
                unit=expected_unit,
                parameter=parameter,
            )
            if result["value"] is not None:
                out["expected_value"] = result["value"]
            if result["unit"] is not None:
                out["expected_unit"] = result["unit"]
            if result["family"] is not None:
                out["measurement_family"] = result["family"]

    return out


def _normalize_measurement(value: Any, unit: Any, parameter: Any = None) -> dict[str, Any]:
    numeric_value = _coerce_float(value)
    normalized_unit = _normalize_unit_symbol(unit)
    family = _infer_measurement_family(parameter=parameter, unit=normalized_unit)

    if numeric_value is None:
        return {"value": None, "unit": normalized_unit, "family": family}

    if normalized_unit is None or family is None:
        return {"value": numeric_value, "unit": normalized_unit, "family": family}

    canonical_unit = _CANONICAL_UNITS.get(family)
    if canonical_unit is None:
        return {"value": numeric_value, "unit": normalized_unit, "family": family}

    try:
        converted_value = _convert_to_canonical(numeric_value, family, normalized_unit)
        return {"value": converted_value, "unit": canonical_unit, "family": family}
    except Exception:
        return {"value": numeric_value, "unit": normalized_unit, "family": family}


def _infer_measurement_family(parameter: Any = None, unit: Any = None) -> str | None:
    normalized_unit = _normalize_unit_symbol(unit)

    parameter_text = str(parameter or "").strip()
    if parameter_text:
        for pattern, family in _PARAMETER_FAMILY_HINTS:
            if pattern.search(parameter_text):
                return family

    if normalized_unit in _UNIT_TO_FAMILY:
        return _UNIT_TO_FAMILY[normalized_unit]

    return None


def _normalize_unit_symbol(unit: Any) -> str | None:
    if unit is None:
        return None

    text = str(unit).strip()
    if not text:
        return None

    compact = text.replace(" ", "").lower()
    return _UNIT_ALIASES.get(compact, text)


def _convert_to_canonical(value: float, family: str, unit: str) -> float:
    # Temperature -> degC
    if family == "temperature":
        table = {
            "degC": lambda x: x,
            "degF": lambda x: (x - 32.0) * 5.0 / 9.0,
            "K": lambda x: x - 273.15,
        }
        return table[unit](value)

    # Pressure -> Pa
    if family == "pressure":
        table = {
            "Pa": lambda x: x,
            "kPa": lambda x: x * 1_000.0,
            "MPa": lambda x: x * 1_000_000.0,
            "bar": lambda x: x * 100_000.0,
            "mbar": lambda x: x * 100.0,
            "atm": lambda x: x * 101_325.0,
            "psi": lambda x: x * 6_894.757293168,
            "Torr": lambda x: x * 133.32236842105263,
            "mTorr": lambda x: x * 0.13332236842105263,
            "mmHg": lambda x: x * 133.32236842105263,
        }
        return table[unit](value)

    # Time -> s
    if family == "time":
        table = {
            "s": lambda x: x,
            "ms": lambda x: x / 1_000.0,
            "us": lambda x: x / 1_000_000.0,
            "ns": lambda x: x / 1_000_000_000.0,
            "min": lambda x: x * 60.0,
            "h": lambda x: x * 3600.0,
        }
        return table[unit](value)

    # Rotational speed -> rpm
    if family == "rotational_speed":
        table = {
            "rpm": lambda x: x,
            "rps": lambda x: x * 60.0,
            "rad/s": lambda x: x * 60.0 / (2.0 * math.pi),
        }
        return table[unit](value)

    # Force -> N
    if family == "force":
        table = {
            "N": lambda x: x,
            "kN": lambda x: x * 1_000.0,
            "lbf": lambda x: x * 4.4482216152605,
        }
        return table[unit](value)

    # Gas flow -> sccm
    if family == "gas_flow":
        table = {
            "sccm": lambda x: x,
            "slm": lambda x: x * 1_000.0,
            "cc/min": lambda x: x,
        }
        return table[unit](value)

    # Liquid flow -> mL/min
    if family == "liquid_flow":
        table = {
            "mL/min": lambda x: x,
            "mL/s": lambda x: x * 60.0,
            "L/min": lambda x: x * 1_000.0,
            "L/h": lambda x: x * (1_000.0 / 60.0),
        }
        return table[unit](value)

    # Power -> W
    if family == "power":
        table = {
            "W": lambda x: x,
            "kW": lambda x: x * 1_000.0,
            "mW": lambda x: x / 1_000.0,
        }
        return table[unit](value)

    # Frequency -> Hz
    if family == "frequency":
        table = {
            "Hz": lambda x: x,
            "kHz": lambda x: x * 1_000.0,
            "MHz": lambda x: x * 1_000_000.0,
            "GHz": lambda x: x * 1_000_000_000.0,
        }
        return table[unit](value)

    # Length -> m
    if family == "length":
        table = {
            "m": lambda x: x,
            "cm": lambda x: x / 100.0,
            "mm": lambda x: x / 1_000.0,
            "um": lambda x: x / 1_000_000.0,
            "nm": lambda x: x / 1_000_000_000.0,
            "A": lambda x: x * 1e-10,
        }
        return table[unit](value)

    # Thickness -> nm
    if family == "thickness":
        table = {
            "nm": lambda x: x,
            "um": lambda x: x * 1_000.0,
            "mm": lambda x: x * 1_000_000.0,
            "m": lambda x: x * 1_000_000_000.0,
            "A": lambda x: x * 0.1,
        }
        return table[unit](value)

    # Mass -> kg
    if family == "mass":
        table = {
            "kg": lambda x: x,
            "g": lambda x: x / 1_000.0,
            "mg": lambda x: x / 1_000_000.0,
            "ug": lambda x: x / 1_000_000_000.0,
            "lb": lambda x: x * 0.45359237,
        }
        return table[unit](value)

    # Percentage -> %
    if family == "percentage":
        table = {
            "%": lambda x: x,
            "fraction": lambda x: x * 100.0,
        }
        return table[unit](value)

    raise KeyError(f"Unsupported conversion family={family}, unit={unit}")


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    return _normalize_scalar(value)


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, (int, float, bool)) or value is None:
        return value

    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return text

    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if any(ch in text for ch in [".", "e", "E"]):
            return float(text)
        return int(text)
    except Exception:
        return text


def _normalize_timestamp_string(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return value.strip()


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if not isinstance(value, str):
        return None

    text = value.strip().replace(",", "")
    if not text:
        return None

    try:
        return float(text)
    except Exception:
        return None


_CANONICAL_UNITS = {
    "temperature": "degC",
    "pressure": "Pa",
    "time": "s",
    "rotational_speed": "rpm",
    "force": "N",
    "gas_flow": "sccm",
    "liquid_flow": "mL/min",
    "power": "W",
    "frequency": "Hz",
    "length": "m",
    "thickness": "nm",
    "mass": "kg",
    "percentage": "%",
}

_UNIT_ALIASES = {
    # temperature
    "c": "degC",
    "degc": "degC",
    "°c": "degC",
    "celsius": "degC",
    "f": "degF",
    "degf": "degF",
    "°f": "degF",
    "fahrenheit": "degF",
    "k": "K",
    "kelvin": "K",

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
    "s": "s",
    "sec": "s",
    "secs": "s",
    "second": "s",
    "seconds": "s",
    "ms": "ms",
    "millisecond": "ms",
    "milliseconds": "ms",
    "us": "us",
    "µs": "us",
    "microsecond": "us",
    "microseconds": "us",
    "ns": "ns",
    "min": "min",
    "mins": "min",
    "minute": "min",
    "minutes": "min",
    "h": "h",
    "hr": "h",
    "hrs": "h",
    "hour": "h",
    "hours": "h",

    # rotational speed
    "rpm": "rpm",
    "rps": "rps",
    "rev/s": "rps",
    "rad/s": "rad/s",

    # force
    "n": "N",
    "kn": "kN",
    "lbf": "lbf",

    # gas flow
    "sccm": "sccm",
    "slm": "slm",
    "cc/min": "cc/min",

    # liquid flow
    "ml/min": "mL/min",
    "ml/s": "mL/s",
    "l/min": "L/min",
    "l/h": "L/h",

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
    "µm": "um",
    "nm": "nm",
    "a": "A",
    "å": "A",
    "angstrom": "A",

    # mass
    "kg": "kg",
    "g": "g",
    "mg": "mg",
    "ug": "ug",
    "µg": "ug",
    "lb": "lb",

    # percentage
    "%": "%",
    "percent": "%",
    "pct": "%",
    "fraction": "fraction",
}

_UNIT_TO_FAMILY = {
    "degC": "temperature",
    "degF": "temperature",
    "K": "temperature",

    "Pa": "pressure",
    "kPa": "pressure",
    "MPa": "pressure",
    "bar": "pressure",
    "mbar": "pressure",
    "atm": "pressure",
    "psi": "pressure",
    "Torr": "pressure",
    "mTorr": "pressure",
    "mmHg": "pressure",

    "s": "time",
    "ms": "time",
    "us": "time",
    "ns": "time",
    "min": "time",
    "h": "time",

    "rpm": "rotational_speed",
    "rps": "rotational_speed",
    "rad/s": "rotational_speed",

    "N": "force",
    "kN": "force",
    "lbf": "force",

    "sccm": "gas_flow",
    "slm": "gas_flow",
    "cc/min": "gas_flow",

    "mL/min": "liquid_flow",
    "mL/s": "liquid_flow",
    "L/min": "liquid_flow",
    "L/h": "liquid_flow",

    "W": "power",
    "kW": "power",
    "mW": "power",

    "Hz": "frequency",
    "kHz": "frequency",
    "MHz": "frequency",
    "GHz": "frequency",

    "m": "length",
    "cm": "length",
    "mm": "length",
    "um": "length",
    "nm": "length",
    "A": "length",

    "kg": "mass",
    "g": "mass",
    "mg": "mass",
    "ug": "mass",
    "lb": "mass",

    "%": "percentage",
    "fraction": "percentage",
}

_PARAMETER_FAMILY_HINTS = [
    (re.compile(r"(temp|temperature|heater|chamber_temp|chuck_temp|platen_tmp)", re.I), "temperature"),
    (re.compile(r"(press|pressure|vacuum)", re.I), "pressure"),
    (re.compile(r"(dur|duration|time|dwell|latency)", re.I), "time"),
    (re.compile(r"(rpm|speed|carrier_spd|platen_spd|spindle)", re.I), "rotational_speed"),
    (re.compile(r"(force|down_force|load)", re.I), "force"),
    (re.compile(r"(gas_flow|n2_flow|ar_flow|o2_flow|h2_flow|mfc)", re.I), "gas_flow"),
    (re.compile(r"(slurry_flow|di_flow|water_flow|liquid_flow|coolant_flow)", re.I), "liquid_flow"),
    (re.compile(r"(power|watt)", re.I), "power"),
    (re.compile(r"(freq|frequency)", re.I), "frequency"),
    (re.compile(r"(thickness|etch_depth|deposition|removal)", re.I), "thickness"),
    (re.compile(r"(length|distance|gap|height|width|travel)", re.I), "length"),
    (re.compile(r"(mass|weight)", re.I), "mass"),
    (re.compile(r"(humidity|yield|efficiency|utilization|percent|ratio)", re.I), "percentage"),
]