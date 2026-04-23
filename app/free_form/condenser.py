from __future__ import annotations

from collections import defaultdict
from typing import Any


def build_condensed_records(detailed_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    promoted = [_promote_generic_record(r) for r in detailed_records]

    by_chunk: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in promoted:
        by_chunk[str(record.get("source_reference"))].append(_copy_record(record))

    condensed: list[dict[str, Any]] = []

    for chunk_ref, chunk_records in by_chunk.items():
        concrete = [r for r in chunk_records if r.get("record_type") != "generic_operational_observation"]
        generics = [r for r in chunk_records if r.get("record_type") == "generic_operational_observation"]

        if not concrete:
            for g in generics:
                if _keep_generic_in_condensed(g):
                    condensed.append(g)
            continue

        absorbed = set()

        for gi, generic in enumerate(generics):
            best_idx = None
            best_score = -1

            for ci, concrete_rec in enumerate(concrete):
                score = _condense_absorption_score(concrete_rec, generic)
                if score > best_score:
                    best_score = score
                    best_idx = ci

            if best_idx is not None and best_score >= 2:
                anchor = concrete[best_idx]
                anchor.setdefault("extra", {})
                anchor["extra"].setdefault("supporting_observations", []).append(
                    {
                        "coarse_type": generic.get("coarse_type"),
                        "subtype": generic.get("subtype"),
                        "confidence": generic.get("confidence"),
                        "evidence_text": generic.get("evidence_text"),
                        "data": generic.get("data", {}),
                        "uncertain": generic.get("uncertain", False),
                    }
                )

                anchor["data"] = _merge_missing(anchor.get("data", {}), generic.get("data", {}))
                anchor["evidence_text"] = _merge_evidence(
                    anchor.get("evidence_text", ""),
                    generic.get("evidence_text", ""),
                )
                anchor["confidence"] = max(
                    float(anchor.get("confidence", 0.0)),
                    float(generic.get("confidence", 0.0)),
                )
                absorbed.add(gi)

        condensed.extend(concrete)

        for gi, generic in enumerate(generics):
            if gi not in absorbed and _keep_generic_in_condensed(generic):
                condensed.append(generic)

    return _dedupe_records(condensed)


def _promote_generic_record(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("record_type") != "generic_operational_observation":
        return _copy_record(record)

    out = _copy_record(record)
    coarse_type = out.get("coarse_type")
    data = out.get("data", {}) or {}
    keys = set(data.keys())

    if coarse_type == "state_change":
        if {"new_state", "curr_state", "system_state", "status"} & keys:
            out["record_type"] = "equipment_state"
            out["data"] = _normalize_equipment_state(data)
            return out

    if coarse_type in {"process_step_event", "logistics_or_disposition", "maintenance_action"}:
        if {"wafer", "lot", "step", "action", "event_name", "status", "process", "disposition"} & keys:
            out["record_type"] = "wafer_processing_sequence"
            return out

    if coarse_type == "measurement_observation":
        if {"parameter", "measurement_name", "value", "measured_value", "reading_pa", "vacuum_pressure_pa", "thickness_A"} & keys:
            out["record_type"] = "sensor_reading"
            out["data"] = _normalize_sensor_record(data)
            return out

    if coarse_type == "configuration_or_recipe":
        if {"recipe", "parameter", "target_value", "steps", "energy_keV", "dose_cm-2", "process"} & keys:
            out["record_type"] = "process_parameter_recipe"
            return out

    if coarse_type == "fault_or_warning":
        if {"fault", "issue", "condition", "threshold", "event_name", "associated_issue"} & keys:
            out["record_type"] = "fault_event"
            return out

    return out


def _normalize_equipment_state(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    if "new_state" in out and "curr_state" not in out:
        out["curr_state"] = out["new_state"]
    if "system_state" in out and "curr_state" not in out:
        out["curr_state"] = out["system_state"]
    if "status" in out and "curr_state" not in out:
        out["curr_state"] = out["status"]
    return out


def _normalize_sensor_record(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)

    if "measurement_name" in out and "parameter" not in out:
        out["parameter"] = out["measurement_name"]
    if "measured_value" in out and "value" not in out:
        out["value"] = out["measured_value"]
    if "reading_pa" in out and "value" not in out:
        out["value"] = out["reading_pa"]
        out.setdefault("unit", "Pa")
    if "vacuum_pressure_pa" in out and "value" not in out:
        out["value"] = out["vacuum_pressure_pa"]
        out.setdefault("unit", "Pa")
    if "thickness_A" in out and "value" not in out:
        out["value"] = out["thickness_A"]
        out.setdefault("unit", "A")

    return out


def _condense_absorption_score(anchor: dict[str, Any], generic: dict[str, Any]) -> int:
    if generic.get("record_type") != "generic_operational_observation":
        return -1

    anchor_type = anchor.get("record_type")
    generic_coarse = generic.get("coarse_type")

    if not _compatible_for_condensed(anchor_type, generic_coarse):
        return -1

    score = _identity_overlap(anchor.get("data", {}) or {}, generic.get("data", {}) or {})

    if generic_coarse == "state_change" and anchor_type == "equipment_state":
        score += 2
    elif generic_coarse == "measurement_observation" and anchor_type == "sensor_reading":
        score += 2
    elif generic_coarse == "configuration_or_recipe" and anchor_type == "process_parameter_recipe":
        score += 2
    elif generic_coarse in {"process_step_event", "maintenance_action", "logistics_or_disposition"} and anchor_type == "wafer_processing_sequence":
        score += 2
    elif generic_coarse == "fault_or_warning" and anchor_type == "fault_event":
        score += 2

    return score


def _compatible_for_condensed(anchor_type: str | None, generic_coarse: str | None) -> bool:
    if anchor_type == "equipment_state":
        return generic_coarse == "state_change"

    if anchor_type == "sensor_reading":
        return generic_coarse == "measurement_observation"

    if anchor_type == "process_parameter_recipe":
        return generic_coarse == "configuration_or_recipe"

    if anchor_type == "fault_event":
        return generic_coarse == "fault_or_warning"

    if anchor_type == "wafer_processing_sequence":
        return generic_coarse in {"process_step_event", "maintenance_action", "logistics_or_disposition"}

    return False


def _keep_generic_in_condensed(record: dict[str, Any]) -> bool:
    subtype = str(record.get("subtype", "") or "").lower()
    confidence = float(record.get("confidence", 0.0))
    data = record.get("data", {}) or {}
    coarse_type = record.get("coarse_type")

    # Keep rich metadata and end-of-run summaries
    if "header" in subtype or "summary" in subtype:
        return True

    # Keep high-value generic state/fault summaries if rich enough
    if coarse_type in {"state_change", "fault_or_warning", "generic_operational_observation"}:
        return confidence >= 0.95 and len(data) >= 3

    # Keep only very strong unmatched generics
    return confidence >= 0.97 and len(data) >= 4


def _identity_overlap(a: dict[str, Any], b: dict[str, Any]) -> int:
    aliases = [
        ["wafer", "unit_id", "item_id", "wafer_id"],
        ["lot", "lot_id"],
        ["timestamp", "time", "timestamp_hours", "approx_time"],
        ["equipment", "tool", "asset"],
        ["step", "process", "event_name"],
        ["component", "equipment_component", "equipment_part", "asset"],
    ]

    matches = 0
    for group in aliases:
        a_val = _first_present(a, group)
        b_val = _first_present(b, group)
        if a_val is not None and b_val is not None and a_val == b_val:
            matches += 1
    return matches


def _first_present(data: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, "", [], {}):
            return data[key]
    return None


def _merge_missing(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for key, value in b.items():
        if key not in out:
            out[key] = value
    return out


def _merge_evidence(a: str, b: str) -> str:
    a = str(a).strip()
    b = str(b).strip()
    if not a:
        return b
    if not b:
        return a
    if a == b:
        return a
    if b in a:
        return a
    if a in b:
        return b
    return f"{a} | {b}"


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()

    for record in records:
        key = (
            record.get("source_reference"),
            record.get("record_type"),
            record.get("subtype"),
            str(record.get("data", {})),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(record)

    return out


def _copy_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "data": dict(record.get("data", {}) or {}),
        "extra": dict(record.get("extra", {}) or {}),
    }