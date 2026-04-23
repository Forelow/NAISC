from __future__ import annotations

from collections import defaultdict
from typing import Any


def postprocess_free_form_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = [r for r in records if _keep_record(r)]
    records = _merge_same_chunk_same_type(records)
    records = _absorb_generic_observations(records)
    records = _dedupe_records(records)
    return _finalize_record_type(records)


def _keep_record(record: dict[str, Any]) -> bool:
    confidence = float(record.get("confidence", 0.0))
    uncertain = bool(record.get("uncertain", False))
    data = record.get("data", {}) or {}
    coarse_type = record.get("coarse_type")
    subtype = str(record.get("subtype", "") or "")

    if confidence < 0.80:
        return False

    if not isinstance(data, dict) or not data:
        return False

    # Keep uncertain records only if they carry enough information
    if uncertain and len(data) < 2:
        return False

    # Drop very weak generic records with almost no operational value
    if coarse_type == "generic_operational_observation" and len(data) < 2 and confidence < 0.95:
        return False

    # Drop ultra-thin configuration notes
    if coarse_type == "configuration_or_recipe":
        if set(data.keys()) <= {"parameter", "action"}:
            return False

    # Preserve header metadata for now, but only if rich enough
    if subtype == "log_header_metadata" and len(data) < 3:
        return False

    return True


def _merge_same_chunk_same_type(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    used = set()

    for i, record in enumerate(records):
        if i in used:
            continue

        current = _copy_record(record)

        for j in range(i + 1, len(records)):
            if j in used:
                continue

            other = records[j]

            if other.get("source_reference") != current.get("source_reference"):
                continue

            if other.get("coarse_type") != current.get("coarse_type"):
                continue

            if other.get("final_type") != current.get("final_type"):
                continue

            if not _share_identity(current.get("data", {}), other.get("data", {})):
                continue

            current["data"] = _merge_dicts(current["data"], other.get("data", {}))
            current["extra"] = _merge_dicts(current["extra"], other.get("extra", {}))
            current["confidence"] = max(
                float(current.get("confidence", 0.0)),
                float(other.get("confidence", 0.0)),
            )
            current["evidence_text"] = _merge_evidence(
                current.get("evidence_text", ""),
                other.get("evidence_text", ""),
            )
            used.add(j)

        merged.append(current)

    return merged


def _absorb_generic_observations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_chunk: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_chunk[str(record.get("source_reference"))].append(_copy_record(record))

    final_records: list[dict[str, Any]] = []

    for chunk_ref, chunk_records in by_chunk.items():
        anchors = [
            r for r in chunk_records
            if r.get("final_type") is not None
        ]
        generics = [
            r for r in chunk_records
            if r.get("final_type") is None
        ]

        absorbed_ids = set()

        for generic_idx, generic in enumerate(generics):
            best_anchor = None
            best_score = -1
            generic_subtype = str(generic.get("subtype", "") or "")
            if any(token in generic_subtype for token in ["summary", "header", "handover"]):
                continue

            for anchor in anchors:
                score = _absorption_score(anchor, generic)
                if score > best_score:
                    best_score = score
                    best_anchor = anchor

            # Absorb if there is a meaningful relationship
            if best_anchor is not None and best_score >= 3:
                best_anchor.setdefault("extra", {})
                absorbed = best_anchor["extra"].setdefault("absorbed_observations", [])
                absorbed.append(
                    {
                        "coarse_type": generic.get("coarse_type"),
                        "subtype": generic.get("subtype"),
                        "confidence": generic.get("confidence"),
                        "evidence_text": generic.get("evidence_text"),
                        "data": generic.get("data", {}),
                        "uncertain": generic.get("uncertain", False),
                    }
                )

                # Promote missing fields into anchor.data where safe
                best_anchor["data"] = _merge_missing_nonconflicting(
                    best_anchor.get("data", {}),
                    generic.get("data", {}),
                )

                best_anchor["evidence_text"] = _merge_evidence(
                    best_anchor.get("evidence_text", ""),
                    generic.get("evidence_text", ""),
                )
                best_anchor["confidence"] = max(
                    float(best_anchor.get("confidence", 0.0)),
                    float(generic.get("confidence", 0.0)),
                )
                absorbed_ids.add(generic_idx)

        final_records.extend(anchors)

        for idx, generic in enumerate(generics):
            if idx not in absorbed_ids:
                final_records.append(generic)

    return final_records


def _dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()

    for record in records:
        key = (
            record.get("source_reference"),
            record.get("final_type"),
            record.get("coarse_type"),
            record.get("subtype"),
            tuple(sorted((record.get("data", {}) or {}).items(), key=lambda x: x[0]))
            if _is_flat_dict(record.get("data", {}))
            else str(record.get("data", {})),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(record)

    return out


def _finalize_record_type(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for record in records:
        final_type = record.get("final_type")
        if final_type is None:
            final_type = "generic_operational_observation"

        out.append(
            {
                "record_type": final_type,
                "coarse_type": record.get("coarse_type"),
                "subtype": record.get("subtype"),
                "source_reference": record.get("source_reference"),
                "confidence": record.get("confidence"),
                "evidence_text": record.get("evidence_text"),
                "data": record.get("data", {}),
                "extra": record.get("extra", {}),
                "uncertain": record.get("uncertain", False),
            }
        )

    return out


def _absorption_score(anchor: dict[str, Any], generic: dict[str, Any]) -> int:
    score = 0

    anchor_data = anchor.get("data", {}) or {}
    generic_data = generic.get("data", {}) or {}

    anchor_final = anchor.get("final_type")
    generic_coarse = generic.get("coarse_type")
    anchor_coarse = anchor.get("coarse_type")

    # First require compatible event families
    if not _compatible_families(anchor_final, anchor_coarse, generic_coarse):
        return -1

    score += _identity_overlap(anchor_data, generic_data)

    if generic_coarse == "state_change" and anchor_final == "equipment_state":
        score += 2
    elif generic_coarse == "measurement_observation" and anchor_final == "sensor_reading":
        score += 2
    elif generic_coarse == "configuration_or_recipe" and anchor_final == "process_parameter_recipe":
        score += 2
    elif generic_coarse in {"process_step_event", "maintenance_action", "logistics_or_disposition"} and anchor_final == "wafer_processing_sequence":
        score += 2
    elif generic_coarse == "fault_or_warning" and anchor_final == "fault_event":
        score += 2

    # Small boost only when identity already overlaps
    if _identity_overlap(anchor_data, generic_data) >= 1:
        score += 1

    return score

def _compatible_families(anchor_final: str | None, anchor_coarse: str | None, generic_coarse: str | None) -> bool:
    if generic_coarse is None:
        return False

    # Strong allowed pairings only
    if generic_coarse == "state_change":
        return anchor_final == "equipment_state"

    if generic_coarse == "measurement_observation":
        return anchor_final == "sensor_reading"

    if generic_coarse == "configuration_or_recipe":
        return anchor_final == "process_parameter_recipe"

    if generic_coarse == "fault_or_warning":
        return anchor_final == "fault_event"

    if generic_coarse in {"process_step_event", "maintenance_action", "logistics_or_disposition"}:
        return anchor_final == "wafer_processing_sequence"

    # generic observations should only absorb into same-family concrete anchors
    if generic_coarse == "generic_operational_observation":
        return anchor_final in {
            "equipment_state",
            "sensor_reading",
            "process_parameter_recipe",
            "fault_event",
            "wafer_processing_sequence",
        }

    return False


def _identity_overlap(a: dict[str, Any], b: dict[str, Any]) -> int:
    aliases = [
        ["wafer", "unit_id", "item_id"],
        ["lot"],
        ["timestamp", "time", "timestamp_hours", "timestamp_approx"],
        ["equipment", "tool", "asset"],
        ["step", "process", "event_name"],
        ["component", "equipment_component", "asset"],
    ]

    matches = 0
    for group in aliases:
        a_val = _first_present(a, group)
        b_val = _first_present(b, group)
        if a_val is not None and b_val is not None and a_val == b_val:
            matches += 1
    return matches


def _share_identity(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return _identity_overlap(a, b) >= 1


def _first_present(data: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, "", [], {}):
            return data[key]
    return None


def _merge_missing_nonconflicting(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for key, value in b.items():
        if key not in out:
            out[key] = value
    return out


def _merge_dicts(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
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


def _copy_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "data": dict(record.get("data", {}) or {}),
        "extra": dict(record.get("extra", {}) or {}),
    }


def _is_flat_dict(data: dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    for value in data.values():
        if isinstance(value, (dict, list)):
            return False
    return True