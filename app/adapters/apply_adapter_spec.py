from __future__ import annotations

from typing import Any


def apply_adapter_spec(canonical_batch: dict[str, Any], adapter_spec: dict[str, Any]) -> dict[str, Any]:
    out = dict(canonical_batch)
    out["adapter_spec"] = {
        "schema_fingerprint": adapter_spec.get("schema_fingerprint"),
        "schema_family": adapter_spec.get("schema_family"),
    }
    out["records"] = [
        _apply_to_record(record, adapter_spec)
        for record in canonical_batch.get("records", [])
    ]
    return out


def _apply_to_record(record: dict[str, Any], adapter_spec: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    candidate_fields = dict(record.get("candidate_fields", {}) or {})
    source_record_type = str(record.get("source_record_type") or record.get("record_type") or "")

    candidate_fields = _inject_field_aliases(candidate_fields, adapter_spec.get("field_aliases", {}) or {})

    out["record_type"] = _resolve_record_type(
        source_record_type=source_record_type,
        candidate_fields=candidate_fields,
        adapter_spec=adapter_spec,
        current_record_type=str(record.get("record_type") or ""),
    )

    candidate_fields = _apply_fallback_rules(
        candidate_fields=candidate_fields,
        record_type=out["record_type"],
        fallback_rules=adapter_spec.get("fallback_rules", []) or [],
    )

    out["candidate_fields"] = candidate_fields
    out["adapter_applied"] = True
    return out


def _resolve_record_type(
    source_record_type: str,
    candidate_fields: dict[str, Any],
    adapter_spec: dict[str, Any],
    current_record_type: str,
) -> str:
    for rule in adapter_spec.get("dispatch_rules", []) or []:
        if str(rule.get("source_record_type")) != source_record_type:
            continue

        field_name = str(rule.get("field", "") or "")
        field_value = _lookup_field_value(candidate_fields, field_name)
        if field_value is None:
            continue

        mapping = rule.get("map", {}) or {}
        value_key = str(field_value)
        if value_key in mapping:
            return str(mapping[value_key])

    direct = adapter_spec.get("record_type_mapping", {}) or {}
    if source_record_type in direct:
        return str(direct[source_record_type])

    return current_record_type or source_record_type


def _inject_field_aliases(candidate_fields: dict[str, Any], field_aliases: dict[str, str]) -> dict[str, Any]:
    out = dict(candidate_fields)

    for source_key, canonical_key in field_aliases.items():
        value = _lookup_field_value(candidate_fields, source_key)
        if value is None:
            continue

        if canonical_key not in out or _is_empty(out.get(canonical_key)):
            out[canonical_key] = value

    return out


def _apply_fallback_rules(
    candidate_fields: dict[str, Any],
    record_type: str,
    fallback_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    out = dict(candidate_fields)

    for rule in fallback_rules:
        if str(rule.get("when_record_type")) != record_type:
            continue

        target = str(rule.get("if_missing", "") or "")
        source = str(rule.get("copy_from", "") or "")

        if not target or not source:
            continue

        if _is_empty(out.get(target)) and not _is_empty(out.get(source)):
            out[target] = out[source]

    return out


def _lookup_field_value(candidate_fields: dict[str, Any], wanted_key: str) -> Any:
    wanted_norm = _canonical_key(wanted_key)

    for actual_key, value in candidate_fields.items():
        if _canonical_key(str(actual_key)) == wanted_norm:
            return value

    return None


def _canonical_key(key: str) -> str:
    return "".join(ch.lower() for ch in str(key) if ch.isalnum())


def _is_empty(value: Any) -> bool:
    return value in (None, "", [], {})