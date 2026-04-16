from __future__ import annotations

from collections import Counter
from typing import Any

from structured.shared.contract import (
    StructuredParseSpec,
    RecordGroupSpec,
    WhereClause,
)


def build_csv_parse_spec(rows: list[dict[str, Any]]) -> StructuredParseSpec:
    if not rows:
        return StructuredParseSpec(
            schema_family="csv_empty",
            record_groups=[],
        )

    # 1) deterministic family detection first
    if _looks_like_sensor_measurement_csv(rows):
        return _build_sensor_measurement_csv_spec(rows)

    # 2) generic discriminator split if applicable
    discriminator = _detect_discriminator(rows)
    if discriminator:
        groups = _build_split_groups(rows, discriminator)
        return StructuredParseSpec(
            schema_family="csv_root_array_event_log",
            record_groups=groups,
        )

    # 3) safe fallback
    field_paths = _collect_common_scalar_fields(rows)

    return StructuredParseSpec(
        schema_family="csv_rows",
        record_groups=[
            RecordGroupSpec(
                record_type="csv_row",
                path="$[]",
                field_paths=field_paths,
                context_paths=[],
                where=None,
            )
        ],
    )


def _looks_like_sensor_measurement_csv(rows: list[dict[str, Any]]) -> bool:
    required = {"timestamp", "sensor_id", "parameter", "value", "unit"}
    first = rows[0] if rows else {}

    if not required.issubset(set(first.keys())):
        return False

    non_empty_required_rows = 0
    for row in rows:
        if all(row.get(col) not in (None, "") for col in ["timestamp", "sensor_id", "parameter", "unit"]):
            non_empty_required_rows += 1

    return non_empty_required_rows >= max(3, int(len(rows) * 0.7))


def _build_sensor_measurement_csv_spec(rows: list[dict[str, Any]]) -> StructuredParseSpec:
    primary_fields = [
        "timestamp",
        "sensor_id",
        "parameter",
        "value",
        "unit",
    ]

    row_metadata_fields = [
        "tool_id",
        "chamber",
        "lot_id",
        "wafer_id",
        "recipe_step",
    ]

    field_paths = [
        p for p in primary_fields + row_metadata_fields
        if _field_exists_in_enough_rows(rows, p, threshold_ratio=0.5)
    ]

    return StructuredParseSpec(
        schema_family="sensor_measurement_csv",
        record_groups=[
            RecordGroupSpec(
                record_type="sensor_measurement_csv",
                path="$[]",
                field_paths=field_paths,
                context_paths=[],
                where=None,
            )
        ],
    )


def _field_exists_in_enough_rows(
    rows: list[dict[str, Any]],
    field: str,
    threshold_ratio: float = 0.5,
) -> bool:
    threshold = max(1, int(len(rows) * threshold_ratio))
    count = 0

    for row in rows:
        if field in row and row.get(field) not in (None, ""):
            count += 1

    return count >= threshold


def _detect_discriminator(rows: list[dict[str, Any]]) -> str | None:
    candidate_fields = ["record_class", "event_type", "type", "category", "kind"]

    for field in candidate_fields:
        values = [row.get(field) for row in rows if isinstance(row.get(field), str) and row.get(field) != ""]
        unique = sorted(set(values))
        if len(values) >= max(3, len(rows) // 2) and 2 <= len(unique) <= 10:
            return field

    return None


def _build_split_groups(rows: list[dict[str, Any]], discriminator: str) -> list[RecordGroupSpec]:
    values = sorted(set(
        row[discriminator]
        for row in rows
        if isinstance(row.get(discriminator), str) and row[discriminator] != ""
    ))

    groups: list[RecordGroupSpec] = []

    for value in values:
        subset = [row for row in rows if row.get(discriminator) == value]
        field_paths = _collect_common_scalar_fields(subset)

        preferred = [
            "record_id", "log_time", discriminator, "tool", "lot", "wafer",
            "step_name", "step_action", "step_seq", "step_result", "duration_s",
            "sensor_id", "measurand", "value", "unit",
            "alarm_id", "severity", "message",
            "from_state", "to_state", "operator", "notes"
        ]

        ordered = [p for p in preferred if p in field_paths] + [
            p for p in field_paths if p not in preferred
        ]

        groups.append(
            RecordGroupSpec(
                record_type=f"{value}_event",
                path="$[]",
                field_paths=ordered[:30],
                context_paths=[],
                where=WhereClause(field=discriminator, equals=value),
            )
        )

    return groups


def _collect_common_scalar_fields(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []

    counts = Counter()

    for row in rows:
        for k, v in row.items():
            if _is_scalar(v):
                counts[k] += 1

    threshold = max(1, int(len(rows) * 0.5))

    return [
        field for field, count in counts.items()
        if count >= threshold
    ]


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))