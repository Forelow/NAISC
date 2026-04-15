from __future__ import annotations

from collections import defaultdict
from typing import Any

from schema.models import FieldSample, RecordBoundary, SchemaProfile


def profile_json_data(data: Any) -> SchemaProfile:
    field_store: dict[str, dict] = defaultdict(
        lambda: {
            "non_null_count": 0,
            "sample_values": [],
            "types": set(),
        }
    )
    boundaries: dict[str, dict] = {}

    _walk_json(data, [], field_store, boundaries)

    top_level_type = _infer_node_type(data)
    top_level_keys = list(data.keys()) if isinstance(data, dict) else []
    schema_family = _guess_json_schema_family(data)

    field_samples = []
    for path, meta in sorted(field_store.items()):
        inferred_type = _collapse_types(meta["types"])
        field_samples.append(
            FieldSample(
                path=path,
                inferred_type=inferred_type,
                non_null_count=meta["non_null_count"],
                sample_values=meta["sample_values"][:5],
            )
        )

    record_boundaries = []
    for path, meta in sorted(boundaries.items()):
        record_boundaries.append(
            RecordBoundary(
                path=path,
                boundary_type=meta["boundary_type"],
                count=meta["count"],
                sample_keys=sorted(meta["sample_keys"]),
            )
        )

    notes = []
    if schema_family == "control_job_family":
        notes.append("Looks like semiconductor control-job style nested JSON.")
    if not record_boundaries:
        notes.append("No repeated list-of-object boundaries found.")

    return SchemaProfile(
        format_name="json",
        schema_family=schema_family,
        top_level_type=top_level_type,
        top_level_keys=top_level_keys,
        record_boundaries=record_boundaries,
        fields=field_samples,
        notes=notes,
    )


def _walk_json(
    node: Any,
    path_parts: list[str],
    field_store: dict[str, dict],
    boundaries: dict[str, dict],
) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            _walk_json(value, path_parts + [key], field_store, boundaries)
        return

    if isinstance(node, list):
        current_path = _join_path(path_parts) + "[]"

        if node and all(isinstance(item, dict) for item in node):
            sample_keys = set()
            for item in node[:5]:
                sample_keys.update(item.keys())

            boundaries[current_path] = {
                "boundary_type": "list_of_objects",
                "count": len(node),
                "sample_keys": sample_keys,
            }

        for item in node[:20]:
            _walk_json(item, path_parts + ["[]"], field_store, boundaries)
        return

    leaf_path = _join_path(path_parts)
    inferred_type = _infer_node_type(node)

    field_store[leaf_path]["types"].add(inferred_type)
    if node is not None:
        field_store[leaf_path]["non_null_count"] += 1
        if len(field_store[leaf_path]["sample_values"]) < 5:
            sample = _safe_sample_value(node)
            if sample not in field_store[leaf_path]["sample_values"]:
                field_store[leaf_path]["sample_values"].append(sample)


def _join_path(parts: list[str]) -> str:
    path = []
    for part in parts:
        if part == "[]":
            if path:
                path[-1] = path[-1] + "[]"
            else:
                path.append("[]")
        else:
            path.append(part)
    return ".".join(path)


def _infer_node_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__


def _collapse_types(type_set: set[str]) -> str:
    if len(type_set) == 1:
        return next(iter(type_set))
    return "mixed:" + ",".join(sorted(type_set))


def _safe_sample_value(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 80:
        return value[:77] + "..."
    return value


def _guess_json_schema_family(data: Any) -> str:
    if isinstance(data, dict):
        keys = set(data.keys())

        if "ControlJob" in keys:
            return "control_job_family"

        if "events" in keys and isinstance(data.get("events"), list):
            return "flat_event_list"

        if "sensor_data" in keys or "SensorData" in keys:
            return "sensor_trace_family"

    return "generic_json"