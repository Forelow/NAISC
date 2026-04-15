from __future__ import annotations

from typing import Any


def normalize_structure_config(config: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize the LLM output into the parser contract:

    - group path: absolute from root
    - context_paths: absolute from root
    - field_paths: relative to the matched node
    - list syntax: [] not .[]
    """
    normalized = {
        "schema_family": config.get("schema_family", "unknown"),
        "record_groups": [],
    }

    for group in config.get("record_groups", []):
        group_path = _normalize_path_syntax(group.get("path", ""))
        context_paths = [
            _normalize_path_syntax(p) for p in group.get("context_paths", [])
        ]
        raw_field_paths = [
            _normalize_path_syntax(p) for p in group.get("field_paths", [])
        ]

        field_paths = []
        for field_path in raw_field_paths:
            # If the model returned absolute field paths under the group,
            # strip the group prefix so fields become relative.
            stripped = _strip_group_prefix(group_path, field_path)
            field_paths.append(stripped)

        normalized["record_groups"].append(
            {
                "record_type": group.get("record_type", "generic_record"),
                "path": group_path,
                "context_paths": _dedupe(context_paths),
                "field_paths": _dedupe(field_paths),
            }
        )

    return normalized


def validate_and_prune_structure_config(
    config: dict[str, Any],
    data: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Validate config against actual data, and prune invalid fields/contexts.
    A group survives only if:
    - its path resolves to at least one node, and
    - it has at least one valid field or context path after pruning
    """
    cleaned = {
        "schema_family": config.get("schema_family", "unknown"),
        "record_groups": [],
    }

    report = {
        "schema_family": config.get("schema_family", "unknown"),
        "group_count_before": len(config.get("record_groups", [])),
        "group_count_after": 0,
        "valid_group_count": 0,
        "groups": [],
    }

    for group in config.get("record_groups", []):
        record_type = group.get("record_type", "generic_record")
        group_path = group.get("path", "")
        context_paths = group.get("context_paths", [])
        field_paths = group.get("field_paths", [])

        matches = _resolve_path_with_bindings(data, group_path)
        if not matches:
            report["groups"].append(
                {
                    "record_type": record_type,
                    "path": group_path,
                    "status": "rejected",
                    "reason": "group path did not resolve",
                }
            )
            continue

        sample_matches = matches[:3]

        valid_field_paths = []
        for field_path in field_paths:
            resolved_values = [
                _get_relative_value(match["node"], field_path)
                for match in sample_matches
            ]

            has_any_value = any(v is not None for v in resolved_values)
            all_non_null_are_scalar = all(
                _is_scalar_value(v) for v in resolved_values if v is not None
            )

            if has_any_value and all_non_null_are_scalar:
                valid_field_paths.append(field_path)

        valid_context_paths = []
        for context_path in context_paths:
            resolved_values = [
                _get_bound_absolute_value(data, context_path, match["bindings"])
                for match in sample_matches
            ]

            has_any_value = any(v is not None for v in resolved_values)
            all_non_null_are_scalar = all(
                _is_scalar_value(v) for v in resolved_values if v is not None
            )

            if has_any_value and all_non_null_are_scalar:
                valid_context_paths.append(context_path)

        cleaned_group = {
            "record_type": record_type,
            "path": group_path,
            "context_paths": _dedupe(valid_context_paths),
            "field_paths": _dedupe(valid_field_paths),
        }
        cleaned["record_groups"].append(cleaned_group)

        report["groups"].append(
            {
                "record_type": record_type,
                "path": group_path,
                "status": "accepted",
                "valid_field_paths": cleaned_group["field_paths"],
                "valid_context_paths": cleaned_group["context_paths"],
            }
        )

    report["group_count_after"] = len(cleaned["record_groups"])
    report["valid_group_count"] = len(cleaned["record_groups"])
    return cleaned, report


# -----------------------------
# helpers
# -----------------------------

def _normalize_path_syntax(path: str) -> str:
    if not path:
        return path

    path = path.strip()

    # Convert slash-style paths to dot-style
    path = path.replace("\\", "/")
    path = path.replace("/", ".")

    # Root-array handling
    # []        -> $[]
    # [].field  -> $[].field
    if path == "[]":
        path = "$[]"
    elif path.startswith("[]."):
        path = "$" + path

    # Clean leading dots from converted slash paths
    path = path.lstrip(".")

    # Normalize list syntax
    path = path.replace(".[]", "[]")

    while ".." in path:
        path = path.replace("..", ".")

    return path


def _strip_group_prefix(group_path: str, field_path: str) -> str:
    if not group_path or not field_path:
        return field_path

    if field_path == group_path:
        return field_path

    prefix = group_path + "."
    if field_path.startswith(prefix):
        return field_path[len(prefix):]

    return field_path


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


# -----------------------------
# parser-compatible path resolution
# -----------------------------

def _resolve_path_with_bindings(data: Any, path: str) -> list[dict]:
    path = _normalize_runtime_path(path)

    if path == "$":
        return [{"node": data, "bindings": {}}]

    parts = path.split(".")
    current = [{"node": data, "bindings": {}}]
    traversed_parts: list[str] = []

    for part in parts:
        next_items = []

        # Root array token
        if part == "$[]":
            traversed_parts.append(part)
            binding_key = ".".join(traversed_parts)

            for item in current:
                node = item["node"]
                bindings = item["bindings"]

                if not isinstance(node, list):
                    continue

                for index, child in enumerate(node):
                    new_bindings = dict(bindings)
                    new_bindings[binding_key] = index
                    next_items.append(
                        {
                            "node": child,
                            "bindings": new_bindings,
                        }
                    )

            current = next_items
            continue

        # Root token
        if part == "$":
            current = [{"node": item["node"], "bindings": dict(item["bindings"])} for item in current]
            continue

        is_list = part.endswith("[]")
        key = part[:-2] if is_list else part

        traversed_parts.append(part)
        binding_key = ".".join(traversed_parts)

        for item in current:
            node = item["node"]
            bindings = item["bindings"]

            if not isinstance(node, dict):
                continue
            if key not in node:
                continue

            value = node[key]

            if is_list:
                if isinstance(value, list):
                    for index, child in enumerate(value):
                        new_bindings = dict(bindings)
                        new_bindings[binding_key] = index
                        next_items.append(
                            {
                                "node": child,
                                "bindings": new_bindings,
                            }
                        )
            else:
                next_items.append(
                    {
                        "node": value,
                        "bindings": dict(bindings),
                    }
                )

        current = next_items

    return current


def _get_relative_value(node: Any, field_path: str) -> Any:
    parts = field_path.split(".")
    current = node

    for part in parts:
        if not isinstance(current, dict):
            return None
        if part not in current:
            return None
        current = current[part]

    return current


def _get_bound_absolute_value(root: Any, path: str, bindings: dict[str, int]) -> Any:
    path = _normalize_runtime_path(path)
    parts = path.split(".")
    current = root
    traversed_parts: list[str] = []

    for part in parts:
        if part == "$":
            continue

        if part == "$[]":
            traversed_parts.append(part)
            binding_key = ".".join(traversed_parts)

            if not isinstance(current, list):
                return None
            if binding_key not in bindings:
                return None

            index = bindings[binding_key]
            if index < 0 or index >= len(current):
                return None

            current = current[index]
            continue

        is_list = part.endswith("[]")
        key = part[:-2] if is_list else part

        traversed_parts.append(part)
        binding_key = ".".join(traversed_parts)

        if not isinstance(current, dict):
            return None
        if key not in current:
            return None

        current = current[key]

        if is_list:
            if not isinstance(current, list):
                return None
            if binding_key not in bindings:
                return None
            index = bindings[binding_key]
            if index < 0 or index >= len(current):
                return None
            current = current[index]

    return current


def find_uncovered_leaf_repeated_paths(
    structure_summary: dict[str, Any],
    config: dict[str, Any],
) -> list[str]:
    """
    Return repeated paths from the structure summary that:
    - are leaf repeated paths (they do not contain another repeated path beneath them)
    - are not currently covered by any record group path
    """
    repeated_paths = [
        _normalize_path_syntax(p)
        for p in structure_summary.get("repeated_paths", [])
        if p
    ]

    leaf_paths = []
    for path in repeated_paths:
        is_parent_of_other_repeated = any(
            other != path and other.startswith(path + ".")
            for other in repeated_paths
        )
        if not is_parent_of_other_repeated:
            leaf_paths.append(path)

    covered_group_paths = {
        _normalize_path_syntax(group.get("path", ""))
        for group in config.get("record_groups", [])
    }

    uncovered = [path for path in leaf_paths if path not in covered_group_paths]
    return uncovered

def _is_scalar_value(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _is_container_value(value: Any) -> bool:
    return isinstance(value, (dict, list))

def _normalize_runtime_path(path: str) -> str:
    if path == "[]":
        return "$[]"
    if path.startswith("[]."):
        return "$" + path
    return path