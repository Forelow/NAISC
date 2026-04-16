from __future__ import annotations

from typing import Any


def parse_with_structure_config(data: Any, structure_config: dict) -> dict:
    schema_family = structure_config.get("schema_family", "unknown")
    record_groups = structure_config.get("record_groups", [])
    

    records = []

    for group in record_groups:
        record_type = group["record_type"]
        group_path = group["path"]
        context_paths = group.get("context_paths", [])
        field_paths = group.get("field_paths", [])
        where = group.get("where")

        matches = _resolve_path_with_bindings(data, group_path)

        for idx, match in enumerate(matches, start=1):
            node = match["node"]
            bindings = match["bindings"]

            if not isinstance(node, dict):
                continue
            if where and not _matches_where(node, where):
                continue

            extracted_data = {}

            # 1) extract local fields from the matched node
            for field_path in field_paths:
                value = _get_relative_value(node, field_path)
                if value is not None and not isinstance(value, (dict, list)):
                    extracted_data[field_path] = value

            # 2) extract context fields from the root using bindings
            for context_path in context_paths:
                value = _get_bound_absolute_value(data, context_path, bindings)
                if value is not None and not isinstance(value, (dict, list)):
                    extracted_data[context_path] = value

            records.append(
                {
                    "record_type": record_type,
                    "source_reference": _build_source_reference(group_path, idx),
                    "data": extracted_data,
                }
            )

    return {
        "schema_family": schema_family,
        "records": records,
        "record_count": len(records),
    }


# -----------------------------
# PATH RESOLUTION
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
    """
    Read a path relative to the matched node.
    Example:
      Recipe.RecipeID
      DateTime
      Value
    """
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


def _build_source_reference(path: str, idx: int) -> str:
    return f"{path}[{idx}]"

def _normalize_runtime_path(path: str) -> str:
    if path == "[]":
        return "$[]"
    if path.startswith("[]."):
        return "$" + path
    return path

def _matches_where(node: Any, where: dict | None) -> bool:
    if not where:
        return True
    if not isinstance(node, dict):
        return False

    field = where.get("field")
    expected = where.get("equals")

    if field not in node:
        return False

    return node[field] == expected