from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any


def load_xml_file(file_path: str) -> dict[str, Any]:
    path = Path(file_path)
    tree = ET.parse(path)
    root = tree.getroot()

    root_tag = _strip_ns(root.tag)
    raw_data = {root_tag: _element_to_data(root)}

    multi_child_tags = _collect_multi_child_tags(raw_data)
    normalized_data = _normalize_multi_child_tags(raw_data, multi_child_tags)

    promotable_object_paths = _collect_promotable_singleton_object_paths(normalized_data)
    normalized_data = _promote_singleton_object_paths_to_lists(
        normalized_data,
        promotable_object_paths,
    )

    return normalized_data


def _element_to_data(elem: ET.Element) -> Any:
    children = list(elem)
    attrs = {f"@{_strip_ns(k)}": _coerce_value(v) for k, v in elem.attrib.items()}
    text = (elem.text or "").strip()

    # leaf node
    if not children and not attrs:
        return _coerce_value(text) if text != "" else None

    data: dict[str, Any] = {}
    data.update(attrs)

    grouped: dict[str, list[Any]] = {}
    for child in children:
        child_tag = _strip_ns(child.tag)
        grouped.setdefault(child_tag, []).append(_element_to_data(child))

    for tag, items in grouped.items():
        if len(items) == 1:
            data[tag] = items[0]
        else:
            data[tag] = items

    if text != "":
        data["_text"] = _coerce_value(text)

    return data


def _collect_multi_child_tags(node: Any) -> dict[str, set[str]]:
    """
    Build a map:
        parent_path -> set(child_tags_that_should_be_lists)

    Rule:
    If any occurrence of a parent path contains child tag X as a list,
    then X should be treated as a list for all occurrences of that parent path.
    """
    result: dict[str, set[str]] = defaultdict(set)

    def visit(current: Any, current_path: str) -> None:
        if isinstance(current, dict):
            for key, value in current.items():
                if isinstance(value, list):
                    result[current_path].add(key)

            for key, value in current.items():
                child_path = f"{current_path}.{key}" if current_path else key

                if isinstance(value, dict):
                    visit(value, child_path)
                elif isinstance(value, list):
                    item_path = f"{child_path}[]"
                    for item in value:
                        visit(item, item_path)

        elif isinstance(current, list):
            for item in current:
                visit(item, current_path)

    visit(node, "")
    return {k: set(v) for k, v in result.items()}


def _normalize_multi_child_tags(node: Any, multi_child_tags: dict[str, set[str]]) -> Any:
    """
    Normalize the tree so that if child tag X is multi-valued for a given parent path,
    then every occurrence under that same parent path is represented as a list.
    """
    def visit(current: Any, current_path: str) -> Any:
        if isinstance(current, dict):
            normalized: dict[str, Any] = {}
            forced_list_children = multi_child_tags.get(current_path, set())

            for key, value in current.items():
                child_path = f"{current_path}.{key}" if current_path else key

                if key in forced_list_children and not isinstance(value, list):
                    value = [value]

                if isinstance(value, dict):
                    normalized[key] = visit(value, child_path)
                elif isinstance(value, list):
                    item_path = f"{child_path}[]"
                    normalized[key] = [visit(item, item_path) for item in value]
                else:
                    normalized[key] = value

            return normalized

        if isinstance(current, list):
            return [visit(item, current_path) for item in current]

        return current

    return visit(node, "")


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _coerce_value(value: str | None) -> Any:
    if value is None:
        return None

    value = value.strip()

    if value == "":
        return None

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if "." in value or "e" in lowered:
            return float(value)
        return int(value)
    except ValueError:
        return value
    
def _collect_promotable_singleton_object_paths(node: Any) -> set[str]:
    """
    Find dict-like child objects under repeated-parent regions that should be promoted
    to one-element lists so they can be treated as record groups later.

    Generic rule:
    - child is a dict
    - child looks record-like (has multiple scalar leaves/attrs)
    - child is not just a pure wrapper object
    """
    promotable: set[str] = set()

    def visit(current: Any, current_path: str, inside_repeated_region: bool) -> None:
        if isinstance(current, dict):
            for key, value in current.items():
                child_path = f"{current_path}.{key}" if current_path else key

                if inside_repeated_region and isinstance(value, dict) and _is_record_like_dict(value):
                    promotable.add(child_path)

                if isinstance(value, dict):
                    visit(value, child_path, inside_repeated_region)
                elif isinstance(value, list):
                    item_path = f"{child_path}[]"
                    for item in value:
                        visit(item, item_path, True)

        elif isinstance(current, list):
            for item in current:
                visit(item, current_path, True)

    visit(node, "", False)
    return promotable


def _promote_singleton_object_paths_to_lists(node: Any, promotable_paths: set[str]) -> Any:
    def visit(current: Any, current_path: str) -> Any:
        if isinstance(current, dict):
            normalized: dict[str, Any] = {}

            for key, value in current.items():
                child_path = f"{current_path}.{key}" if current_path else key

                if child_path in promotable_paths and isinstance(value, dict):
                    value = [value]

                if isinstance(value, dict):
                    normalized[key] = visit(value, child_path)
                elif isinstance(value, list):
                    item_path = f"{child_path}[]"
                    normalized[key] = [visit(item, item_path) for item in value]
                else:
                    normalized[key] = value

            return normalized

        if isinstance(current, list):
            return [visit(item, current_path) for item in current]

        return current

    return visit(node, "")


def _is_record_like_dict(obj: dict[str, Any]) -> bool:
    """
    Generic heuristic:
    - has at least 2 scalar leaves/attrs somewhere inside
    - not merely a wrapper with one nested child and no own scalar content
    """
    scalar_leaf_count = _count_scalar_leaves(obj)
    own_scalar_keys = [
        k for k, v in obj.items()
        if not k.startswith("_") and _is_scalar(v)
    ]
    non_meta_keys = [k for k in obj.keys() if not k.startswith("_")]

    if scalar_leaf_count < 2:
        return False

    # skip pure wrappers like {"Fault": {...}} or {"SensorTrace": [...]}
    if len(non_meta_keys) == 1 and len(own_scalar_keys) == 0:
        only_key = non_meta_keys[0]
        only_value = obj[only_key]
        if isinstance(only_value, (dict, list)):
            return False

    return True


def _count_scalar_leaves(value: Any) -> int:
    if _is_scalar(value):
        return 1
    if isinstance(value, dict):
        return sum(_count_scalar_leaves(v) for v in value.values())
    if isinstance(value, list):
        return sum(_count_scalar_leaves(v) for v in value)
    return 0


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))