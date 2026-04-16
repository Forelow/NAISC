from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


def load_xml_file(file_path: str) -> dict[str, Any]:
    path = Path(file_path)
    tree = ET.parse(path)
    root = tree.getroot()

    root_tag = _strip_ns(root.tag)
    return {
        root_tag: _element_to_data(root)
    }


def _element_to_data(elem: ET.Element) -> Any:
    children = list(elem)
    attrs = {f"@{_strip_ns(k)}": _coerce_value(v) for k, v in elem.attrib.items()}
    text = (elem.text or "").strip()
    ALWAYS_LIST_TAGS = {"Alarm", "Error", "Warning", "Fault"}

    # Case 1: leaf node with no children and no attrs -> scalar
    if not children and not attrs:
        return _coerce_value(text) if text != "" else None

    # Case 2: has attrs and/or children -> dict-like object
    data: dict[str, Any] = {}
    data.update(attrs)

    # group repeated child tags
    grouped: dict[str, list[Any]] = {}
    for child in children:
        child_tag = _strip_ns(child.tag)
        grouped.setdefault(child_tag, []).append(_element_to_data(child))

    for tag, items in grouped.items():
        if len(items) == 1 and tag not in ALWAYS_LIST_TAGS:
            data[tag] = items[0]
        else:
            data[tag] = items

    # preserve non-empty text in mixed-content nodes
    if text != "":
        data["_text"] = _coerce_value(text)

    return data


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
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value