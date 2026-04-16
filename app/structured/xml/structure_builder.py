from __future__ import annotations

from typing import Any

from structured.shared.contract import StructuredParseSpec
from structured.xml.group_builder import xml_groups_from_existing_config


def build_xml_parse_spec(
    raw_data: Any,
    structure_config: dict,
) -> StructuredParseSpec:
    """
    Temporary XML adapter:
    takes the current XML-derived structure_config dict
    and converts it into the shared downstream contract.
    """
    return xml_groups_from_existing_config(structure_config)