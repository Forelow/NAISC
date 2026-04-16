from __future__ import annotations

from typing import Any

from structured.shared.contract import StructuredParseSpec
from structured.json.group_builder import json_groups_from_existing_config


def build_json_parse_spec(
    raw_data: Any,
    structure_config: dict,
) -> StructuredParseSpec:
    """
    Temporary adapter:
    takes the current JSON structure_config dict
    and converts it into the new shared contract.
    """
    return json_groups_from_existing_config(structure_config)