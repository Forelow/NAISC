from __future__ import annotations

from structured.shared.contract import StructuredParseSpec, spec_from_dict


def json_groups_from_existing_config(config: dict) -> StructuredParseSpec:
    return spec_from_dict(config)