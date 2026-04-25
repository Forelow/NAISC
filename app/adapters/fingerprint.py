from __future__ import annotations

import hashlib
import json
from typing import Any


def build_schema_fingerprint(result_payload: dict[str, Any]) -> str:
    structure_summary = result_payload.get("structure_summary", {}) or {}
    structure_config = result_payload.get("structure_config", {}) or {}
    detection = result_payload.get("detection", {}) or {}

    summary_core = {
        "format": structure_summary.get("format"),
        "top_level_type": structure_summary.get("top_level_type"),
        "top_level_keys": structure_summary.get("top_level_keys", []),
        "columns": structure_summary.get("columns", []),
        "repeated_paths": structure_summary.get("repeated_paths", []),
        "sample_leaf_paths": structure_summary.get("sample_leaf_paths", [])[:40],
        "format_guess": detection.get("format_guess"),
    }

    groups = []
    for group in structure_config.get("record_groups", []) or []:
        if not isinstance(group, dict):
            continue
        groups.append(
            {
                "record_type": group.get("record_type"),
                "path": group.get("path"),
                "field_paths": sorted(group.get("field_paths", []) or []),
                "context_paths": sorted(group.get("context_paths", []) or []),
            }
        )

    payload = {
        "summary": summary_core,
        "groups": sorted(groups, key=lambda x: (str(x.get("record_type")), str(x.get("path")))),
    }

    text = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]