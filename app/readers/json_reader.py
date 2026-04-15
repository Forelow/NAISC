from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json_file(file_path: str) -> Any:
    path = Path(file_path)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return json.load(f)


def summarize_json_structure(data: Any) -> dict:
    summary = {
        "format": "json",
        "top_level_type": type(data).__name__,
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
        "repeated_paths": [],
        "sample_leaf_paths": [],
    }

    repeated_paths = []
    leaf_paths = []

    def walk(node: Any, path: list[str]) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, path + [k])
            return

        if isinstance(node, list):
            current_path = ".".join(path) + "[]"
            if node and all(isinstance(item, dict) for item in node):
                repeated_paths.append(current_path)

            for item in node[:3]:
                walk(item, path + ["[]"])
            return

        leaf_paths.append(".".join(path))

    walk(data, [])

    summary["repeated_paths"] = sorted(set(repeated_paths))
    summary["sample_leaf_paths"] = sorted(set(leaf_paths))[:50]
    return summary