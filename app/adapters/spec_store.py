from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SPEC_DIR = Path(__file__).resolve().parents[1] / "adapter_specs"
SPEC_DIR.mkdir(parents=True, exist_ok=True)


def get_spec_path(schema_fingerprint: str) -> Path:
    return SPEC_DIR / f"{schema_fingerprint}.json"


def load_adapter_spec(schema_fingerprint: str) -> dict[str, Any] | None:
    path = get_spec_path(schema_fingerprint)
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_adapter_spec(spec: dict[str, Any]) -> Path:
    schema_fingerprint = str(spec.get("schema_fingerprint", "") or "").strip()
    if not schema_fingerprint:
        raise ValueError("adapter spec missing schema_fingerprint")

    path = get_spec_path(schema_fingerprint)
    path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")
    return path