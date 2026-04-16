from __future__ import annotations

import csv
from pathlib import Path


def load_csv_rows(file_path: str) -> list[dict]:
    path = Path(file_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [
            {k: _coerce_value(v) for k, v in dict(row).items()}
            for row in reader
        ]


def _coerce_value(value: str):
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