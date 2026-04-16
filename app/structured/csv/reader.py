from __future__ import annotations

import csv
from pathlib import Path


def load_csv_rows(file_path: str) -> list[dict]:
    path = Path(file_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]