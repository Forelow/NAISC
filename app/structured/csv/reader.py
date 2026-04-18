from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

CANDIDATE_DELIMITERS = [",", ";", "\t", "|"]


def load_csv_rows(file_path: str) -> list[dict[str, Any]]:
    text = Path(file_path).read_text(encoding="utf-8-sig", errors="replace")
    delimiter = sniff_csv_delimiter_from_text(text)

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    rows: list[dict[str, Any]] = []
    for row in reader:
        if row is None:
            continue

        cleaned: dict[str, Any] = {}
        for key, value in row.items():
            if key is None:
                continue
            cleaned[key.strip()] = _coerce_value(value)

        if any(v is not None for v in cleaned.values()):
            rows.append(cleaned)

    return rows


def sniff_csv_delimiter_from_text(text: str) -> str:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return ","

    sample = "\n".join(lines[:10])

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(CANDIDATE_DELIMITERS))
        if dialect.delimiter in CANDIDATE_DELIMITERS:
            return dialect.delimiter
    except csv.Error:
        pass

    best_delimiter = ","
    best_score = (-1, -1)

    for delimiter in CANDIDATE_DELIMITERS:
        counts = [len(line.split(delimiter)) for line in lines[:10]]

        if len(counts) < 2 or max(counts) <= 1:
            continue

        first = counts[0]
        consistent = sum(1 for count in counts[1:] if count == first and count > 1)
        score = (consistent, first)

        if score > best_score:
            best_score = score
            best_delimiter = delimiter

    return best_delimiter


def looks_like_delimited_table(text: str) -> bool:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return False

    delimiter = sniff_csv_delimiter_from_text(text)
    counts = [len(line.split(delimiter)) for line in lines[:6]]

    return (
        counts[0] > 1
        and counts[0] >= 3
        and all(count == counts[0] for count in counts[1:])
    )


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