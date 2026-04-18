from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CANDIDATE_DELIMITERS = [",", ";", "\t", "|"]


@dataclass
class CSVLoadResult:
    rows: list[dict[str, Any]]
    delimiter: str
    header: list[str]
    warnings: list[str] = field(default_factory=list)
    malformed_row_numbers: list[int] = field(default_factory=list)


def load_csv_with_diagnostics(file_path: str) -> CSVLoadResult:
    text = Path(file_path).read_text(encoding="utf-8-sig", errors="replace")
    delimiter = sniff_csv_delimiter_from_text(text)

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    raw_rows = [
        [cell.strip() for cell in row]
        for row in reader
        if row and any(cell.strip() for cell in row)
    ]

    if not raw_rows:
        return CSVLoadResult(
            rows=[],
            delimiter=delimiter,
            header=[],
            warnings=["Empty delimited file."],
            malformed_row_numbers=[],
        )

    header = raw_rows[0]
    expected_width = len(header)

    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    malformed_row_numbers: list[int] = []

    for line_no, row in enumerate(raw_rows[1:], start=2):
        if len(row) != expected_width:
            malformed_row_numbers.append(line_no)
            warnings.append(
                f"Row {line_no} has {len(row)} fields; expected {expected_width}. Skipped as malformed."
            )
            continue

        record: dict[str, Any] = {}
        for key, value in zip(header, row):
            record[key] = _coerce_value(value)

        if any(v is not None for v in record.values()):
            rows.append(record)

    return CSVLoadResult(
        rows=rows,
        delimiter=delimiter,
        header=header,
        warnings=warnings,
        malformed_row_numbers=malformed_row_numbers,
    )


def load_csv_rows(file_path: str) -> list[dict[str, Any]]:
    return load_csv_with_diagnostics(file_path).rows


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