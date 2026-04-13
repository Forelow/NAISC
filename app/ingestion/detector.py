from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import csv
import json
import re
import xml.etree.ElementTree as ET


@dataclass
class DetectionResult:
    file_class: str
    format_name: str
    confidence: float
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


SYSLOG_REGEX = re.compile(
    r"^\w{3}\s+\d{1,2}\s\d{2}:\d{2}:\d{2}|^\d{4}-\d{2}-\d{2}"
)

KV_REGEX = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\s*=\s*[^=\s]+")


def is_probably_text(data: bytes, threshold: float = 0.90) -> bool:
    if not data:
        return True
    printable = sum(
        1 for b in data if 32 <= b <= 126 or b in (9, 10, 13)
    )
    return (printable / len(data)) >= threshold


def try_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except Exception:
        return False


def try_xml(text: str) -> bool:
    try:
        ET.fromstring(text)
        return True
    except Exception:
        return False


def try_csv(text: str) -> bool:
    try:
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) < 2:
            return False
        sample = "\n".join(lines[:10])
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(lines[:10], dialect)
        row_lengths = [len(row) for row in reader]
        return len(set(row_lengths)) == 1 and row_lengths[0] > 1
    except Exception:
        return False


def detect_text_pattern(text: str) -> DetectionResult:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return DetectionResult("unstructured", "empty_text", 0.60, "Empty text file")

    sample_lines = lines[:20]

    syslog_hits = sum(1 for line in sample_lines if SYSLOG_REGEX.search(line))
    kv_hits = sum(1 for line in sample_lines if KV_REGEX.search(line))

    if syslog_hits >= max(3, len(sample_lines) // 3):
        return DetectionResult(
            "semi_structured",
            "syslog_like_text",
            0.88,
            "Repeated timestamp/syslog-like line pattern detected",
        )

    if kv_hits >= max(3, len(sample_lines) // 3):
        return DetectionResult(
            "semi_structured",
            "key_value_text",
            0.86,
            "Repeated key=value pattern detected",
        )

    hex_chars = set("0123456789abcdefABCDEF \n\r\t")
    if all(ch in hex_chars for ch in text[:500]):
        return DetectionResult(
            "unstructured",
            "hex_dump_text",
            0.75,
            "Content resembles hexadecimal dump text",
        )

    return DetectionResult(
        "unstructured",
        "free_text_log",
        0.70,
        "No strong deterministic line structure detected",
    )


def detect_file(file_path: str) -> DetectionResult:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    data = path.read_bytes()

    if not is_probably_text(data):
        return DetectionResult(
            "unstructured",
            "binary_blob",
            0.90,
            "High proportion of non-printable bytes",
        )

    text = data.decode("utf-8", errors="replace").strip()

    if try_json(text):
        return DetectionResult(
            "structured",
            "json",
            0.98,
            "Valid JSON detected",
        )

    if try_xml(text):
        return DetectionResult(
            "structured",
            "xml",
            0.98,
            "Valid XML detected",
        )

    if try_csv(text):
        return DetectionResult(
            "structured",
            "csv",
            0.92,
            "Consistent delimited rows detected",
        )

    return detect_text_pattern(text)