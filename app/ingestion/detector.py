from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import csv
import json
import re
import xml.etree.ElementTree as ET

from structured.csv.reader import looks_like_delimited_table, sniff_csv_delimiter_from_text


@dataclass
class DetectionResult:
    format_guess: str
    content_class: str
    confidence: float
    notes: str
    extension_hint: str
    is_text: bool

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
    return looks_like_delimited_table(text)

def detect_delimited_text(text: str, extension_hint: str) -> DetectionResult | None:
    if not looks_like_delimited_table(text):
        return None

    delimiter = sniff_csv_delimiter_from_text(text)

    return DetectionResult(
        format_guess="csv",
        content_class="structured_text",
        confidence=0.95,
        notes=f"Consistent delimited table detected (delimiter={repr(delimiter)})",
        extension_hint=extension_hint,
        is_text=True,
    )


def looks_like_hex_dump(text: str) -> bool:
    allowed = set("0123456789abcdefABCDEF \n\r\t:")
    snippet = text[:1000]
    if not snippet.strip():
        return False
    ratio = sum(1 for ch in snippet if ch in allowed) / len(snippet)
    return ratio > 0.95


def detect_text_pattern(text: str, extension_hint: str) -> DetectionResult:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return DetectionResult(
            format_guess="empty_text",
            content_class="unstructured_text",
            confidence=0.60,
            notes="Empty text file",
            extension_hint=extension_hint,
            is_text=True,
        )

    sample_lines = lines[:20]

    syslog_hits = sum(1 for line in sample_lines if SYSLOG_REGEX.search(line))
    kv_hits = sum(1 for line in sample_lines if KV_REGEX.search(line))

    if syslog_hits >= max(3, len(sample_lines) // 3):
        return DetectionResult(
            format_guess="syslog_like_text",
            content_class="semi_structured_text",
            confidence=0.88,
            notes="Repeated timestamp/syslog-like pattern detected",
            extension_hint=extension_hint,
            is_text=True,
        )

    if kv_hits >= max(3, len(sample_lines) // 3):
        return DetectionResult(
            format_guess="key_value_text",
            content_class="semi_structured_text",
            confidence=0.86,
            notes="Repeated key=value pattern detected",
            extension_hint=extension_hint,
            is_text=True,
        )

    if looks_like_hex_dump(text):
        return DetectionResult(
            format_guess="hex_dump_text",
            content_class="unstructured_text",
            confidence=0.78,
            notes="Text resembles hexadecimal dump",
            extension_hint=extension_hint,
            is_text=True,
        )

    return DetectionResult(
        format_guess="free_text_log",
        content_class="unstructured_text",
        confidence=0.70,
        notes="No strong deterministic line structure detected",
        extension_hint=extension_hint,
        is_text=True,
    )


def detect_file(file_path: str) -> DetectionResult:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    extension_hint = path.suffix.lower()
    data = path.read_bytes()

    # Cheap extension hint first
    if extension_hint == ".parquet":
        return DetectionResult(
            format_guess="parquet",
            content_class="structured_binary",
            confidence=0.95,
            notes="Extension indicates Parquet",
            extension_hint=extension_hint,
            is_text=False,
        )

    if extension_hint == ".json":
        try:
            text = data.decode("utf-8", errors="replace").strip()
            if try_json(text):
                return DetectionResult(
                    format_guess="json",
                    content_class="structured_text",
                    confidence=0.99,
                    notes="Valid JSON detected",
                    extension_hint=extension_hint,
                    is_text=True,
                )
        except Exception:
            pass

    if extension_hint == ".xml":
        try:
            text = data.decode("utf-8", errors="replace").strip()
            if try_xml(text):
                return DetectionResult(
                    format_guess="xml",
                    content_class="structured_text",
                    confidence=0.99,
                    notes="Valid XML detected",
                    extension_hint=extension_hint,
                    is_text=True,
                )
        except Exception:
            pass

    if extension_hint in {".csv", ".tsv"}:
        try:
            text = data.decode("utf-8", errors="replace").strip()
            detected = detect_delimited_text(text, extension_hint)
            if detected is not None:
                return detected
        except Exception:
            pass

    # Content sniffing next
    if not is_probably_text(data):
        return DetectionResult(
            format_guess="opaque_binary",
            content_class="binary",
            confidence=0.90,
            notes="High proportion of non-printable bytes",
            extension_hint=extension_hint,
            is_text=False,
        )

    text = data.decode("utf-8", errors="replace").strip()

    if try_json(text):
        return DetectionResult(
            format_guess="json",
            content_class="structured_text",
            confidence=0.98,
            notes="Valid JSON detected from content",
            extension_hint=extension_hint,
            is_text=True,
        )

    if try_xml(text):
        return DetectionResult(
            format_guess="xml",
            content_class="structured_text",
            confidence=0.98,
            notes="Valid XML detected from content",
            extension_hint=extension_hint,
            is_text=True,
        )

    detected_csv = detect_delimited_text(text, extension_hint)
    if detected_csv is not None:
        return detected_csv

    return detect_text_pattern(text, extension_hint)