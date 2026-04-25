from __future__ import annotations

import re
from pathlib import Path
from typing import Any


KNOWN_TEXT_EXTENSIONS = {
    ".json": "json",
    ".csv": "csv",
    ".xml": "xml",
    ".syslog": "syslog",
    ".log": "log",
    ".kv": "kv",
    ".txt": "txt",
}

KNOWN_BINARY_EXTENSIONS = {
    ".bin": "bin",
    ".hex": "hex",
    ".parquet": "parquet_like_binary",
}


def detect_unknown_file_route(file_path: str, sample_size: int = 65536) -> dict[str, Any]:
    path = Path(file_path)
    ext = path.suffix.lower()

    raw = path.read_bytes()[:sample_size]

    result = {
        "extension": ext,
        "is_known_text_extension": ext in KNOWN_TEXT_EXTENSIONS,
        "is_known_binary_extension": ext in KNOWN_BINARY_EXTENSIONS,
        "format_guess": "unknown",
        "content_class": "unknown",
        "parser_family": None,
        "support_status": "unknown",
        "next_route": None,
        "processing_mode": None,
        "should_attempt_parse": False,
        "notes": [],
    }

    # Known text extensions: let your existing text parsers handle them
    if ext in KNOWN_TEXT_EXTENSIONS:
        result["format_guess"] = KNOWN_TEXT_EXTENSIONS[ext]
        result["content_class"] = "known_text"
        result["notes"].append(f"Known text extension: {ext}")
        return result

    # Known binary extensions: do not fake-parse as text
    if ext in KNOWN_BINARY_EXTENSIONS:
        result["format_guess"] = KNOWN_BINARY_EXTENSIONS[ext]
        result["content_class"] = "structured_binary" if ext == ".parquet" else "opaque_unknown"
        result["parser_family"] = "binary_fallback"
        result["support_status"] = "limited"
        result["next_route"] = "binary_unknown_handler"
        result["processing_mode"] = "safe_reject_or_metadata_only"
        result["should_attempt_parse"] = False
        result["notes"].append(f"Known binary-like extension: {ext}")
        return result

    if not raw:
        result["format_guess"] = "empty_file"
        result["content_class"] = "empty"
        result["support_status"] = "unsupported"
        result["notes"].append("File is empty.")
        return result

    # Detect parquet by magic bytes even if extension is weird
    if _looks_like_parquet(raw):
        result["format_guess"] = "parquet_like_binary"
        result["content_class"] = "structured_binary"
        result["parser_family"] = "binary_fallback"
        result["support_status"] = "limited"
        result["next_route"] = "binary_unknown_handler"
        result["processing_mode"] = "safe_reject_or_metadata_only"
        result["should_attempt_parse"] = False
        result["notes"].append("Content looks like Parquet (PAR1 signature).")
        return result

    # Other binary-like / opaque unknowns
    if _looks_binary(raw):
        result["format_guess"] = "unknown_binary"
        result["content_class"] = "opaque_unknown"
        result["parser_family"] = "binary_fallback"
        result["support_status"] = "limited"
        result["next_route"] = "binary_unknown_handler"
        result["processing_mode"] = "safe_reject_or_metadata_only"
        result["should_attempt_parse"] = False
        result["notes"].append("Unknown extension and content looks binary/opaque.")
        return result

    # Readable text path
    text = _decode_text(raw)
    lines = _nonempty_lines(text)

    if _looks_json_text(text):
        result["format_guess"] = "json"
        result["content_class"] = "structured_text"
        result["parser_family"] = "structured_parser"
        result["support_status"] = "supported"
        result["next_route"] = "structured_parser"
        result["processing_mode"] = "direct_parse"
        result["should_attempt_parse"] = True
        result["notes"].append("Unknown extension, but content looks like JSON.")
        return result

    if _looks_xml_text(text):
        result["format_guess"] = "xml"
        result["content_class"] = "structured_text"
        result["parser_family"] = "structured_parser"
        result["support_status"] = "supported"
        result["next_route"] = "structured_parser"
        result["processing_mode"] = "direct_parse"
        result["should_attempt_parse"] = True
        result["notes"].append("Unknown extension, but content looks like XML.")
        return result

    delim = _detect_csv_delimiter(lines)
    if delim is not None:
        result["format_guess"] = "csv"
        result["content_class"] = "structured_text"
        result["parser_family"] = "structured_parser"
        result["support_status"] = "supported"
        result["next_route"] = "structured_parser"
        result["processing_mode"] = "direct_parse"
        result["should_attempt_parse"] = True
        result["notes"].append(f"Unknown extension, but content looks CSV-like ({repr(delim)} delimiter).")
        return result

    if _looks_kv_text(lines):
        result["format_guess"] = "key_value_text"
        result["content_class"] = "semi_structured_text"
        result["parser_family"] = "semi_structured_parser"
        result["support_status"] = "supported"
        result["next_route"] = "semi_structured_parser"
        result["processing_mode"] = "regex_or_rule_parse"
        result["should_attempt_parse"] = True
        result["notes"].append("Unknown extension, but content looks like key=value text.")
        return result

    if _looks_syslog_text(lines):
        result["format_guess"] = "syslog_like_text"
        result["content_class"] = "semi_structured_text"
        result["parser_family"] = "semi_structured_parser"
        result["support_status"] = "supported"
        result["next_route"] = "semi_structured_parser"
        result["processing_mode"] = "regex_or_rule_parse"
        result["should_attempt_parse"] = True
        result["notes"].append("Unknown extension, but content looks syslog-like.")
        return result

    # Final fallback: readable but messy text
    result["format_guess"] = "free_form_text"
    result["content_class"] = "unstructured_text"
    result["parser_family"] = "free_text_parser"
    result["support_status"] = "supported"
    result["next_route"] = "free_text_parser"
    result["processing_mode"] = "llm_or_text_parse"
    result["should_attempt_parse"] = True
    result["notes"].append("Unknown extension, readable text, no stronger structure detected.")
    return result


def _looks_like_parquet(raw: bytes) -> bool:
    if len(raw) < 4:
        return False
    return raw[:4] == b"PAR1"


def _looks_binary(raw: bytes) -> bool:
    if not raw:
        return False

    if b"\x00" in raw:
        return True

    printable = 0
    total = len(raw)

    for b in raw:
        if b in (9, 10, 13):
            printable += 1
        elif 32 <= b <= 126:
            printable += 1

    ratio = printable / max(total, 1)
    return ratio < 0.75


def _decode_text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def _nonempty_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def _looks_json_text(text: str) -> bool:
    stripped = text.lstrip()
    return bool(stripped) and stripped[0] in "{["


def _looks_xml_text(text: str) -> bool:
    stripped = text.lstrip()
    if not stripped.startswith("<"):
        return False
    if stripped.startswith("<?xml"):
        return True
    return bool(re.search(r"<[A-Za-z_][^>]*>", stripped))


def _detect_csv_delimiter(lines: list[str]) -> str | None:
    if len(lines) < 2:
        return None

    for delim in [",", "\t", ";", "|"]:
        counts = [line.count(delim) for line in lines[:10]]
        positives = [c for c in counts if c > 0]
        if len(positives) >= 2 and len(set(positives)) <= 2:
            return delim
    return None


def _looks_kv_text(lines: list[str]) -> bool:
    if not lines:
        return False

    matched = 0
    checked = 0
    for line in lines[:10]:
        checked += 1
        tokens = line.split()
        kv_count = sum(1 for token in tokens if "=" in token and not token.startswith("="))
        if kv_count >= 2:
            matched += 1

    return checked > 0 and (matched / checked) >= 0.5


def _looks_syslog_text(lines: list[str]) -> bool:
    if not lines:
        return False

    pattern = re.compile(r"^<\d+>")
    matched = 0
    checked = 0

    for line in lines[:10]:
        checked += 1
        if pattern.match(line):
            matched += 1

    return checked > 0 and (matched / checked) >= 0.5