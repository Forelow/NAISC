from __future__ import annotations

import re
from typing import Any


TIMESTAMP_PREFIX_RE = re.compile(
    r"^\s*(?:\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?|[A-Z][a-z]{2}\s+\d{1,2}\s\d{2}:\d{2}:\d{2})"
)

KV_TOKEN_RE = re.compile(r"\b[\w.-]+=([^\s]+)")
FACILITY_LEVEL_RE = re.compile(r"\b[A-Za-z0-9_.-]+\.(?:debug|info|notice|warn|warning|error|critical|alert|emerg|fatal):", re.IGNORECASE)
LEADING_TAG_RE = re.compile(r"\[[^\]]+\]")


def detect_semi_structured_family(text_payload: dict[str, Any]) -> dict[str, Any]:
    lines = text_payload.get("lines", [])
    non_empty_lines = [x["text"] for x in lines if x["text"].strip()]

    if not non_empty_lines:
        return {
            "family": "unknown_text",
            "confidence": 0.0,
            "notes": "Empty text file.",
        }

    kv_score = _score_kv_lines(non_empty_lines[:20])
    syslog_score = _score_syslog_like(non_empty_lines[:20])
    labeled_block_score = _score_labeled_blocks(non_empty_lines[:20])

    if syslog_score >= 0.6 and syslog_score >= kv_score:
        family, confidence, notes = ("syslog_like_log", syslog_score, "Timestamp/message style line log")
    else:
        best = max(
            [
                ("kv_line_log", kv_score, "Many key=value pairs per line"),
                ("syslog_like_log", syslog_score, "Timestamp/message style line log"),
                ("labeled_text_block", labeled_block_score, "Repeated label:value style blocks"),
            ],
            key=lambda x: x[1],
        )
        family, confidence, notes = best

    if confidence < 0.4:
        return {
            "family": "unknown_text",
            "confidence": confidence,
            "notes": "No strong semi-structured pattern detected.",
        }

    return {
        "family": family,
        "confidence": round(confidence, 2),
        "notes": notes,
    }


def _score_kv_lines(lines: list[str]) -> float:
    if not lines:
        return 0.0

    strong = 0
    for line in lines:
        kv_count = len(re.findall(r"\b[\w.-]+=[^\s]+", line))
        if kv_count >= 2:
            strong += 1

    return strong / len(lines)


def _score_syslog_like(lines: list[str]) -> float:
    if not lines:
        return 0.0

    strong = 0
    for line in lines:
        has_ts = bool(TIMESTAMP_PREFIX_RE.search(line))
        has_level = any(token in line for token in ["INFO", "WARN", "WARNING", "ERROR", "CRITICAL"])
        has_facility = bool(FACILITY_LEVEL_RE.search(line))
        has_tag = bool(LEADING_TAG_RE.search(line))
        if (has_ts and (has_facility or has_tag)) or (has_ts and has_level) or (has_level and len(line.split()) >= 4):
            strong += 1

    return strong / len(lines)


def _score_labeled_blocks(lines: list[str]) -> float:
    if not lines:
        return 0.0

    strong = 0
    for line in lines:
        if ":" in line:
            left, _, right = line.partition(":")
            if left.strip() and right.strip():
                strong += 1

    return strong / len(lines)