from __future__ import annotations


def handle_unsupported_file(file_info: dict, detection: dict, routing: dict) -> dict:
    return {
        "status": "unsupported_or_quarantined",
        "file_id": file_info.get("file_id"),
        "filename": file_info.get("filename"),
        "format_guess": detection.get("format_guess"),
        "notes": routing.get("notes"),
    }