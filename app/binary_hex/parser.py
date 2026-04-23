from __future__ import annotations

from typing import Any

from binary_hex.reader import read_binary_or_hex_input
from binary_hex.decoder import is_known_mvp_binary, decode_mvp_binary_container


def is_binary_or_hex_candidate(file_path: str, detection: Any) -> bool:
    path = str(file_path).lower()

    if path.endswith(".bin") or path.endswith(".hex"):
        return True

    format_guess = getattr(detection, "format_guess", "")
    if format_guess in {
        "binary_file",
        "binary_blob",
        "hex_dump",
        "hex_text",
        "binary_hex",
    }:
        return True

    return False


def parse_binary_or_hex_file(file_path: str) -> dict[str, Any]:
    blob = read_binary_or_hex_input(file_path)
    raw_bytes = blob["raw_bytes"]

    known = is_known_mvp_binary(raw_bytes)

    if known:
        decoded = decode_mvp_binary_container(raw_bytes)

        return {
            "structure_summary": {
                "format": "binary_hex",
                "input_mode": blob["input_mode"],
                "byte_count": blob["byte_count"],
                "magic_ascii": decoded["header"]["magic_ascii"],
                "known_container": True,
            },
            "structure_config": {
                "family": "binary_hex",
                "parser_strategy": "magic_header_then_tlv_kv",
                "container_type": "SLOG",
                "version": decoded["header"]["version"],
            },
            "parsed_result": {
                "schema_family": "binary_hex_mvp",
                "records": decoded["records"],
                "record_count": len(decoded["records"]),
                "status": "ok",
                "container_header": decoded["header"],
            },
            "agent_debug": {
                "final_source": "deterministic_binary_parser",
                "input_mode": blob["input_mode"],
                "head_hex": blob["head_hex"],
                "head_ascii": blob["head_ascii"],
            },
        }

    return {
        "structure_summary": {
            "format": "binary_hex",
            "input_mode": blob["input_mode"],
            "byte_count": blob["byte_count"],
            "known_container": False,
        },
        "structure_config": {
            "family": "binary_hex",
            "parser_strategy": "metadata_only_unknown_binary",
        },
        "parsed_result": {
            "schema_family": "binary_hex_mvp",
            "records": [],
            "record_count": 0,
            "status": "unknown_binary_format",
            "binary_preview": {
                "byte_count": blob["byte_count"],
                "head_hex": blob["head_hex"],
                "head_ascii": blob["head_ascii"],
            },
        },
        "agent_debug": {
            "final_source": "binary_metadata_only",
            "input_mode": blob["input_mode"],
        },
    }