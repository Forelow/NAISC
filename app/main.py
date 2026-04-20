from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

from ingestion.receiver import ingest_file
from ingestion.detector import detect_file
from ingestion.support_registry import check_support
from ingestion.router import route_file

from readers.json_reader import load_json_file, summarize_json_structure
from ai.structure_agent import detect_structure_with_agent
from parser.generic_structured_parser import parse_with_structure_config
from structured.json.structure_builder import build_json_parse_spec
from structured.csv.reader import load_csv_rows
from structured.csv.reader import load_csv_with_diagnostics
from structured.csv.structure_builder import build_csv_parse_spec
from readers.json_reader import load_json_file, summarize_json_structure
from structured.xml.reader import load_xml_file
from structured.xml.structure_builder import build_xml_parse_spec
from semi_structured.reader import load_text_file
from semi_structured.family_detector import detect_semi_structured_family
from semi_structured.parser import parse_semi_structured



OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_result_to_file(file_info: dict, result_payload: dict) -> str:
    original_name = Path(file_info["filename"]).stem
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"{original_name}_{timestamp}_result.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result_payload, f, indent=4, ensure_ascii=False)

    return str(output_path)


def run_pipeline(file_path: str) -> str:
    ingested = ingest_file(file_path)
    file_info = ingested.to_dict()

    detection = detect_file(file_info["raw_path"])
    support = check_support(detection.format_guess)
    routing = route_file(detection, support)

    result_payload = {
        "ingested": file_info,
        "detection": detection.to_dict(),
        "support": support.to_dict(),
        "routing": routing.to_dict(),
        "structure_summary": None,
        "structure_config": None,
        "parsed_result": None,
        "agent_debug": None,
    }

    if detection.format_guess == "json":
        raw_json = load_json_file(file_info["raw_path"])
        structure_summary = summarize_json_structure(raw_json)
        structure_config, agent_debug = detect_structure_with_agent(structure_summary, raw_json)
        parse_spec = build_json_parse_spec(raw_json, structure_config)
        parsed_result = parse_with_structure_config(raw_json, parse_spec)

        result_payload["structure_summary"] = structure_summary
        result_payload["structure_config"] = structure_config
        result_payload["agent_debug"] = agent_debug
        result_payload["parsed_result"] = parsed_result
    elif detection.format_guess == "csv":
        csv_result = load_csv_with_diagnostics(file_info["raw_path"])
        rows = csv_result.rows

        parse_spec = build_csv_parse_spec(rows)
        parsed_result = parse_with_structure_config(rows, parse_spec)

        result_payload["structure_summary"] = {
            "format": "csv",
            "top_level_type": "list",
            "row_count": len(rows),
            "columns": csv_result.header,
            "delimiter": csv_result.delimiter,
        }
        result_payload["structure_config"] = parse_spec.to_dict()
        result_payload["agent_debug"] = {
            "final_source": "deterministic_csv_builder",
            "csv_warnings": csv_result.warnings,
            "malformed_row_numbers": csv_result.malformed_row_numbers,
        }
        result_payload["parsed_result"] = parsed_result
    elif detection.format_guess == "xml":
        raw_xml = load_xml_file(file_info["raw_path"])
        structure_summary = summarize_json_structure(raw_xml)
        structure_config, agent_debug = detect_structure_with_agent(structure_summary, raw_xml)
        parse_spec = build_xml_parse_spec(raw_xml, structure_config)
        parsed_result = parse_with_structure_config(raw_xml, parse_spec)

        result_payload["structure_summary"] = structure_summary
        result_payload["structure_config"] = structure_config
        result_payload["agent_debug"] = agent_debug
        result_payload["parsed_result"] = parsed_result
    elif routing.next_route == "semi_structured_parser":
        text_payload = load_text_file(file_info["raw_path"])

        if detection.format_guess == "syslog_like_text":
            family_info = {
                "family": "syslog_like_log",
                "confidence": detection.confidence,
                "notes": "Upstream detector identified syslog-like text",
            }
        else:
            family_info = detect_semi_structured_family(text_payload)

        parsed_result = parse_semi_structured(text_payload, family_info)

        result_payload["structure_summary"] = {
            "format": "text",
            "line_count": len(text_payload["lines"]),
            "family_info": family_info,
        }
        result_payload["structure_config"] = {
            "family": family_info["family"],
            "confidence": family_info["confidence"],
            "notes": family_info["notes"],
        }
        result_payload["parsed_result"] = parsed_result
        result_payload["agent_debug"] = {
            "final_source": "deterministic_semi_structured_parser"
        }
    else:
        result_payload["parsed_result"] = {
            "status": "not_implemented_for_this_format_yet"
        }

    output_file = save_result_to_file(file_info, result_payload)
    print(f"Result written to: {output_file}")
    return output_file


if __name__ == "__main__":
    run_pipeline("data/synthetic_logs/vendorA.syslog")