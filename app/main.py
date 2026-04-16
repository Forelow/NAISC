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
        parsed_result = parse_with_structure_config(raw_json, structure_config)

        result_payload["structure_summary"] = structure_summary
        result_payload["structure_config"] = structure_config
        result_payload["agent_debug"] = agent_debug
        result_payload["parsed_result"] = parsed_result
    else:
        result_payload["parsed_result"] = {
            "status": "not_implemented_for_this_format_yet"
        }

    output_file = save_result_to_file(file_info, result_payload)
    print(f"Result written to: {output_file}")
    return output_file


if __name__ == "__main__":
    run_pipeline("data/synthetic_logs/vendorA.json")
    run_pipeline("data/synthetic_logs/vendorB.json")
    run_pipeline("data/synthetic_logs/vendorF.json")