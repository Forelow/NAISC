from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

from ingestion.receiver import ingest_file
from ingestion.detector import detect_file
from ingestion.support_registry import check_support
from ingestion.router import route_file
from parser.structured_parser import parse_structured_file


OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def save_result_to_file(file_info: dict, result_payload: dict) -> str:
    """
    Save pipeline output to a readable JSON file.
    """
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
        "parsed_result": None,
    }

    if routing.next_route == "structured_parser":
        parsed = parse_structured_file(file_info, detection.format_guess)
        result_payload["parsed_result"] = parsed.to_dict()
    else:
        result_payload["parsed_result"] = {
            "status": "not_executed_in_this_phase",
            "reason": f"Current route is '{routing.next_route}'"
        }

    output_file = save_result_to_file(file_info, result_payload)
    print(f"Result written to: {output_file}")
    return output_file


if __name__ == "__main__":
    run_pipeline("data/synthetic_logs/vendorB_etch_tool_log.json")