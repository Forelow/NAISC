from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import re

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
from semi_structured.spec_builder import build_semi_structured_parse_spec
from semi_structured.family_detector import detect_semi_structured_family
from free_form.reader import load_text_document
from free_form.parser import parse_free_form_text
from binary_hex.parser import parse_binary_or_hex_file, is_binary_or_hex_candidate
from pipeline.runner import process_result_payload
from db.writer import write_pipeline_output_to_db
from pipeline.content_sniffer import detect_unknown_file_route


OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
KV_REGEX = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*[^,\s]+")


def _looks_like_kv_lines(lines: list[str]) -> bool:
    non_empty = [line.strip() for line in lines if line.strip()]
    if not non_empty:
        return False

    kv_hits = sum(1 for line in non_empty if KV_REGEX.search(line))
    return kv_hits >= max(3, len(non_empty) // 4)

def _force_kv_to_semi_structured(raw_path: str, detection: object) -> bool:
    ext = Path(raw_path).suffix.lower()
    format_guess = str(getattr(detection, "format_guess", "") or "")
    return format_guess in {"key_value_text", "key_value_log"} or (ext == ".kv" and format_guess == "free_text_log")

def save_result_to_file(file_info: dict, result_payload: dict) -> str:
    original_name = Path(file_info["filename"]).stem
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"{original_name}_{timestamp}_result.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result_payload, f, indent=4, ensure_ascii=False)

    return str(output_path)

def _should_fallback_to_free_form(parsed_result: dict, agent_debug: dict | None) -> bool:
    record_count = parsed_result.get("record_count", 0)

    if record_count == 0:
        return True

    validation = (agent_debug or {}).get("validation", {})
    if isinstance(validation, dict) and validation.get("accepted") is False:
        return True

    return False


def run_pipeline(file_path: str) -> str:
    ingested = ingest_file(file_path)
    file_info = ingested.to_dict()

    detection = detect_file(file_info["raw_path"])
    support = check_support(detection.format_guess)
    routing = route_file(detection, support)

    sniff = detect_unknown_file_route(file_info["raw_path"])

    # Use the sniffer only for:
    # 1) unknown / weird extensions that are not your normal text types
    # 2) parquet-like binary files
    # but do NOT override your existing .bin / .hex handling
    use_sniffer = (
        sniff["format_guess"] == "parquet_like_binary"
        or (
            not sniff["is_known_text_extension"]
            and not is_binary_or_hex_candidate(file_info["raw_path"], detection)
        )
    )

    if use_sniffer:
        detection_view = {
            "format_guess": sniff["format_guess"],
            "content_class": sniff["content_class"],
            "confidence": getattr(detection, "confidence", None),
            "extension_hint": sniff["extension"],
            "is_text": sniff["content_class"] in {
                "structured_text",
                "semi_structured_text",
                "unstructured_text",
                "known_text",
            },
            "notes": sniff["notes"],
        }

        support_view = {
            "format_guess": sniff["format_guess"],
            "is_supported": sniff["should_attempt_parse"],
            "parser_family": sniff["parser_family"],
            "support_status": sniff["support_status"],
            "notes": "; ".join(sniff["notes"]),
        }

        routing_view = {
            "next_route": sniff["next_route"],
            "processing_mode": sniff["processing_mode"],
            "should_attempt_parse": sniff["should_attempt_parse"],
            "notes": "; ".join(sniff["notes"]),
        }

        effective_format_guess = sniff["format_guess"]
        effective_next_route = sniff["next_route"]
        effective_is_text = detection_view["is_text"]
        effective_confidence = getattr(detection, "confidence", 0.0) or 0.0
    else:
        detection_view = detection.to_dict()
        support_view = support.to_dict()
        routing_view = routing.to_dict()

        effective_format_guess = detection.format_guess
        effective_next_route = routing.next_route
        effective_is_text = detection.is_text
        effective_confidence = detection.confidence

    result_payload = {
        "ingested": file_info,
        "detection": detection_view,
        "support": support_view,
        "routing": routing_view,
        "structure_summary": None,
        "structure_config": None,
        "parsed_result": None,
        "agent_debug": None,
    }
    if effective_next_route == "binary_unknown_handler":
        result_payload["structure_summary"] = {
            "format": "binary",
            "family_info": {
                "family": "binary_unknown",
                "confidence": effective_confidence,
                "notes": "Binary-like unknown or parquet-like file safely isolated.",
            },
        }
        result_payload["structure_config"] = {
            "family": "binary_unknown",
            "parser_strategy": "safe_reject_or_metadata_only",
        }
        result_payload["parsed_result"] = {
            "schema_family": "unsupported_binary",
            "records": [],
            "record_count": 0,
        }
        result_payload["agent_debug"] = {
            "final_source": "content_sniffer_safe_binary_fallback",
            "notes": sniff["notes"] if use_sniffer else ["Safe binary fallback used."],
        }

        output_file = save_result_to_file(file_info, result_payload)
        print(f"Result written to: {output_file}")
        return output_file

    if effective_format_guess == "json":
        raw_json = load_json_file(file_info["raw_path"])
        structure_summary = summarize_json_structure(raw_json)
        structure_config, agent_debug = detect_structure_with_agent(structure_summary, raw_json)
        parse_spec = build_json_parse_spec(raw_json, structure_config)
        parsed_result = parse_with_structure_config(raw_json, parse_spec)

        result_payload["structure_summary"] = structure_summary
        result_payload["structure_config"] = structure_config
        result_payload["agent_debug"] = agent_debug
        result_payload["parsed_result"] = parsed_result
    elif effective_format_guess == "csv":
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
    elif effective_format_guess == "xml":
        raw_xml = load_xml_file(file_info["raw_path"])
        structure_summary = summarize_json_structure(raw_xml)
        structure_config, agent_debug = detect_structure_with_agent(structure_summary, raw_xml)
        parse_spec = build_xml_parse_spec(raw_xml, structure_config)
        parsed_result = parse_with_structure_config(raw_xml, parse_spec)

        result_payload["structure_summary"] = structure_summary
        result_payload["structure_config"] = structure_config
        result_payload["agent_debug"] = agent_debug
        result_payload["parsed_result"] = parsed_result
    elif effective_format_guess == "semi_structured_parser":
        text_payload = load_text_file(file_info["raw_path"])

        family_info = {
            "family": "semi_structured_text",
            "confidence": effective_confidence,
            "notes": f"Unified semi-structured lane from {detection.format_guess}",
        }

        parse_spec, agent_debug = build_semi_structured_parse_spec(text_payload, family_info)
        parsed_result = parse_semi_structured(text_payload, family_info, parse_spec)

        result_payload["structure_summary"] = {
            "format": "text",
            "line_count": len(text_payload["lines"]),
            "family_info": family_info,
        }
        result_payload["structure_config"] = parse_spec
        result_payload["parsed_result"] = parsed_result
        result_payload["agent_debug"] = agent_debug
         # Fallback to free-form only if semi-structured clearly failed
        if _should_fallback_to_free_form(parsed_result, agent_debug):
            doc_payload = load_text_document(file_info["raw_path"])
            free_form_result = parse_free_form_text(doc_payload)

            if free_form_result.get("record_count", 0) > 0:
                result_payload["structure_summary"] = {
                    "format": "text",
                    "line_count": doc_payload["line_count"],
                    "char_count": doc_payload["char_count"],
                    "family_info": {
                        "family": "free_form_text",
                        "confidence": detection.confidence,
                        "notes": "Fallback from semi-structured lane to free-form text lane",
                    },
                }
                result_payload["structure_config"] = {
                    "family": "free_form_text",
                    "parser_strategy": "chunk_then_llm_extract",
                    "chunk_count": free_form_result.get("chunk_count", 0),
                }
                result_payload["parsed_result"] = free_form_result
                result_payload["agent_debug"] = {
                    "final_source": "free_form_fallback",
                    "semi_structured_attempt": {
                        "record_count": parsed_result.get("record_count", 0),
                        "validation": (agent_debug or {}).get("validation"),
                    },
                }
    elif is_binary_or_hex_candidate(file_info["raw_path"], detection):
        binary_result = parse_binary_or_hex_file(file_info["raw_path"])

        result_payload["structure_summary"] = binary_result["structure_summary"]
        result_payload["structure_config"] = binary_result["structure_config"]
        result_payload["parsed_result"] = binary_result["parsed_result"]
        result_payload["agent_debug"] = binary_result["agent_debug"]            
    elif effective_is_text:
        # For text files not claimed by the semi-structured route, use free-form directly
        doc_payload = load_text_document(file_info["raw_path"])
        parsed_result = parse_free_form_text(doc_payload)

        result_payload["structure_summary"] = {
            "format": "text",
            "line_count": doc_payload["line_count"],
            "char_count": doc_payload["char_count"],
            "family_info": {
                "family": "free_form_text",
                "confidence": effective_confidence,
                "notes": f"Direct free-form text lane from {detection.format_guess}",
            },
        }
        result_payload["structure_config"] = {
            "family": "free_form_text",
            "parser_strategy": "chunk_then_llm_extract",
            "chunk_count": parsed_result.get("chunk_count", 0),
        }
        result_payload["parsed_result"] = parsed_result
        result_payload["agent_debug"] = {
            "final_source": "free_form_direct",
        }            
    else:
        result_payload["parsed_result"] = {
            "status": "not_implemented_for_this_format_yet"
        }
    if isinstance(result_payload.get("parsed_result"), dict):
        result_payload["pipeline_output"] = process_result_payload(result_payload)
        result_payload["db_output"] = write_pipeline_output_to_db(result_payload)
    output_file = save_result_to_file(file_info, result_payload)
    print(f"Result written to: {output_file}")
    return output_file


if __name__ == "__main__":
    run_pipeline("data/testing/vendorA.json")
    run_pipeline("data/testing/vendorF.json")   

    run_pipeline("data/testing/vendorC.csv")
    run_pipeline("data/testing/vendorD.csv")

    run_pipeline("data/testing/vendorB.xml")
    run_pipeline("data/testing/vendorC.xml")

    run_pipeline("data/testing/vendorC.syslog")
    run_pipeline("data/testing/etch_tool_syslog.log")

    run_pipeline("data/testing/wet_clean_bench_kvlog.log")
    run_pipeline("data/testing/vendorA.kv")

    run_pipeline("data/testing/ion_implanter_freeform.txt")
    run_pipeline("data/testing/pvd_sputter_shift_log.txt")

    run_pipeline("data/testing/demo_tool_log.bin")
    run_pipeline("data/testing/demo_tool_log.hex")

    run_pipeline("data/testing/mystery_kv_like.abc")
    run_pipeline("data/testing/mystery_syslog_like.odd")




