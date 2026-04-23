from __future__ import annotations

from typing import Any

from free_form.chunker import chunk_free_form_text
from free_form.extractor import extract_records_from_chunk
from free_form.validator import validate_extracted_records
from free_form.postprocessor import postprocess_free_form_records
from free_form.condenser import build_condensed_records


def parse_free_form_text(doc_payload: dict[str, Any]) -> dict[str, Any]:
    chunks = chunk_free_form_text(doc_payload)

    raw_validated_records: list[dict[str, Any]] = []
    chunk_debug: list[dict[str, Any]] = []

    for chunk in chunks:
        extracted = extract_records_from_chunk(chunk)
        validated = validate_extracted_records(extracted)

        raw_validated_records.extend(validated["records"])
        chunk_debug.append(
            {
                "chunk_id": chunk["chunk_id"],
                "debug": validated.get("debug", {}),
                "record_count": len(validated["records"]),
            }
        )

    detailed_records = postprocess_free_form_records(raw_validated_records)
    condensed_records = build_condensed_records(detailed_records)

    return {
        "schema_family": "free_form_text",
        "records": condensed_records,
        "record_count": len(condensed_records),
        "condensed_records": condensed_records,
        "condensed_record_count": len(condensed_records),
        "detailed_records": detailed_records,
        "detailed_record_count": len(detailed_records),
        "chunk_count": len(chunks),
        "chunk_debug": chunk_debug,
    }