from __future__ import annotations

from typing import Any

from free_form.chunker import chunk_free_form_text
from free_form.extractor import extract_records_from_chunk
from free_form.validator import validate_extracted_records


def parse_free_form_text(doc_payload: dict[str, Any]) -> dict[str, Any]:
    chunks = chunk_free_form_text(doc_payload)

    all_records: list[dict[str, Any]] = []
    chunk_debug: list[dict[str, Any]] = []

    for chunk in chunks:
        extracted = extract_records_from_chunk(chunk)
        validated = validate_extracted_records(extracted)

        all_records.extend(validated["records"])
        chunk_debug.append(
            {
                "chunk_id": chunk["chunk_id"],
                "debug": validated.get("debug", {}),
                "record_count": len(validated["records"]),
            }
        )

    return {
        "schema_family": "free_form_text",
        "records": all_records,
        "record_count": len(all_records),
        "chunk_count": len(chunks),
        "chunk_debug": chunk_debug,
    }