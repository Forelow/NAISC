from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore


load_dotenv()


ALLOWED_RECORD_TYPES = [
    "equipment_state",
    "process_parameter_recipe",
    "sensor_reading",
    "fault_event",
    "wafer_processing_sequence",
]


def extract_records_from_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
    text = chunk.get("text", "").strip()
    chunk_id = chunk.get("chunk_id")

    if not text:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {
                "status": "empty_chunk",
            },
        }

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {
                "status": "llm_unavailable",
                "reason": "OPENAI_API_KEY missing",
            },
        }

    if OpenAI is None:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {
                "status": "llm_unavailable",
                "reason": "openai package not installed",
            },
        }

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    system_prompt = (
        "You are an extraction agent for low-structure technical text. "
        "Your task is to read one chunk of free-form technical text and extract zero or more operational records. "
        "Only use these record types: equipment_state, process_parameter_recipe, sensor_reading, fault_event, wafer_processing_sequence. "
        "Return JSON only. "
        "Do not invent facts. "
        "If the chunk does not contain a usable record, return an empty records list. "
        "Each record must contain: record_type, confidence, evidence_text, data. "
        "confidence must be a float between 0 and 1. "
        "evidence_text must be a short excerpt from the chunk supporting the extraction. "
        "data must be an object with only fields actually supported by the text."
    )

    user_payload = {
        "chunk_id": chunk_id,
        "text": text,
        "allowed_record_types": ALLOWED_RECORD_TYPES,
    }

    try:
        client = OpenAI(api_key=api_key)

        if hasattr(client, "responses"):
            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
                ],
            )
            output_text = getattr(response, "output_text", None)
            parsed = json.loads(output_text) if output_text else {"records": []}
        else:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
                ],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
            parsed = json.loads(content) if content else {"records": []}

        return {
            "chunk_id": chunk_id,
            "records": parsed.get("records", []),
            "debug": {
                "status": "ok",
                "raw_response": parsed,
            },
        }

    except Exception as e:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {
                "status": "error",
                "error": str(e),
            },
        }