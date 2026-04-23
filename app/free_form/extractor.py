from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

from free_form.schema import ALLOWED_COARSE_TYPES

load_dotenv()


def extract_records_from_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
    text = chunk.get("text", "").strip()
    chunk_id = chunk.get("chunk_id")

    if not text:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {"status": "empty_chunk"},
        }

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {"status": "llm_unavailable", "reason": "OPENAI_API_KEY missing"},
        }

    if OpenAI is None:
        return {
            "chunk_id": chunk_id,
            "records": [],
            "debug": {"status": "llm_unavailable", "reason": "openai package not installed"},
        }

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    system_prompt = (
        "You are an extraction agent for low-structure technical text. "
        "Read one chunk of free-form technical text and extract zero or more operational observations. "
        "Return JSON only. "
        "Prefer this exact outer shape: {\"records\": [...]} "
        "but if needed a top-level JSON array is also acceptable. "
        "Do not invent facts. "
        "Use these coarse types only: "
        + ", ".join(ALLOWED_COARSE_TYPES)
        + ". "
        "Each record must contain: coarse_type, subtype, confidence, evidence_text, data, extra, uncertain. "
        "confidence must be between 0 and 1. "
        "evidence_text must quote or closely paraphrase the supporting text. "
        "data should contain the most important structured fields. "
        "extra may contain useful leftover context. "
        "uncertain must be true if the chunk is ambiguous or the mapping is weak. "
        "Prefer fewer, richer records over many tiny redundant records."
    )

    user_payload = {
        "chunk_id": chunk_id,
        "text": text,
        "allowed_coarse_types": ALLOWED_COARSE_TYPES,
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

        records = _normalize_parsed_records(parsed)

        return {
            "chunk_id": chunk_id,
            "records": records,
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


def _normalize_parsed_records(parsed: Any) -> list[dict[str, Any]]:
    if parsed is None:
        return []

    # Case 1: model returned {"records": [...]}
    if isinstance(parsed, dict):
        records = parsed.get("records", [])
        if isinstance(records, list):
            return [r for r in records if isinstance(r, dict)]
        return []

    # Case 2: model returned [...] directly
    if isinstance(parsed, list):
        return [r for r in parsed if isinstance(r, dict)]

    return []