from __future__ import annotations

import re
from typing import Any


def chunk_free_form_text(doc_payload: dict[str, Any], max_chars: int = 1800) -> list[dict[str, Any]]:
    text = doc_payload.get("text", "")
    if not text.strip():
        return []

    # First split by blank-line paragraphs
    raw_blocks = re.split(r"\n\s*\n+", text)
    raw_blocks = [block.strip() for block in raw_blocks if block.strip()]

    chunks: list[dict[str, Any]] = []
    chunk_id = 0

    for block in raw_blocks:
        # If block is already small enough, keep it whole
        if len(block) <= max_chars:
            chunk_id += 1
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "text": block,
                }
            )
            continue

        # Otherwise split into sentence-ish windows
        sentences = re.split(r"(?<=[.!?])\s+", block)
        current = ""

        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue

            if not current:
                current = sentence
            elif len(current) + 1 + len(sentence) <= max_chars:
                current += " " + sentence
            else:
                chunk_id += 1
                chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "text": current.strip(),
                    }
                )
                current = sentence

        if current.strip():
            chunk_id += 1
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "text": current.strip(),
                }
            )

    return chunks