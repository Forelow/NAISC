from __future__ import annotations

from pathlib import Path


def load_text_document(file_path: str) -> dict:
    path = Path(file_path)
    text = path.read_text(encoding="utf-8", errors="replace")

    lines = text.splitlines()

    return {
        "file_path": str(path),
        "text": text,
        "lines": [{"line_no": i + 1, "text": line} for i, line in enumerate(lines)],
        "line_count": len(lines),
        "char_count": len(text),
    }