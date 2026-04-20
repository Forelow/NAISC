from __future__ import annotations

from pathlib import Path
from typing import Any


def load_text_file(file_path: str) -> dict[str, Any]:
    text = Path(file_path).read_text(encoding="utf-8-sig", errors="replace")
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = []
    for idx, line in enumerate(text.split("\n"), start=1):
        lines.append(
            {
                "line_no": idx,
                "text": line.rstrip("\n"),
            }
        )

    return {
        "text": text,
        "lines": lines,
    }