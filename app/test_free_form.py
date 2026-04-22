from __future__ import annotations

import json
import sys
from pathlib import Path

from free_form.reader import load_text_document
from free_form.parser import parse_free_form_text


def main():
    if len(sys.argv) < 2:
        print("Usage: python app/test_free_form.py <path_to_text_file>")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"File not found: {input_path}")
        sys.exit(1)

    doc_payload = load_text_document(str(input_path))
    parsed_result = parse_free_form_text(doc_payload)

    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{input_path.stem}_free_form_result.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "input_file": str(input_path),
                "parsed_result": parsed_result,
            },
            f,
            indent=4,
            ensure_ascii=False,
        )

    print(f"Result written to: {output_path}")


if __name__ == "__main__":
    main()