from __future__ import annotations

import json


def build_structure_detection_prompt(structure_summary: dict) -> str:
    return f"""
You are analyzing the structure of a structured machine log file.

Your task:
1. Identify the schema family if possible.
2. Identify repeated record boundaries.
3. Suggest record groups that should be extracted into Python dictionaries.
4. Return ONLY valid JSON.
5. Do not explain anything outside the JSON.

The JSON output format must be:

{{
  "schema_family": "string",
  "record_groups": [
    {{
      "record_type": "string",
      "path": "string",
      "context_paths": ["string"],
      "field_paths": ["string"]
    }}
  ]
}}

Here is the structure summary:

{json.dumps(structure_summary, indent=2)}
""".strip()