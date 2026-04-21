from __future__ import annotations

import json
import os
import re
from typing import Any

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore


KV_SPACE_RE = re.compile(r'([\w.-]+)=(".*?"|[^\s]+)')
LABEL_VALUE_RE = re.compile(r'^\s*([A-Za-z0-9_.-]+)\s*:\s*(.+?)\s*$')

SYSLOG_TS_RE = re.compile(
    r"^\s*(?P<ts>[A-Z][a-z]{2}\s+\d{1,2}\s\d{2}:\d{2}:\d{2})\s+(?P<rest>.+)$"
)
ISO_TS_RE = re.compile(
    r"^\s*(?P<ts>(?:\d{4}-\d{2}-\d{2}|\d{4}/\d{2}/\d{2})[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(?P<rest>.+)$"
)

HOST_FACILITY_RE = re.compile(
    r"^(?P<tool_id>\S+)\s+(?P<channel>[A-Za-z0-9_.-]+)\.(?P<severity>[A-Za-z0-9_.-]+):\s*(?P<body>.*)$"
)
BRACKET_TOOL_SEVERITY_EVENT_RE = re.compile(
    r"^\[(?P<tool_id>[^\]]+)\]\s+(?P<severity>[A-Z]+)\s*:\s*(?P<event_code>[A-Z0-9_]+)\s*>\s*(?P<body>.*)$"
)
TOOL_SEVERITY_RE = re.compile(
    r"^(?P<tool_id>\S+)\s+(?P<severity>DEBUG|DBG|INFO|INF|NOTICE|WARN|WARNING|ERROR|ERR|CRITICAL|ALERT|EMERG|FATAL)\s+(?P<body>.*)$",
    re.IGNORECASE,
)

VC_CONTEXT_HEADER_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+Machine:(?P<tool_id>\S+)\s+(?P<context>\(.*\))$"
)

VC_EVENT_LINE_RE = re.compile(
    r"^(?P<event_prefix>[A-Z ]+):\s+(?P<event_ref>ER-[A-Z0-9-]+)\s+(?P<event_code>[A-Z_]+)$"
)

LEADING_BRACKET_EVENT_RE = re.compile(r"^\[(?P<event_code>[^\]]+)\]\s*(?P<body>.*)$")
TOKEN_BEFORE_GT_RE = re.compile(r"^(?P<event_code>[A-Z][A-Z0-9_]+)\s*>\s*(?P<body>.*)$")
LEADING_UPPER_EVENT_RE = re.compile(r"^(?P<event_code>[A-Z][A-Z0-9_]+)\s+(?P<body>.*)$")

DEFAULT_CLASSIFICATION_HINTS = {
    "equipment_state": {
        "event_codes": ["STATE", "EQUIP_STATE", "SYS_BOOT", "STARTUP", "SHUTDOWN"],
        "required_any_fields": ["prev", "curr", "state", "from", "to", "from_state", "to_state", "trigger"],
    },
    "process_parameter_recipe": {
        "event_codes": ["RCP_LOAD", "RCP_PARAM", "RECIPE", "RECIPE_STEP", "LOT_OPEN"],
        "required_any_fields": ["recipe", "recipe_id", "name", "rev", "parameter", "setpoint", "target_temp_C", "duration_s", "version"],
    },
    "sensor_reading": {
        "event_codes": ["SENSOR", "TRACE", "READING", "MEASURE"],
        "required_any_fields": ["sensor", "sensor_id", "value", "reading", "temp_C", "pressure_mTorr", "base_pres_Pa", "proc_pres_Pa", "dep_rate_A_s", "zone"],
    },
    "fault_event": {
        "event_codes": ["WARN", "WARNING", "ALARM", "FAULT", "FAULT_CLEAR", "ALARM_CLR", "ERROR", "ERR", "CRITICAL", "INTERLOCK"],
        "required_any_fields": ["fault", "fault_code", "alarm_code", "code", "error_code", "reason"],
    },
    "wafer_processing_sequence": {
        "event_codes": ["WAFER", "STEP", "STEP_START", "STEP_END", "STEP_ABORT", "WFR_SEQ", "WF_MOVE", "ACTION", "LOAD_COMPLETE", "UNLOAD_COMPLETE", "LOT_IN", "LOT_OUT", "LOT_RESUME", "MAINT"],
        "required_any_fields": ["wafer", "wafer_id", "wfr", "step", "step_name", "seq", "action", "slot", "lot"],
    },
    "generic_text_record": {
        "event_codes": [],
        "required_any_fields": [],
    },
}


def build_semi_structured_parse_spec(
    text_payload: dict[str, Any],
    family_info: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    sample_records = _sample_records(text_payload, limit=12)
    fallback_spec = _build_fallback_spec(sample_records)

    debug: dict[str, Any] = {
        "final_source": "deterministic_fallback_spec",
        "sample_lines": sample_records,
        "llm_requested": False,
        "llm_raw_spec": None,
        "validation": None,
    }

    llm_spec = _request_llm_semi_structured_spec(sample_records)
    if llm_spec is not None:
        debug["llm_requested"] = True
        debug["llm_raw_spec"] = llm_spec
        merged_spec = _merge_specs(fallback_spec, llm_spec)
        validation = validate_semi_structured_parse_spec(merged_spec, sample_records)
        debug["validation"] = validation
        if validation["accepted"]:
            debug["final_source"] = "llm_semi_structured_spec"
            return merged_spec, debug

    fallback_validation = validate_semi_structured_parse_spec(fallback_spec, sample_records)
    debug["validation"] = fallback_validation
    return fallback_spec, debug


def validate_semi_structured_parse_spec(
    spec: dict[str, Any],
    sample_records: list[str],
) -> dict[str, Any]:
    required_keys = {
        "family",
        "record_boundary",
        "timestamp_styles",
        "header_layout_candidates",
        "event_extraction_candidates",
        "payload_candidates",
        "classification_hints",
    }
    missing = sorted(required_keys - set(spec.keys()))
    if missing:
        return {
            "accepted": False,
            "reason": "missing_required_keys",
            "missing_keys": missing,
            "coverage": 0.0,
        }

    if not sample_records:
        return {"accepted": False, "reason": "no_sample_records", "coverage": 0.0}

    parse_hits = 0
    timestamp_hits = 0
    structure_hits = 0

    for record_text in sample_records:
        parsed = preview_parse_record(record_text, spec)
        if not parsed:
            continue
        parse_hits += 1
        if parsed.get("ts"):
            timestamp_hits += 1
        if (
            parsed.get("tool_id")
            or parsed.get("channel")
            or parsed.get("severity")
            or parsed.get("event_code")
            or parsed.get("event_type")
        ):
            structure_hits += 1

    coverage = parse_hits / len(sample_records)
    timestamp_ratio = timestamp_hits / len(sample_records)
    structure_ratio = structure_hits / len(sample_records)
    accepted = coverage >= 0.60 and (timestamp_ratio >= 0.30 or structure_ratio >= 0.40)

    return {
        "accepted": accepted,
        "coverage": round(coverage, 3),
        "timestamp_ratio": round(timestamp_ratio, 3),
        "structure_ratio": round(structure_ratio, 3),
        "parse_hits": parse_hits,
        "sample_size": len(sample_records),
    }


def preview_parse_record(record_text: str, spec: dict[str, Any]) -> dict[str, Any] | None:
    raw = record_text.strip()
    if not raw:
        return None

    data: dict[str, Any] = {}
    ts, remainder = _extract_timestamp(raw, spec.get("timestamp_styles", []))
    if ts:
        data["ts"] = ts
        working = remainder or ""
    else:
        working = raw

    header = _parse_header(working, spec.get("header_layout_candidates", []))
    if header:
        body = header.pop("body", "")
        data.update(header)
    else:
        body = working

    body, event_data = _extract_event(body, spec.get("event_extraction_candidates", []), data)
    data.update(event_data)

    body, payload_data = _extract_payload(body, spec.get("payload_candidates", []))
    data.update(payload_data)

        # Promote explicit payload event field into classifier-friendly fields
    if not data.get("event_code") and isinstance(data.get("event"), str):
        data["event_code"] = data["event"]
        data["event_type"] = data["event"]

    # Promote embedded datetime field as timestamp when timestamp is not in the line prefix
    if not data.get("ts") and isinstance(data.get("datetime"), str):
        data["ts"] = data["datetime"]

    body = body.strip()
    if body and "message" not in data:
        data["message"] = body

    meaningful_keys = set(data.keys()) - {"message"}
    if not meaningful_keys and "message" not in data:
        return None

    if "severity" in data and isinstance(data["severity"], str):
        data["severity"] = data["severity"].upper()

    return data


def _sample_records(text_payload: dict[str, Any], limit: int = 12) -> list[str]:
    lines = [item.get("text", "") for item in text_payload.get("lines", [])]
    non_empty = [line.strip() for line in lines if line.strip()]
    if not non_empty:
        return []

    # Try blank-line blocks only if they produce multiple meaningful blocks
    blank_count = sum(1 for line in lines if not line.strip())
    if blank_count >= 2:
        blocks: list[str] = []
        current: list[str] = []
        for line in lines:
            if line.strip():
                current.append(line.rstrip())
            elif current:
                blocks.append("\n".join(current).strip())
                current = []
        if current:
            blocks.append("\n".join(current).strip())

        # Only use block mode if it actually creates multiple blocks
        if len(blocks) >= 2:
            return blocks[:limit]

    # Otherwise default to normal per-line sampling
    return non_empty[:limit]


def _build_fallback_spec(sample_records: list[str]) -> dict[str, Any]:
    return {
        "family": "semi_structured_text",
        "parser_strategy": "llm_spec_with_deterministic_validation",
        "record_boundary": {"type": _detect_record_boundary(sample_records)},
        "timestamp_styles": _detect_timestamp_styles(sample_records),
        "header_layout_candidates": _detect_header_layout_candidates(sample_records),
        "event_extraction_candidates": _detect_event_extraction_candidates(sample_records),
        "payload_candidates": _detect_payload_candidates(sample_records),
        "classification_hints": json.loads(json.dumps(DEFAULT_CLASSIFICATION_HINTS)),
        "notes": "Unified semi-structured fallback spec inferred deterministically from sampled records.",
    }


def _request_llm_semi_structured_spec(sample_records: list[str]) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or OpenAI is None or not sample_records:
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    system_prompt = (
        "You are a pattern-discovery agent for semi-structured equipment logs. "
        "Do not parse the whole file. Infer a deterministic parse specification from sampled records. "
        "Return JSON only. "
        "Allowed record_boundary types: per_line, blank_line_blocks. "
        "Allowed timestamp_styles: syslog_mmm_dd_hh_mm_ss, iso8601, none. "
        "Allowed header_layout_candidates: syslog_host_facility_level, bracket_tool_severity_event, tool_severity_message, tool_token_message, none. "
        "Allowed event_extraction_candidates: header_event_field, leading_bracket_token, token_before_gt, leading_upper_token, none. "
        "Allowed payload_candidates: brace_kv, space_kv, label_value_lines, none. "
        "Output keys: family, record_boundary, timestamp_styles, header_layout_candidates, event_extraction_candidates, payload_candidates, classification_hints, notes. "
        "Prefer reusable structural descriptions, not vendor names."
    )
    user_prompt = {
        "sample_records": sample_records,
        "allowed_record_types": list(DEFAULT_CLASSIFICATION_HINTS.keys()),
        "baseline_classification_hints": DEFAULT_CLASSIFICATION_HINTS,
    }

    try:
        client = OpenAI(api_key=api_key)
        if hasattr(client, "responses"):
            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False, indent=2)},
                ],
            )
            text = getattr(response, "output_text", None)
            return json.loads(text) if text else None

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False, indent=2)},
            ],
            response_format={"type": "json_object"},
        )
        text = response.choices[0].message.content
        return json.loads(text) if text else None
    except Exception:
        return None


def _merge_specs(base_spec: dict[str, Any], llm_spec: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base_spec)

    rb = llm_spec.get("record_boundary")
    if isinstance(rb, dict) and rb.get("type") in {"per_line", "blank_line_blocks"}:
        merged["record_boundary"] = {"type": rb["type"]}
    elif isinstance(rb, str) and rb in {"per_line", "blank_line_blocks"}:
        merged["record_boundary"] = {"type": rb}

    if isinstance(llm_spec.get("timestamp_styles"), list):
        merged["timestamp_styles"] = [
            x
            for x in llm_spec["timestamp_styles"]
            if x in {"syslog_mmm_dd_hh_mm_ss", "iso8601", "none"}
        ] or base_spec["timestamp_styles"]

    if isinstance(llm_spec.get("header_layout_candidates"), list):
        merged["header_layout_candidates"] = [
            x
            for x in llm_spec["header_layout_candidates"]
            if x in {
                "syslog_host_facility_level",
                "bracket_tool_severity_event",
                "tool_severity_message",
                "tool_token_message",
                "none",
            }
        ] or base_spec["header_layout_candidates"]

    if isinstance(llm_spec.get("event_extraction_candidates"), list):
        merged["event_extraction_candidates"] = [
            x
            for x in llm_spec["event_extraction_candidates"]
            if x in {
                "header_event_field",
                "leading_bracket_token",
                "token_before_gt",
                "leading_upper_token",
                "none",
            }
        ] or base_spec["event_extraction_candidates"]

    if isinstance(llm_spec.get("payload_candidates"), list):
        merged["payload_candidates"] = [
            x
            for x in llm_spec["payload_candidates"]
            if x in {"brace_kv", "space_kv", "label_value_lines", "none"}
        ] or base_spec["payload_candidates"]

    llm_hints = llm_spec.get("classification_hints")
    if isinstance(llm_hints, dict):
        merged_hints = json.loads(json.dumps(base_spec["classification_hints"]))
        for record_type, hint in llm_hints.items():
            if record_type not in merged_hints or not isinstance(hint, dict):
                continue
            if isinstance(hint.get("event_codes"), list):
                merged_hints[record_type]["event_codes"] = list(
                    dict.fromkeys(
                        [str(x) for x in merged_hints[record_type].get("event_codes", [])]
                        + [str(x) for x in hint["event_codes"] if isinstance(x, str)]
                    )
                )
            if isinstance(hint.get("required_any_fields"), list):
                merged_hints[record_type]["required_any_fields"] = list(
                    dict.fromkeys(
                        [str(x) for x in merged_hints[record_type].get("required_any_fields", [])]
                        + [str(x) for x in hint["required_any_fields"] if isinstance(x, str)]
                    )
                )

        for rt in merged_hints.values():
            broad = {"severity", "message", "ts", "tool_id", "channel"}
            required = rt.get("required_any_fields", [])
            if isinstance(required, list):
                rt["required_any_fields"] = [x for x in required if x not in broad]

        merged["classification_hints"] = merged_hints

    if isinstance(llm_spec.get("notes"), (str, list)):
        merged["notes"] = llm_spec["notes"]

    return merged


def _detect_record_boundary(sample_records: list[str]) -> str:
    return "blank_line_blocks" if any("\n" in record for record in sample_records) else "per_line"


def _detect_timestamp_styles(sample_records: list[str]) -> list[str]:
    styles: list[str] = []
    if any(SYSLOG_TS_RE.match(record) for record in sample_records):
        styles.append("syslog_mmm_dd_hh_mm_ss")
    if any(ISO_TS_RE.match(record) for record in sample_records):
        styles.append("iso8601")
    if not styles:
        styles = ["none"]
    return styles


def _detect_header_layout_candidates(sample_records: list[str]) -> list[str]:
    scores = {
        "syslog_host_facility_level": 0,
        "bracket_tool_severity_event": 0,
        "tool_severity_message": 0,
        "tool_token_message": 0,
        "none": 0,
    }

    for record in sample_records:
        _, remainder = _extract_timestamp(record, ["syslog_mmm_dd_hh_mm_ss", "iso8601"])
        if remainder is None:
            remainder = record

        first_tokens = remainder.split(None, 2)
        starts_like_flat_kv = (
            len(first_tokens) >= 2
            and "=" in first_tokens[0]
            and "=" in first_tokens[1]
        )

        if HOST_FACILITY_RE.match(remainder):
            scores["syslog_host_facility_level"] += 3
        if BRACKET_TOOL_SEVERITY_EVENT_RE.match(remainder):
            scores["bracket_tool_severity_event"] += 4
        if TOOL_SEVERITY_RE.match(remainder):
            scores["tool_severity_message"] += 2

        if starts_like_flat_kv:
            scores["none"] += 4
        elif len(remainder.split()) >= 2:
            scores["tool_token_message"] += 1

    ranked = [k for k, v in sorted(scores.items(), key=lambda item: item[1], reverse=True) if v > 0]
    return ranked or ["none"]


def _detect_event_extraction_candidates(sample_records: list[str]) -> list[str]:
    candidates: list[str] = []
    for record in sample_records:
        _, remainder = _extract_timestamp(record, ["syslog_mmm_dd_hh_mm_ss", "iso8601"])
        working = remainder if remainder is not None else record

        if HOST_FACILITY_RE.match(working):
            body = HOST_FACILITY_RE.match(working).group("body")
        elif BRACKET_TOOL_SEVERITY_EVENT_RE.match(working):
            candidates.append("header_event_field")
            body = BRACKET_TOOL_SEVERITY_EVENT_RE.match(working).group("body")
        elif TOOL_SEVERITY_RE.match(working):
            body = TOOL_SEVERITY_RE.match(working).group("body")
        else:
            body = working

        if LEADING_BRACKET_EVENT_RE.match(body):
            candidates.append("leading_bracket_token")
        if TOKEN_BEFORE_GT_RE.match(body):
            candidates.append("token_before_gt")
        if LEADING_UPPER_EVENT_RE.match(body):
            candidates.append("leading_upper_token")

    deduped = list(dict.fromkeys(candidates))
    return deduped or ["none"]


def _detect_payload_candidates(sample_records: list[str]) -> list[str]:
    candidates: list[str] = []
    brace_hits = 0
    space_hits = 0
    label_hits = 0

    for record in sample_records:
        if "{" in record and "}" in record and "=" in record:
            brace_hits += 1
        if len(KV_SPACE_RE.findall(record)) >= 2:
            space_hits += 1
        if any(LABEL_VALUE_RE.match(line) for line in record.splitlines()):
            label_hits += 1

    if brace_hits:
        candidates.append("brace_kv")
    if space_hits:
        candidates.append("space_kv")
    if label_hits:
        candidates.append("label_value_lines")
    return candidates or ["none"]


def _extract_timestamp(raw: str, timestamp_styles: list[str]) -> tuple[str | None, str | None]:
    if not timestamp_styles:
        timestamp_styles = ["syslog_mmm_dd_hh_mm_ss", "iso8601", "none"]

    for style in timestamp_styles:
        if style == "syslog_mmm_dd_hh_mm_ss":
            match = SYSLOG_TS_RE.match(raw)
        elif style == "iso8601":
            match = ISO_TS_RE.match(raw)
        elif style == "none":
            return None, raw
        else:
            match = None

        if match:
            return match.group("ts"), match.group("rest")

    return None, raw


def _parse_header(working: str, layouts: list[str]) -> dict[str, Any] | None:
    if not layouts:
        layouts = ["none"]

    for layout in layouts:
        if layout == "syslog_host_facility_level":
            match = HOST_FACILITY_RE.match(working)
            if match:
                return {
                    "tool_id": match.group("tool_id"),
                    "channel": match.group("channel"),
                    "severity": match.group("severity"),
                    "body": match.group("body"),
                }

        elif layout == "bracket_tool_severity_event":
            match = BRACKET_TOOL_SEVERITY_EVENT_RE.match(working)
            if match:
                return {
                    "tool_id": match.group("tool_id"),
                    "severity": match.group("severity"),
                    "event_code": match.group("event_code"),
                    "event_type": match.group("event_code"),
                    "body": match.group("body"),
                }

        elif layout == "tool_severity_message":
            match = TOOL_SEVERITY_RE.match(working)
            if match:
                return {
                    "tool_id": match.group("tool_id"),
                    "severity": match.group("severity"),
                    "body": match.group("body"),
                }

        elif layout == "tool_token_message":
            parts = working.split(None, 2)
            if len(parts) >= 2:
                # If the line starts like key=value key=value..., this is not a header
                if "=" in parts[0] or "=" in parts[1]:
                    continue

                result: dict[str, Any] = {"tool_id": parts[0]}
                second = parts[1]
                result["body"] = parts[2] if len(parts) > 2 else ""
                if second.isupper():
                    result["event_type"] = second
                else:
                    result["header_token"] = second
                return result

        elif layout == "none":
            return {"body": working}

    return None


def _extract_event(
    body: str,
    methods: list[str],
    existing_data: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if existing_data.get("event_code"):
        return body, {}

    if not methods:
        methods = ["none"]

    text = body.strip()
    for method in methods:
        if method == "leading_bracket_token":
            match = LEADING_BRACKET_EVENT_RE.match(text)
            if match:
                event_code = match.group("event_code").strip()
                return match.group("body").strip(), {"event_code": event_code, "event_type": event_code}

        elif method == "token_before_gt":
            match = TOKEN_BEFORE_GT_RE.match(text)
            if match:
                event_code = match.group("event_code").strip()
                return match.group("body").strip(), {"event_code": event_code, "event_type": event_code}

        elif method == "leading_upper_token":
            match = LEADING_UPPER_EVENT_RE.match(text)
            if match:
                event_code = match.group("event_code").strip()
                return match.group("body").strip(), {"event_code": event_code, "event_type": event_code}

        elif method == "none":
            continue

    return text, {}


def _extract_payload(body: str, payload_candidates: list[str]) -> tuple[str, dict[str, Any]]:
    text = body.strip()
    parsed: dict[str, Any] = {}

    for candidate in payload_candidates or ["none"]:
        if candidate == "brace_kv":
            brace_match = re.search(r"\{(?P<inner>.*)\}", text)
            if brace_match:
                inner = brace_match.group("inner").strip()
                if inner:
                    for piece in inner.split(","):
                        piece = piece.strip()
                        if "=" not in piece:
                            continue
                        key, value = piece.split("=", 1)
                        parsed[key.strip()] = _coerce_value(value.strip())
                start, end = brace_match.span()
                text = f"{text[:start]} {text[end:]}"

        elif candidate == "space_kv":
            for match in reversed(list(KV_SPACE_RE.finditer(text))):
                key = match.group(1)
                value = match.group(2)
                parsed[key] = _coerce_value(value)
                start, end = match.span()
                text = f"{text[:start]} {text[end:]}"

        elif candidate == "label_value_lines":
            labels: dict[str, Any] = {}
            lines = []
            changed = False
            for line in text.splitlines():
                m = LABEL_VALUE_RE.match(line)
                if m:
                    labels[m.group(1)] = _coerce_value(m.group(2))
                    changed = True
                else:
                    lines.append(line)
            if changed:
                parsed.update(labels)
                text = "\n".join(lines)

    text = re.sub(r"\s+", " ", text).strip(" -:;")
    return text, parsed


def _coerce_value(value: str) -> Any:
    value = value.strip().strip(",}").strip()

    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]

    if value == "":
        return None

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if "." in value or "e" in lowered:
            return float(value)
        return int(value)
    except ValueError:
        return value