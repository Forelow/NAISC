from __future__ import annotations

from dataclasses import dataclass, asdict

from ingestion.detector import DetectionResult
from ingestion.support_registry import SupportDecision


@dataclass
class RoutingDecision:
    next_route: str
    processing_mode: str
    should_attempt_parse: bool
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


def route_file(
    detection: DetectionResult,
    support: SupportDecision,
) -> RoutingDecision:
    if support.is_supported:
        if detection.format_guess in ("json", "csv", "xml"):
            return RoutingDecision(
                next_route="structured_parser",
                processing_mode="direct_parse",
                should_attempt_parse=True,
                notes="Known structured format",
            )

        if detection.format_guess in ("syslog_like_text", "key_value_text"):
            return RoutingDecision(
                next_route="semi_structured_parser",
                processing_mode="regex_or_rule_parse",
                should_attempt_parse=True,
                notes="Known semi-structured text pattern",
            )

        if detection.format_guess in ("free_text_log", "hex_dump_text"):
            return RoutingDecision(
                next_route="unstructured_parser",
                processing_mode="llm_or_extraction_fallback",
                should_attempt_parse=True,
                notes="Text-like content with weak or no deterministic pattern",
            )

        if detection.format_guess == "opaque_binary":
            return RoutingDecision(
                next_route="binary_parser",
                processing_mode="partial_decode_or_synthetic_decoder",
                should_attempt_parse=True,
                notes="Binary content requires controlled decoding path",
            )

    if support.format_guess == "parquet":
        return RoutingDecision(
            next_route="unsupported_handler",
            processing_mode="recognized_but_unsupported",
            should_attempt_parse=False,
            notes="Recognized structured format, but not enabled in current prototype",
        )

    if detection.content_class in ("semi_structured_text", "unstructured_text"):
        return RoutingDecision(
            next_route="adaptive_text_fallback",
            processing_mode="pattern_discovery_then_parse",
            should_attempt_parse=True,
            notes="Unknown text-like format; use adaptive fallback",
        )

    if detection.content_class in ("structured_text", "structured_binary"):
        return RoutingDecision(
            next_route="unsupported_handler",
            processing_mode="recognized_structure_but_no_parser",
            should_attempt_parse=False,
            notes="Structured content recognized, but no parser/plugin exists",
        )

    return RoutingDecision(
        next_route="unsupported_handler",
        processing_mode="metadata_only",
        should_attempt_parse=False,
        notes="Opaque or unknown format; store metadata and quarantine safely",
    )