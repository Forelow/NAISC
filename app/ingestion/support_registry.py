from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass
class SupportDecision:
    format_guess: str
    is_supported: bool
    parser_family: str
    support_status: str
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


SUPPORTED_FORMATS = {
    "json": ("structured_parser", "supported"),
    "csv": ("structured_parser", "supported"),
    "xml": ("structured_parser", "supported"),
    "syslog_like_text": ("semi_structured_parser", "supported"),
    "key_value_text": ("semi_structured_parser", "supported"),
    "free_text_log": ("unstructured_parser", "supported"),
    "hex_dump_text": ("unstructured_parser", "supported"),
    "opaque_binary": ("binary_parser", "partially_supported"),
    # Optional extension:
    # "parquet": ("structured_parser", "supported"),
}


def check_support(format_guess: str) -> SupportDecision:
    if format_guess in SUPPORTED_FORMATS:
        parser_family, support_status = SUPPORTED_FORMATS[format_guess]
        return SupportDecision(
            format_guess=format_guess,
            is_supported=True,
            parser_family=parser_family,
            support_status=support_status,
            notes=f"{format_guess} is handled by {parser_family}",
        )

    if format_guess == "parquet":
        return SupportDecision(
            format_guess=format_guess,
            is_supported=False,
            parser_family="structured_parser",
            support_status="recognized_but_unsupported",
            notes="Parquet recognized, but parser not enabled in current prototype",
        )

    return SupportDecision(
        format_guess=format_guess,
        is_supported=False,
        parser_family="fallback",
        support_status="unknown_or_unsupported",
        notes="No direct parser available",
    )