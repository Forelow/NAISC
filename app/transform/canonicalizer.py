from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Any
import uuid


@dataclass
class CanonicalRecord:
    record_id: str
    record_type: str
    raw_fields: dict[str, Any]
    candidate_fields: dict[str, Any]
    source_reference: str | None = None
    confidence: float = 1.0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CanonicalParseResult:
    file_id: str
    filename: str
    source_format: str
    parser_name: str
    parse_status: str
    record_count: int
    records: list[CanonicalRecord] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def build_canonical_record(
    raw_fields: dict[str, Any],
    source_reference: str | None = None,
    record_type: str = "generic_structured_log",
    confidence: float = 1.0,
    warnings: list[str] | None = None,
) -> CanonicalRecord:
    return CanonicalRecord(
        record_id=str(uuid.uuid4()),
        record_type=record_type,
        raw_fields=raw_fields,
        candidate_fields=dict(raw_fields),
        source_reference=source_reference,
        confidence=confidence,
        warnings=warnings or [],
    )