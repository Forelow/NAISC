from __future__ import annotations

from dataclasses import dataclass, asdict, field
from typing import Any


@dataclass
class FieldSample:
    path: str
    inferred_type: str
    non_null_count: int
    sample_values: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RecordBoundary:
    path: str
    boundary_type: str
    count: int
    sample_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SchemaProfile:
    format_name: str
    schema_family: str
    top_level_type: str
    top_level_keys: list[str] = field(default_factory=list)
    record_boundaries: list[RecordBoundary] = field(default_factory=list)
    fields: list[FieldSample] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MappingSuggestion:
    canonical_field: str
    matched_paths: list[str]
    confidence: float
    notes: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SchemaMappingSuggestionSet:
    format_name: str
    schema_family: str
    suggestions: list[MappingSuggestion] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)