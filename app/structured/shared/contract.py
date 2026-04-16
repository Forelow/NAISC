from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional


@dataclass
class WhereClause:
    field: str
    equals: Any

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RecordGroupSpec:
    record_type: str
    path: str
    field_paths: list[str] = field(default_factory=list)
    context_paths: list[str] = field(default_factory=list)
    where: Optional[WhereClause] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": self.record_type,
            "path": self.path,
            "field_paths": list(self.field_paths),
            "context_paths": list(self.context_paths),
            "where": self.where.to_dict() if self.where else None,
        }


@dataclass
class StructuredParseSpec:
    schema_family: str
    record_groups: list[RecordGroupSpec] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_family": self.schema_family,
            "record_groups": [group.to_dict() for group in self.record_groups],
        }


def spec_from_dict(data: dict[str, Any]) -> StructuredParseSpec:
    groups: list[RecordGroupSpec] = []

    for raw_group in data.get("record_groups", []):
        raw_where = raw_group.get("where")
        where = None
        if isinstance(raw_where, dict) and "field" in raw_where and "equals" in raw_where:
            where = WhereClause(
                field=raw_where["field"],
                equals=raw_where["equals"],
            )

        groups.append(
            RecordGroupSpec(
                record_type=raw_group["record_type"],
                path=raw_group["path"],
                field_paths=list(raw_group.get("field_paths", [])),
                context_paths=list(raw_group.get("context_paths", [])),
                where=where,
            )
        )

    return StructuredParseSpec(
        schema_family=data["schema_family"],
        record_groups=groups,
    )