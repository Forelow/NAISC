from __future__ import annotations

from schema.models import (
    MappingSuggestion,
    SchemaMappingSuggestionSet,
    SchemaProfile,
)


CANONICAL_ALIASES = {
    "control_job_id": ["ControlJobID", "control_job_id"],
    "equipment_id": ["EquipmentID", "ToolID", "MachineID", "tool_id", "equipment_id"],
    "operator_id": ["OperatorID", "operator_id"],
    "process_job_id": ["PRJobID", "ProcessJobID", "process_job_id"],
    "lot_id": ["LotID", "LOTID", "lot_id"],
    "wafer_id": ["WaferID", "wafer_id"],
    "slot_id": ["SlotID", "slot_id"],
    "recipe_name": ["RecipeName", "Recipe.RecipeID", "RecipeID", "recipe_name"],
    "recipe_step_name": ["RecipeStepName", "Recipe.Type", "RecipeStepID", "recipe_step_name"],
    "module_id": ["ModuleID", "module_id"],
    "sensor_id": ["SensorID", "Keys.SensorID", "sensor_id"],
    "sensor_name": ["SensorName", "sensor_name"],
    "timestamp": ["DateTime", "Timestamp", "TimeStamp", "timestamp"],
    "message": ["Text", "Message", "Name", "message"],
    "severity": ["Severity", "severity"],
    "value": ["Value", "value"],
    "unit": ["Unit", "unit"],
    "threshold": ["Threshold", "threshold"],
}


def suggest_mappings(profile: SchemaProfile) -> SchemaMappingSuggestionSet:
    available_paths = [field.path for field in profile.fields]
    suggestions: list[MappingSuggestion] = []

    for canonical_field, aliases in CANONICAL_ALIASES.items():
        matched_paths = _find_matches(available_paths, aliases)

        if matched_paths:
            confidence = 0.90 if len(matched_paths) == 1 else 0.70
            suggestions.append(
                MappingSuggestion(
                    canonical_field=canonical_field,
                    matched_paths=matched_paths,
                    confidence=confidence,
                    notes="Alias/path similarity match",
                )
            )

    return SchemaMappingSuggestionSet(
        format_name=profile.format_name,
        schema_family=profile.schema_family,
        suggestions=suggestions,
    )


def _find_matches(paths: list[str], aliases: list[str]) -> list[str]:
    alias_lower = [a.lower() for a in aliases]
    matched = []

    for path in paths:
        normalized_path = path.lower()

        for alias in alias_lower:
            if normalized_path.endswith(alias.lower()):
                matched.append(path)
                break

            # relaxed containment for nested paths like Recipe.RecipeID
            if alias.lower() in normalized_path:
                matched.append(path)
                break

    # deduplicate while preserving order
    seen = set()
    result = []
    for item in matched:
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result