from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

from ai.config_guard import (
    normalize_structure_config,
    validate_and_prune_structure_config,
    find_uncovered_leaf_repeated_paths,
)

load_dotenv()


class RecordGroup(BaseModel):
    record_type: str
    path: str
    context_paths: list[str] = Field(default_factory=list)
    field_paths: list[str] = Field(default_factory=list)


class StructureConfig(BaseModel):
    schema_family: str
    record_groups: list[RecordGroup] = Field(default_factory=list)


def detect_structure_with_agent(
    structure_summary: dict[str, Any],
    raw_data: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4o-2024-08-06")

    debug: dict[str, Any] = {
        "final_source": None,
        "initial_config_raw": None,
        "initial_config_normalized": None,
        "initial_validation_report": None,
        "invalid_repair_config_raw": None,
        "invalid_repair_config_normalized": None,
        "invalid_repair_validation_report": None,
        "uncovered_leaf_paths_before": [],
        "coverage_repair_config_raw": None,
        "coverage_repair_config_normalized": None,
        "coverage_repair_validation_report": None,
        "uncovered_leaf_paths_after": [],
    }

    if not api_key:
        fallback = _fallback_structure_config(structure_summary)
        normalized = normalize_structure_config(fallback)
        cleaned, report = validate_and_prune_structure_config(normalized, raw_data)

        debug["final_source"] = "fallback_no_api_key"
        debug["initial_config_raw"] = fallback
        debug["initial_config_normalized"] = normalized
        debug["initial_validation_report"] = report

        return cleaned, debug

    client = OpenAI(api_key=api_key)

    try:
        # -------------------------
        # Attempt 1: initial config
        # -------------------------
        initial_config = _request_structure_config(
            client=client,
            model=model,
            structure_summary=structure_summary,
        )
        debug["initial_config_raw"] = initial_config

        normalized = normalize_structure_config(initial_config)
        debug["initial_config_normalized"] = normalized

        cleaned, report = validate_and_prune_structure_config(normalized, raw_data)
        debug["initial_validation_report"] = report

        current_config = cleaned
        current_source = "initial_valid"

        # If nothing valid survived, do invalid-repair pass
        if not cleaned["record_groups"]:
            repaired_config = _request_invalid_repair(
                client=client,
                model=model,
                structure_summary=structure_summary,
                invalid_config=initial_config,
                validation_report=report,
            )
            debug["invalid_repair_config_raw"] = repaired_config

            normalized_repaired = normalize_structure_config(repaired_config)
            debug["invalid_repair_config_normalized"] = normalized_repaired

            cleaned_repaired, repaired_report = validate_and_prune_structure_config(
                normalized_repaired, raw_data
            )
            debug["invalid_repair_validation_report"] = repaired_report

            if cleaned_repaired["record_groups"]:
                current_config = cleaned_repaired
                current_source = "invalid_repair"
            else:
                fallback = _fallback_structure_config(structure_summary)
                normalized_fallback = normalize_structure_config(fallback)
                cleaned_fallback, fallback_report = validate_and_prune_structure_config(
                    normalized_fallback, raw_data
                )

                debug["final_source"] = "fallback_after_invalid_repair"
                debug["invalid_repair_validation_report"] = repaired_report
                debug["coverage_repair_validation_report"] = None

                return cleaned_fallback, debug

        # --------------------------------------------
        # Attempt 2: coverage repair for missing leaves
        # --------------------------------------------
        uncovered_before = find_uncovered_leaf_repeated_paths(
            structure_summary, current_config
        )
        debug["uncovered_leaf_paths_before"] = uncovered_before

        if uncovered_before:
            coverage_repaired_config = _request_coverage_repair(
                client=client,
                model=model,
                structure_summary=structure_summary,
                current_valid_config=current_config,
                uncovered_leaf_paths=uncovered_before,
            )
            debug["coverage_repair_config_raw"] = coverage_repaired_config

            normalized_coverage = normalize_structure_config(coverage_repaired_config)
            debug["coverage_repair_config_normalized"] = normalized_coverage

            cleaned_coverage, coverage_report = validate_and_prune_structure_config(
                normalized_coverage, raw_data
            )
            debug["coverage_repair_validation_report"] = None

            uncovered_after = find_uncovered_leaf_repeated_paths(
                structure_summary, cleaned_coverage
            )
            debug["uncovered_leaf_paths_after"] = uncovered_after

            if (
                cleaned_coverage["record_groups"]
                and len(uncovered_after) < len(uncovered_before)
            ):
                debug["final_source"] = "coverage_repair"
                return cleaned_coverage, debug

        debug["final_source"] = current_source
        return current_config, debug

    except Exception as e:
        fallback = _fallback_structure_config(structure_summary)
        normalized = normalize_structure_config(fallback)
        cleaned, report = validate_and_prune_structure_config(normalized, raw_data)

        debug["final_source"] = f"fallback_exception: {str(e)}"
        debug["initial_config_raw"] = fallback
        debug["initial_config_normalized"] = normalized
        debug["initial_validation_report"] = report

        return cleaned, debug

def _request_structure_config(
    client: OpenAI,
    model: str,
    structure_summary: dict[str, Any],
) -> dict[str, Any]:
    response = client.responses.parse(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You are a schema and structure detector for machine log files. "
                    "Return only a valid config matching the provided schema. "
                    "Parser contract rules: "
                    "1) path must be absolute from the root; "
                    "2) context_paths must be absolute from the root; "
                    "3) field_paths must be relative to the matched node, not absolute; "
                    "4) use [] for lists, never .[]; "
                    "5) do not invent paths not present in the summary; "
                    "6) prefer a small number of useful record groups."
                    "7) use dot-separated paths like ControlJob.ProcessJobs[].ModuleProcessReports[]; "
                    "8) never use slash-style paths like /ControlJob/ProcessJobs[]; "
                    "context_paths must point only to specific scalar fields, never whole objects or arrays; "
                    "for example use ControlJob.ControlJobID, not ControlJob; "
                    "use ControlJob.ProcessJobs[].PRJobID, not ControlJob.ProcessJobs[]; "
                ),
            },
            {
                "role": "user",
                "content": json.dumps(structure_summary, indent=2),
            },
        ],
        text_format=StructureConfig,
    )

    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("Model returned no parsed structure config.")
    return parsed.model_dump()


def _request_invalid_repair(
    client: OpenAI,
    model: str,
    structure_summary: dict[str, Any],
    invalid_config: dict[str, Any],
    validation_report: dict[str, Any],
) -> dict[str, Any]:
    response = client.responses.parse(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You are repairing a structure config for a generic parser. "
                    "You must obey these parser contract rules exactly: "
                    "1) path must be absolute from the root; "
                    "2) context_paths must be absolute from the root; "
                    "3) field_paths must be relative to the matched node only; "
                    "4) use [] for lists, never .[]; "
                    "5) remove groups or fields that do not validate. "
                    "6) use dot-separated paths like ControlJob.ProcessJobs[].ModuleProcessReports[]; "
                    "7) never use slash-style paths like /ControlJob/ProcessJobs[]; "
                    "context_paths must point only to specific scalar fields, never whole objects or arrays; "
                    "for example use ControlJob.ControlJobID, not ControlJob; "
                    "use ControlJob.ProcessJobs[].PRJobID, not ControlJob.ProcessJobs[]; "
                    "Return only a valid config."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "structure_summary": structure_summary,
                        "invalid_config": invalid_config,
                        "validation_report": validation_report,
                    },
                    indent=2,
                ),
            },
        ],
        text_format=StructureConfig,
    )

    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("Model returned no repaired structure config.")
    return parsed.model_dump()


def _request_coverage_repair(
    client: OpenAI,
    model: str,
    structure_summary: dict[str, Any],
    current_valid_config: dict[str, Any],
    uncovered_leaf_paths: list[str],
) -> dict[str, Any]:
    response = client.responses.parse(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You are improving a valid parser config by increasing coverage of important repeated leaf paths. "
                    "You must obey these parser contract rules exactly: "
                    "1) path must be absolute from the root; "
                    "2) context_paths must be absolute from the root; "
                    "3) field_paths must be relative to the matched node only; "
                    "4) use [] for lists, never .[]; "
                    "5) keep all currently valid groups unless clearly redundant; "
                    "6) add record groups for uncovered repeated leaf paths when they represent meaningful extractable records. "
                    "7) use dot-separated paths like ControlJob.ProcessJobs[].ModuleProcessReports[]; "
                    "8) never use slash-style paths like /ControlJob/ProcessJobs[]; "
                    "context_paths must point only to specific scalar fields, never whole objects or arrays; "
                    "for example use ControlJob.ControlJobID, not ControlJob; "
                    "use ControlJob.ProcessJobs[].PRJobID, not ControlJob.ProcessJobs[]; "
                    "Return only a valid config."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "structure_summary": structure_summary,
                        "current_valid_config": current_valid_config,
                        "uncovered_leaf_paths": uncovered_leaf_paths,
                    },
                    indent=2,
                ),
            },
        ],
        text_format=StructureConfig,
    )

    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("Model returned no coverage-repaired structure config.")
    return parsed.model_dump()


def _fallback_structure_config(structure_summary: dict[str, Any]) -> dict[str, Any]:
    top_level_keys = set(structure_summary.get("top_level_keys", []))

    if "ControlJob" in top_level_keys:
        return {
            "schema_family": "control_job_family",
            "record_groups": [
                {
                    "record_type": "control_job",
                    "path": "ControlJob",
                    "context_paths": [],
                    "field_paths": [
                        "ControlJobID",
                        "EquipmentID",
                        "OperatorID",
                        "StartTime",
                        "EndTime",
                    ],
                },
                {
                    "record_type": "process_job",
                    "path": "ControlJob.ProcessJobs[]",
                    "context_paths": [
                        "ControlJob.ControlJobID",
                        "ControlJob.EquipmentID",
                    ],
                    "field_paths": [
                        "PRJobID",
                        "LotID",
                        "WaferID",
                        "SlotID",
                        "RecipeName",
                        "Recipe.RecipeID",
                        "RecipeStepName",
                        "Recipe.Type",
                    ],
                },
                {
                    "record_type": "control_state_event",
                    "path": "ControlJob.ProcessJobs[].ModuleProcessReports[].Attributes.Events.ControlStateEvents[]",
                    "context_paths": [
                        "ControlJob.ControlJobID",
                        "ControlJob.EquipmentID",
                        "ControlJob.ProcessJobs[].PRJobID",
                        "ControlJob.ProcessJobs[].LotID",
                        "ControlJob.ProcessJobs[].WaferID",
                        "ControlJob.ProcessJobs[].SlotID",
                    ],
                    "field_paths": [
                        "EventID",
                        "Name",
                        "Text",
                        "DateTime",
                    ],
                },
                {
                    "record_type": "sensor_measurement",
                    "path": "ControlJob.ProcessJobs[].ModuleProcessReports[].SensorData[].Measurements[]",
                    "context_paths": [
                        "ControlJob.ControlJobID",
                        "ControlJob.EquipmentID",
                        "ControlJob.ProcessJobs[].PRJobID",
                        "ControlJob.ProcessJobs[].LotID",
                        "ControlJob.ProcessJobs[].WaferID",
                        "ControlJob.ProcessJobs[].SlotID",
                        "ControlJob.ProcessJobs[].RecipeName",
                        "ControlJob.ProcessJobs[].Recipe.RecipeID",
                        "ControlJob.ProcessJobs[].RecipeStepName",
                        "ControlJob.ProcessJobs[].Recipe.Type",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].Keys.ModuleID",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].Keys.RecipeStepID",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].Keys.WaferID",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].SensorData[].SensorID",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].SensorData[].Keys.SensorID",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].SensorData[].SensorName",
                        "ControlJob.ProcessJobs[].ModuleProcessReports[].SensorData[].Unit",
                    ],
                    "field_paths": [
                        "DateTime",
                        "Value",
                    ],
                },
            ],
        }

    return {
        "schema_family": "generic_json",
        "record_groups": [
            {
                "record_type": "generic_json_object",
                "path": "$",
                "context_paths": [],
                "field_paths": [],
            }
        ],
    }