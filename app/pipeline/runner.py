from __future__ import annotations

from pipeline.canonicalize import canonicalize_result_payload
from pipeline.standardize import standardize_records
from pipeline.normalize import normalize_records
from pipeline.validate import validate_records
from pipeline.route import route_records

from adapters.fingerprint import build_schema_fingerprint
from adapters.spec_store import load_adapter_spec, save_adapter_spec
from adapters.llm_adapter_builder import build_adapter_spec, enrich_adapter_spec
from adapters.apply_adapter_spec import apply_adapter_spec


def process_result_payload(result_payload: dict, use_llm_adapter: bool = True) -> dict:
    schema_fingerprint = build_schema_fingerprint(result_payload)

    canonical_batch = canonicalize_result_payload(result_payload)

    adapter_spec = load_adapter_spec(schema_fingerprint)

    if adapter_spec is None and use_llm_adapter:
        adapter_spec = build_adapter_spec(result_payload)
        if adapter_spec and adapter_spec.get("schema_fingerprint"):
            save_adapter_spec(adapter_spec)

    if adapter_spec:
        adapter_spec = enrich_adapter_spec(adapter_spec, result_payload)
        canonical_batch = apply_adapter_spec(canonical_batch, adapter_spec)

    standardized_batch = standardize_records(canonical_batch)
    normalized_batch = normalize_records(standardized_batch)
    validated_batch = validate_records(normalized_batch)
    routed_batch = route_records(validated_batch)

    return {
        "schema_fingerprint": schema_fingerprint,
        "adapter_spec_used": adapter_spec,
        "canonical_output": canonical_batch,
        "standardized_output": standardized_batch,
        "normalized_output": normalized_batch,
        "validated_output": {
            "accepted_count": validated_batch["accepted_count"],
            "rejected_count": validated_batch["rejected_count"],
            "accepted_records": validated_batch["accepted_records"],
            "rejected_records": validated_batch["rejected_records"],
        },
        "routing_output": routed_batch,
    }