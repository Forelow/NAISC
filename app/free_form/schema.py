from __future__ import annotations

ALLOWED_FINAL_TYPES = [
    "equipment_state",
    "process_parameter_recipe",
    "sensor_reading",
    "fault_event",
    "wafer_processing_sequence",
]

ALLOWED_COARSE_TYPES = [
    "state_change",
    "process_step_event",
    "measurement_observation",
    "fault_or_warning",
    "maintenance_action",
    "logistics_or_disposition",
    "configuration_or_recipe",
    "generic_operational_observation",
]

COARSE_TO_FINAL = {
    "state_change": "equipment_state",
    "process_step_event": "wafer_processing_sequence",
    "measurement_observation": "sensor_reading",
    "fault_or_warning": "fault_event",
    "maintenance_action": "wafer_processing_sequence",
    "logistics_or_disposition": "wafer_processing_sequence",
    "configuration_or_recipe": "process_parameter_recipe",
    "generic_operational_observation": None,
}