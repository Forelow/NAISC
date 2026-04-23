from __future__ import annotations

import struct
from pathlib import Path


MAGIC = b"SLOG"

TYPE_MAP = {
    "equipment_state": 1,
    "process_parameter_recipe": 2,
    "sensor_reading": 3,
    "fault_event": 4,
    "wafer_processing_sequence": 5,
}


def build_record(record_type: str, payload_text: str) -> bytes:
    rec_type = TYPE_MAP[record_type]
    payload = payload_text.encode("utf-8")
    return bytes([rec_type]) + struct.pack("<H", len(payload)) + payload


def build_container(records: list[bytes], version: int = 1, flags: int = 0) -> bytes:
    header = MAGIC + bytes([version, flags]) + struct.pack("<H", len(records))
    return header + b"".join(records)


def write_demo_files(output_dir: str = "data/binary_samples") -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    records = [
        build_record(
            "equipment_state",
            "ts=1712821200;tool=ETCH01;state=IDLE"
        ),
        build_record(
            "process_parameter_recipe",
            "ts=1712821260;tool=ETCH01;recipe=RCP_A1;step=PreClean;gas=Ar"
        ),
        build_record(
            "sensor_reading",
            "ts=1712821320;tool=ETCH01;parameter=pressure;value=4.2;unit=mTorr"
        ),
        build_record(
            "fault_event",
            "ts=1712821380;tool=ETCH01;fault_code=ARC_17;fault_summary=Arc detected"
        ),
        build_record(
            "wafer_processing_sequence",
            "ts=1712821440;tool=ETCH01;wafer=WFR_0001;event=loaded;slot=1"
        ),
    ]

    container = build_container(records)

    bin_path = out / "demo_tool_log.bin"
    hex_path = out / "demo_tool_log.hex"

    bin_path.write_bytes(container)
    hex_path.write_text(container.hex(" "), encoding="utf-8")

    print(f"Wrote: {bin_path}")
    print(f"Wrote: {hex_path}")


if __name__ == "__main__":
    write_demo_files()