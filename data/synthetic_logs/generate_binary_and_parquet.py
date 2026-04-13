"""
Synthetic Binary Log Generator and Parquet Generator
for Micron AISG Challenge

Binary Format Spec (TLOG v1):
-------------------------------
File Header (16 bytes):
  [0:4]   Magic: b'TLOG'
  [4:5]   Version: uint8 = 1
  [5:6]   Record type: uint8 (0=sensor, 1=alarm, 2=state, 3=step)
  [6:8]   Reserved: 2 bytes
  [8:16]  Timestamp: int64 Unix epoch milliseconds

Per Record (32 bytes each):
  [0:8]   Timestamp: int64 Unix epoch ms
  [8:9]   Record type: uint8
  [9:10]  Severity: uint8 (0=INFO, 1=WARN, 2=ERROR, 3=CRITICAL)
  [10:14] Tool ID hash: uint32
  [14:16] Event code: uint16
  [16:20] Value1 (float): float32  (e.g. pressure in Pa)
  [20:24] Value2 (float): float32  (e.g. temperature in C)
  [24:28] Value3 (float): float32  (e.g. rf_power in W)
  [28:30] Chamber ID: uint16 (1=C1, 2=C2)
  [30:32] Checksum: uint16 (sum of bytes 0-29 mod 65536)
"""

import struct
import os
import datetime
import pandas as pd

OUTPUT_DIR = "/home/claude/synthetic_logs"

# --- BINARY LOG GENERATOR ---

MAGIC = b'TLOG'
VERSION = 1
TOOL_ID_HASH = 0xA3F1  # Represents TOOL_ETCH_03

RECORD_TYPE = {"sensor": 0, "alarm": 1, "state": 2, "step": 3}
SEVERITY    = {"INFO": 0, "WARN": 1, "ERROR": 2, "CRITICAL": 3}
EVENT_CODES = {
    "SENSOR_READING": 0x0001,
    "STEP_START":     0x0010,
    "STEP_END":       0x0011,
    "STATE_CHANGE":   0x0020,
    "ALARM_V101":     0x0101,
    "ALARM_V201":     0x0201,
    "ALARM_RF002":    0x0302,
    "ALARM_CLEARED":  0x00FF,
}

def make_timestamp(dt_str):
    dt = datetime.datetime.fromisoformat(dt_str)
    return int(dt.timestamp() * 1000)

def checksum(data):
    return sum(data) % 65536

def write_record(f, ts_ms, rec_type, severity, event_code, v1, v2, v3, chamber):
    data = struct.pack(
        ">qBBHHfffHH",
        ts_ms,
        RECORD_TYPE[rec_type],
        SEVERITY[severity],
        TOOL_ID_HASH,
        EVENT_CODES[event_code],
        v1, v2, v3,
        chamber,
        0  # placeholder checksum
    )
    cs = checksum(data[:30])
    data = data[:30] + struct.pack(">H", cs)
    f.write(data)

def write_file_header(f, first_ts_ms, rec_type_byte=0):
    header = MAGIC + struct.pack(">BBxx", VERSION, rec_type_byte) + struct.pack(">q", first_ts_ms)
    f.write(header)

binary_path = os.path.join(OUTPUT_DIR, "etch_tool_binary.bin")

records = [
    # ts_str,                          type,    sev,     event,            p,     T,     rf,   ch
    ("2026-04-13T08:00:15+00:00", "sensor", "INFO",  "SENSOR_READING", 0.92,  85.2, 0.0,   1),
    ("2026-04-13T08:00:16+00:00", "sensor", "INFO",  "SENSOR_READING", 0.90,  85.4, 0.0,   1),
    ("2026-04-13T08:00:17+00:00", "sensor", "INFO",  "SENSOR_READING", 0.87,  85.7, 150.0, 1),
    ("2026-04-13T08:00:18+00:00", "sensor", "INFO",  "SENSOR_READING", 0.85,  86.0, 150.0, 1),
    ("2026-04-13T08:00:19+00:00", "alarm",  "WARN",  "ALARM_V101",     0.85,  86.1, 148.0, 1),
    ("2026-04-13T08:00:20+00:00", "sensor", "INFO",  "SENSOR_READING", 0.83,  86.1, 148.0, 1),
    ("2026-04-13T08:00:22+00:00", "step",   "INFO",  "STEP_START",     0.0,   0.0,  0.0,   1),
    ("2026-04-13T08:00:30+00:00", "sensor", "INFO",  "SENSOR_READING", 0.80,  88.0, 150.0, 1),
    ("2026-04-13T08:01:10+00:00", "alarm",  "ERROR", "ALARM_RF002",    0.78,  88.8, 132.0, 1),
    ("2026-04-13T08:01:11+00:00", "sensor", "INFO",  "SENSOR_READING", 0.78,  88.9, 138.0, 1),
    ("2026-04-13T08:01:15+00:00", "sensor", "INFO",  "SENSOR_READING", 0.78,  89.0, 150.0, 1),
    ("2026-04-13T08:02:00+00:00", "step",   "INFO",  "STEP_END",       0.0,   0.0,  0.0,   1),
    ("2026-04-13T08:03:10+00:00", "alarm",  "ERROR", "ALARM_V201",     0.55,  71.5, 0.0,   1),
    ("2026-04-13T08:03:11+00:00", "state",  "ERROR", "STATE_CHANGE",   0.0,   0.0,  0.0,   1),
    ("2026-04-13T08:07:00+00:00", "alarm",  "INFO",  "ALARM_CLEARED",  0.0,   0.0,  0.0,   1),
    ("2026-04-13T08:07:01+00:00", "state",  "INFO",  "STATE_CHANGE",   0.0,   0.0,  0.0,   1),
]

first_ts = make_timestamp(records[0][0])

with open(binary_path, "wb") as f:
    write_file_header(f, first_ts)
    for r in records:
        ts = make_timestamp(r[0])
        write_record(f, ts, r[1], r[2], r[3], r[4], r[5], r[6], r[7])

print(f"Binary log written: {binary_path} ({os.path.getsize(binary_path)} bytes)")
print(f"  Header: 16 bytes | Records: {len(records)} x 32 bytes = {len(records)*32} bytes")

# --- PARQUET GENERATOR (Vendor C style) ---

sensor_data = []
import numpy as np

base_ts = datetime.datetime(2026, 4, 13, 8, 0, 0, tzinfo=datetime.timezone.utc)

for i in range(60):
    ts = base_ts + datetime.timedelta(seconds=i)
    sensor_data.append({
        "ControlJobKeys": '{"CJobID": "CJOB_0003", "EquipmentID": "EQP_"}',
        "ControlJobAttributes": '{"FileType": "SensorData", "ParquetSchemaVersion": "v1.2"}',
        "ProcessJobKeys": '{"PRJobID": "PRJOB_0003"}',
        "ProcessJobAttributes": '{"LotID": "LOT_0046", "RecipeStartTime": "2026-04-13T08:00:00Z"}',
        "ModuleProcessReportKeys": '{"ModuleID": "MOD_0001", "WaferID": "WFR_0020"}',
        "ModuleProcessReportAttributes": f'{{"RecipeStepStartTime": "{ts.isoformat()}", "RecipeStepID": "ETCH"}}',
        "SensorKey": '{"SensorID": "SENSOR_0001"}',
        "Measurements": f'{{"DateTime": "{ts.isoformat()}", "Value": {round(0.90 - i*0.002 + (i%3)*0.001, 4)}}}'
    })

    sensor_data.append({
        "ControlJobKeys": '{"CJobID": "CJOB_0003", "EquipmentID": "EQP_"}',
        "ControlJobAttributes": '{"FileType": "SensorData", "ParquetSchemaVersion": "v1.2"}',
        "ProcessJobKeys": '{"PRJobID": "PRJOB_0003"}',
        "ProcessJobAttributes": '{"LotID": "LOT_0046", "RecipeStartTime": "2026-04-13T08:00:00Z"}',
        "ModuleProcessReportKeys": '{"ModuleID": "MOD_0001", "WaferID": "WFR_0020"}',
        "ModuleProcessReportAttributes": f'{{"RecipeStepStartTime": "{ts.isoformat()}", "RecipeStepID": "ETCH"}}',
        "SensorKey": '{"SensorID": "SENSOR_0002"}',
        "Measurements": f'{{"DateTime": "{ts.isoformat()}", "Value": {round(85.0 + i*0.05, 2)}}}'
    })

    sensor_data.append({
        "ControlJobKeys": '{"CJobID": "CJOB_0003", "EquipmentID": "EQP_"}',
        "ControlJobAttributes": '{"FileType": "SensorData", "ParquetSchemaVersion": "v1.2"}',
        "ProcessJobKeys": '{"PRJobID": "PRJOB_0003"}',
        "ProcessJobAttributes": '{"LotID": "LOT_0046", "RecipeStartTime": "2026-04-13T08:00:00Z"}',
        "ModuleProcessReportKeys": '{"ModuleID": "MOD_0001", "WaferID": "WFR_0020"}',
        "ModuleProcessReportAttributes": f'{{"RecipeStepStartTime": "{ts.isoformat()}", "RecipeStepID": "ETCH"}}',
        "SensorKey": '{"SensorID": "SENSOR_0003"}',
        "Measurements": f'{{"DateTime": "{ts.isoformat()}", "Value": {150 if i < 5 or i > 10 else 132}}}'
    })

df = pd.DataFrame(sensor_data)
parquet_path = os.path.join(OUTPUT_DIR, "vendorC_sensor_trace.parquet")
df.to_parquet(parquet_path, index=False)
print(f"\nParquet file written: {parquet_path}")
print(f"  Rows: {len(df)} | Columns: {list(df.columns)}")

# Print parquet preview
print("\nParquet preview (first 3 rows):")
print(df.head(3).to_string())
