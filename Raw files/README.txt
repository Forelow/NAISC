Synthetic semiconductor tool log dataset (realistic version)

Contents:
- 2 files each for JSON, CSV, XML, Syslog, Key-Value, TXT, Binary, Hex
- Each file contains at least 120 records or packet-equivalents

Clean files:
- Consistent schema
- Valid timestamps
- Stable delimiters and field types

Unclean files:
- Missing fields
- Malformed timestamps
- Mixed delimiters
- Unexpected extra fields
- Type inconsistency (numeric -> string)
- Multi-line text logs
- Broken syslog pri header
- Truncated / corrupted binary packets

Domain realism:
- Multiple tool families: ETCH, CVD, LITHO, CMP, METROLOGY, IMPLANT
- Lot / wafer / recipe / run_id / chamber context
- Process states and alarm/fault events
- Sensor and process parameter fields tied to tool type
