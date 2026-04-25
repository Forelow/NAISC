Unknown File Type Test Pack

These files are meant to test your content-based unknown-file handling.

Expected behavior:
- mystery_json_like.dat -> should be detected as JSON-like text
- mystery_xml_like.blob -> should be detected as XML-like text
- mystery_csv_like.weird -> should be detected as CSV-like text
- mystery_kv_like.abc -> should be detected as key=value / semi-structured text
- mystery_syslog_like.odd -> should be detected as syslog-like / semi-structured text
- mystery_free_text.unknown -> should fall back to free-form text
- mystery_binary_junk.zzz -> should be treated as binary / opaque unknown
- mystery_parquet_signature.pqx -> should trigger parquet-like binary signature detection if your sniffer checks PAR1
- Real parquet file was not created in this environment, so only the PAR1 signature test file is included.