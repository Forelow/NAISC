from ingestion.receiver import ingest_file

if __name__ == "__main__":
    file_info = ingest_file("data/synthetic_logs/vendorA_etch_tool_log.json")
    print(file_info.to_dict())