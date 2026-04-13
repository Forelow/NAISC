from ingestion.receiver import ingest_file
from ingestion.detector import detect_file
from ingestion.support_registry import check_support
from ingestion.router import route_file


def run_pipeline(file_path: str) -> None:
    ingested = ingest_file(file_path)
    detection = detect_file(ingested.raw_path)
    support = check_support(detection.format_guess)
    routing = route_file(detection, support)

    print("INGESTED")
    print(ingested.to_dict())

    print("\nDETECTION")
    print(detection.to_dict())

    print("\nSUPPORT")
    print(support.to_dict())

    print("\nROUTING")
    print(routing.to_dict())


if __name__ == "__main__":
    run_pipeline("data/synthetic_logs/vendorC_sensor_trace.parquet")