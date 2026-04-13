from ingestion.receiver import ingest_file
from ingestion.detector import detect_file

if __name__ == "__main__":
    file_info = ingest_file("data/synthetic_logs/sensor_readings.csv")
    detection = detect_file(file_info.raw_path)

    print("INGESTED:")
    print(file_info.to_dict())

    print("\nDETECTED:")
    print(detection.to_dict())