import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DATABASE", "semicon_parser"),
)

tables = [
    "rejected_records",
    "generic_observations_staging",
    "wafer_processing_sequences",
    "fault_events",
    "sensor_readings",
    "process_parameters_recipes",
    "equipment_states",
    "files",
]

cur = conn.cursor()

try:
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table}")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("Database cleared successfully.")
finally:
    cur.close()
    conn.close()