from __future__ import annotations

import mysql.connector


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "Imjohn25",
    "database": "log_parser",
}


def get_server_connection():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def init_db() -> None:
    server_conn = get_server_connection()
    try:
        cur = server_conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        server_conn.commit()
    finally:
        server_conn.close()

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                file_id VARCHAR(128) PRIMARY KEY,
                filename VARCHAR(255),
                extension VARCHAR(32),
                sha256 VARCHAR(128),
                source_format VARCHAR(64),
                parser_version VARCHAR(64),
                schema_fingerprint VARCHAR(64),
                accepted_count INT DEFAULT 0,
                rejected_count INT DEFAULT 0,
                ingestion_time TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS equipment_states (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                curr_state VARCHAR(255),
                prev_state VARCHAR(255),
                lot VARCHAR(255),
                wafer VARCHAR(255),
                recipe VARCHAR(255),
                step VARCHAR(255),
                severity VARCHAR(255),
                event_name TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS process_parameters_recipes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                recipe VARCHAR(255),
                step VARCHAR(255),
                parameter VARCHAR(255),
                value DOUBLE,
                unit VARCHAR(255),
                status VARCHAR(255),
                lot VARCHAR(255),
                wafer VARCHAR(255),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                parameter VARCHAR(255),
                value DOUBLE,
                unit VARCHAR(255),
                lot VARCHAR(255),
                wafer VARCHAR(255),
                recipe VARCHAR(255),
                step VARCHAR(255),
                severity VARCHAR(255),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fault_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                fault_code VARCHAR(255),
                fault_summary TEXT,
                severity VARCHAR(255),
                lot VARCHAR(255),
                wafer VARCHAR(255),
                recipe VARCHAR(255),
                step VARCHAR(255),
                status VARCHAR(255),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wafer_processing_sequences (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                lot VARCHAR(255),
                wafer VARCHAR(255),
                slot VARCHAR(255),
                recipe VARCHAR(255),
                step VARCHAR(255),
                status VARCHAR(255),
                action VARCHAR(255),
                event_name TEXT,
                duration_s DOUBLE,
                wafer_count INT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS generic_observations_staging (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                tool_id VARCHAR(255),
                event_ts TEXT,
                record_type VARCHAR(255),
                note TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rejected_records (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                record_type VARCHAR(255),
                validation_reason VARCHAR(255),
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.commit()
    finally:
        conn.close()