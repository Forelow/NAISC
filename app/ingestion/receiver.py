from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import hashlib
import shutil
import uuid


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class IngestedFile:
    file_id: str
    filename: str
    extension: str
    size_bytes: int
    raw_path: str
    ingestion_time: str
    sha256: str

    def to_dict(self) -> dict:
        return asdict(self)


def compute_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ingest_file(source_path: str) -> IngestedFile:
    src = Path(source_path)

    if not src.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    if not src.is_file():
        raise ValueError(f"Source path is not a file: {source_path}")

    file_id = str(uuid.uuid4())
    dest_name = f"{file_id}_{src.name}"
    dest_path = RAW_DIR / dest_name

    shutil.copy2(src, dest_path)

    ingested = IngestedFile(
        file_id=file_id,
        filename=src.name,
        extension=src.suffix.lower(),
        size_bytes=dest_path.stat().st_size,
        raw_path=str(dest_path),
        ingestion_time=datetime.utcnow().isoformat(),
        sha256=compute_sha256(dest_path),
    )

    return ingested