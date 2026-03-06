"""
cap_loader.py
-----------------------------------------------------------------------------------
Loads Harvard Caselaw Access Project (CAP) bulk JSON files.
Georgia jurisdiction bulk data available at: https://case.law/bulk/download/

Usage:
    loader = CAPLoader()
    records = loader.load_bulk(limit=500)
    loader.save_raw(records)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

import yaml

logger = logging.getLogger(__name__)


class CAPLoader:
    """Loader for Caselaw Access Project bulk JSONL files."""

    def __init__(self, config_path: str = "config/settings.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        cap_cfg = self.config["caselaw_access_project"]
        self.bulk_dir = Path(cap_cfg["bulk_data_dir"])
        self.jurisdiction = cap_cfg["jurisdiction"]

        self.raw_dir = Path(self.config["storage"]["raw_dir"]) / "cap"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def load_bulk(self, limit: int = None) -> list:
        """
        Read all JSONL files in the bulk data directory.

        Args:
            limit: Max records to return (None = all).
                   Use limit for M2 proof-of-concept testing.

        Returns:
            List of case metadata dicts.
        """
        jsonl_files = sorted(self.bulk_dir.glob("**/*.jsonl"))

        if not jsonl_files:
            logger.warning(
                f"No .jsonl files found in {self.bulk_dir}. "
                "Download Georgia bulk data from: "
                "https://case.law/bulk/download/"
            )
            return []

        logger.info(f"Found {len(jsonl_files)} JSONL files in {self.bulk_dir}")

        records = []
        for fpath in jsonl_files:
            for rec in self._read_jsonl(fpath):
                records.append(self._tag_record(rec))
                if limit and len(records) >= limit:
                    logger.info(f"Reached limit of {limit} records")
                    return records

        logger.info(f"Loaded {len(records)} total CAP records")
        return records

    def save_raw(self, records: list, label: str = "cap_georgia") -> Path:
        """Persist records as newline-delimited JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = self.raw_dir / f"{label}_{timestamp}.jsonl"

        with open(out_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        logger.info(f"Saved {len(records)} CAP records to {out_path}")
        return out_path

    def check_bulk_files(self) -> dict:
        """Check what bulk files are available."""
        jsonl_files = list(self.bulk_dir.glob("**/*.jsonl"))
        return {
            "bulk_dir": str(self.bulk_dir),
            "files_found": len(jsonl_files),
            "file_list": [str(f) for f in jsonl_files[:10]],
            "status": "ready" if jsonl_files else "missing - download required",
        }

    def _read_jsonl(self, path: Path) -> Iterator[dict]:
        """Yield one dict per line from a JSONL file."""
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping malformed line in {path}: {e}")

    def _tag_record(self, record: dict) -> dict:
        """Add pipeline metadata tags to a raw CAP record."""
        record["_source"] = "cap"
        record["_jurisdiction"] = self.jurisdiction
        record["_ingested_at"] = datetime.now().isoformat()
        return record