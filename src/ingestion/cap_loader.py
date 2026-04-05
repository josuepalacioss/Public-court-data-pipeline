"""
cap_loader.py
-----------------------------------------------------------------------------------
Loads Harvard Caselaw Access Project (CAP) bulk JSON files.
Georgia bulk data downloaded from: https://static.case.law/ga/1.zip

File structure after unzipping:
    data/raw/cap/
        json/           <- individual case JSON files (0001-01.json, etc.)
        html/           <- HTML versions (not used)
        metadata/       <- volume/reporter metadata

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
    """Loader for Caselaw Access Project bulk JSON files."""

    def __init__(self, config_path: str = "config/settings.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        cap_cfg = self.config["caselaw_access_project"]
        self.bulk_dir = Path(cap_cfg["bulk_data_dir"])
        self.jurisdiction = cap_cfg["jurisdiction"]

        self.raw_dir = Path(self.config["storage"]["raw_dir"]) / "cap"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # JSON files are extracted to data/raw/cap/json/
        self.json_dir = self.bulk_dir / "json"

    def load_bulk(self, limit: int = None) -> list:
        """
        Read all JSON files across all volume directories.
        Scans the primary json/ directory plus all vol{N}/json/ subdirectories.

        Args:
            limit: Max records to return (None = all).

        Returns:
            List of case metadata dicts.
        """
        # Collect all json directories — primary + volume subdirectories
        json_dirs = []

        if self.json_dir.exists():
            json_dirs.append(self.json_dir)

        for vol_dir in sorted(self.bulk_dir.glob("vol*/json")):
            if vol_dir.is_dir():
                json_dirs.append(vol_dir)

        if not json_dirs:
            logger.warning(
                f"No json/ directories found under {self.bulk_dir}. "
                "Download and unzip Georgia bulk data from: "
                "https://static.case.law/bulk/download/"
            )
            return []

        logger.info(f"Found {len(json_dirs)} volume directories to scan")

        records = []
        for json_dir in json_dirs:
            json_files = sorted(json_dir.glob("*.json"))
            logger.info(f"  {json_dir}: {len(json_files)} files")

            for fpath in json_files:
                for rec in self._read_case_json(fpath):
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
        json_files = list(self.json_dir.glob("*.json")) if self.json_dir.exists() else []
        return {
            "bulk_dir": str(self.bulk_dir),
            "json_dir": str(self.json_dir),
            "files_found": len(json_files),
            "file_list": [str(f) for f in json_files[:10]],
            "status": "ready" if json_files else "missing - download and unzip required",
        }

    def _read_case_json(self, path: Path) -> Iterator[dict]:
        """
        Read a CAP case JSON file.
        Each file may contain a single case object or a list of cases.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Some files are a list of cases, others are a single case object
            if isinstance(data, list):
                for case in data:
                    yield case
            elif isinstance(data, dict):
                # Could be a single case or a wrapper with 'cases' key
                if "cases" in data:
                    for case in data["cases"]:
                        yield case
                else:
                    yield data

        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Skipping {path.name}: {e}")

    def _tag_record(self, record: dict) -> dict:
        """Add pipeline metadata tags to a raw CAP record."""
        record["_source"] = "cap"
        record["_jurisdiction"] = self.jurisdiction
        record["_ingested_at"] = datetime.now().isoformat()
        return record
