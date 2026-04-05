"""
db_handler.py
----------------------------------------------------------
Normalizes raw court records into a unified schema and
persists them to Parquet, CSV, and SQLite storage.

Unified schema:
    case_id         - unique identifier
    court           - court name
    jurisdiction    - federal | state
    filing_date     - ISO date string (YYYY-MM-DD Format)
    decision_date   - ISO date string or None
    case_name       - string
    document_count  - int
    source          - courtlistener | cap
    year            - int (from filing_date)
    month           - int (from filing_date)
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


class StorageHandler:
    """Normalize and persist court metadata records."""

    UNIFIED_SCHEMA = [
        "case_id", "court", "jurisdiction", "filing_date",
        "decision_date", "case_name", "document_count",
        "source", "year", "month",
    ]

    def __init__(self, config_path: str = "config/settings.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.processed_dir = Path(self.config["storage"]["processed_dir"])
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.processed_dir / "court_metadata.db"

    def normalize_courtlistener(self, records: list) -> pd.DataFrame:
        """Map CourtListener records to the unified schema."""
        rows = []
        for rec in records:
            filing_date = rec.get("date_created", "")[:10] or None
            decision_date = rec.get("date_filed", "")[:10] or None
            year, month = self._extract_year_month(filing_date)

            rows.append({
                "case_id":        str(rec.get("id", "")),
                "court":          rec.get("_source_court", ""),
                "jurisdiction":   "federal",
                "filing_date":    filing_date,
                "decision_date":  decision_date,
                "case_name":      rec.get("case_name", ""),
                "document_count": 1,
                "source":         "courtlistener",
                "year":           year,
                "month":          month,
            })

        df = pd.DataFrame(rows, columns=self.UNIFIED_SCHEMA)
        logger.info(f"Normalized {len(df)} CourtListener records")
        return df

    def normalize_cap(self, records: list) -> pd.DataFrame:
        """Map CAP bulk records to the unified schema."""
        rows = []
        for rec in records:
            decision_date = rec.get("decision_date", rec.get("date", ""))
            if decision_date:
                decision_date = str(decision_date)[:10]
            year, month = self._extract_year_month(decision_date)
            year = year if year is not None else 0
            month = month if month is not None else 0

            court_info = rec.get("court", {})
            court_name = (
                court_info.get("slug", court_info.get("name", ""))
                if isinstance(court_info, dict) else str(court_info)
            )

            rows.append({
                "case_id":        str(rec.get("id", "")),
                "court":          court_name,
                "jurisdiction":   "state",
                "filing_date":    decision_date,
                "decision_date":  decision_date,
                "case_name":      rec.get("name", ""),
                "document_count": len(rec.get("opinions", [])) or 1,
                "source":         "cap",
                "year":           year,
                "month":          month,
            })

        df = pd.DataFrame(rows, columns=self.UNIFIED_SCHEMA)
        logger.info(f"Normalized {len(df)} CAP records")
        return df

    def save_parquet(self, df: pd.DataFrame, label: str = "court_metadata") -> Path:
        """Save DataFrame as partitioned Parquet."""
        out_dir = self.processed_dir / "parquet" / label
        out_dir.mkdir(parents=True, exist_ok=True)

        df.to_parquet(
            out_dir,
            partition_cols=["source", "year", "month"],
            engine="pyarrow",
            index=False,
            existing_data_behavior="overwrite_or_ignore",
        )
        logger.info(f"Saved Parquet to {out_dir} ({len(df)} rows)")
        return out_dir

    def save_csv(self, df: pd.DataFrame, label: str = "court_metadata") -> Path:
        """Save flat CSV for inspection."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = self.processed_dir / f"{label}_{timestamp}.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"Saved CSV to {out_path} ({len(df)} rows)")
        return out_path

    def save_sqlite(self, df: pd.DataFrame, table: str = "court_metadata") -> Path:
        """Save records to SQLite database."""
        conn = sqlite3.connect(self.db_path)
        df.to_sql(table, conn, if_exists="append", index=False)
        conn.close()
        logger.info(f"Saved {len(df)} rows to SQLite at {self.db_path}")
        return self.db_path

    def read_parquet(self, label: str = "court_metadata") -> pd.DataFrame:
        """Read back stored Parquet data for verification."""
        path = self.processed_dir / "parquet" / label
        if not path.exists():
            raise FileNotFoundError(f"No Parquet data found at {path}")
        df = pd.read_parquet(path, engine="pyarrow", dtype_backend="numpy_nullable")
        logger.info(f"Read {len(df)} rows from {path}")
        return df

    def summary(self) -> dict:
        """Return storage health summary."""
        parquet_dir = self.processed_dir / "parquet" / "court_metadata"
        csv_files = list(self.processed_dir.glob("*.csv"))
        return {
            "sqlite_exists": self.db_path.exists(),
            "sqlite_path":   str(self.db_path),
            "parquet_exists": parquet_dir.exists(),
            "parquet_path":  str(parquet_dir),
            "csv_files":     [str(f) for f in csv_files],
        }

    @staticmethod
    def _extract_year_month(date_str: str):
        """Extract (year, month) from an ISO date string.
        Handles full dates (YYYY-MM-DD) and partial dates (YYYY-MM or YYYY).
        """
        if not date_str:
            return None, None
        try:
            date_str = str(date_str).strip()
            parts = date_str.split("-")
            year = int(parts[0])
            month = int(parts[1]) if len(parts) >= 2 else 1
            return year, month
        except (ValueError, IndexError):
            return None, None