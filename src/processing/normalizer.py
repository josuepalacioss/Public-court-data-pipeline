"""
normalizer.py
------------------------------------------------------------
Basic schema validation and summary statistics for M2.

M3 planned additions:
    - PySpark DataFrame transformations
    - MapReduce aggregations by (court, jurisdiction, year)
    - Completeness statistics per metadata field
    - Case volume trend
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ["case_id", "court", "filing_date", "source"]
ALL_FIELDS = [
    "case_id", "court", "jurisdiction", "filing_date",
    "decision_date", "case_name", "document_count",
    "source", "year", "month",
]


def validate_schema(df: pd.DataFrame) -> dict:
    """
    Check that a normalized DataFrame has all required fields.
    Returns validation results including null rates per column.
    """
    missing = [c for c in REQUIRED_FIELDS if c not in df.columns]
    null_rates = {
        col: float(df[col].isna().mean())
        for col in df.columns if col in ALL_FIELDS
    }

    result = {
        "valid": len(missing) == 0,
        "missing_required_cols": missing,
        "null_rates": null_rates,
        "row_count": len(df),
    }

    if missing:
        logger.warning(f"Schema validation failed - missing: {missing}")
    else:
        logger.info(f"Schema valid - {len(df)} rows")

    return result


def basic_summary(df: pd.DataFrame) -> dict:
    """
    Compute lightweight analytics on a normalized DataFrame.
    Full distributed analytics via Spark SQL planned for M3.
    """
    summary = {
        "total_records": len(df),
        "sources": {},
        "jurisdictions": {},
        "year_range": None,
        "courts_represented": 0,
    }

    if "source" in df.columns:
        summary["sources"] = df["source"].value_counts().to_dict()

    if "jurisdiction" in df.columns:
        summary["jurisdictions"] = df["jurisdiction"].value_counts().to_dict()

    if "year" in df.columns:
        years = pd.to_numeric(df["year"], errors="coerce").dropna()
        if not years.empty:
            summary["year_range"] = {
                "min": int(years.min()),
                "max": int(years.max())
            }

    if "court" in df.columns:
        summary["courts_represented"] = df["court"].nunique()

    return summary