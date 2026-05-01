"""
recover_cl.py
----------------------------------------------------------------------
Loads a previously saved CourtListener JSONL file directly into
Parquet/CSV/SQLite without making any API calls.

Usage:
    py -3.11 recover_cl.py
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.storage.db_handler import StorageHandler
from src.processing.normalizer import validate_schema, basic_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

JSONL_PATH = Path("data/raw/courtlistener/opinions_20260421_201451.jsonl")

def main():
    if not JSONL_PATH.exists():
        print(f"File not found: {JSONL_PATH}")
        return

    print(f"\n--- Loading saved CourtListener JSONL ---")
    records = []
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    print(f"  Loaded {len(records)} records from {JSONL_PATH.name}")

    storage = StorageHandler()
    df = storage.normalize_courtlistener(records)
    validation = validate_schema(df)
    print(f"  Schema valid: {validation['valid']}")
    print(f"  Row count: {validation['row_count']}")

    storage.save_parquet(df)
    storage.save_csv(df, label="courtlistener_recovered")
    storage.save_sqlite(df)

    print(f"\n  Summary: {basic_summary(df)}")
    print("\n--- Recovery complete ---")

if __name__ == "__main__":
    main()