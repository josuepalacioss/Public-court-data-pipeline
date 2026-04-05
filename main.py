"""
main.py
----------------------------------------------------------------------
Pipeline entry point and script.

Usage:
    python main.py --mode test      # Test API connection only
    python main.py --mode cl        # CourtListener acquisition only
    python main.py --mode cap       # CAP bulk loading only
    python main.py --mode full      # Both sources
    python main.py --mode verify    # Read back stored data
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.ingestion.courtlistener_client import CourtListenerClient
from src.ingestion.cap_loader import CAPLoader
from src.storage.db_handler import StorageHandler
from src.processing.normalizer import validate_schema, basic_summary
from src.processing.spark_analyzer import SparkAnalyzer
import yaml


def setup_logging(config_path: str = "config/settings.yaml"):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    log_cfg = cfg.get("logging", {})
    logging.basicConfig(
        level=getattr(logging, log_cfg.get("level", "INFO")),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_cfg.get("log_file", "data/pipeline.log")),
        ],
    )


def run_test(storage: StorageHandler):
    """Test API connection and check storage status."""
    print("\n── Testing CourtListener API connection ──")
    client = CourtListenerClient()
    result = client.test_connection()
    for k, v in result.items():
        print(f"  {k}: {v}")

    print("\n── Storage summary ──")
    for k, v in storage.summary().items():
        print(f"  {k}: {v}")


def run_courtlistener(storage: StorageHandler):
    """Acquire CourtListener data and persist it."""
    print("\n── CourtListener ingestion ──")
    client = CourtListenerClient()

    records = client.fetch_opinions()
    raw_path = client.save_raw(records, label="opinions")
    print(f"  Raw JSON saved: {raw_path} ({len(records)} records)")

    df = storage.normalize_courtlistener(records)
    validation = validate_schema(df)
    print(f"  Schema valid: {validation['valid']}")
    print(f"  Row count: {validation['row_count']}")

    storage.save_parquet(df)
    storage.save_csv(df, label="courtlistener_sample")
    storage.save_sqlite(df)

    print(f"\n  Summary: {basic_summary(df)}")


def run_cap(storage: StorageHandler):
    """Load CAP bulk data and persist it."""
    print("\n── CAP bulk ingestion ──")
    loader = CAPLoader()
    records = loader.load_bulk()

    if not records:
        print("  No CAP records found.")
        print("  Download bulk files from: https://case.law/bulk/download/")
        return

    raw_path = loader.save_raw(records)
    print(f"  Raw JSON saved: {raw_path} ({len(records)} records)")

    df = storage.normalize_cap(records)
    validation = validate_schema(df)
    print(f"  Schema valid: {validation['valid']}")
    print(f"  Row count: {validation['row_count']}")

    storage.save_parquet(df)
    storage.save_csv(df, label="cap_sample")
    storage.save_sqlite(df)

    print(f"\n  Summary: {basic_summary(df)}")


def run_verify(storage: StorageHandler):
    """Read back stored data to confirm persistence."""
    print("\n── Verifying stored data ──")
    try:
        df = storage.read_parquet()
        print(f"  Parquet read-back successful: {len(df)} rows")
        print(f"\n  First 5 records:")
        print(df.head().to_string())
        print(f"\n  Summary: {basic_summary(df)}")
    except FileNotFoundError as e:
        print(f"  No data found yet: {e}")


def main():
    parser = argparse.ArgumentParser(description="Court Data Pipeline - M2")
    parser.add_argument(
        "--mode",
        choices=["test", "cl", "cap", "full", "verify", "spark"],
        default="test",
        help="test=connection check, cl=CourtListener, cap=CAP, full=both, verify=read stored data"
    )
    parser.add_argument("--config", default="config/settings.yaml")
    args = parser.parse_args()

    setup_logging(args.config)
    logger = logging.getLogger(__name__)
    logger.info(f"Pipeline starting - mode={args.mode}")

    storage = StorageHandler(config_path=args.config)

    if args.mode == "test":
        run_test(storage)
    elif args.mode == "cl":
        run_courtlistener(storage)
    elif args.mode == "cap":
        run_cap(storage)
    elif args.mode == "full":
        run_courtlistener(storage)
        run_cap(storage)
    elif args.mode == "verify":
        run_verify(storage)
    elif args.mode == "spark":
        analyzer = SparkAnalyzer(config_path=args.config)
        analyzer.run_all()

    logger.info("Pipeline complete")


if __name__ == "__main__":
    main()