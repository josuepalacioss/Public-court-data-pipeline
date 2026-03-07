"""
courtlistener_client.py
-----------------------------------------------------------------------------
Fetches metadata from the CourtListener REST API v4.
Handles authentication, pagination, retries, and raw JSON persistence.
"""

import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class CourtListenerClient:
    """Client for the CourtListener REST API v4."""

    def __init__(self, config_path: str = "config/settings.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.token = os.getenv("COURTLISTENER_API_TOKEN")
        if not self.token:
            raise EnvironmentError(
                "COURTLISTENER_API_TOKEN not set.\n"
                "Copy config/.env.example to .env and add your token.\n"
                "Register at: https://www.courtlistener.com/register/"
            )

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
        })

        self.cl_config = self.config["courtlistener"]
        self.raw_dir = Path(self.config["storage"]["raw_dir"]) / "courtlistener"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def test_connection(self) -> dict:
        """Verify credentials by hitting the courts endpoint."""
        try:
            url = f"{self.cl_config['base_url']}/courts/"
            resp = self._get(url, params={"page_size": 3, "format": "json"})
            sample = resp.get("results", [])[:3]
            return {
                "status": "success",
                "message": "API connection authenticated successfully",
                "sample_courts": [c.get("short_name", "") for c in sample],
                "total_courts": resp.get("count", "unknown"),
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def fetch_opinions(self, court: str = None, page_limit: int = None) -> list:
        """Fetch cluster metadata for one or all configured courts."""
        courts = [court] if court else self.cl_config["courts"]
        page_limit = page_limit or self.cl_config["max_pages"]

        all_records = []
        for c in courts:
            logger.info(f"Fetching opinions for court: {c}")
            records = self._paginate_opinions(c, page_limit)
            all_records.extend(records)
            logger.info(f"  -> {len(records)} records retrieved for {c}")

        return all_records

    def save_raw(self, records: list, label: str = "opinions") -> Path:
        """Persist raw records as newline-delimited JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = self.raw_dir / f"{label}_{timestamp}.jsonl"

        with open(out_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        logger.info(f"Saved {len(records)} records to {out_path}")
        return out_path

    def _paginate_opinions(self, court: str, page_limit: int) -> list:
        """Paginate through the clusters endpoint for a single court."""
        url = f"{self.cl_config['base_url']}/clusters/"
        params = {
            "format": "json",
            "docket__court": court,
            "page_size": 20,
            "order_by": "id",
        }

        records = []
        page = 0

        while url and page < page_limit:
            response = self._get(url, params=params if page == 0 else None)
            batch = response.get("results", [])

            for rec in batch:
                rec["_source_court"] = court
                rec["_ingested_at"] = datetime.now().isoformat()

            records.extend(batch)
            url = response.get("next")
            page += 1

            if url:
                time.sleep(0.5)

        return records

    def _get(self, url: str, params: dict = None) -> dict:
        """Execute a GET request with retry logic for timeouts."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                break
            except requests.exceptions.ReadTimeout:
                if attempt == 2:
                    raise
                logger.warning(f"Timeout on attempt {attempt + 1}, retrying in 10s...")
                time.sleep(10)

        if resp.status_code == 401:
            raise PermissionError(
                "API token invalid or expired. "
                "Check your COURTLISTENER_API_TOKEN in .env"
            )
        if resp.status_code == 429:
            logger.warning("Rate limited - sleeping 60s")
            time.sleep(60)
            return self._get(url, params)

        resp.raise_for_status()
        return resp.json()
