# Pipeline Documentation

## Overview

The pipeline is a batch-oriented, multi-stage system that ingests public court metadata
from two heterogeneous sources, normalizes them into a unified schema, persists the
results across three storage formats, and runs distributed analytics via PySpark.

All stages are orchestrated through `main.py` and controlled via the `--mode` argument.
Each mode can be run independently or in sequence.

```
Sources → Ingestion → Normalization → Storage → Analytics
```

---

## Execution Modes

| Mode | Command | Description |
|---|---|---|
| `test` | `py -3.11 main.py --mode test` | Verify API connection and storage status |
| `cl` | `py -3.11 main.py --mode cl` | Ingest CourtListener federal records only |
| `cap` | `py -3.11 main.py --mode cap` | Ingest CAP Georgia bulk records only |
| `full` | `py -3.11 main.py --mode full` | Run both sources end-to-end |
| `verify` | `py -3.11 main.py --mode verify` | Read back stored Parquet and confirm integrity |
| `spark` | `py -3.11 main.py --mode spark` | Run distributed Spark analytics pipeline |

---

## Stage 1 — CourtListener Ingestion

**Module:** `src/ingestion/courtlistener_client.py`  
**Class:** `CourtListenerClient`  
**Triggered by:** `--mode cl`, `--mode full`

### What it does
Fetches opinion cluster metadata from the CourtListener REST API v4. Paginates through
the clusters endpoint for each configured court, collecting metadata records including
case name, filing date, decision date, and court identifier.

### Inputs
- CourtListener API token (from `.env` → `COURTLISTENER_API_TOKEN`)
- Court list (from `config/settings.yaml` → `courtlistener.courts`)
- Date range (from `settings.yaml` → `courtlistener.date_after`, `date_before`)
- Page size and max pages (from `settings.yaml` → `courtlistener.page_size`, `max_pages`)

### Outputs
- Raw JSONL file saved to `data/raw/courtlistener/opinions_{timestamp}.jsonl`
- List of raw record dicts returned to `main.py` for normalization

### Configuration (`config/settings.yaml`)
```yaml
courtlistener:
  base_url: "https://www.courtlistener.com/api/rest/v4"
  courts:
    - "scotus"
    - "ca1"
    - "ca9"
  page_size: 100
  max_pages: 10
  date_after: "2020-01-01"
  date_before: "2024-12-31"
```

### Error handling
- **Rate limiting (429):** Sleeps 60 seconds and retries automatically
- **Timeout:** Retries up to 3 times with 10-second delay between attempts
- **Invalid token (401):** Raises `PermissionError` with instructions to check `.env`

### M3 output
600 records across scotus, ca1, ca9 covering 2014-2016.

---

## Stage 2 — CAP Bulk Ingestion

**Module:** `src/ingestion/cap_loader.py`  
**Class:** `CAPLoader`  
**Triggered by:** `--mode cap`, `--mode full`

### What it does
Reads Harvard Caselaw Access Project bulk JSON files from the local filesystem.
Scans the primary `json/` directory plus all `vol{N}/json/` subdirectories, loading
individual case JSON files and tagging each record with pipeline metadata.

### Inputs
- Bulk data directory (from `settings.yaml` → `caselaw_access_project.bulk_data_dir`)
- Georgia volume JSON files downloaded and unzipped locally
- Optional `limit` parameter to cap the number of records loaded

### Outputs
- Raw JSONL file saved to `data/raw/cap/cap_georgia_{timestamp}.jsonl`
- List of raw record dicts returned to `main.py` for normalization

### Directory structure expected
```
data/raw/cap/
    json/           <- Volume 1 (original)
    vol2/json/      <- Volume 2
    vol3/json/      <- Volume 3
    ...
    vol55/json/     <- Volume 55
```

### Configuration (`config/settings.yaml`)
```yaml
caselaw_access_project:
  jurisdiction: "ga"
  bulk_data_dir: "data/raw/cap"
```

### Error handling
- **Missing directory:** Logs a warning with download instructions, returns empty list
- **Malformed JSON:** Skips the file with a warning log, continues processing
- **Empty directory:** Logs a warning and returns empty list gracefully

### M3 output
6,809 records across 55 volumes covering Georgia Supreme Court cases from 1846 to 1876.

---

## Stage 3 — Normalization and Storage

**Module:** `src/storage/db_handler.py`  
**Class:** `StorageHandler`  
**Triggered by:** All modes except `test`, `verify`, `spark`

### What it does
Maps raw records from each source into the unified 10-column schema, validates the
result, and persists it across three storage formats. Each source has a dedicated
normalization method that handles field mapping and type coercion.

### Unified schema
```
case_id, court, jurisdiction, filing_date, decision_date,
case_name, document_count, source, year, month
```

See `DATA_DICTIONARY.md` for full field definitions.

### Normalization decisions
- **CourtListener:** `date_created` maps to `filing_date`, `date_filed` maps to `decision_date`,
  `_source_court` tag maps to `court`, hardcoded `"federal"` assigned to `jurisdiction`
- **CAP:** `decision_date` maps to both `filing_date` and `decision_date` (single date field),
  `court.slug` or `court.name` maps to `court`, hardcoded `"state"` assigned to `jurisdiction`
- **Dates:** Partial dates (`YYYY-MM`) expanded to `YYYY-MM-01` via custom parser
- **Year/month:** Extracted from `filing_date` for use as Parquet partition columns

### Outputs
| Format | Location | Notes |
|---|---|---|
| Parquet | `data/processed/parquet/court_metadata/` | Partitioned by source/year/month |
| CSV | `data/processed/{label}_{timestamp}.csv` | Flat file for inspection |
| SQLite | `data/processed/court_metadata.db` | Table: `court_metadata`, append mode |

### Configuration (`config/settings.yaml`)
```yaml
storage:
  raw_dir: "data/raw"
  processed_dir: "data/processed"
  partition_cols:
    - "source"
    - "year"
    - "month"
```

### Error handling
- Schema validation run after normalization — missing required columns logged as warnings
- Parquet uses `overwrite_or_ignore` to allow clean reruns without data corruption
- SQLite uses `append` mode — clear the database manually if a full reset is needed

---

## Stage 4 — Verification

**Module:** `main.py` → `run_verify()`  
**Triggered by:** `--mode verify`

### What it does
Reads the processed Parquet dataset back from disk using pandas and prints a summary
of total rows, source breakdown, jurisdiction breakdown, year range, and courts
represented. Confirms that the storage layer is functioning correctly end-to-end.

### Inputs
- Parquet dataset at `data/processed/parquet/court_metadata/`

### Outputs
- Terminal printout of first 5 records
- Summary statistics dict

### Error handling
- Raises `FileNotFoundError` with a clear message if no Parquet data exists yet

### M3 output
7,409 rows read back successfully — cap: 6,809 + courtlistener: 600.

---

## Stage 5 — Spark Analytics

**Module:** `src/processing/spark_analyzer.py`  
**Class:** `SparkAnalyzer`  
**Triggered by:** `--mode spark`

### What it does
Loads the normalized Parquet dataset into a Spark DataFrame and runs a full suite of
distributed analytics including MapReduce aggregations, completeness statistics, case
volume trends, and Spark SQL queries. Results are saved as CSV files.

### Prerequisites
- Python 3.11 (required — PySpark 3.5.3 is incompatible with Python 3.14)
- Java 17 (`JAVA_HOME` must point to JDK 17)
- WinUtils (`HADOOP_HOME` must point to winutils directory on Windows)

### Inputs
- Parquet dataset at `data/processed/parquet/court_metadata/`
- Loaded via pandas first, then converted to Spark DataFrame (Windows compatibility)

### Analytics performed

**MapReduce aggregations:**
- Case counts by court and jurisdiction (groupBy = map/shuffle/reduce)
- Case counts by source
- Case volume by year and source

**Completeness statistics:**
- Null rate per field across all 10 schema columns
- Total, complete, and null counts per field

**Case volume trends:**
- Peak filing years by source
- Year range summary per source (earliest, latest, total records)

**Spark SQL queries:**
- Q1: Top 10 most active courts
- Q2: Federal vs state coverage comparison
- Q3: Cases per decade by source

### Outputs
All analytics results saved to `data/processed/analytics/`:

| File | Contents |
|---|---|
| `by_court.csv` | Case counts grouped by court and jurisdiction |
| `by_jurisdiction.csv` | Federal vs state aggregate counts |
| `by_source.csv` | Records per source |
| `by_year.csv` | Case volume by year and source |
| `completeness.csv` | Null rates across all 10 schema fields |
| `peak_years.csv` | Peak filing years ranked by case count |
| `year_range.csv` | Earliest/latest year and total records per source |
| `top_courts.csv` | Top 10 most active courts (SQL Q1) |
| `coverage_comparison.csv` | Federal vs state detailed comparison (SQL Q2) |
| `by_decade.csv` | Cases grouped by decade and source (SQL Q3) |

### Configuration
Spark session configured programmatically in `_build_session()`:
- Master: `local[*]` (uses all available CPU cores)
- Shuffle partitions: `4` (optimized for local small-scale execution)
- Driver memory: `2g`
- UI disabled for clean local execution

### Error handling
- Missing Parquet data raises `FileNotFoundError` with instructions to run `--mode full` first
- Individual analytics output save failures are caught and logged as warnings
- Spark session always stopped in a `finally` block to prevent resource leaks

### M3 output
7,409 records processed — 100% completeness across all 10 fields, all 10 analytics CSVs saved.

---

## Logging

All pipeline stages log to both stdout and `data/pipeline.log` simultaneously.

Log format:
```
{timestamp} [{level}] {module} - {message}
```

Example:
```
2026-04-04 16:00:53 [INFO] src.ingestion.cap_loader - Found 55 volume directories to scan
2026-04-04 16:00:55 [INFO] src.ingestion.cap_loader - Loaded 6809 total CAP records
2026-04-04 16:00:56 [INFO] src.storage.db_handler - Normalized 6809 CAP records
2026-04-04 16:00:56 [INFO] src.storage.db_handler - Saved Parquet to data\processed\parquet\court_metadata (6809 rows)
2026-04-04 16:00:57 [INFO] __main__ - Pipeline complete
```

Log level and output file configured in `config/settings.yaml`:
```yaml
logging:
  level: "INFO"
  log_file: "data/pipeline.log"
```
