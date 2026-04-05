# CS4265_Josue_Palacios_M3

**Big Data Pipeline for Public Court Document Analytics**


## Project Overview

**Domain:** Public court data analytics

This project focuses on processing the structured metadata contained within court cases
and legal documents. These public court datasets are readily available through APIs and
bulk archives, but they are often fragmented across institutions. By ingesting and analyzing
the document metadata we can extract courts, dates, case types, document counts, and
completeness indicators. This domain is motivated by the need for scalable infrastructure
to support legal research and analysis of judicial activity using large public datasets.


## Problem Statement

Public court data exists on a large scale but is difficult to analyze efficiently due to the sheer
volume, heterogeneity, and different access constraints. Federal and state court datasets are
published by different organizations, through different access methods. The ones tackled here
include REST APIs provided by CourtListener and bulk archives provided by the Caselaw
Access Project by Harvard Law School. As a result, answering basic questions will require
great effort.

_This project aims to address the following questions:_

- How does the volume of court cases and documents change over time across jurisdictions?
- How do federal and state court datasets differ in coverage and completeness?
- What trends exist in case counts, document counts, and court activity over time?
- How complete are metadata fields across sources?

_At scale, these questions introduce several challenges:_

- Data volume by the hundreds of thousands to over a million records and the resulting space.
- Heterogeneous ingestion through combining incremental API based data with large bulk datasets.
- Schema variability that offers differing field structures and naming across sources.


## Scope

This project focuses on the design and implementation of a distributed, batch-oriented Big
Data pipeline for analyzing structured metadata derived from public court documents.

_In Scope_

Batch ingestion of heterogeneous public court datasets:

- Federal court metadata from the CourtListener REST API.
- State court metadata from the Harvard Caselaw Access Project (Georgia) bulk archives.

Schema normalization and validation to unify heterogeneous JSON sources into a shared
Parquet-based data model.

Metadata analytics:

- Case and document volume trends over time.
- Court and jurisdiction activity summaries.
- Completeness and coverage statistics for key metadata fields.

Distributed query execution using Spark SQL with partitioned Parquet datasets.
Local execution with a cloud-ready design that supports scaling via additional partitions
and executors.

_Out of Scope_

- Parsing, indexing, or analyzing the contents of court case PDF documents.
- Natural language processing, machine learning, or data mining.
- Legal interpretation or reasoning over court opinions.
- Real time or streaming data ingestion.


## Dataset Sources

- Harvard's Caselaw Access Project: https://case.law/caselaw/?reporter=ga
- CourtListener: https://www.courtlistener.com/help/api/

---

## M2 Update — Pipeline Implementation

### Setup Instructions

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Environment setup**

Create a `.env` file in the project root by copying the provided template:

```bash
cp config/.env.example .env
```

Then open `.env` and fill in your credentials:

```
COURTLISTENER_API_TOKEN=your_token_here
```

A free CourtListener API token can be obtained by registering at:
https://www.courtlistener.com/register/

No API key is required for CAP bulk data. Download the Georgia Volume 1 zip from
https://static.case.law/ga/1.zip and unzip it so that the `json/` folder is located at:

```
data/raw/cap/json/
```

**3. How to run**

```bash
# Test API connection only
python main.py --mode test

# Ingest CourtListener federal court records
python main.py --mode cl

# Ingest CAP Georgia bulk records
python main.py --mode cap

# Run both sources in sequence
python main.py --mode full

# Verify all stored data reads back correctly
python main.py --mode verify
```

### Current Status (M2)

**Working:**
- CourtListener REST API ingestion - 600 records across `scotus`, `ca1`, `ca9` (2020–2024)
- CAP Georgia bulk ingestion - 93 state court records from Volume 1
- Schema normalization - both sources mapped to a shared 10 column schema
- Storage — partitioned Parquet, flat CSV, and SQLite all persisting correctly
- Verification - `--mode verify` confirms total rows read back from Parquet

---

## M3 Update — Complete Implementation

### What's New in M3

- **PySpark analytics** — distributed MapReduce aggregations, completeness statistics,
  case volume trends, and Spark SQL queries via `--mode spark`
- **CAP data scaled** — expanded from 93 records (1 volume) to 6,809 records (55 volumes),
  spanning Georgia Supreme Court cases from 1846 to 1876
- **Multi-volume loader** — `cap_loader.py` now scans all `vol{N}/json/` subdirectories
  automatically
- **Year field fix** — CAP partial dates (`YYYY-MM`) now parse correctly
- **Analytics outputs** — 10 CSV analytics files saved to `data/processed/analytics/`
- **Total dataset** — 7,409 records (6,809 CAP state + 600 CourtListener federal)

---

### M3 Prerequisites

M3 requires Python 3.11 and Java 17 specifically due to PySpark compatibility requirements.

#### Python 3.11

Download and install Python 3.11 from:
```
https://www.python.org/downloads/release/python-3110/
```

Verify installation:
```bash
py -3.11 --version
# Expected: Python 3.11.x
```

#### Java 17

Download and install Java 17 JDK from:
```
https://www.oracle.com/java/technologies/downloads/#java17-windows
```

Select the **x64 Installer** for Windows.

Set `JAVA_HOME` to Java 17 for the current session:
```bash
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17.0.18"
```

#### WinUtils (Windows only)

PySpark requires WinUtils to access the local filesystem on Windows.

**Step 1** — Create the WinUtils directory:
```bash
New-Item -ItemType Directory -Path "$HOME\winutils\bin" -Force
```

**Step 2** — Download WinUtils and Hadoop DLL:
```bash
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.5/bin/winutils.exe" -OutFile "$HOME\winutils\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/refs/heads/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile "$HOME\winutils\bin\hadoop.dll"
```

**Step 3** — Set `HADOOP_HOME` for the current session:
```bash
$env:HADOOP_HOME = "$HOME\winutils"
```

> **Note:** Set both `JAVA_HOME` and `HADOOP_HOME` at the start of each terminal session
> before running `--mode spark`. The pipeline code sets `JAVA_HOME` programmatically,
> but `HADOOP_HOME` must be set in the shell environment.

---

### M3 Setup Instructions

**1. Install dependencies using Python 3.11**

```bash
py -3.11 -m pip install -r requirements.txt
```

**2. Environment setup**

Create a `.env` file in the project root:

```bash
cp config/.env.example .env
```

Add your CourtListener API token:
```
COURTLISTENER_API_TOKEN=your_token_here
```

**3. Download and prepare CAP bulk data**

Download Georgia volumes and unzip them into the `data/raw/cap/` directory.

Volume 1 (original):
```bash
# Download from: https://static.case.law/ga/1.zip
# Unzip so that data/raw/cap/json/ contains the case JSON files
```

Additional volumes (M3 — volumes 2 through 55):
```bash
for ($i = 2; $i -le 55; $i++) {
    Invoke-WebRequest -Uri "https://static.case.law/ga/$i.zip" -OutFile "data/raw/cap/$i.zip"
    Expand-Archive -Path "data/raw/cap/$i.zip" -DestinationPath "data/raw/cap/vol$i" -Force
}
```

Expected directory structure after download:
```
data/raw/cap/
    json/           <- Volume 1 cases
    vol2/json/      <- Volume 2 cases
    vol3/json/      <- Volume 3 cases
    ...
    vol55/json/     <- Volume 55 cases
```

---

### M3 How to Run

> **Important:** Use `py -3.11` instead of `python` for all commands in M3.
> PySpark 3.5.3 requires Python 3.11 and will not work with Python 3.14+.

```bash
# Set environment variables (required once per terminal session)
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17.0.18"
$env:HADOOP_HOME = "$HOME\winutils"

# Test API connection
py -3.11 main.py --mode test

# Ingest CourtListener federal court records (600 records)
py -3.11 main.py --mode cl

# Ingest all CAP Georgia bulk records (6,809 records across 55 volumes)
py -3.11 main.py --mode cap

# Run both sources end-to-end (recommended)
py -3.11 main.py --mode full

# Verify stored data reads back correctly
py -3.11 main.py --mode verify

# Run distributed Spark analytics (MapReduce + Spark SQL)
py -3.11 main.py --mode spark
```

---

### M3 Current Status

**Pipeline:**
- End-to-end pipeline runs with single command sequence — no manual intervention required
- CourtListener ingestion — 600 federal court records (scotus, ca1, ca9), 2014–2016
- CAP bulk ingestion — 6,809 Georgia Supreme Court records across 55 volumes, 1846–1876
- Total dataset — 7,409 records unified into a single 10-column schema
- All three storage formats persisting correctly — Parquet, CSV, SQLite

**Spark Analytics (`--mode spark`):**
- Loads normalized Parquet into Spark DataFrame via pandas bridge
- MapReduce aggregations — case counts by court, jurisdiction, source, year
- Completeness statistics — 100% across all 10 schema fields on 9,795 records
- Case volume trends — peak filing years, year range summary per source
- Spark SQL queries — top courts, federal vs state coverage, cases per decade
- Analytics outputs saved to `data/processed/analytics/` as CSV files

**Stack concepts demonstrated:**
- Storage — partitioned Parquet by source/year/month
- Syntax — JSON ingestion → Parquet + CSV + SQLite output
- Data Models — unified schema enforced via Spark DataFrames
- Processing — MapReduce batch aggregations via PySpark groupBy
- Querying — Spark SQL over registered `court_metadata` temp view

---

### Repository Structure

```
Public-court-data-pipeline/
├── main.py                         # Pipeline entry point, all --mode options
├── config/
│   ├── settings.yaml               # Pipeline configuration
│   └── .env.example                # Environment variable template
├── src/
│   ├── ingestion/
│   │   ├── courtlistener_client.py # CourtListener REST API client
│   │   └── cap_loader.py           # CAP multi-volume bulk loader
│   ├── processing/
│   │   ├── normalizer.py           # Schema validation and summary stats
│   │   └── spark_analyzer.py       # PySpark analytics module (M3)
│   └── storage/
│       └── db_handler.py           # Normalization and storage handler
├── data/
│   ├── raw/
│   │   ├── courtlistener/          # Raw CourtListener JSONL files
│   │   └── cap/                    # CAP bulk JSON files (55 volumes)
│   └── processed/
│       ├── parquet/                # Partitioned Parquet dataset
│       ├── analytics/              # Spark analytics CSV outputs
│       └── court_metadata.db       # SQLite database
└── requirements.txt
```
