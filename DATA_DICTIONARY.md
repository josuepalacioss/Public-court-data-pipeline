# Data Dictionary

**Project:** Public Court Data Pipeline    
**Author:** Josue Palacios  


---

## Overview

All ingested court records are normalized into a unified
10-column schema before storage. This schema is enforced at ingestion time by
`StorageHandler` which is located in `src/storage/db_handler.py` and persists across three storage
formats: partitioned Parquet, flat CSV, and SQLite.

**Total records (M3):** 7,409  
**Sources:** CourtListener (federal) + Caselaw Access Project (state)  
**Storage location:** `data/processed/parquet/court_metadata/`

---

## Unified Schema

### `case_id`
| Attribute | Value |
|---|---|
| Type | string |
| Nullable | No |
| Source (CourtListener) | `id` field from the clusters endpoint |
| Source (CAP) | `id` field from the case JSON |
| Description | Unique identifier for each court case record. Sourced directly from the originating API or bulk file. Cast to string to unify integer IDs from CourtListener with string IDs from CAP. |
| Example (CourtListener) | `"1373795"` |
| Example (CAP) | `"2394801"` |
| Constraints | Non-null, non-empty. Uniqueness not enforced across sources - IDs are unique within each source but may overlap across sources. |

---

### `court`
| Attribute | Value |
|---|---|
| Type | string |
| Nullable | No |
| Source (CourtListener) | `_source_court` tag added during ingestion (e.g. `"scotus"`, `"ca1"`, `"ca9"`) |
| Source (CAP) | `court.slug` or `court.name` from the case JSON |
| Description | Name or identifier of the court that issued the case. CourtListener uses short slugs; CAP uses full court names. No normalization is applied between the two - the original identifier is preserved. |
| Example (CourtListener) | `"scotus"`, `"ca1"`, `"ca9"` |
| Example (CAP) | `"Supreme Court of Georgia"` |
| Constraints | Non-null. Values are source-dependent and not cross-normalized. |

---

### `jurisdiction`
| Attribute | Value |
|---|---|
| Type | string |
| Nullable | No |
| Source (CourtListener) | Hardcoded as `"federal"` for all CourtListener records |
| Source (CAP) | Hardcoded as `"state"` for all CAP records |
| Description | Indicates whether the case belongs to the federal or state court system. Assigned at ingestion time based on the data source rather than extracted from the record itself. |
| Allowed values | `"federal"`, `"state"` |
| Constraints | Non-null. Binary classification - all records belong to one of two jurisdictions. |

---

### `filing_date`
| Attribute | Value |
|---|---|
| Type | string (ISO date format) |
| Nullable | Yes |
| Source (CourtListener) | `date_created` field, truncated to `YYYY-MM-DD` |
| Source (CAP) | `decision_date` field (CAP does not provide a separate filing date) |
| Description | The date the case was filed or created. For CAP records, `filing_date` and `decision_date` are set to the same value since CAP only provides a single date field. Stored as an ISO date string rather than a date object for cross-format compatibility. |
| Format | `YYYY-MM-DD` |
| Example | `"2014-09-15"`, `"1846-11-01"` |
| Constraints | May be null if the source record has no date. Partial dates (`YYYY-MM`) are expanded to `YYYY-MM-01`. |

---

### `decision_date`
| Attribute | Value |
|---|---|
| Type | string (ISO date format) |
| Nullable | Yes |
| Source (CourtListener) | `date_filed` field, truncated to `YYYY-MM-DD` |
| Source (CAP) | `decision_date` field |
| Description | The date the court issued its decision. For CourtListener, this is the filing date of the opinion. For CAP, this is the primary date field available in the bulk data. Stored as a string for cross-format compatibility. |
| Format | `YYYY-MM-DD` |
| Example | `"2015-03-22"`, `"1855-06-01"` |
| Constraints | May be null. Partial dates expanded same as `filing_date`. |

---

### `case_name`
| Attribute | Value |
|---|---|
| Type | string |
| Nullable | Yes |
| Source (CourtListener) | `case_name` field from the clusters endpoint |
| Source (CAP) | `name` field from the case JSON |
| Description | The full name of the case as provided by the source. No normalization or standardization is applied - the original case name is preserved verbatim. |
| Example (CourtListener) | `"Smith v. United States"` |
| Example (CAP) | `"Luke Robinson, plaintiff in error, vs. The State of Georgia, defendant in error"` |
| Constraints | May be null or empty string if the source record has no case name. |

---

### `document_count`
| Attribute | Value |
|---|---|
| Type | integer |
| Nullable | No |
| Source (CourtListener) | Hardcoded as `1` per cluster record |
| Source (CAP) | `len(opinions)` from the case JSON, defaulting to `1` if no opinions list present |
| Description | The number of documents associated with the case. CourtListener returns cluster-level metadata rather than individual documents, so each cluster counts as 1. CAP records include an opinions list; the count reflects the number of opinion documents attached to the case. |
| Example | `1`, `2`, `3` |
| Constraints | Non-null. Minimum value of 1. |

---

### `source`
| Attribute | Value |
|---|---|
| Type | string |
| Nullable | No |
| Source (CourtListener) | Hardcoded as `"courtlistener"` |
| Source (CAP) | Hardcoded as `"cap"` |
| Description | Identifies the originating data source for each record. Used as a partition column in the Parquet dataset, enabling source-specific queries and filtering without full dataset scans. |
| Allowed values | `"courtlistener"`, `"cap"` |
| Constraints | Non-null. Used as a Parquet partition column - all records must have a valid source value. |

---

### `year`
| Attribute | Value |
|---|---|
| Type | integer |
| Nullable | Yes |
| Derived from | `filing_date` (4-digit year component) |
| Description | The year extracted from `filing_date`. Used as a partition column in the Parquet dataset for time-based filtering and trend analysis. For CAP records with partial dates (`YYYY-MM`), the year is extracted correctly from the first 4 characters. |
| Example | `1846`, `2014`, `2016` |
| Constraints | May be null if `filing_date` is null or unparseable. Used as a Parquet partition column. Values range from 1846 (earliest CAP record) to 2016 (latest CourtListener record). |

---

### `month`
| Attribute | Value |
|---|---|
| Type | integer |
| Nullable | Yes |
| Derived from | `filing_date` (2-digit month component) |
| Description | The month extracted from `filing_date`. Used alongside `year` as a partition column in the Parquet dataset. Enables monthly trend analysis and fine-grained time filtering. Defaults to `1` if only a year is available. |
| Example | `1`, `6`, `11` |
| Constraints | May be null if `filing_date` is null. Integer range 1–12. Used as a Parquet partition column. |

---

## Partition Strategy

The Parquet dataset is partitioned by three columns in this order:

```
data/processed/parquet/court_metadata/
    source=cap/
        year=1846/
            month=11/
                *.parquet
    source=courtlistener/
        year=2014/
            month=1/
                *.parquet
```

**Rationale:** Partitioning by `source` first enables source-specific queries to skip
irrelevant partitions entirely. Partitioning by `year` and `month` within each source
supports time-range queries and trend analysis with predicate pushdown, significantly
reducing I/O costs at scale.



