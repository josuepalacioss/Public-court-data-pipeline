"""
spark_analyzer.py
----------------------------------------------------------------------
PySpark analytics module for M3 Complete Implementation.
 
Loads normalized court metadata from Parquet, runs MapReduce-style
aggregations, case volume trends, and
Spark SQL queries. Saves all analytics outputs to data/processed/analytics/.
 
Usage:
    python main.py --mode spark
 
Direct usage:
    from src.processing.spark_analyzer import SparkAnalyzer
    analyzer = SparkAnalyzer()
    analyzer.run_all()
"""
 
import logging
import os
from pathlib import Path
 
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
 
logger = logging.getLogger(__name__)
 
 
class SparkAnalyzer:
    """
    Distributed analytics over normalized court metadata using PySpark.
 
    Stack layers demonstrated:
        - Storage:    Reads partitioned Parquet (source/year/month)
        - Data Model: Enforces schema via Spark DataFrame
        - Processing: MapReduce aggregations (groupBy = shuffle+reduce)
        - Querying:   Spark SQL over registered temp views
    """
 
    def __init__(self, config_path: str = "config/settings.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
 
        self.processed_dir = Path(self.config["storage"]["processed_dir"])
        self.parquet_dir = self.processed_dir / "parquet" / "court_metadata"
        self.analytics_dir = self.processed_dir / "analytics"
        self.analytics_dir.mkdir(parents=True, exist_ok=True)
 
        self.spark = self._build_session()
        self.df = None
 
    def _build_session(self) -> SparkSession:
        """
        Initialize a local Spark session with stable settings for Windows.
        Suppresses verbose Spark logs to keep pipeline output readable.
        """
        # Point PySpark at Java 17 if JAVA_HOME not already set
        import sys
        java_home = os.environ.get("JAVA_HOME", r"C:\Program Files\Java\jdk-17.0.18")
        os.environ["JAVA_HOME"] = java_home
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
 
        spark = (
            SparkSession.builder
            .appName("CourtDataPipeline-M3")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .getOrCreate()
        )
 
        # Suppress INFO/WARN logs from Spark internals
        spark.sparkContext.setLogLevel("ERROR")
        logger.info(f"Spark session started - version {spark.version}")
        return spark
 
    def load_parquet(self) -> None:
        """
        Load normalized court metadata into a Spark DataFrame.
        Uses pandas as a bridge to bypass Hadoop native IO on Windows.
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(
                f"No Parquet data found at {self.parquet_dir}.\n"
                "Run: py -3.11 main.py --mode full  first."
            )

        import pandas as pd
        pandas_df = pd.read_parquet(
            self.parquet_dir.resolve(),
            engine="pyarrow"
        )

        self.df = self.spark.createDataFrame(pandas_df)

        self.df = (
            self.df
            .withColumn("year", F.col("year").cast(IntegerType()))
            .withColumn("month", F.col("month").cast(IntegerType()))
        )

        self.df.createOrReplaceTempView("court_metadata")

        count = self.df.count()
        logger.info(f"Loaded {count} records into Spark DataFrame")
        print(f"\n  [Spark] Loaded {count} total records")
        print(f"  [Spark] Schema:")
        self.df.printSchema()
    
    def run_aggregations(self) -> dict:
        """
        MapReduce-style aggregations over court metadata.
 
        Map phase:    each record emits (key, 1) where key = grouping field
        Shuffle phase: Spark groups records by key across partitions
        Reduce phase:  count(), sum(), avg() collapse each group
 
        Returns dict of Spark DataFrames keyed by aggregation name.
        """
        logger.info("Running MapReduce aggregations...")
        print("\n── MapReduce Aggregations ──")
        results = {}
 
        # 1. Case counts by court (map by court, reduce by count)
        by_court = (
            self.df
            .groupBy("court", "jurisdiction")
            .agg(
                F.count("*").alias("case_count"),
                F.avg("document_count").alias("avg_docs"),
                F.countDistinct("year").alias("years_active")
            )
            .orderBy(F.desc("case_count"))
        )
        results["by_court"] = by_court
        print("\n  Case counts by court:")
        by_court.show(10, truncate=False)
 
        # 2. Case counts by jurisdiction (federal vs state)
        by_jurisdiction = (
            self.df
            .groupBy("jurisdiction")
            .agg(
                F.count("*").alias("case_count"),
                F.sum("document_count").alias("total_docs"),
                F.countDistinct("court").alias("court_count")
            )
            .orderBy(F.desc("case_count"))
        )
        results["by_jurisdiction"] = by_jurisdiction
        print("\n  Case counts by jurisdiction:")
        by_jurisdiction.show(truncate=False)
 
        # 3. Case counts by source
        by_source = (
            self.df
            .groupBy("source")
            .agg(F.count("*").alias("case_count"))
            .orderBy(F.desc("case_count"))
        )
        results["by_source"] = by_source
        print("\n  Case counts by source:")
        by_source.show(truncate=False)
 
        # 4. Case counts by year and source (time series)
        by_year = (
            self.df
            .filter(F.col("year").isNotNull() & (F.col("year") > 1800))
            .groupBy("year", "source")
            .agg(F.count("*").alias("case_count"))
            .orderBy("year", "source")
        )
        results["by_year"] = by_year
        print("\n  Case volume by year and source (sample):")
        by_year.show(20, truncate=False)
 
        return results
 
    def run_completeness(self) -> dict:
        """
        Compute null rates per field across all 10 schema columns.
        Demonstrates metadata quality analysis over the full dataset.
 
        Returns dict with field-level completeness statistics.
        """
        logger.info("Running completeness statistics...")
        print("\n── Completeness Statistics ──")
 
        total = self.df.count()
        schema_fields = [
            "case_id", "court", "jurisdiction", "filing_date",
            "decision_date", "case_name", "document_count",
            "source", "year", "month"
        ]
 
        completeness_rows = []
        for field in schema_fields:
            if field in self.df.columns:
                null_count = self.df.filter(
                    F.col(field).isNull() | (F.col(field).cast("string") == "")
                ).count()
                complete_count = total - null_count
                completeness_pct = round((complete_count / total) * 100, 2) if total > 0 else 0
                completeness_rows.append((field, total, complete_count, null_count, completeness_pct))
 
        completeness_df = self.spark.createDataFrame(
            completeness_rows,
            ["field", "total_records", "complete_count", "null_count", "completeness_pct"]
        ).orderBy(F.asc("completeness_pct"))
 
        print("\n  Field completeness across all records:")
        completeness_df.show(truncate=False)
 
        return {"completeness": completeness_df}
 
    def run_trends(self) -> dict:
        """
        Case volume trend analysis - records per year, per source.
        Identifies peak filing years and year-over-year patterns.
        """
        logger.info("Running case volume trend analysis...")
        print("\n── Case Volume Trends ──")
        results = {}
 
        # Peak years by source
        peak_years = (
            self.df
            .filter(F.col("year").isNotNull() & (F.col("year") > 1800))
            .groupBy("source", "year")
            .agg(F.count("*").alias("case_count"))
            .orderBy("source", F.desc("case_count"))
        )
        results["peak_years"] = peak_years
        print("\n  Peak filing years by source:")
        peak_years.show(10, truncate=False)
 
        # Year range summary per source
        year_range = (
            self.df
            .filter(F.col("year").isNotNull() & (F.col("year") > 1800))
            .groupBy("source")
            .agg(
                F.min("year").alias("earliest_year"),
                F.max("year").alias("latest_year"),
                F.count("*").alias("total_records"),
                F.countDistinct("year").alias("years_with_data")
            )
        )
        results["year_range"] = year_range
        print("\n  Year range summary by source:")
        year_range.show(truncate=False)
 
        return results
 
    def run_sql_queries(self) -> dict:
        """
        Spark SQL queries over the registered court_metadata temp view.
        Demonstrates distributed SQL execution over partitioned Parquet.
        """
        logger.info("Running Spark SQL queries...")
        print("\n── Spark SQL Queries ──")
        results = {}
 
        # Q1: Top 10 most active courts
        q1 = self.spark.sql("""
            SELECT
                court,
                jurisdiction,
                COUNT(*) AS case_count,
                MIN(year) AS first_year,
                MAX(year) AS last_year
            FROM court_metadata
            WHERE court IS NOT NULL AND court != ''
            GROUP BY court, jurisdiction
            ORDER BY case_count DESC
            LIMIT 10
        """)
        results["top_courts"] = q1
        print("\n  SQL Q1 - Top 10 most active courts:")
        q1.show(truncate=False)
 
        # Q2: Federal vs state coverage comparison
        q2 = self.spark.sql("""
            SELECT
                jurisdiction,
                source,
                COUNT(*) AS records,
                COUNT(DISTINCT court) AS unique_courts,
                ROUND(AVG(document_count), 2) AS avg_docs_per_case,
                SUM(CASE WHEN decision_date IS NULL OR decision_date = '' THEN 1 ELSE 0 END) AS missing_decision_date
            FROM court_metadata
            GROUP BY jurisdiction, source
            ORDER BY records DESC
        """)
        results["coverage_comparison"] = q2
        print("\n  SQL Q2 - Federal vs state coverage:")
        q2.show(truncate=False)
 
        # Q3: Records per decade
        q3 = self.spark.sql("""
            SELECT
                FLOOR(year / 10) * 10 AS decade,
                source,
                COUNT(*) AS case_count
            FROM court_metadata
            WHERE year IS NOT NULL AND year > 1800
            GROUP BY decade, source
            ORDER BY decade, source
        """)
        results["by_decade"] = q3
        print("\n  SQL Q3 - Cases per decade by source:")
        q3.show(20, truncate=False)
 
        return results
 
    def save_outputs(self, aggregations: dict, completeness: dict, trends: dict, sql_results: dict) -> None:
        """
        Persist all analytics results to data/processed/analytics/ as CSV.
        Uses pandas to write files, bypassing Hadoop native IO on Windows.
        """
        logger.info("Saving analytics outputs...")
        print("\n── Saving Analytics Outputs ──")

        all_results = {**aggregations, **completeness, **trends, **sql_results}

        for name, spark_df in all_results.items():
            out_path = self.analytics_dir / f"{name}.csv"
            try:
                pandas_df = spark_df.toPandas()
                pandas_df.to_csv(out_path, index=False)
                logger.info(f"Saved analytics/{name}.csv ({len(pandas_df)} rows)")
                print(f"  Saved: data/processed/analytics/{name}.csv ({len(pandas_df)} rows)")
            except Exception as e:
                logger.warning(f"Could not save {name}: {e}")
 
    def run_all(self) -> None:
        """
        Run the complete Spark analytics pipeline:
            1. Load Parquet into Spark DataFrame
            2. MapReduce aggregations
            3. Completeness statistics
            4. Case volume trends
            5. Spark SQL queries
            6. Save all outputs
        """
        print("\n══ Spark Analytics Pipeline ══")
        logger.info("Starting Spark analytics run")
 
        try:
            self.load_parquet()
            aggregations = self.run_aggregations()
            completeness = self.run_completeness()
            trends = self.run_trends()
            sql_results = self.run_sql_queries()
            self.save_outputs(aggregations, completeness, trends, sql_results)
 
            print("\n══ Spark analytics complete ══")
            logger.info("Spark analytics run complete")
 
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")
 