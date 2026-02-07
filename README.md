# CS4265_Josue_Palacios_M1

**Big Data Pipeline for Public Court Document Analytics**


Project Overview
-------------------------------
Domain: Public court data analytics
This project focuses on processing the structured metadata contained within court cases
and legal documents. These public court datasets are readily available through APIs and
bulk archives, but they are often fragmented across institutions. By ingesting and analyzing
the document metadata we can extract courts, dates, case types, document counts, and
completeness indicators. This domain is motivated by the need for scalable infrastructure
to support legal research and analysis of judicial activity using large public datasets.



Problem Statement
Public court data exists on a large scale but is difficult to analyze efficiently due to the sheer
volume, heterogeneity, and different access constraints. Federal and state court datasets are
published by different organizations, through different access methods. The ones tackled here
include REST APIs provided by CourtListener and bulk archives provided by the Caselaw
Access Project by Harvard Law School. As a result, answering basic questions will require
great effort.

_This project aims to address the following questions:_

• How does the volume of court cases and documents change over time across jurisdic-
tions?

• How do federal and state court datasets differ in coverage and completeness?

• What trends exist in case counts, document counts, and court activity over time?

• How complete are metadata fields across sources?

_At scale, these questions introduce several challenges:_

• Data volume by the hundreds of thousands to over a million records and the resulting
space.

• Heterogeneous ingestion through combining incremental API based data with large
bulk datasets.

• Schema variability that offers differing field structures and naming across sources.


Scope

This project focuses on the design and implementation of a distributed, batch-oriented Big
Data pipeline for analyzing structured metadata derived from public court documents.

_In Scope_

Batch ingestion of heterogeneous public court datasets:

• Federal court metadata from the CourtListener REST API.

• State court metadata from the Harvard Caselaw Access Project (Georgia) bulk archives.

Schema normalization and validation to unify heterogeneous JSON sources into a shared
Parquet-based data model.

Metadata analytics

• Case and document volume trends over time.

• Court and jurisdiction activity summaries.

• Completeness and coverage statistics for key metadata fields.

Distributed query execution using Spark SQL with partitioned Parquet datasets.
Local execution with a cloud-ready design that supports scaling via additional partitions
and executors.

_Out of Scope_

• Parsing, indexing, or analyzing the contents of court case PDF documents.

• Natural language processing, machine learning, or data mining.

• Legal interpretation or reasoning over court opinions.

• Real time or streaming data ingestion.

Dataset Source
-------------------------------------------------------
Harvard's Caselaw Access Project: https://case.law/caselaw/?reporter=ga

CourtListener: https://www.courtlistener.com/help/api/
