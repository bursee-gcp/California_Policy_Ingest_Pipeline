# California Policy Ingest Pipeline

This repository contains a standalone template for the end-to-end data ingestion pipeline that fetches, parses, and loads the California Legislative data into BigQuery.

## Overview

The pipeline runs as a **Google Cloud Run Job**. It downloads the daily legislative data dump (a ZIP file containing `.dat` files and an SQL schema script), converts the data into Parquet format, handles Large Object (LOB) inlining on-the-fly, stages the data in Google Cloud Storage (GCS), and loads it into BigQuery with explicit schema enforcement to maintain typing.

### Key Optimizations:
*   **Streaming Extraction**: Processes files one at a time using local `/tmp` storage, immediately deleting processed files to stay below resource limits with zero Out-of-Memory (OOM) risks.
*   **Explicit Schema Mapping**: Extracts definitions from `capublic.sql` to map robust types (e.g., `VARCHAR` -> `STRING`, `LONGBLOB` -> `BYTES`) rather than depending on BigQuery auto-detection.
*   **Zero-FUSE Penalty**: Processes on ephemeral storage to avoid GCS FUSE latency overheads.

---

## Prerequisites

Before deploying the pipeline, ensure you have:
1.  **GCP Project**: An active Google Cloud Project with billing enabled.
2.  **Terraform**: Installed on your local machine.
3.  **Authenticated gcloud CLI**: Standard Google Cloud SDK initialized. 

**CRITICAL:** Because Terraform will build and push the Docker image from your local machine, you must authenticate your environment first:
```bash
gcloud auth login
gcloud auth application-default login