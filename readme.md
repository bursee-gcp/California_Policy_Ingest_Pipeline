# California Policy Ingest Pipeline

This repository contains a standalone template for the end-to-end data ingestion pipeline that fetches, parses, and loads the California Legislative data into BigQuery.

## Overview

The pipeline runs as a single **Google Cloud Run Job** designed to handle both dynamic daily current-year syncs and massive historical backfills safely via **Idempotent Partitioning**. 

During operation, every parsed record is automatically tagged with a dynamic `session_year` column based on the source archive. Instead of performing full, destructive table truncations, the pipeline relies on a precise pre-load SQL deletion to wipe the target session year, followed seamlessly by a `WRITE_APPEND` load operation. This modern architecture allows the pipeline to safely replace existing partitions without touching historical records.

---

## Prerequisites & Deployment

Before deploying the pipeline, ensure you have:
1.  **GCP Project**: An active Google Cloud Project with billing enabled.
2.  **Terraform**: Installed on your local machine.
3.  **Authenticated gcloud CLI**: Standard Google Cloud SDK initialized. 

**CRITICAL:** Because Terraform will build and push the Docker image from your local machine, you must authenticate your environment first:
```bash
gcloud auth login
gcloud auth application-default login
```

Once authenticated, deploy the entire pipeline infrastructure safely using Terraform:
```bash
terraform init
terraform apply -auto-approve
```

---

## ⚠️ CRITICAL DEPLOYMENT NOTE

> [!WARNING]
> **Schema Incompatibility Alert:** If you have run previous versions of this pipeline template, you **MUST** manually delete your existing `cal_legislature_data` dataset from BigQuery before executing the backfill or daily syncs.
> 
> The newly introduced Idempotent Partitioning architecture enforces a required `session_year` column that will cause down-stream `400 Errors (Schema Mismatch)` if the pipeline attempts to append or pre-delete from the older table schemas.

---

## Operation 1: The Daily Sync (Current Year)

To execute a standard daily sync pulling the latest current-year legislative activity, simply trigger the Cloud Run job directly:

```bash
gcloud run jobs execute policy-agent-ingest --region us-central1
```

This command defaults to fetching the current year's dataset. It will precisely prune today's existing year records in BigQuery and insert the updated dataset without disrupting the historical archive.

---

## Operation 2: The Historical Backfill (1989-2024)

To perform a massive historical data synchronization covering 36 years of California policy activity, execute the included bash orchestration script:

```bash
./backfill.sh
```

This script leverages Cloud Run's concurrency, dispatching 36 standalone container invocations utilizing the `--async` pattern to process decades of text and file artifacts in parallel. Because of the pipeline's idempotent design, you can also manually re-run specific historical years to fix state without data duplication by using argument overrides:

```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--zip-url=http://downloads.leginfo.legislature.ca.gov/pubinfo_1994.zip"
```