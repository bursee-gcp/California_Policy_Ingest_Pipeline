# California Policy Ingest Pipeline

This repository contains a standalone template for the end-to-end data ingestion pipeline that fetches, parses, and loads the California Legislative data into BigQuery.

## Overview

The pipeline runs as a **Google Cloud Run Job**. It downloads the daily legislative data dump (a ZIP file containing `.dat` files and an SQL schema script), converts the data into Parquet format, handles Large Object (LOB) inlining on-the-fly, stages the data in Google Cloud Storage (GCS), and loads it into BigQuery with explicit schema enforcement to maintain typing.

### Key Optimizations:
* **Streaming Extraction**: Processes files one at a time using local `/tmp` storage, immediately deleting processed files to stay below resource limits with zero Out-of-Memory (OOM) risks.
* **Explicit Schema Mapping**: Extracts definitions from `capublic.sql` to map robust types (e.g., `VARCHAR` -> `STRING`, `LONGBLOB` -> `BYTES`) rather than depending on BigQuery auto-detection.
* **Zero-FUSE Penalty**: Processes on ephemeral storage to avoid GCS FUSE latency overheads.

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
```

---

## Deployment Steps

### 1. Configure Variables
Create a `terraform.tfvars` file from the example format:
```bash
cp terraform.tfvars.example terraform.tfvars
```
Edit the `terraform.tfvars` to fill in your `project_id` and preferred `region`.

### 2. Deploy Infrastructure
Initialize and apply the Terraform configuration. This will enable APIs, create the staging bucket, dataset, and Artifact Registry repository before building the Docker image and deploying the Cloud Run job:

```bash
terraform init
terraform apply
```
*Note: The Docker image build is triggered automatically if the code hashes change, ensuring the Cloud Run job always deploys with the latest code.*

---

## Pipeline Operations

### Execute the Pipeline
Trigger the job manually from the GCP Console or via the CLI:

```bash
gcloud run jobs execute policy-agent-ingest --region us-central1
```

### Advanced Usage

#### Debugging with Limits
To test with small chunks (highly recommended for validation cycles), pass a limit parameter strictly enforcing row-limits:

```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--limit=200"
```

#### Custom Zip Source
By default, the script calculates and downloads the correct static yearly dump (e.g., `pubinfo_2026.zip`). You can override this to run historical data or point to a manual extraction link:

```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--zip-url=http://custom-url/pubinfo_2025.zip"
```

Or use a GCS staging address:
```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--zip-file=gs://your-bucket-name/backup.zip"
```

---

## Clean Up
To avoid incurring future charges for resources you no longer need, tear down the infrastructure using Terraform:

```bash
terraform destroy
```
*(Note: You may need to manually empty the GCS staging bucket or Artifact Registry repository if Terraform halts on deleting non-empty resources).*