# PolicyAgent Ingestion Pipeline Template

This repository contains a standalone template for the end-to-end data ingestion pipeline that fetches, parses, and loads the California Legislative data into BigQuery.

## Overview

The pipeline runs as a **Google Cloud Run Job**. It downloads the daily legislative data dump (a ZIP file containing `.dat` files and an SQL schema script), converts the data into Parquet format, handles Large Object (LOB) inlining for bill text XML, stages the data in Google Cloud Storage (GCS), and loads it into BigQuery.

### Assets Included:
- `ingest.py`: The core ingestion Python script.
- `capublic.sql`: The schema mapping tool to correctly type BigQuery columns based on the original MySQL dump.
- `Dockerfile`: Container definition for Cloud Run.
- `requirements.txt`: Python package dependencies.
- `env.yaml`: Environment variable configuration for deployment.

---

## Prerequisites

Before deploying the pipeline, ensure the following Google Cloud resources exist:

1.  **GCP Project**: An active Google Cloud Project with billing enabled.
2.  **Enabled APIs**:
    ```bash
    gcloud services enable run.googleapis.com storage.googleapis.com bigquery.googleapis.com artifactregistry.googleapis.com
    ```
3.  **BigQuery Dataset**: Create a dataset (e.g., `cal_legislature_data`).
    ```bash
    bq mk --dataset your-gcp-project-id:cal_legislature_data
    ```
4.  **Cloud Storage Bucket**: Create a staging bucket.
    ```bash
    gcloud storage buckets create gs://your-gcs-staging-bucket-name
    ```
5.  **Artifact Registry repository**: Create a Docker repository for the container image.
    ```bash
    gcloud artifacts repositories create policy-agent-repo --repository-format=docker --location=us-central1
    ```
6.  **Service Account**: Create a service account with `roles/bigquery.dataEditor` and `roles/storage.objectAdmin` to run the job.

---

## Deployment Steps

### 1. Configure Environment Variables
Edit the `env.yaml` file to include your specific project details:
```yaml
GCP_PROJECT: "your-gcp-project-id"
GCS_BUCKET: "your-gcs-staging-bucket-name"
```

### 2. Build and Push the Docker Image
```bash
# Update the region and project ID below
IMAGE_URI="us-central1-docker.pkg.dev/your-gcp-project-id/policy-agent-repo/ingest:latest"

gcloud builds submit --tag $IMAGE_URI .
```

### 3. Deploy the Cloud Run Job
Deploy the job using the built image and the `env.yaml` file:

```bash
gcloud run jobs create policy-agent-ingest \
    --image $IMAGE_URI \
    --env-vars-file env.yaml \
    --region us-central1 \
    --task-timeout 60m \
    --memory 4Gi \
    --cpu 2 \
    --service-account your-service-account@your-gcp-project-id.iam.gserviceaccount.com
```
*Note: The 60-minute timeout and 4Gi memory are required due to the size of the initial ZIP file download (~680MB) and the intensive padding/LOB inlining process.*

### 4. Execute the Pipeline
You can trigger the job manually from the GCP Console or via the CLI:
```bash
gcloud run jobs execute policy-agent-ingest --region us-central1
```

## Advanced Operation

### Direct URL vs ZIP Upload
By default, the script downloads directly from `http://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip`.

If you experience slow download speeds from the state server, you can manually upload the ZIP to your GCS bucket and override the argument when executing the job:
```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--zip-file=gs://your-gcs-staging-bucket-name/pubinfo_2025.zip"
```

### Debugging with a Limit
When testing, you can pass a `--limit` argument to restrict the number of rows processed per table:
```bash
gcloud run jobs execute policy-agent-ingest \
    --region us-central1 \
    --args="--limit=200"
```
