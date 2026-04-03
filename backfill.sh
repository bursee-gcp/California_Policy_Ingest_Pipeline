#!/bin/bash

# backfill.sh
# Orchestrates the massive historical load of California Legislative Data (1989-2024).
# Leverages Cloud Run's massive concurrency via --async to load 36 years of data in minutes.

echo "Starting massive historical backfill orchestration (1989-2024)..."

for YEAR in {1989..2024}; do
    echo "Triggering Cloud Run ingestion job for session year: ${YEAR}"
    
    gcloud run jobs execute policy-agent-ingest \
        --region us-central1 \
        --args="--zip-url=http://downloads.leginfo.legislature.ca.gov/pubinfo_${YEAR}.zip" \
        --async
done

echo "All historical containers successfully dispatched! Review execution progress in the Google Cloud console."
