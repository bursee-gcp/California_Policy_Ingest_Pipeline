output "job_name" {
  description = "The name of the Cloud Run Job"
  value       = google_cloud_run_v2_job.ingest_job.name
}

output "bucket_name" {
  description = "The GCS bucket name for staging"
  value       = google_storage_bucket.staging_bucket.name
}

output "dataset_id" {
  description = "The BigQuery Dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}
