variable "project_id" {
  description = "The GCP Project ID to deploy resources into."
  type        = string
}

variable "region" {
  description = "The GCP region for resources (e.g., us-central1)."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Suffix for the GCS staging bucket name to ensure uniqueness."
  type        = string
  default     = "staging"
}

variable "dataset_id" {
  description = "The BigQuery Dataset ID."
  type        = string
  default     = "cal_legislature_data"
}

variable "repository_name" {
  description = "The Artifact Registry repository name."
  type        = string
  default     = "policy-agent-repo"
}

variable "job_name" {
  description = "The name of the Cloud Run Job."
  type        = string
  default     = "policy-agent-ingest"
}
