terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Enable Required APIs
resource "google_project_service" "run_api" {
  service                    = "run.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "storage_api" {
  service                    = "storage.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "bigquery_api" {
  service                    = "bigquery.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "artifactregistry_api" {
  service                    = "artifactregistry.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "cloudbuild_api" {
  service                    = "cloudbuild.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "iam_api" {
  service                    = "iam.googleapis.com"
  disable_dependent_services = true
}

# 2. Create Cloud Storage Bucket for Staging
resource "google_storage_bucket" "staging_bucket" {
  name          = "${var.project_id}-${var.bucket_name}"
  location      = var.region
  force_destroy = true # Safe for templates/staging, maybe false for prod

  uniform_bucket_level_access = true

  depends_on = [google_project_service.storage_api]
}

# 3. Create BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.dataset_id
  location   = var.region

  delete_contents_on_destroy = true # Safe for templates/staging

  depends_on = [google_project_service.bigquery_api]
}

# 4. Create Artifact Registry Repository
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repository_name
  description   = "Docker repository for Ingest Pipeline"
  format        = "DOCKER"

  depends_on = [google_project_service.artifactregistry_api]
}

# 5. Create Dedicated Service Account
resource "google_service_account" "job_sa" {
  account_id   = "${var.job_name}-sa"
  display_name = "Service Account for Cloud Run Job"

  depends_on = [google_project_service.iam_api]
}

# IAM Permissions for Service Account
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.job_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.job_sa.email}"
}

# 6. Docker Build (null_resource)
resource "null_resource" "docker_build" {
  triggers = {
    # Hash source files to only rebuild on change
    src_hash = sha1(join("", [
      for f in fileset(path.module, "{ingest.py,Dockerfile,requirements.txt,capublic.sql}") :
      filesha1("${path.module}/${f}")
    ]))
  }

  provisioner "local-exec" {
    command = "gcloud builds submit --tag ${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/ingest:latest ${path.module}"
  }

  depends_on = [
    google_artifact_registry_repository.repo,
    google_project_service.cloudbuild_api
  ]
}

# 7. Cloud Run v2 Job
resource "google_cloud_run_v2_job" "ingest_job" {
  name     = var.job_name
  location = var.region

  template {
    template {
      max_retries = 1
      timeout     = "3600s" # 60m

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/ingest:latest"

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }

        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.staging_bucket.name
        }
      }

      # Explicitly use Generation 2
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
      service_account      = google_service_account.job_sa.email
    }
  }

  # Beta is required for some features, usually standard for jobs
  launch_stage = "BETA"

  depends_on = [
    null_resource.docker_build,
    google_project_service.run_api
  ]
}
