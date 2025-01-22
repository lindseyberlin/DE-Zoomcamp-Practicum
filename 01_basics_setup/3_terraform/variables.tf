variable "credentials" {
  description = "Service credentials"
  default     = "/home/slytherinds/Documents/code/practicum-de-zoomcamp/keys/creds.json"
}

variable "project" {
  description = "Project"
  default     = "carbon-pride-446318-e0"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "carbon-pride-446318-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

