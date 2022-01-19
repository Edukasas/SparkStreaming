terraform {
	backend "gcs" {}
}

provider "google" {
	project = var.project
	region = var.region
	zone = var.zone
}

provider "random" {}

resource "random_pet" "random_suffix" {
	keepers = {
		project = var.project
		region = var.region
		zone = var.zone
	}
}

resource "google_storage_bucket" "storage_bucket" {
	name = "storage-bucket-${random_pet.random_suffix.id}"
	location = var.location
	force_destroy = false
	storage_class = "STANDARD"
}

resource "google_service_account" "service_account" {
    account_id = "gcs-${random_pet.random_suffix.id}-rw"
    display_name = "Service Account for RW"
}

resource "google_storage_bucket_iam_binding" "binding" {
  bucket = "storage-bucket-${random_pet.random_suffix.id}"
  role = "roles/storage.admin"
  members = [
    "serviceAccount:${google_service_account.service_account.email}",
  ]
}