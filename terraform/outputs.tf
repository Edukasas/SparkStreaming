output "storage_bucket_name" {
	description = "The name of created bucket"
	value = google_storage_bucket.storage_bucket.name
}

output "service_account_name" {
	description = "The name of the created service account"
	value = google_service_account.service_account.email
}