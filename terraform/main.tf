resource "google_storage_bucket" "steam_data_bucket" {
    name = "${var.project_name}-steam-data-bucket"
    project = var.project_name
    location = var.region
    storage_class = "STANDARD"
    
    #Manage access to everything using IAM at bucket level
    uniform_bucket_level_access = true

    #Deletes everything no matter what's in it
    force_destroy = true

    #Security, prevents public access to my bucket
    public_access_prevention = "enforced"

    #Deletes after 30 days
    lifecycle_rule {
      condition {
        age = 30
        matches_prefix = ["temp/"]
      }
      action {
        type = "Delete"
      }
    }

}

resource "google_bigquery_dataset" "steam_bronze" {
  dataset_id = var.bq_bronze_dataset
  location = var.region
  project = var.project_name

  friendly_name = "steam_bronze"
  description = "Contains the raw data, cleaned and staged with PySpark"

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "steam_gold" {
  dataset_id = var.bq_prod_dataset
  location = var.region
  project = var.project_name

  friendly_name = "steam_gold"
  description = "Contains the transformed data ready for analysis"

  delete_contents_on_destroy = true
}

resource "google_service_account" "account" {
    account_id = var.service_account_id
    display_name = "service_account"
    description = "Service account used by the pipeline"
    project = var.project_name
  
}

resource "google_project_iam_member" "project_iam" {
  # gives the project permission to use bigquery
  project = var.project_name
  role = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.account.email}"
  
}

resource "google_storage_bucket_iam_member" "bucket_iam" {
  # READ/WRITE/DELETE access for the data bucket
  # needed for ingestion scripts and pyspark for raw .json and temporary partitions
  bucket = google_storage_bucket.steam_data_bucket.name
  role = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.account.email}"
}

resource "google_bigquery_dataset_iam_member" "bigquery_iam_bronze" {
  # read/write in bronze dataset
  # allows pyspark to create and overwrite raw tables
  project = var.project_name
  dataset_id = google_bigquery_dataset.steam_bronze.dataset_id
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.account.email}"
  
}
resource "google_bigquery_dataset_iam_member" "bigquery_iam_prod" {
  # read/write for gold production dataset
  # allows dbt to manipulate and use the data
  project = var.project_name
  dataset_id = google_bigquery_dataset.steam_gold.dataset_id
  role = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.account.email}"
  
}

resource "google_project_service" "apis" {
  # enables APIs
  for_each = toset([
    "bigquery.googleapis.com",
    
    "storage.googleapis.com",
    "dataproc.googleapis.com"
  ])

  project = var.project_name
  service = each.value

  #prevents terraform destroy from shutting off APIs 
  disable_on_destroy = false

}