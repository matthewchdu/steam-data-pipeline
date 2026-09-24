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

    #Deletes after set lifecycle days
    lifecycle_rule {
      condition {
        age = var.lifecycle_days
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

resource "google_project_iam_member" "bigquery_read_session" {
  project = var.project_name
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.account.email}"
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
resource "google_project_iam_member" "dataproc_worker" {
  #Execute Spark jobs
  project = var.project_name
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.account.email}"
}

resource "google_project_iam_member" "dataproc_admin" {
  # Allows the service account to create and delete clusters
  project = var.project_name
  role    = "roles/dataproc.admin"
  member  = "serviceAccount:${google_service_account.account.email}"
}

resource "google_project_iam_member" "service_account_user" {
  # Allows Kestra to attach service account to Dataproc
  project = var.project_name
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.account.email}"
}

resource "google_project_iam_member" "compute_admin" {
  # Allows dataproc to provision and delete clusters
  project = var.project_name
  role    = "roles/compute.admin"
  member  = "serviceAccount:${google_service_account.account.email}"
}

resource "google_service_account_key" "kestra_sa_key" {
  # Creates a service account key
  service_account_id = google_service_account.account.name
}

resource "local_file" "service_account_json" {
  # Converts key into base64
  content  = base64decode(google_service_account_key.kestra_sa_key.private_key)
  # Creates Service Account Key File in root
  filename = "${path.module}/../SERVICE_ACC_KEY.json"
}

#Provisions a Postgre DB in GCP 
resource "google_sql_database_instance" "postgres" {
  name             = "steam-postgres"
  database_version = "POSTGRES_14"
  region           = var.region
  
  deletion_protection = false 

  settings {
    tier = "db-custom-1-4096"

    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "allow-all"
        value = "0.0.0.0/0"
      }
    }
  }
}

resource "google_sql_database" "steam_metadata" {
  name     = "steam_metadata"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "users" {
  name     = "postgres"
  instance = google_sql_database_instance.postgres.name
  password = "root"
}

output "cloud_sql_ip" {
  value       = google_sql_database_instance.postgres.public_ip_address
  description = "The public IP address of the Cloud PostgreSQL DB"
}

# Collects project ID, bucket name, postgre SQL IP, and region into a config file
resource "local_file" "pipeline_config" {
  content = jsonencode({
    project_id   = var.project_name
    bucket_name  = google_storage_bucket.steam_data_bucket.name
    cloud_sql_ip = google_sql_database_instance.postgres.public_ip_address
    region       = var.region
  })
  filename = "${path.module}/../config.json"
}

#Upload flatten_reviews to scripts in the bucket
resource "google_storage_bucket_object" "pyspark_script" {
  name   = "scripts/flatten_reviews.py"
  bucket = google_storage_bucket.steam_data_bucket.name
  source = "${path.module}/../spark/flatten_reviews.py"
}

resource "google_project_service" "apis" {
  # enables APIs
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "storage.googleapis.com",
    "dataproc.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com"
  ])

  project = var.project_name
  service = each.value

  #prevents terraform destroy from shutting off APIs 
  disable_on_destroy = false

}