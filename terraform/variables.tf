variable "region" {
  description = "Region, pick which one is best for you"
  type = string
  default = "europe-west2"
}
variable "project_name" {
  description = "Use the name of your GCP project here"
  type = string
  default = "steam-data-pipeline-507614"
}



variable "bq_bronze_dataset" {
  description = "The name of the dataset containing raw and intermediate data"
  type = string
  default = "bronze_dataset"
}

variable "bq_prod_dataset" {
  description = "The name of the dataset containing fully transformed data ready for analysis"
  type = string
  default = "prod_dataset"
}

variable "service_account_id" {
  description = "username, set to any valid username"
  default = "steam-serv-acc"
}