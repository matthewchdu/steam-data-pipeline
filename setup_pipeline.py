import json
import requests
import time
import os
from google.cloud import storage

def main():

#Gets Config Variables
    print("Reading config file from Terraform")
    #Reading Config
    with open("config.json", "r") as f:
        config = json.load(f)

    #Reading Service Account Key
    with open("SERVICE_ACC_KEY.json", "r") as f:
        key = f.read()

# Uploads raw steam_reviews.json.gz to the bucket
    print("Uploading steam_reviews.json.gz")
    #Gets Bucket
    client = storage.Client.from_service_account_info(json.loads(key))
    bucket = client.bucket(config["bucket_name"])
    reviews_path = "raw/steam_reviews.json.gz"

    #Uploads review file to the bucket
    if os.path.exists(reviews_path):
        blob = bucket.blob(reviews_path)
        if not blob.exists():
            blob.upload_from_filename(reviews_path)
            print("Uploaded steam_reviews.json.gz")
    else:
        print(f"{reviews_path} does not exist, please download the reviews file")

    # Update dbt sources
    sources_file = "steam_analytics/models/staging/sources.yaml"
    if os.path.exists(sources_file):
        print("Configuring dbt sources.yaml...")
        with open(sources_file, "r") as f:
            sources_content = f.read()

        sources_content = sources_content.replace("REPLACE_WITH_YOUR_PROJECT_ID", config["project_id"])

        with open(sources_file, "w") as f:
            f.write(sources_content)
# Configuring Kestra Flow variables and then uploading it to Kestra
    print("Configuring Kestra Flow variables")
    # Get File
    with open("flows/dev.steam_data_pipeline.yaml", "r") as f:
        flow = f.read()

    #Replace all variables
    flow = flow.replace("REPLACE_WITH_YOUR_PROJECT_ID", config["project_id"])
    flow = flow.replace("REPLACE_WITH_YOUR_BUCKET_NAME", config["bucket_name"])
    flow = flow.replace("REPLACE_WITH_CLOUD_SQL_IP", config["cloud_sql_ip"])

    #Commit Changes
    with open("flows/dev.steam_data_pipeline.yaml", "w") as f:
        f.write(flow)


    #Upload Flow to Kestra using REST API
    time.sleep(5)
    kestra_upload = requests.post(
        "http://localhost:8080/api/v1/main/flows",
        headers={"Content-Type": "application/x-yaml"},
        data=flow
    )
    print(f"Flow Registration Status: {kestra_upload.status_code}")

if __name__ == "__main__":
    main()
    print("Pipeline setup complete!")