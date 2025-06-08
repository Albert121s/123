from google.cloud import bigquery, storage
import os

project_id = "liquid-tractor-462013-t5"
dataset_id = "football_dataset"
bucket_name = "weather-json-data"
gcs_folder = "data/"

bq_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)
bucket = storage_client.get_bucket(bucket_name)

def load_csv_to_bigquery(blob_name):
    table_name = blob_name.replace(".csv", "").replace("/", "_")
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    uri = f"gs://{bucket_name}/{blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        # Sprawdź, czy plik istnieje w GCS
        if not storage.Blob(bucket=bucket, name=blob_name).exists(storage_client):
            print(f"⚠️ Plik nie istnieje w GCS: {blob_name}")
            return

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # czekaj na zakończenie
        print(f"✅ Załadowano do BigQuery: {table_id}")
    except Exception as e:
        print(f"❌ Błąd przy ładowaniu {blob_name}: {e}")

# Lista plików CSV w GCS/data/
blobs = list(bucket.list_blobs(prefix=gcs_folder))
csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]

# Ładuj każdy plik
for file in csv_files:
    load_csv_to_bigquery(file)
