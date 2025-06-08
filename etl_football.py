import os
import re
import pandas as pd
import requests
from io import StringIO
from google.cloud import storage, bigquery

# ---- KONFIGURACJA ----
PROJECT_ID = "liquid-tractor-462013-t5"
BUCKET_NAME = "weather-json-data"
GCS_FOLDER = "data/"
BQ_DATASET = "football_dataset"
LOCAL_FOLDER = "data"

# Generuj sezony od 2003/2004 do 2023/2024
seasons = [f"{str(y).zfill(2)}{str(y+1)[-2:]}" for y in range(3, 24)]

leagues = {
    'E0': 'Premier League',
    'D1': 'Bundesliga',
    'I1': 'Serie A',
    'SP1': 'La Liga',
    'F1': 'Ligue 1'
}

# ---- INICJALIZACJA ----
os.makedirs(LOCAL_FOLDER, exist_ok=True)
storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

# ---- FUNKCJE POMOCNICZE ----
def sanitize_column_names(columns):
    seen = {}
    new_cols = []
    for col in columns:
        clean = re.sub(r'[^\w]', '_', col)
        # Jeśli już było takie pole — dodaj numer
        if clean in seen:
            seen[clean] += 1
            clean += f"_{seen[clean]}"
        else:
            seen[clean] = 0
        new_cols.append(clean)
    return new_cols

def download_and_clean_csv(url):
    r = requests.get(url)
    r.raise_for_status()
    try:
        # Standardowe wczytanie CSV
        df = pd.read_csv(StringIO(r.text), engine="python", on_bad_lines="skip")
    except Exception as e:
        raise Exception(f"Nie można sparsować pliku: {e}")
    
    df.columns = sanitize_column_names(df.columns)
    return df


def save_to_local(df, filename):
    path = os.path.join(LOCAL_FOLDER, filename)
    df.to_csv(path, index=False)
    return path

def upload_to_gcs(local_path, blob_name):
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"✅ Załadowano do GCS: {blob_name}")

def load_to_bigquery(blob_name):
    table_name = blob_name.replace(".csv", "").replace("/", "_")
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    uri = f"gs://{BUCKET_NAME}/{blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        encoding="UTF-8",
        allow_quoted_newlines=True,
        allow_jagged_rows=True
    )

    try:
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        print(f"📊 Załadowano do BigQuery: {table_id}")
    except Exception as e:
        print(f"❌ Błąd przy ładowaniu {blob_name}: {e}")

# ---- GŁÓWNA PĘTLA ----
for season in seasons:
    for code in leagues:
        url = f"https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"
        filename = f"{code}_{season}.csv"
        blob_name = GCS_FOLDER + filename

        try:
            df = download_and_clean_csv(url)
            local_path = save_to_local(df, filename)
            upload_to_gcs(local_path, blob_name)
            load_to_bigquery(blob_name)
        except Exception as e:
            print(f"⚠️ Pomiń {filename} – {e}")
