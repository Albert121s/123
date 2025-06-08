from google.cloud import storage
import os

BUCKET_NAME = "weather-json-data"
UPLOAD_FOLDER = "data/"
LOCAL_FOLDER = "data"

def upload_files_to_gcs():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)

    for filename in os.listdir(LOCAL_FOLDER):
        if filename.endswith(".csv"):
            local_path = os.path.join(LOCAL_FOLDER, filename)
            blob_path = UPLOAD_FOLDER + filename
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"✅ Załadowano: {blob_path}")

if __name__ == "__main__":
    upload_files_to_gcs()
