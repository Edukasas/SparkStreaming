import os
import time
from google.cloud import storage

client = storage.Client()
bucket = client.bucket('storage-bucket-proud-cattle')

local_directory = r'C:\Users\Edas\Downloads\m13sparkstreaming\m13sparkstreaming\hotel-weather'

def upload_folder_to_gcs(local_folder_path):
    """Uploads all files from a local folder to a GCS folder path."""
    for root, _, files in os.walk(local_folder_path):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            
            relative_path = os.path.relpath(local_file_path, local_directory)
            blob_path = os.path.join("hotel-weather", relative_path).replace("\\", "/")

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded: {local_file_path} to {blob_path}")

def upload_day_folders_with_delay():
    """Finds all day folders (year/month/day) and uploads them one at a time every 60 seconds."""
    for year in os.listdir(local_directory):
        year_path = os.path.join(local_directory, year)
        if not os.path.isdir(year_path): continue

       
        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path): continue

            
            for day in os.listdir(month_path):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path): continue

                print(f"Uploading folder: {day_path}")

                upload_folder_to_gcs(day_path)

                print(f"Completed uploading: {day_path}")
                time.sleep(60)

if __name__ == '__main__':
    upload_day_folders_with_delay()

print("All day folders uploaded successfully.")
