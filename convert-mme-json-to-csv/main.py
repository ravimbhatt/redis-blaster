import functions_framework
import os
import pandas as pd
from google.cloud import storage
import tempfile


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def mme_file_created(cloud_event):
   data = cloud_event.data

   event_id = cloud_event["id"]
   event_type = cloud_event["type"]

   bucket = data["bucket"]
   name = data["name"]
   timeCreated = data["timeCreated"]
   updated = data["updated"]

   print(f"Bucket: {bucket}")
   print(f"File: {name}")
   print(f"Created: {timeCreated}")

   client = storage.Client()
   source_bucket = client.get_bucket(bucket)
   file_blob = source_bucket.blob(name)
   file_name = os.path.splitext(name)[0]
   detination_bucket = client.get_bucket("mme_csv")

   with file_blob.open("r") as f:
      with tempfile.NamedTemporaryFile() as tmp:
      #df = pd.read_json(f.read(), lines=True)
         header = True
         for chunk in pd.read_json(f.read(), lines=True, chunksize=5000):
            chunk.to_csv(tmp.name, header = header, mode='a')
            header = False
         
         destination = detination_bucket.blob(f"{file_name}.csv")
         destination.content_type = 'text/csv'
         destination.upload_from_filename(tmp.name)



   #df.to_csv(f"gs://mme_csv/{file_name}.csv")


