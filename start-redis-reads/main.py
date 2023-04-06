import base64
import functions_framework
from google.cloud import storage
import time

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def copy_files_for_reads(cloud_event):
    message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    tokens = message.split(":")

    if(tokens[0] != "start-ipfr"):
        return

    run_id = tokens[1]
    source = tokens[2]
    destination = tokens[3]
    files_to_copy = int(tokens[4])

    #start copying files
    client = storage.Client()
    source_bucket = client.bucket(source)
    destination_bucket = client.bucket(destination)

    copied = 0
    batch = 0
    for blob in client.list_blobs(source, prefix=''):
          print(f'{blob.name}, batch: {batch}, copied: {copied}')
          blob_copy = source_bucket.copy_blob(blob, destination_bucket, f'{run_id}_{blob.name}')
          copied = copied + 1
          #sleep for 2 minute if we have copied certain files
          # e.g. 30 files i.e > 3GB of data
          if(copied == files_to_copy):
              copied = 0
              batch = batch + 1
              time.sleep(120)



