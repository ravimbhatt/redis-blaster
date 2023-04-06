import base64
import functions_framework
from google.cloud import storage
import redis
from google.cloud import secretmanager
import os
import urllib.request
import time

def get_project_id():
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    project_id = urllib.request.urlopen(req).read().decode()
    return project_id

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def begin_perf_test_run(cloud_event):
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    tokens = message.split(":")
    
    if(tokens[0] != "start-mme"):
        return 

    run_id = tokens[1]
    redis_ip = tokens[2]
    chunk_size = tokens[3]
    max_threads = tokens[4]
    source = tokens[5]
    destination = tokens[6]
    files_to_copy = int(tokens[7])

    print(f'run id: {run_id}, redis ip: {redis_ip}, source: {source}, destination: {destination} f_to_copy: {files_to_copy}')

    r_c = redis.Redis(host=redis_ip, port=6379, db=0)
    r_c.set(run_id + "-record-count", 0)

    #create config in secret manager 
    project_id = get_project_id()
    sm_client = secretmanager.SecretManagerServiceClient()
    # Build the parent name from the project.
    parent = f"projects/{project_id}"

    # Create the parent secret.
    secret = sm_client.create_secret(
        request={
            "parent": parent,
            "secret_id": run_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )

    # Add the secret version.
    config = f"{redis_ip}_{chunk_size}_{max_threads}"
    version = sm_client.add_secret_version(
        request={"parent": secret.name, "payload": {"data": config.encode('UTF-8')}}
    )

    #start copying files
    client = storage.Client()
    source_bucket = client.bucket(source)
    destination_bucket = client.bucket(destination)

    copied = 0
    batch = 1 
    for blob in client.list_blobs(source, prefix=''):
          print(f'{blob.name}, copied: {copied}')
          blob_copy = source_bucket.copy_blob(blob, destination_bucket, f'{run_id}_{batch}_{blob.name}')
          copied = copied + 1
          #sleep for 1 minute if we have copied 20 files i.e > 2GB of data
          if(copied == files_to_copy):
              copied = 0
              batch = batch + 1
              time.sleep(60)



