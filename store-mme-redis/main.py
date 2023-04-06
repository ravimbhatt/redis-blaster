import functions_framework
import redis
from google.cloud import storage
import csv
import pandas as pd
import os
import time
import concurrent.futures
from google.cloud import secretmanager
import urllib.request
from uhashring import HashRing

def get_project_id():
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    project_id = urllib.request.urlopen(req).read().decode()
    return project_id


def get_redis_nodes():
   # TODO: get the nodes from a config manager, SM. 
   # we store a connection pool in place an instance. 
   nodes = {
      'redis1': {
            'instance': redis.ConnectionPool(host="<ip-here>", port=6379, db=0)
        },
      'redis2': {
            'instance': redis.ConnectionPool(host="<ip-here>", port=6379, db=0)
        },
      'redis3': {
            'instance': redis.ConnectionPool(host="<ip-here>", port=6379, db=0)       
            }
   }
   
   return nodes

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def mme2redis(cloud_event):
   data = cloud_event.data

   bucket = data["bucket"]
   name = data["name"]
   print(f"Processing file: {name}")

   tokens = name.split("_")
   run_id = tokens[0]
   batch_id = tokens[1]
   real_file_name = tokens[2]

   #fetch config for this run id
   project_id = get_project_id()
   sm_client = secretmanager.SecretManagerServiceClient()
   config_name = f"projects/{project_id}/secrets/{run_id}/versions/latest"
   response = sm_client.access_secret_version(name=config_name)
   config_value = response.payload.data.decode("UTF-8")

   configs = config_value.split("_")
   redis_host = configs[0]
   chunk_size = int(configs[1])
   max_threads = int(configs[2])

   client = storage.Client()
   source_bucket = client.get_bucket(bucket)

   #create a consistent hash ring
   hr = HashRing(get_redis_nodes())

   #redis_pool = redis.ConnectionPool(host=redis_host, port=6379, db=0)
   count = 0 

   start = time.time()
   reader = pd.read_csv(f"gs://{bucket}/{name}", chunksize=chunk_size)
   with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
      futures = []
      for chunk in reader:
         chunk.drop(chunk.columns.difference(['TIME','TIME_MILLISECOND','eNodeB','eCI','MSISDN']), axis=1, inplace=True)   
         futures.append(executor.submit(send_chunk_to_redis, chunk, hr))

      for future in concurrent.futures.as_completed(futures):
         chunkCount = future.result()
         count = count + chunkCount

   end = time.time()

   #update redis with count for this run on all redis instances. 
   for node, instance in get_redis_nodes().items():
      r_c = redis.Redis(connection_pool=instance['instance'])
      r_c.incr(run_id + "-record-count", count)
      r_c.lpush(run_id + "-files", f"{real_file_name}:{end-start}")

   print(f'uploaded {name} with {count} records in {end-start} seconds.')


def send_chunk_to_redis(chunk, hr):
   count = 0
   #create pipeline for each available redis server. 
   pipelines = {}
   for node, instance in get_redis_nodes().items():
      #print(f'node: {node} instance: {instance}')
      r_c = redis.Redis(connection_pool=instance['instance'])
      pipelines[str(instance['instance'])] = r_c.pipeline()

   for index in range(len(chunk)):
      r_time = chunk['TIME'].iloc[index] * 1000.0 + chunk['TIME_MILLISECOND'].iloc[index]
      r_loc = str(chunk['eNodeB'].iloc[index]) + "-" + str(chunk['eCI'].iloc[index])
      r_msisdn = chunk['MSISDN'].iloc[index]

      pipelines[str(hr[r_msisdn])].zadd(r_msisdn,{r_loc: r_time})
      #TODO: also add keys to a list or set to match against the total count 
      count = count + 1   
   
   for node_name, pipeline in pipelines.items():
      pipeline.execute()

   return count
