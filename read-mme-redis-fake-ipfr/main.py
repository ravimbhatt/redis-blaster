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
import random
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
            'instance': redis.ConnectionPool(host="<primary-ip-here>", port=6379, db=0),
            'hostname': redis.ConnectionPool(host="<read-ip-here>", port=6379, db=0)
        },
      'redis2': {
            'instance': redis.ConnectionPool(host="<primary-ip-here>", port=6379, db=0),
            'hostname': redis.ConnectionPool(host="<read-ip-here>", port=6379, db=0)
        },
      'redis3': {
            'instance': redis.ConnectionPool(host="<primary-ip-here>", port=6379, db=0),
            'hostname': redis.ConnectionPool(host="<read-ip-here>", port=6379, db=0)       
            }
   }
   
   return nodes

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def process_ipfr_read_redis(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    print(f"Bucket: {bucket}, File: {name}")
    tokens = name.split("_")
    run_id = tokens[0]
    real_file_name = tokens[1]

    '''
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
    '''

    client = storage.Client()
    source_bucket = client.get_bucket(bucket)
    #redis_pool = redis.ConnectionPool(host="10.40.208.22", port=6379, db=0)

    #create a consistent hash ring
    hr = HashRing(get_redis_nodes())

    count = 0 
    start = time.time()
    reader = pd.read_csv(f"gs://{bucket}/{name}", chunksize=5000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for chunk in reader:
            chunk.drop(chunk.columns.difference(['TIME','TIME_MILLISECOND','eNodeB','eCI','MSISDN']), axis=1, inplace=True)   
           
            futures.append(executor.submit(read_loc_from_redis, chunk, hr))
            #add the same chunk ramdom times more to increase read volume
            r = random.randint(0, 1)
            for i in range(0,r):
                #pipeline1 = r_c.pipeline()
                futures.append(executor.submit(read_loc_from_redis, chunk, hr))

        print(f'number of futures (or chunks): {len(futures)}')
        fcount=0       
        
        for future in concurrent.futures.as_completed(futures):
            fcount = fcount + 1
            chunkCount = future.result()
            count = count + chunkCount
        
        print(f'futures compelete: {fcount}')
    end = time.time()

    #update redis with count for this run on all redis instances. 
    for node, instance in get_redis_nodes().items():
        r_c = redis.Redis(connection_pool=instance['instance'])
        r_c.incr(run_id + "-read-record-count", count)
        r_c.lpush(run_id + "-ipfr-files", f"{real_file_name}:{end-start}")

    print(f'read {name} with {count} records in {end-start} seconds.')


def read_loc_from_redis(chunk, hr):
    count = 0

    #create pipeline for each available redis server. 
    pipelines = {}
    for node, instance in get_redis_nodes().items():
        #print(f'node: {node} instance: {instance}')
        #create a pipeline on the reader endpoint and store mapping from write node to read node
        r_c = redis.Redis(connection_pool=instance['hostname'])
        pipelines[str(instance['instance'])] = r_c.pipeline() 

    for index in range(len(chunk)):
        #r_time = chunk['TIME'].iloc[index] * 1000.0 + chunk['TIME_MILLISECOND'].iloc[index]
        r_msisdn = chunk['MSISDN'].iloc[index]
        pipelines[str(hr[r_msisdn])].zrevrange(r_msisdn, 0, 0)
        #TODO: also add keys to a list or set to match against the total count 
        count = count + 1   
        
    for node_name, pipeline in pipelines.items():
        key_location_list = pipeline.execute()
        # print(key_location_list[:10])
    
    return count
