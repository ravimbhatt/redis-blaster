# Redis Blaster - Perf Testing Redis
------------------------------------

This is WIP service to load test redis cluster(s). The idea is to have an end to end setup that allows configurable load
going to either a single or multiple Redis Clusters. 

Performance of applications and services are highly dependent on the data being used. One will get different set of performance numbers 
if KBs of data is being used versus MBs of data. 

## Design
---------

This services uses `Dataflow` `Streaming Data Generator Template` to generate fake data for performance tests. Once we have fake data,
in the examples included, we convert this JSON data to CSV - this step is completely optional and one could work with JSON files directly. 

We control Writing to and Reading from Redis via Pub/Sub. We publish below messages for starting the write and read flows. 

`Begin Write:
start-mme:run-0504-1040:10.40.208.21:10000:60:mme_csv:mme_minute_dropzone:60
`

`Begin Read: 
start-ipfr:run-0504-0803:mme_csv:fake_ipfr:75
`













