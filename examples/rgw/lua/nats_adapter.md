# Introduction

This directory contains examples on how to use [Lua Scripting](https://docs.ceph.com/en/latest/radosgw/lua-scripting/) together with a [NATS Lua client](https://github.com/dawnangel/lua-nats) to add NATS to the list of bucket notifications endpoints.

## NATS
To test your setup:
* Install [NATS](https://docs.nats.io/nats-server/installation) and start a nats-server.

* Subscribe to the NATS server using a [nats subscriber](https://github.com/nats-io/go-nats-examples/tree/master/patterns/publish-subscribe), choosing the topic to be 'Bucket_Notification' (as defined in the [script]())


```bash
nats-sub "Bucket_Notification"
```


[Full documentation for subscribing](https://docs.nats.io/nats-server/clients).

Alternatively, configure the script to point to an existing NATS broker by editing the following part in the script to match the parameters of your existing nats server. 

```
nats_host = '{host}',
nats_port = {port},
```

## Usage

* Upload the [script]():

```bash
radosgw-admin script put --infile=nats_adapter.lua --context=postRequest
```
* Add the packages used in the script:

```bash
radosgw-admin script-package add --package=nats --allow-compilation
radosgw-admin script-package add --package=lunajson --allow-compilation
radosgw-admin script-package add --package='lua-cjson 2.1.0-1' --allow-compilation
```
* Restart radosgw.
* create a bucket:
```
s3cmd --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" mb s3://mybucket
```
* upload a file to the bucket and make sure that the nats server received the notification

```
s3cmd --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" put hello.txt s3://mybucket
```

Expected output:
```
Received on [Bucket_Notification]:
   {"Records":[
       {
           "eventVersion":"2.1",
           "eventSource":"ceph:s3",
           "awsRegion":"default",
           "eventTime":"2019-11-22T13:47:35.124724Z",
           "eventName":"ObjectCreated:Put",
           "userIdentity":{
               "principalId":"tester"
           },
           "requestParameters":{
               "sourceIPAddress":""
           },
           "responseElements":{
               "x-amz-request-id":"503a4c37-85eb-47cd-8681-2817e80b4281.5330.903595",
               "x-amz-id-2":"14d2-zone1-zonegroup1"
           },
           "s3":{
               "s3SchemaVersion":"1.0",
               "configurationId":"mynotif1",
               "bucket":{
                   "name":"mybucket",
                   "ownerIdentity":{
                       "principalId":"tester"
                   },
                   "arn":"arn:aws:s3:us-east-1::mybucket1",
                   "id":"503a4c37-85eb-47cd-8681-2817e80b4281.5332.38"
               },
               "object":{
                   "key":"hello.txt",
                   "size":"1024",
                   "eTag":"",
                   "versionId":"",
                   "sequencer": "F7E6D75DC742D108",
                   "metadata":[],
                   "tags":[]
               }
           },
           "eventId":"",
           "opaqueData":"me@example.com"
       }
   ]}

```

## Requirements
* Lua 5.3 (or higher)
* Luarocks
