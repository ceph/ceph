# Introduction

This directory contains an example `elasticsearch_adapter.lua` on how to
use [Lua Scripting](https://docs.ceph.com/en/latest/radosgw/lua-scripting/)
to push fields of the RGW requests
to [Elasticsearch](https://www.elastic.co/elasticsearch/).

## Elasticsearch

Install and run Elasticsearch using docker:
```bash
docker network create elastic
docker pull elasticsearch:2.4.6
docker run --net elastic -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:2.4.6
```

[Full documentation for Elasticsearch installation](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)

## Usage

* Upload the script:

```bash
radosgw-admin script put --infile=elasticsearch_adapter.lua --context=postRequest
```

* Add the packages used in the script:

```bash
radosgw-admin script-package add --package='elasticsearch 1.0.0-1' --allow-compilation
radosgw-admin script-package add --package='lunajson' --allow-compilation
radosgw-admin script-package add --package='lua-cjson 2.1.0-1' --allow-compilation
```

* Restart radosgw.

* Send a request:
```bash
s3cmd --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" --access_key=0555b35654ad1656d804 --secret_key=h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== mb s3://mybucket
s3cmd --host=localhost:8000 --host-bucket="localhost:8000/%(bucket)" --access_key=0555b35654ad1656d804 --secret_key=h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q== put -P /etc/hosts s3://mybucket
curl http://localhost:8000/mybucket/hosts
```

* Search by bucket id from Elasticsearch:
```bash
curl -X GET "localhost:9200/rgw/_search?pretty" -H 'Content-Type: application/json' -d' 
{ 
  "query": { 
    "match": { 
      "Bucket.Id": "05382336-b2db-409f-82dc-f28ab5fef978.4471.4471" 
    } 
  } 
} 
'
```

## Requirements
* Lua 5.3

