# Ceph-brag

`ceph-brag` is going to be an anonymized cluster reporting tool designed to collect a "registry" of Ceph clusters for community knowledge.
This data will be displayed on a public web page using UUID by default, but users can claim their cluster and publish information about ownership if they so desire.

For more information please visit:

* [Blueprint](http://wiki.ceph.com/Planning/Blueprints/Firefly/Ceph-Brag)
* [CDS Etherpad](http://pad.ceph.com/p/cdsfirefly-ceph-brag)

# Client

## How to use:

### Pre-requisites:
ceph-brag uses 'ceph' python script. Hence, before executing ceph-brag script ensure that ceph services are all running and 'ceph' script is in 'PATH' environment

### Runtime instructions:
Run 'ceph-brag -h' to get the usage information of this tool.

### Sample output:

    {
      "cluster_creation_date": "2014-01-16 13:38:41.928551",
      "uuid": "20679d0e-04b1-4004-8ee9-45ac271510e9",
      "components_count": {
        "bytes": {
          "count": 0,
          "scale": "bytes"
        },
        "osds": 1,
        "objects": 0,
        "pgs": 192,
        "pools": 3,
        "mdss": 1,
        "mons": 1
      },
      "crush_types": [
        "osd",
        "host",
        "chassis",
        "rack",
        "row",
        "pdu",
        "pod",
        "room",
        "datacenter",
        "region",
        "root"
      ],
      "ownership": {
        "organization": "eNovance",
        "description": "Use case1",
        "email": "mail@enovance.com",
        "name": "Cluster1"
      },
      "pool_metadata": [
        {
          "rep_size": 3,
          "id": "0",
          "name": "data"
        },
        {
          "rep_size": 3,
          "id": "1",
          "name": "metadata"
        },
        {
          "rep_size": 3,
          "id": "2",
          "name": "rbd"
        }
      ],
      "sysinfo": [
        {
          "nw_info": {
            "hostname": "ceph-brag",
            "address": "127.0.0.1"
          },
          "hw_info": {
            "swap_kb": 0,
            "arch": "x86_64",
            "cpu": "Intel Xeon E312xx (Sandy Bridge)",
            "mem_kb": 2051648
          },
          "id": 0,
          "os_info": {
            "version": "3.2.0-23-virtual",
            "os": "Linux",
            "description": "#36-Ubuntu SMP Tue Apr 10 22:29:03 UTC 2012",
            "distro": "Ubuntu 12.04 precise (Ubuntu 12.04 LTS)"
          },
          "ceph_version": "ceph version 0.75-229-g4050eae (4050eae32cd77a1c210ca11d0f12c74daecb1bd3)"
        }
      ]
    }


# Server

## Info
The ceph-brag server code is a python based web application. 

## How to use

### Prerequisites
* [pecan](http://pecanpy.org) is the web framework that is used by this application.
* [sqlalchemy](www.sqlalchemy.org) is the ORM that is used by this application

### How to deploy
* [Common recipes to deploy](http://pecan.readthedocs.org/en/latest/deployment.html#common-recipes)
* Modify server/config.py:sqlalchemy['url'] to point the correct database connection

## URLs
Following are the REST urls that are implemented with 'url-prefix' being the mount point for the WSGI script

### GET

##### * GET /url-prefix/
Returns the list of clusters that are registered so far. 
Outputs - On success application/json of the following format is returned

    [
      {
       "num_versions": 3, 
       "cluster_creation_date": "2014-01-16 13:38:41.928551", 
       "uuid": "20679d0e-04b1-4004-8ee9-45ac271510e9", 
       "cluster_name": "Cluster1", 
       "organization": "eNovance", 
       "email": "mail@enovance.com"
      },
      ...
    ]

##### * GET /url-prefix/UUID
Returns the list of version information for a particular UUID.
Outputs - On success application/json of the following format is returned

    [
      {
        "version_number": 1, 
        "version_date": "2014-02-10 10:17:56.283499", 
        "version_id": 10
      },
      ...
    ]

##### * GET /url-prefix/UUID/version\_number
Returns the entire brag report as mentioned in client's sample output for a particular version of a UUID

### PUT

##### * PUT /url-prefix
Uploads the brag report and creates a new version for the UUID mentioned in the payload

### DELETE

##### * DELETE /url-prefix?uuid=xxxx
Deletes all the versions of a cluster whose UUID is sent as a parameter


