================
cephadm Exporter
================

There are a number of long running tasks that the cephadm 'binary' runs which can take several seconds
to run. This latency represents a scalability challenge to the Ceph orchestrator management plane.

To address this, cephadm needs to be able to run some of these longer running tasks asynchronously - this
frees up processing on the mgr by offloading tasks to each host, reduces latency and improves scalability.

This document describes the implementation requirements and design for an 'exporter' feature


Requirements
============
The exporter should address these functional and non-functional requirements;

* run as a normal systemd unit
* utilise the same filesystem schema as other services deployed with cephadm
* require only python3 standard library modules (no external dependencies)
* use encryption to protect the data flowing from a host to Ceph mgr
* execute data gathering tasks as background threads
* be easily extended to include more data gathering tasks
* monitor itself for the health of the data gathering threads
* cache metadata to respond to queries quickly
* respond to a metadata query in <30ms to support large Ceph clusters (000's nodes)
* provide CLI interaction to enable the exporter to be deployed either at bootstrap time, or once the
  cluster has been deployed.
* be deployed as a normal orchestrator service (similar to the node-exporter)

High Level Design
=================

This section will focus on the exporter logic **only**.

.. code::

    Establish a metadata cache object (tasks will be represented by separate attributes)
    Create a thread for each data gathering task; host, ceph-volume and list_daemons
        each thread updates it's own attribute within the cache object
    Start a server instance passing requests to a specific request handler
        the request handler only interacts with the cache object
        the request handler passes metadata back to the caller
    Main Loop
        Leave the loop if a 'stop' request is received
        check thread health
            if a thread that was active, is now inactive
                update the cache marking the task as inactive
                update the cache with an error message for that task
        wait for n secs
        

In the initial exporter implementation, the exporter has been implemented as a RESTful API.


Security
========

The cephadm 'binary' only supports standard python3 features, which has meant the RESTful API has been
developed using the http module, which itself is not intended for production use. However, the implementation
is not complex (based only on HTTPServer and BaseHHTPRequestHandler) and only supports the GET method - so the
security risk is perceived as low.

Current mgr to host interactions occurs within an ssh connection, so the goal of the exporter is to adopt a similar
security model.

The initial REST API is implemented with the following features;

* generic self-signed, or user provided SSL crt/key to encrypt traffic between the mgr and the host
* 'token' based authentication of the request

All exporter instances will use the **same** crt/key to secure the link from the mgr to the host(s), in the same way
that the ssh access uses the same public key and port for each host connection.

.. note:: Since the same SSL configuration is used on every exporter, when you supply your own settings you must
  ensure that the CN or SAN components of the distinguished name are either **not** used or created using wildcard naming.

The crt, key and token files are all defined with restrictive permissions (600), to help mitigate against the risk of exposure
to any other user on the Ceph cluster node(s).

Administrator Interaction
=========================
Several new commands are required to configure the exporter, and additional parameters should be added to the bootstrap
process to allow the exporter to be deployed automatically for new clusters.


Enhancements to the 'bootstrap' process
---------------------------------------
bootstrap should support additional parameters to automatically configure exporter daemons across hosts

``--with-exporter``

By using this flag, you're telling the bootstrap process to include the cephadm-exporter service within the 
cluster. If you do not provide a specific configuration (SSL, token, port) to use, defaults would be applied.

``--exporter-config``

With the --exporter-config option, you may pass your own SSL, token and port information. The file must be in 
JSON format and contain the following fields; crt, key, token and port. The JSON content should be validated, and any
errors detected passed back to the user during the argument parsing phase (before any changes are done).


Additional ceph commands
------------------------
::

# ceph cephadm generate-exporter-config

This command will create generate a default configuration consisting of; a self signed certificate, a randomly generated
32 character token and the default port of 9443 for the REST API.
::

# ceph cephadm set-exporter-config -i <config.json>

Use a JSON file to define the crt, key, token and port for the REST API. The crt, key and token are validated by
the mgr/cephadm module prior storing the values in the KV store. Invalid or missing entries should be reported to the 
user.
::

# ceph cephadm clear-exporter-config

Clear the current configuration (removes the associated keys from the KV store)
::

# ceph cephadm get-exporter-config

Show the current exporter configuration, in JSON format


.. note:: If the service is already deployed any attempt to change or clear the configuration will
    be denied. In order to change settings you must remove the service, apply the required configuration
    and re-apply (``ceph orch apply cephadm-exporter``)



New Ceph Configuration Keys
===========================
The exporter configuration is persisted to the monitor's KV store, with the following keys:

| mgr/cephadm/exporter_config
| mgr/cephadm/exporter_enabled



RESTful API
===========
The primary goal of the exporter is the provision of metadata from the host to the mgr. This interaction takes
place over a simple GET interface. Although only the GET method is supported, the API provides multiple URLs to
provide different views on the metadata that has been gathered.

.. csv-table:: Supported URL endpoints
    :header: "URL", "Purpose"

    "/v1/metadata", "show all metadata including health of all threads"
    "/v1/metadata/health", "only report on the health of the data gathering threads"
    "/v1/metadata/disks", "show the disk output (ceph-volume inventory data)"
    "/v1/metadata/host", "show host related metadata from the gather-facts command"
    "/v1/metatdata/daemons", "show the status of all ceph cluster related daemons on the host"

Return Codes
------------
The following HTTP return codes are generated by the API

.. csv-table:: Supported HTTP Responses
    :header: "Status Code", "Meaning"

    "200", "OK"
    "204", "the thread associated with this request is no longer active, no data is returned"
    "206", "some threads have stopped, so some content is missing"
    "401", "request is not authorised - check your token is correct"
    "404", "URL is malformed, not found"
    "500", "all threads have stopped - unable to provide any metadata for the host"


Deployment
==========
During the initial phases of the exporter implementation, deployment is regarded as optional but is available
to new clusters and existing clusters that have the feature (Pacific and above).

* new clusters : use the ``--with-exporter`` option
* existing clusters : you'll need to set the configuration and deploy the service manually

.. code::

    # ceph cephadm generate-exporter-config
    # ceph orch apply cephadm-exporter

If you choose to remove the cephadm-exporter service, you may simply 

.. code::

    # ceph orch rm cephadm-exporter

This will remove the daemons, and the exporter releated settings stored in the KV store.


Management
==========
Once the exporter is deployed, you can use the following snippet to extract the host's metadata.

.. code-block:: python

    import ssl
    import json
    import sys
    import tempfile
    import time
    from urllib.request import Request, urlopen

    # CHANGE THIS V
    hostname = "rh8-1.storage.lab"
    
    print("Reading config.json")
    try:
        with open('./config.json', 'r') as f:
            raw=f.read()
    except FileNotFoundError as e:
        print("You must first create a config.json file using the cephadm get-exporter-config command")
        sys.exit(1)

    cfg = json.loads(raw)
    with tempfile.NamedTemporaryFile(buffering=0) as t:
        print("creating a temporary local crt file from the json")
        t.write(cfg['crt'].encode('utf-8'))

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.load_verify_locations(t.name)
        hdrs={"Authorization":f"Bearer {cfg['token']}"}
        print("Issuing call to gather metadata")
        req=Request(f"https://{hostname}:9443/v1/metadata",headers=hdrs)
        s_time = time.time()
        r = urlopen(req,context=ctx)
        print(r.status)
        print("call complete")
        # assert r.status == 200
        if r.status in [200, 206]:

            raw=r.read()  # bytes string
            js=json.loads(raw.decode())
            print(json.dumps(js, indent=2))
        elapsed = time.time() - s_time
        print(f"Elapsed secs : {elapsed}")


.. note:: the above example uses python3, and assumes that you've extracted the config using the ``get-exporter-config`` command.


Implementation Specific Details
===============================

In the same way as a typical container based deployment, the exporter is deployed to a directory under ``/var/lib/ceph/<fsid>``. The 
cephadm binary is stored in this cluster folder, and the daemon's configuration and systemd settings are stored
under ``/var/lib/ceph/<fsid>/cephadm-exporter.<id>/``.

.. code::

    [root@rh8-1 cephadm-exporter.rh8-1]# pwd
    /var/lib/ceph/cb576f70-2f72-11eb-b141-525400da3eb7/cephadm-exporter.rh8-1
    [root@rh8-1 cephadm-exporter.rh8-1]# ls -al 
    total 24
    drwx------. 2 root root  100 Nov 25 18:10 .
    drwx------. 8 root root  160 Nov 25 23:19 ..
    -rw-------. 1 root root 1046 Nov 25 18:10 crt
    -rw-------. 1 root root 1704 Nov 25 18:10 key
    -rw-------. 1 root root   64 Nov 25 18:10 token
    -rw-------. 1 root root   38 Nov 25 18:10 unit.configured
    -rw-------. 1 root root   48 Nov 25 18:10 unit.created
    -rw-r--r--. 1 root root  157 Nov 25 18:10 unit.run


In order to respond to requests quickly, the CephadmDaemon uses a cache object (CephadmCache) to hold the results
of the cephadm commands.

The exporter doesn't introduce any new data gathering capability - instead it merely calls the existing cephadm commands.

The CephadmDaemon class creates a local HTTP server(uses ThreadingMixIn), secured with TLS and uses the CephadmDaemonHandler
to handle the requests. The request handler inspects the request header and looks for a valid Bearer token - if this is invalid
or missing the caller receives a 401 Unauthorized error.

The 'run' method of the CephadmDaemon class, places the scrape_* methods into different threads with each thread supporting
a different refresh interval. Each thread then periodically issues it's cephadm command, and places the output
in the cache object.

In addition to the command output, each thread also maintains it's own timestamp record in the cache so the caller can 
very easily determine the age of the data it's received.

If the underlying cephadm command execution hits an exception, the thread passes control to a _handle_thread_exception method.
Here the exception is logged to the daemon's log file and the exception details are added to the cache, providing visibility
of the problem to the caller.

Although each thread is effectively given it's own URL endpoint (host, disks, daemons), the recommended way to gather data from
the host is to simply use the ``/v1/metadata`` endpoint. This will provide all of the data, and indicate whether any of the
threads have failed.

The run method uses "signal" to establish a reload hook, but in the initial implementation this doesn't take any action and simply
logs that a reload was received.


Future Work
===========

#. Consider the potential of adding a restart policy for threads
#. Once the exporter is fully integrated into mgr/cephadm, the goal would be to make the exporter the 
   default means of data gathering. However, until then the exporter will remain as an opt-in 'feature
   preview'.
