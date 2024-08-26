==================
D3N RGW Data Cache
==================

.. contents::

Datacenter-Data-Delivery Network (D3N) uses high-speed storage such as NVMe flash or DRAM to cache
datasets on the access side.
Such caching allows big data jobs to use the compute and fast storage resources available on each
Rados Gateway node at the edge.

Many datacenters include low-cost, centralized storage repositories, called data lakes,
to store and share terabyte and petabyte-scale datasets.
By necessity most distributed big-data analytic clusters such as Hadoop and Spark must
depend on accessing a centrally located data lake that is relatively far away.
Even with a well-designed datacenter network, cluster-to-data lake bandwidth is typically much less
than the bandwidth of a solid-state storage located at an edge node.

| D3N improves the performance of big-data jobs running in analysis clusters by speeding up recurring reads from the data lake.
| The Rados Gateways act as cache servers for the back-end object store (OSDs), storing data locally for reuse.

Architecture
============

D3N improves the performance of big-data jobs by speeding up repeatedly accessed dataset reads from the data lake.
Cache servers are located in the datacenter on the access side of potential network and storage bottlenecks.
D3Ns two-layer logical cache forms a traditional caching hierarchy :sup:`*`
where caches nearer the client have the lowest access latency and overhead,
while caches in higher levels in the hierarchy are slower (requiring multiple hops to access),
The layer 1 cache server nearest to the client handles object requests by breaking them into blocks,
returning any blocks which are cached locally, and forwarding missed requests to the block home location
(as determined by consistent hashing) in the next layer.
Cache misses are forwarded to successive logical caching layers until a miss at the top layer is resolved
by a request to the data lake (Rados)

:sup:`*` currently only layer 1 cache has been upstreamed.

See `MOC D3N (Datacenter-scale Data Delivery Network)`_ and `Red Hat Research D3N Cache for Data Centers`_.

Implementation
==============

- The D3N cache supports both the `S3` and `Swift` object storage interfaces.
- D3N currently caches only tail objects, because they are immutable (by default it is parts of objects that are larger than 4MB).
  (the NGINX `RGW Data cache and CDN`_ supports caching of all object sizes)


Requirements
------------

- An SSD (/dev/nvme,/dev/pmem,/dev/shm) or similar block storage device, formatted
  (filesystems other than XFS were not tested) and mounted.
  It will be used as the cache backing store.
  (depending on device performance, multiple RGWs may share a single device but each requires
  a discrete directory on the device filesystem)

Limitations
-----------

- D3N will not cache objects compressed by `Rados Gateway Compression`_ (OSD level compression is supported).
- D3N will not cache objects encrypted by `Rados Gateway Encryption`_.
- D3N will be disabled if the ``rgw_max_chunk_size`` config variable value differs from the ``rgw_obj_stripe_size`` config variable value.


D3N Environment Setup
=====================

Running
-------

To enable D3N on an existing RGWs the following configuration entries are required
in each Rados Gateways ceph.conf client section, for example for ``[client.rgw.8000]``::

    [client.rgw.8000]
      rgw_d3n_l1_local_datacache_enabled = true
      rgw_d3n_l1_datacache_persistent_path = "/mnt/nvme0/rgw_datacache/client.rgw.8000/"
      rgw_d3n_l1_datacache_size = 10737418240

The above example assumes that the cache backing-store solid state device
is mounted at `/mnt/nvme0` and has `10 GB` of free space available for the cache.

The persistent path directory has to be created before starting the Gateway.
(``mkdir -p /mnt/nvme0/rgw_datacache/client.rgw.8000/``)

In containerized deployments the cache directory should be mounted as a volume::

    extra_container_args:
      - "-v"
      - "/mnt/nvme0/rgw_datacache/client.rgw.8000/:/mnt/nvme0/rgw_datacache/client.rgw.8000/"

(Reference: `Service Management - Mounting Files with Extra Container Arguments`_)

If another Gateway is co-located on the same machine, configure it's persistent path to a discrete directory,
for example in the case of `[client.rgw.8001]` configure
``rgw_d3n_l1_datacache_persistent_path = "/mnt/nvme0/rgw_datacache/client.rgw.8001/"``
in the ``[client.rgw.8001]`` ceph.conf client section.

In a multiple co-located Gateways configuration consider assigning clients with different workloads
to each Gateway without a balancer in order to avoid cached data duplication.

    NOTE: each time the Rados Gateway is restarted the content of the cache directory is purged.

Logs
----
- D3N related log lines in `radosgw.*.log` contain the string ``d3n`` (case insensitive).
- low level D3N logs can be enabled by the ``debug_rgw_datacache`` subsystem (up to ``debug_rgw_datacache=30``)


CONFIG REFERENCE
================
The following D3N related settings can be added to the Ceph configuration file
(i.e., usually `ceph.conf`) under the ``[client.rgw.{instance-name}]`` section.

.. confval:: rgw_d3n_l1_local_datacache_enabled
.. confval:: rgw_d3n_l1_datacache_persistent_path
.. confval:: rgw_d3n_l1_datacache_size
.. confval:: rgw_d3n_l1_eviction_policy


.. _MOC D3N (Datacenter-scale Data Delivery Network): https://massopen.cloud/research-and-development/cloud-research/d3n/
.. _Red Hat Research D3N Cache for Data Centers: https://research.redhat.com/blog/research_project/d3n-multilayer-cache/
.. _Rados Gateway Compression: ../compression/
.. _Rados Gateway Encryption: ../encryption/
.. _RGW Data cache and CDN: ../rgw-cache/
.. _Service Management - Mounting Files with Extra Container Arguments: ../cephadm/services/#mounting-files-with-extra-container-arguments
