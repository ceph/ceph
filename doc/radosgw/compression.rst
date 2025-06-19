===========
Compression
===========

.. versionadded:: Kraken

The Ceph Object Gateway supports server-side compression of uploaded objects.

.. note:: The Reef release added a :ref:`feature_compress_encrypted` zonegroup
   feature to enable compression with `Server-Side Encryption`_.

Supported compression plugins include the following:

* lz4
* snappy
* zlib
* zstd

.. note:: Ceph Object Gateway compression is performed by RGW daemons only
   for RGW objects, and is distinct from BlueStore compression that is performed 
   by OSDs at pool granularity. It is typical to only enable one or the other. 
   Enabling at both levels does not cause a problem, but one should make the decision 
   based on the use case. If your cluster only serves object storage and the nodes 
   where RGW runs have more available CPU than OSD nodes, RGW level compression may be appealing. 
   Compressing at the OSD level does mean compressing the same user data more 
   than once since it is post-replication, but in a cluster with far more OSDs 
   than RGWs this strategy may result in better performance.

Configuration
=============

Compression can be enabled on a storage class in the Zone's placement target
by providing the ``--compression=<type>`` option to the command
``radosgw-admin zone placement modify``.

The compression ``type`` refers to the name of the compression plugin that will
be used when writing new object data. Each compressed object remembers which
plugin was used, so any change to this setting will neither affect Ceph's
ability to decompress existing objects nor require existing objects to be
recompressed.

Compression settings apply to all new objects uploaded to buckets using this
placement target. Compression can be disabled by setting the ``type`` to an
empty string or ``none``.

For example:

.. prompt:: bash #

   radosgw-admin zone placement modify --rgw-zone default \
                                         --placement-id default-placement \
                                         --storage-class STANDARD \
                                         --compression zlib

::

  {
  ...
      "placement_pools": [
          {
              "key": "default-placement",
              "val": {
                  "index_pool": "default.rgw.buckets.index",
                  "storage_classes": {
                      "STANDARD": {
                          "data_pool": "default.rgw.buckets.data",
                          "compression_type": "zlib"
                      }
                  },
                  "data_extra_pool": "default.rgw.buckets.non-ec",
                  "index_type": 0,
              }
          }
      ],
  ...
  }

.. note:: A ``default`` zone is created for you if you have not done any
   previous `Multisite Configuration`_.


Statistics
==========

Run the ``radosgw-admin bucket stats`` command to see compression statistics
for a given bucket:

.. prompt:: bash #

   radosgw-admin bucket stats --bucket=<name>

::

  {
  ...
      "usage": {
          "rgw.main": {
              "size": 1075028,
              "size_actual": 1331200,
              "size_utilized": 592035,
              "size_kb": 1050,
              "size_kb_actual": 1300,
              "size_kb_utilized": 579,
              "num_objects": 104
          }
      },
  ...
  }

Other commands and APIs will report object and bucket sizes based on their
uncompressed data. 

The ``size_utilized`` and ``size_kb_utilized`` fields represent the total
size of compressed data, in bytes and kilobytes respectively.


.. _`Server-Side Encryption`: ../encryption
.. _`Multisite Configuration`: ../multisite
