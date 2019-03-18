===========
Compression
===========

.. versionadded:: Kraken

The Ceph Object Gateway supports server-side compression of uploaded objects,
using any of Ceph's existing compression plugins.


Configuration
=============

Compression can be enabled on a storage class in the Zone's placement target
by providing the ``--compression=<type>`` option to the command
``radosgw-admin zone placement modify``.

The compression ``type`` refers to the name of the compression plugin to use
when writing new object data. Each compressed object remembers which plugin
was used, so changing this setting does not hinder the ability to decompress
existing objects, not does it force existing objects to be recompressed.

This compression setting applies to all new objects uploaded to buckets using
this placement target. Compression can be disabled by setting the ``type`` to
an empty string or ``none``.

For example::

  $ radosgw-admin zone placement modify \
        --rgw-zone default \
        --placement-id default-placement \
        --storage-class STANDARD \
        --compression zlib
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

While all existing commands and APIs continue to report object and bucket
sizes based their uncompressed data, compression statistics for a given bucket
are included in its ``bucket stats``::

  $ radosgw-admin bucket stats --bucket=<name>
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

The ``size_utilized`` and ``size_kb_utilized`` fields represent the total
size of compressed data, in bytes and kilobytes respectively.


.. _`Multisite Configuration`: ../multisite
