.. _radosgw-compression:

===========
Compression
===========

The Ceph Object Gateway supports server-side compression of uploaded objects.

.. note:: The Reef release added a :ref:`feature_compress_encrypted` zonegroup
   feature to enable compression with :ref:`Server-Side Encryption <radosgw-encryption>`.

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

.. toctree::
   :maxdepth: 1

   QAT Acceleration for Encryption and Compression <qat-accel>
   UADK Acceleration for Compression <uadk-accel>


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
   previous :ref:`Multisite Configuration <multisite>`.

Minimum Object Size
-------------------

Small objects gain little from compression but still cost CPU time and
compression metadata. The ``--compression-min-size=<size>`` option of
``radosgw-admin zone placement modify`` sets the smallest object size that is
compressed for a storage class. Objects smaller than this are stored
uncompressed. The default of ``0`` compresses objects of any size.

For example, to compress only objects of 64 KiB or larger:

.. prompt:: bash #

   radosgw-admin zone placement modify --rgw-zone default \
                                         --placement-id default-placement \
                                         --storage-class STANDARD \
                                         --compression zstd \
                                         --compression-min-size 64K

The size is stored in bytes:

::

  "STANDARD": {
      "data_pool": "default.rgw.buckets.data",
      "compression_type": "zstd",
      "compression_min_size": 65536
  }

The size is compared with an object's logical (uncompressed) size. It applies
to S3 and Swift object uploads whose size is known in advance, object copies,
lifecycle transitions, and objects replicated by multisite sync. Multipart
uploads, uploads without a known size, POST uploads, Swift bulk uploads,
writes through librgw and ``radosgw-admin object put`` ignore the minimum size.

Copies and lifecycle transitions store an object that is below the minimum
size uncompressed even if its source is compressed, unless the object is
encrypted and is copied without being decrypted. Multisite sync applies the
destination zone's setting only to objects that it compresses itself (see
`Multisite Sync`_).

.. note:: ``radosgw-admin`` from a release without this option drops the
   setting, for every storage class, whenever it writes the zone.

Multipart Uploads
-----------------

The compression ``type`` of a multipart upload is determined when the upload is
initiated, and all of its parts use it. Changing the setting while a multipart
upload is in progress does not affect that upload; only multipart uploads
initiated afterwards use the new ``type``.

.. note:: Multipart uploads initiated before upgrading to a release with this
   behavior do not record their compression ``type``, so their remaining parts
   use the current setting. If the setting changes while such an upload is in
   progress, CompleteMultipartUpload can fail with ``InvalidPart``. Avoid
   changing the compression ``type`` until all RGW daemons are upgraded and
   multipart uploads that were in progress during the upgrade have completed.

.. note:: For encrypted multipart uploads, toggling the
   :ref:`feature_compress_encrypted` zonegroup feature while an upload is in
   progress can still cause CompleteMultipartUpload to fail with
   ``InvalidPart``.

Multisite Sync
--------------

Replicated objects follow the destination zone's compression settings for
their storage class. If an object is already compressed with the ``type`` that
the destination zone uses for its storage class, multisite sync transfers it as
stored, so the source zone doesn't decompress it and the destination zone
doesn't recompress it. Otherwise the source zone decompresses the object, and
the destination zone compresses it according to its settings.

An object that is transferred as stored keeps its compression even if it is
smaller than the destination zone's minimum size. When the destination zone
compresses an object itself, its minimum size applies to the whole object, so
a small object that was uploaded in parts can be stored uncompressed.

Objects are transferred decompressed if the source zone runs an older release,
if the gateway that syncs the object cannot load the plugin for the type, or
if ``rgw_sync_obj_skip_decompress`` is disabled on the gateways that run sync
in the destination zone. Unencrypted objects copied from another
zonegroup are always transferred decompressed. ``rgw_sync_obj_etag_verify``
does not verify objects that are transferred compressed.


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

The ``size_utilized`` and ``size_kb_utilized`` fields represent the actual
bytes stored on disk, in bytes and kilobytes respectively. This value reflects
the effects of both compression and encryption:

* With compression only: size after compression
* With AEAD encryption only (e.g., AES-256-GCM): size after encryption
  (includes authentication tag overhead)
* With both compression and encryption: size after compression then encryption

See :ref:`radosgw-encryption` for details on encryption algorithms.

