=============
Zone Features
=============

Some features require support from all cooperating radosgws before they can be enabled. Each zone lists its ``supported_features``, and each zonegroup lists its ``enabled_features``. Before a feature can be enabled in the zonegroup, it must be supported by all of its zones.

On creation of new zones and zonegroups, all known features are supported and some features (see table below) are enabled by default. After upgrading an existing zone, however, new features must be enabled manually.

Supported Features
------------------

+-----------------------------------+---------+----------+
| Feature                           | Release | Default  |
+===================================+=========+==========+
| :ref:`feature_resharding`         | Reef    | Enabled  |
+-----------------------------------+---------+----------+
| :ref:`feature_compress_encrypted` | Reef    | Disabled |
+-----------------------------------+---------+----------+

.. _feature_resharding:

resharding
~~~~~~~~~~

This feature allows buckets to be resharded in a multisite configuration
without interrupting the replication of their objects. When
``rgw_dynamic_resharding`` is enabled, it runs on each zone independently, and
zones may choose different shard counts for the same bucket. When buckets are
resharded manually with ``radosgw-admin bucket reshard``, only that zone's
bucket is modified. A zone feature should only be marked as supported after all
of its RGWs and OSDs have upgraded.

.. note:: Dynamic resharding is not supported in multisite deployments prior to
   the Reef release.


.. _feature_compress_encrypted:

compress-encrypted
~~~~~~~~~~~~~~~~~~

This feature enables support for combining `Server-Side Encryption`_ and
`Compression`_ on the same object. Object data gets compressed before encryption.
Prior to Reef, multisite would not replicate such objects correctly, so all zones
must upgrade to Reef or later before enabling.

.. warning:: The compression ratio may leak information about the encrypted data,
   and allow attackers to distinguish whether two same-sized objects might contain
   the same data. Due to these security considerations, this feature is disabled
   by default.

Commands
--------

Add support for a zone feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the cluster that contains the given zone:

.. prompt:: bash $

   radosgw-admin zone modify --rgw-zone={zone-name} --enable-feature={feature-name}
   radosgw-admin period update --commit

.. note:: The ``period update`` command only works if the zone belongs to a realm.
   Otherwise, all radosgws will need to restart before they notice the change.


Remove support for a zone feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the cluster that contains the given zone:

.. prompt:: bash $

   radosgw-admin zone modify --rgw-zone={zone-name} --disable-feature={feature-name}
   radosgw-admin period update --commit

Enable a zonegroup feature
~~~~~~~~~~~~~~~~~~~~~~~~~~

On any cluster in the realm:

.. prompt:: bash $

   radosgw-admin zonegroup modify --rgw-zonegroup={zonegroup-name} --enable-feature={feature-name}
   radosgw-admin period update --commit

Disable a zonegroup feature
~~~~~~~~~~~~~~~~~~~~~~~~~~~

On any cluster in the realm:

.. prompt:: bash $

   radosgw-admin zonegroup modify --rgw-zonegroup={zonegroup-name} --disable-feature={feature-name}
   radosgw-admin period update --commit


.. _`Server-Side Encryption`: ../encryption
.. _`Compression`: ../compression
