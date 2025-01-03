==================================
Pool Placement and Storage Classes
==================================

.. contents::

Placement Targets
=================

.. versionadded:: Jewel

Placement targets control which `Pools`_ are associated with a particular
bucket. A bucket's placement target is selected on creation, and cannot be
modified. The ``radosgw-admin bucket stats`` command will display its
``placement_rule``.

The zonegroup configuration contains a list of placement targets with an
initial target named ``default-placement``. The zone configuration then maps
each zonegroup placement target name onto its local storage. This zone
placement information includes the ``index_pool`` name for the bucket index,
the ``data_extra_pool`` name for metadata about incomplete multipart uploads,
and a ``data_pool`` name for each storage class.

.. _storage_classes:

Storage Classes
===============

.. versionadded:: Nautilus

Storage classes specify the placement of object data. S3 Bucket
Lifecycle (LC) rules can automate the transition of objects between storage classes.

Storage classes are defined in terms of placement targets. Each zonegroup
placement target lists its available storage classes with an initial class
named ``STANDARD``. The zone configuration is responsible for providing a
``data_pool`` pool name for each of the zonegroup's storage classes.

Zonegroup/Zone Configuration
============================

Placement configuration is performed with ``radosgw-admin`` commands on
the zonegroups and zones.

The zonegroup placement configuration can be queried with:

::

  $ radosgw-admin zonegroup get
  {
      "id": "ab01123f-e0df-4f29-9d71-b44888d67cd5",
      "name": "default",
      "api_name": "default",
      ...
      "placement_targets": [
          {
              "name": "default-placement",
              "tags": [],
              "storage_classes": [
                  "STANDARD"
              ]
          }
      ],
      "default_placement": "default-placement",
      ...
  }

The zone placement configuration can be queried with:

::

  $ radosgw-admin zone get
  {
      "id": "557cdcee-3aae-4e9e-85c7-2f86f5eddb1f",
      "name": "default",
      "domain_root": "default.rgw.meta:root",
      ...
      "placement_pools": [
          {
              "key": "default-placement",
              "val": {
                  "index_pool": "default.rgw.buckets.index",
                  "storage_classes": {
                      "STANDARD": {
                          "data_pool": "default.rgw.buckets.data"
                      }
                  },
                  "data_extra_pool": "default.rgw.buckets.non-ec",
                  "index_type": 0,
                  "inline_data": true
              }
          }
      ],
      ...
  }

.. note:: If you have not done any previous `Multisite Configuration`_,
          a ``default`` zone and zonegroup are created for you, and changes
          to the zone/zonegroup will not take effect until the Ceph Object
          Gateways are restarted. If you have created a realm for multisite,
          the zone/zonegroup changes will take effect once the changes are
          committed with ``radosgw-admin period update --commit``.

Adding a Placement Target
-------------------------

To create a new placement target named ``temporary``, start by adding it to
the zonegroup:

::

  $ radosgw-admin zonegroup placement add \
        --rgw-zonegroup default \
        --placement-id temporary

Then provide the zone placement info for that target:

::

  $ radosgw-admin zone placement add \
        --rgw-zone default \
        --placement-id temporary \
        --data-pool default.rgw.temporary.data \
        --index-pool default.rgw.temporary.index \
        --data-extra-pool default.rgw.temporary.non-ec

.. note:: With default placement target settings, RGW stores an object's first data chunk in the RADOS "head" object along
          with XATTR metadata. The `--placement-inline-data=false` flag may be passed with the `zone placement add` or
          `zone placement modify` commands to change this behavior for new objects stored on the target.
          When data is stored inline (default), it may provide an advantage for read/write workloads since the first chunk of
          an object's data can be retrieved/stored in a single librados call along with object metadata. On the other hand, a
          target that does not store data inline can provide a performance benefit for RGW client delete requests when
          the BlueStore DB is located on faster storage than bucket data since it eliminates the need to access
          slower devices synchronously while processing the client request. In that case, data associated with the deleted
          objects is removed asynchronously in the background by garbage collection.                                          

.. _adding_a_storage_class:

Adding a Storage Class
----------------------

To add a new storage class named ``STANDARD_IA`` to the ``default-placement`` target,
start by adding it to the zonegroup:

::

  $ radosgw-admin zonegroup placement add \
        --rgw-zonegroup default \
        --placement-id default-placement \
        --storage-class STANDARD_IA

Then provide the zone placement info for that storage class:

::

  $ radosgw-admin zone placement add \
        --rgw-zone default \
        --placement-id default-placement \
        --storage-class STANDARD_IA \
        --data-pool default.rgw.glacier.data \
        --compression lz4

Customizing Placement
=====================

Default Placement
-----------------

By default, new buckets will use the zonegroup's ``default_placement`` target.
This zonegroup setting can be changed with:

::

  $ radosgw-admin zonegroup placement default \
        --rgw-zonegroup default \
        --placement-id new-placement

User Placement
--------------

A Ceph Object Gateway user can override the zonegroup's default placement
target by setting a non-empty ``default_placement`` field in the user info.
Similarly, the ``default_storage_class`` can override the ``STANDARD``
storage class applied to objects by default.

::

  $ radosgw-admin user info --uid testid
  {
      ...
      "default_placement": "",
      "default_storage_class": "",
      "placement_tags": [],
      ...
  }

If a zonegroup's placement target contains any ``tags``, users will be unable
to create buckets with that placement target unless their user info contains
at least one matching tag in its ``placement_tags`` field. This can be useful
to restrict access to certain types of storage.

The ``radosgw-admin`` command can modify these fields directly with:

::

  $ radosgw-admin user modify \
        --uid <user-id> \
        --placement-id <default-placement-id> \
        --storage-class <default-storage-class> \
        --tags <tag1,tag2>

.. _s3_bucket_placement:

S3 Bucket Placement
-------------------

When creating a bucket with the S3 protocol, a placement target can be
provided as part of the LocationConstraint to override the default placement
targets from the user and zonegroup.

Normally, the LocationConstraint must match the zonegroup's ``api_name``:

::

  <LocationConstraint>default</LocationConstraint>

A custom placement target can be added to the ``api_name`` following a colon:

::

  <LocationConstraint>default:new-placement</LocationConstraint>

Swift Bucket Placement
----------------------

When creating a bucket with the Swift protocol, a placement target can be
provided in the HTTP header ``X-Storage-Policy``:

::

  X-Storage-Policy: new-placement

Using Storage Classes
=====================

All placement targets have a ``STANDARD`` storage class which is applied to
new objects by default. The user can override this default with its
``default_storage_class``.

To create an object in a non-default storage class, provide that storage class
name in an HTTP header with the request. The S3 protocol uses the
``X-Amz-Storage-Class`` header, while the Swift protocol uses the
``X-Object-Storage-Class`` header.

S3 Object Lifecycle Management can then be used to move object data between
storage classes using ``Transition`` actions.

When using AWS S3 SDKs such as ``boto3``, it is important that
storage class names match those provided by AWS S3, or else the SDK
will drop the request and raise an exception.  Moreover, some S3 clients
and libraries expect AWS-specific behavior when a storage class named
or prefixed with ``GLACIER`` is used and thus will fail when accessing
Ceph RGW services.  For this reason we advise that other storage class
names be used with Ceph, including ``INTELLIGENT-TIERING``, ``STANDARD_IA``,
``REDUCED_REDUNDANCY``, and ``ONEZONE_IA``. Custom storage class names like
``CHEAPNDEEP`` are accepted by Ceph but might not be by some clients and
libraries.

.. _`Pools`: ../pools
.. _`Multisite Configuration`: ../multisite
