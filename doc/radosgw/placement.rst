==============
Pool Placement
==============

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
and a ``data_pool`` name for object data.

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
                  "data_pool": "default.rgw.buckets.data",
                  "data_extra_pool": "default.rgw.buckets.non-ec",
                  "index_type": 0
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
        --data-extra-pool default.rgw.temporary.non-ec \
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

::

  $ radosgw-admin user info --uid testid
  {
      ...
      "default_placement": "",
      "placement_tags": [],
      ...
  }

If a zonegroup's placement target contains any ``tags``, users will be unable
to create buckets with that placement target unless their user info contains
at least one matching tag in its ``placement_tags`` field. This can be useful
to restrict access to certain types of storage.

The ``radosgw-admin`` command cannot modify these fields directly, so the json
format must be edited manually:

::

  $ radosgw-admin metadata get user:<user-id> > user.json
  $ vi user.json
  $ radosgw-admin metadata put user:<user-id> < user.json

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

.. _`Pools`: ../pools
.. _`Multisite Configuration`: ../multisite
