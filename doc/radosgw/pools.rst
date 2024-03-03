=====
Pools
=====

The Ceph Object Gateway uses several pools for its various storage needs,
which are listed in the Zone object (see ``radosgw-admin zone get``). A
single zone named ``default`` is created automatically with pool names
starting with ``default.rgw.``, but a `Multisite Configuration`_ will have
multiple zones.

Tuning
======

When ``radosgw`` first tries to operate on a zone pool that does not exist, it
will create that pool with the default values from ``osd pool default pg num``
and ``osd pool default pgp num``. These defaults are sufficient for some pools,
but others (especially those listed in ``placement_pools`` for the bucket index
and data) will require additional tuning. See `Pools
<http://docs.ceph.com/en/latest/rados/operations/pools/#pools>`__ for details
on pool creation.

.. _radosgw-pool-namespaces:

Pool Namespaces
===============

.. versionadded:: Luminous

Pool names particular to a zone follow the naming convention
``{zone-name}.pool-name``. For example, a zone named ``us-east`` will
have the following pools:

-  ``.rgw.root``

-  ``us-east.rgw.control``

-  ``us-east.rgw.meta``

-  ``us-east.rgw.log``

-  ``us-east.rgw.buckets.index``

-  ``us-east.rgw.buckets.data``

The zone definitions list several more pools than that, but many of those
are consolidated through the use of rados namespaces. For example, all of
the following pool entries use namespaces of the ``us-east.rgw.meta`` pool::

    "user_keys_pool": "us-east.rgw.meta:users.keys",
    "user_email_pool": "us-east.rgw.meta:users.email",
    "user_swift_pool": "us-east.rgw.meta:users.swift",
    "user_uid_pool": "us-east.rgw.meta:users.uid",

.. _`Multisite Configuration`: ../multisite
