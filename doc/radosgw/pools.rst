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

If ``radosgw`` attempts an operation on a zone pool that does not
exist, it first creates that pool with the default values from
``osd pool default pg num`` and ``osd pool default pgp num``. These defaults
are sufficient for some pools, but others (especially those listed in
``placement_pools`` for the bucket index and data) require additional
tuning. Use the `Ceph Placement Group’s per Pool
Calculator <https://old.ceph.com/pgcalc/>`__ to calculate a suitable number of
placement groups for these pools. See
`Pools <http://docs.ceph.com/en/latest/rados/operations/pools/#pools>`__
for details on pool creation.

.. note:: ``radosgw`` by default creates replicated pools.

.. note:: 

   All changes to the default settings must be made before ``radosgw`` is started.
   This means that any pools that deviate from the default must be created prior
   to the starting of ``radosgw``.

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


.. _create-ec-pool: 

Create an Erasure-coded Pool
----------------------------

The process of creating an erasure-coded pool has two steps: 

   #. Create a pool with the type "erasure". 
   #. Associate the pool with the "rgw" application.

.. warning::
   The creation of an erasure-coded pool must happen before RGW deployment. After RGW starts up, it 
   automatically creates pools of the "replicated" type unless pools of the "erasure" type already
   exist.

.. note::
   This procedure has been designed to be as close to copy-and-pasteable as possible. However, you 
   cannot use this procedure blindly. You must gather the following pieces of information and insert 
   them into the correct places in the commands in this procedure:

   #. The total number of placement groups in the pool.
   #. The total number of placement groups for placement purposes (probably the same as the total number of placement groups in the pool).
   #. The erasure code profile. See :ref:`erasure-code-profiles` and :ref:`osd-ec-profile-set` for more information about this.
   #. The number of objects expected to be in the pool.
   #. A preferred autoscale mode ("on", "off", and "warn" are the options).

   If you are unsure of any of these pieces of information, read :ref:`createpool`.

**Procedure for Creating an Erasure-coded Pool**

#. Create a pool with type "erasure":

   .. prompt:: bash $

      ceph osd pool create default.rgw.buckets.data [{pg-num} [{pgp-num}]] erasure [erasure-code-profile] erasure-code [expected_num_objects] [--autoscale-mode=<on,off,warn>]

   .. note::
      In this command, "default.rgw.buckets.data" is the name of the object data pool (this is referred to in the second
      step of this procedure as {pool-name}, so make a note of it or make a note of whatever your object data pool's name 
      is). RGW can make use of several different kinds of pools, but only the object data pool can use erasure coding.

      Pool names vary by zone. For example, a zone named "foo" uses an object data pool called "foo.rgw.buckets.data". Read
      :ref:`radosgw-pool-namespaces` for more information about pool namespaces.

#. Associate the pool with the application name "rgw":

   .. prompt:: bash $

      ceph osd pool application enable {pool-name} rgw
