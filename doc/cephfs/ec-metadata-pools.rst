.. _cephfs-ec-metadata-pools:

============================================================
Erasure-coded metadata and default data pools (Tech Preview)
============================================================

.. warning:: Using erasure-coded (EC) pools as the CephFS metadata pool or the
   default data pool is a **tech preview** feature. Performance testing of these
   configurations has not been completed. It is not recommended for production
   use, and the behavior and recommendations described on this page may change
   in future releases. See `Why this is a tech preview`_.

By default, CephFS uses replicated pools for the metadata pool and the default
data pool. This page describes how to use erasure-coded pools for these
pools instead.

Using EC pools as extra data pools (data pools other than the default data
pool) is not a tech preview feature. See :ref:`create-fs` and
:ref:`adding-data-pool-to-file-system`.

Why use EC metadata or default data pools
=========================================

Replicated pools remain the recommended choice and are the best-tested
configuration. Allowing EC metadata and default data pools has the following
benefits:

- It removes a barrier that currently makes it harder for users to set up
  CephFS file systems. Users are no longer required to create a replicated
  pool solely to satisfy the default data pool requirement.
- It gives users more options about the configuration they wish to use.
- It enables performance testing of EC metadata and default data pools with
  typical CephFS workloads.

We do not expect EC metadata pools to be recommended ahead of replicated pools
for metadata. Replicated pools are expected to have better performance than EC
pools for metadata.

EC pools may be preferable to replicated pools as the default data pool, if
performance testing shows good results.

Why this is a tech preview
==========================

Performance testing of EC metadata pools and EC default data pools with typical
CephFS workloads has not been completed. Until it has, the performance of these
configurations compared with replicated pools is not known, and the
recommendation to use replicated pools remains in place. Use EC metadata and
default data pools only in test environments, or where you are able to evaluate
their performance for your workload.

Requirements
============

- The OSDs backing the pools must use :term:`BlueStore`.
- EC overwrites must be enabled on the pool.
- EC optimizations are required for EC metadata pools, and recommended for EC
  default data pools. Enabling EC optimizations improves the performance of an
  EC pool, in particular for small-object reads and writes, such as when CephFS
  updates inode backtraces in the default data pool.
- A pool used for CephFS metadata must support RADOS *omap* data structures.
  Replicated pools always support them. EC pools support them only when EC
  optimizations are enabled.

Using EC pools with CephFS
==========================

Enable EC overwrites, and then EC optimizations, on each EC pool that will be
used for the metadata pool or the default data pool:

.. code:: bash

    ceph osd pool set my_ec_pool allow_ec_overwrites true
    ceph osd pool set my_ec_pool allow_ec_optimizations true

When you create the file system, you must use the ``--force`` flag to specify
an EC pool as the metadata pool or the default data pool.

Here is a complete example of how a file system can be set up using an EC
metadata pool and an EC default data pool:

.. code:: bash

    ceph osd erasure-code-profile set ec_profile plugin=isa k=4 m=2 technique=reed_sol_van stripe_unit=4k crush-failure-domain=osd

    ceph osd pool create cephfs_metadata_ec erasure ec_profile
    ceph osd pool set cephfs_metadata_ec allow_ec_overwrites true
    ceph osd pool set cephfs_metadata_ec allow_ec_optimizations true

    ceph osd pool create cephfs_data_ec erasure ec_profile
    ceph osd pool set cephfs_data_ec allow_ec_overwrites true
    ceph osd pool set cephfs_data_ec allow_ec_optimizations true

    ceph fs new fs_name cephfs_metadata_ec cephfs_data_ec --force

The erasure-code-profile values used above were selected at random for this
example. You may use any valid erasure-code-profile for this purpose. The
metadata and data pools can use different erasure-code-profiles; there is no
requirement for them to match.

Creating a file system with an EC metadata pool and a replicated default data
pool is discouraged. Creating a file system with this configuration also
requires the ``--yes-i-really-really-mean-it`` flag:

.. code:: bash

    ceph fs new fs_name cephfs_metadata_ec cephfs_data_replicated --force --yes-i-really-really-mean-it

See also
========

- :ref:`create-fs`
- :ref:`ecpool`
- :ref:`file-layouts`
- :doc:`/rados/operations/pools`
