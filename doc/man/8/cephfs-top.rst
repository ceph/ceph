:orphan:

==========================================
 cephfs-top -- Ceph Filesystem Top Utility
==========================================

.. program:: cephfs-top

Synopsis
========

| **cephfs-top** [flags]


Description
===========

**cephfs-top** provides top(1) like functionality for Ceph Filesystem.
Various client metrics are displayed and updated in realtime.

Ceph Metadata Servers periodically send client metrics to Ceph Manager.
``Stats`` plugin in Ceph Manager provides an interface to fetch these metrics.

Options
=======

.. option:: --cluster

   Cluster: Ceph cluster to connect. Defaults to ``ceph``.

.. option:: --id

   Id: Client used to connect to Ceph cluster. Defaults to ``fstop``.

.. option:: --selftest

   Perform a selftest. This mode performs a sanity check of ``stats`` module.

Availability
============

**cephfs-top** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to the Ceph documentation at
http://ceph.com/ for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8)
