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

.. option:: --conffile [CONFFILE]

   Path to cluster configuration file

.. option:: -d [DELAY], --delay [DELAY]

   Refresh interval in seconds (default: 1)

.. option:: --dump

   Dump the metrics to stdout

.. option:: --dumpfs <fs_name>

   Dump the metrics of the given filesystem to stdout

Descriptions of fields
======================

.. describe:: chit

   cap hit rate

.. describe:: dlease

   dentry lease rate

.. describe:: ofiles

   number of opened files

.. describe:: oicaps

   number of pinned caps

.. describe:: oinodes

   number of opened inodes

.. describe:: rtio

   total size of read IOs

.. describe:: wtio

   total size of write IOs

.. describe:: raio

   average size of read IOs

.. describe:: waio

   average size of write IOs

.. describe:: rsp

   speed of read IOs compared with the last refresh

.. describe:: wsp

   speed of write IOs compared with the last refresh

.. describe:: rlatavg

   average read latency

.. describe:: rlatsd

   standard deviation (variance) for read latency

.. describe:: wlatavg

   average write latency

.. describe:: wlatsd

   standard deviation (variance) for write latency

.. describe:: mlatavg

   average metadata latency

.. describe:: mlatsd

   standard deviation (variance) for metadata latency

Availability
============

**cephfs-top** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to the Ceph documentation at
http://ceph.com/ for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8)
