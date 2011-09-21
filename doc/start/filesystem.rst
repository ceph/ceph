========================
 Starting to use CephFS
========================

Introduction
============

The Ceph Distributed File System is a scalable network file system
aiming for high performance, large data storage, and POSIX
compliance. For more information, see :ref:`cephfs`.


Installation
============

To use `Ceph DFS`, you need to install a Ceph cluster. Follow the
instructions in :doc:`/ops/install/index`. Continue with these
instructions once you have a healthy cluster running.


Setup
=====

First, we need a ``client`` key that is authorized to access the
filesystem. Follow the instructions in :ref:`add-new-key`. Let's set
the ``id`` of the key to be ``foo``. You could set up one key per
machine mounting the filesystem, or let them share a single key; your
call. Make sure the keyring containing the new key is available on the
machine doing the mounting.


Usage
=====

There are two main ways of using the filesystem. You can use the Ceph
client implementation that is included in the Linux kernel, or you can
use the FUSE userspace filesystem. For an explanation of the
tradeoffs, see :ref:`Status <cfuse-kernel-tradeoff>`. Follow the
instructions in :ref:`mounting`.

Once you have the filesystem mounted, you can use it like any other
filesystem. The changes you make on one client will be visible to
other clients that have mounted the same filesystem.

You can now use snapshots, automatic disk usage tracking, and all
other features `Ceph DFS` has. All read and write operations will be
automatically distributed across your whole storage cluster, giving
you the best performance available.

.. todo:: links for snapshots, disk usage

You can use :doc:`cephfs </man/8/cephfs>`\(8) to interact with
``cephfs`` internals.


.. rubric:: Example: Home directories

If you locate UNIX user account home directories under a Ceph
filesystem mountpoint, the same files will be available from all
machines set up this way.

Users can move between hosts, or even use them simultaneously, and
always access the same files.


.. rubric:: Example: HPC

In a HPC (High Performance Computing) scenario, hundreds or thousands
of machines could all mount the Ceph filesystem, and worker processes
on all of the machines could then access the same files for
input/output.

.. todo:: point to the lazy io optimization
