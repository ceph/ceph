:orphan:

===============================================
ceph-create-keys -- ceph keyring generate tool
===============================================

.. program:: ceph-create-keys

Synopsis
========

| **ceph-create-keys**


Description
===========

:program:`ceph-create-keys` is obsolete. Since the Nautilus release the
Monitors create the ``client.admin`` and ``client.bootstrap-*`` keys
themselves when they form a quorum. This command does nothing except print a
message that says so. It accepts no options, and it will be removed in a
future release. Remove any call to it from scripts and tools.

To list all users and their keys in the cluster, run::

    ceph auth ls

To retrieve a bootstrap key, run a command of the following form::

    ceph auth get client.bootstrap-osd


Availability
============

**ceph-create-keys** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please refer
to the Ceph documentation at https://docs.ceph.com for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
