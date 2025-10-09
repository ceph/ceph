:orphan:

===============================================
ceph-create-keys -- ceph keyring generate tool
===============================================

.. program:: ceph-create-keys

Synopsis
========

| **ceph-create-keys** [-h] [-v] [-t seconds] [--cluster *name*] --id *id*


Description
===========

:program:`ceph-create-keys` is a utility to generate bootstrap keyrings using
the given monitor when it is ready.

It creates following auth entities (or users)

``client.admin``

    and its key for your client host.

``client.bootstrap-{osd, rgw, mds}``

    and their keys for bootstrapping corresponding services

To list all users in the cluster::

    ceph auth ls


Options
=======

.. option:: --cluster

   name of the cluster (default 'ceph').

.. option:: -t

   time out after **seconds** (default: 600) waiting for a response from the monitor

.. option:: -i, --id

   id of a ceph-mon that is coming up. **ceph-create-keys** will wait until it joins quorum.

.. option:: -v, --verbose

   be more verbose.


Availability
============

**ceph-create-keys** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please refer
to the Ceph documentation at https://docs.ceph.com for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
