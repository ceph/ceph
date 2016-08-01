:orphan:

===============================================
ceph-create-keys -- ceph keyring generate tool
===============================================

.. program:: ceph-create-keys

Synopsis
========

| **ceph-create-keys** [-h] [-v] [--cluster *name*] --id *id*


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

    ceph auth list


Options
=======

.. option:: --cluster

   name of the cluster (default 'ceph').

.. option:: -i, --id

   id of a ceph-mon that is coming up. **ceph-create-keys** will wait until it joins quorum.

.. option:: -v, --verbose

   be more verbose.


Availability
============

**ceph-create-keys** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8)
