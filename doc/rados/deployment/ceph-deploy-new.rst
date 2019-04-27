==================
 Create a Cluster
==================

The first step in using Ceph with ``ceph-deploy`` is to create a new Ceph
cluster. A new Ceph cluster has:

- A Ceph configuration file, and
- A monitor keyring.

The Ceph configuration file consists of at least:

- Its own filesystem ID (``fsid``)
- The initial monitor(s) hostname(s), and
- The initial monitor(s) and IP address(es).

For additional details, see the `Monitor Configuration Reference`_.

The ``ceph-deploy`` tool also creates a monitor keyring and populates it with a
``[mon.]`` key.  For additional details, see the `Cephx Guide`_.


Usage
-----

To create a cluster with ``ceph-deploy``, use the ``new`` command and specify
the host(s) that will be initial members of the monitor quorum. ::

	ceph-deploy new {host [host], ...}

For example::

	ceph-deploy new mon1.foo.com
	ceph-deploy new mon{1,2,3}

The ``ceph-deploy`` utility will use DNS to resolve hostnames to IP
addresses.  The monitors will be named using the first component of
the name (e.g., ``mon1`` above).  It will add the specified host names
to the Ceph configuration file. For additional details, execute::

	ceph-deploy new -h



.. _Monitor Configuration Reference: ../../configuration/mon-config-ref
.. _Cephx Guide: ../../../dev/mon-bootstrap#secret-keys
