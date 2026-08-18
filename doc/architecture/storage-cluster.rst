.. _arch-ceph-storage-cluster:

The Ceph Storage Cluster
========================

Ceph provides an infinitely scalable :term:`Ceph Storage Cluster` based upon
:abbr:`RADOS (Reliable Autonomic Distributed Object Store)`, a reliable,
distributed storage service that uses the intelligence in each of its nodes to
secure the data it stores and to provide that data to :term:`client`\s. See
Sage Weil's "`The RADOS Object Store
<https://ceph.io/en/news/blog/2009/the-rados-distributed-object-store/>`_" blog
post for a brief explanation of RADOS and see `RADOS - A Scalable, Reliable
Storage Service for Petabyte-scale Storage Clusters`_ for an exhaustive
explanation of :term:`RADOS`.

A Ceph Storage Cluster consists of multiple types of daemons:

- :term:`Ceph Monitor`
- :term:`Ceph OSD Daemon`
- :term:`Ceph Manager`
- :term:`Ceph Metadata Server`

.. _arch_monitor:

Ceph Monitors maintain the master copy of the cluster map, which they provide
to Ceph clients. The existence of multiple monitors in the Ceph cluster ensures
availability if one of the monitor daemons or its host fails.

A Ceph OSD Daemon checks its own state and the state of other OSDs and reports
back to monitors.

A Ceph Manager serves as an endpoint for monitoring, orchestration, and plug-in
modules.

A Ceph Metadata Server (MDS) manages file metadata when CephFS is used to
provide file services.

Storage cluster clients and :term:`Ceph OSD Daemon`\s use the CRUSH algorithm
to compute information about the location of data.  By using the CRUSH
algorithm, clients and OSDs avoid being bottlenecked by a central lookup table.
Ceph's high-level features include a native interface to the Ceph Storage
Cluster via ``librados`` and a number of service interfaces built on top of
``librados``.

Storing Data
------------

The Ceph Storage Cluster receives data from :term:`Ceph Client`\s--whether it
comes through a :term:`Ceph Block Device`, :term:`Ceph Object Storage`, the
:term:`Ceph File System`, or a custom implementation that you create by using
``librados``. The data received by the Ceph Storage Cluster is stored as RADOS
objects. Each object is stored on an :term:`Object Storage Device` (this is
also called an "OSD"). Ceph OSDs control read, write, and replication
operations on storage drives. The default BlueStore back end stores objects
in a monolithic, database-like fashion.

.. ditaa::

           /------\       +-----+       +-----+
           | obj  |------>| {d} |------>| {s} |
           \------/       +-----+       +-----+

            Object         OSD          Drive

Ceph OSD Daemons store data as objects in a flat namespace. This means that
objects are not stored in a hierarchy of directories. An object has an
identifier, binary data, and metadata consisting of name/value pairs.
:term:`Ceph Client`\s determine the semantics of the object data. For example,
CephFS uses metadata to store file attributes such as the file owner, the
created date, and the last modified date.


.. ditaa::

           /------+------------------------------+----------------\
           | ID   | Binary Data                  | Metadata       |
           +------+------------------------------+----------------+
           | 1234 | 0101010101010100110101010010 | name1 = value1 |
           |      | 0101100001010100110101010010 | name2 = value2 |
           |      | 0101100001010100110101010010 | nameN = valueN |
           \------+------------------------------+----------------/

.. note:: An object ID is unique across the entire cluster, not just the local
   filesystem.

.. _RADOS - A Scalable, Reliable Storage Service for Petabyte-scale Storage Clusters: https://ceph.io/assets/pdfs/weil-rados-pdsw07.pdf
