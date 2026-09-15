.. index:: architecture; Ceph Clients

.. _architecture_ceph_clients:

Ceph Clients
============

Ceph Clients include a number of service interfaces. These include:

- **Block Devices:** The :term:`Ceph Block Device` (a.k.a., RBD) service
  provides resizable, thin-provisioned block devices that can be snapshotted
  and cloned. Ceph stripes a block device across the cluster for high
  performance. Ceph supports both kernel objects (KO) and a QEMU hypervisor
  that uses ``librbd`` directly--avoiding the kernel object overhead for
  virtualized systems.

- **Object Storage:** The :term:`Ceph Object Storage` (a.k.a., RGW) service
  provides RESTful APIs with interfaces that are compatible with Amazon S3
  and OpenStack Swift.

- **Filesystem**: The :term:`Ceph File System` (CephFS) service provides
  a POSIX compliant filesystem usable with ``mount`` or as
  a filesystem in user space (FUSE).

Ceph can run additional instances of OSDs, MDSs, and monitors for scalability
and high availability. The following diagram depicts the high-level
architecture.

.. ditaa::

            +--------------+  +----------------+  +-------------+
            | Block Device |  | Object Storage |  |   CephFS    |
            +--------------+  +----------------+  +-------------+

            +--------------+  +----------------+  +-------------+
            |    librbd    |  |     librgw     |  |  libcephfs  |
            +--------------+  +----------------+  +-------------+

            +---------------------------------------------------+
            |      Ceph Storage Cluster Protocol (librados)     |
            +---------------------------------------------------+

            +---------------+ +---------------+ +---------------+
            |      OSDs     | |      MDSs     | |    Monitors   |
            +---------------+ +---------------+ +---------------+


.. index:: architecture; Ceph Object Storage

Ceph Object Storage
-------------------

The Ceph Object Storage daemon, ``radosgw``, is a FastCGI service that provides
a RESTful_ HTTP API to store objects and metadata. It layers on top of the Ceph
Storage Cluster with its own data formats, and maintains its own user database,
authentication, and access control. The RADOS Gateway uses a unified namespace,
which means you can use either the OpenStack Swift-compatible API or the Amazon
S3-compatible API. For example, you can write data using the S3-compatible API
with one application and then read data using the Swift-compatible API with
another application.

.. topic:: S3/Swift Objects and Store Cluster Objects Compared

   Ceph's Object Storage uses the term *object* to describe the data it stores.
   S3 and Swift objects are not the same as the objects that Ceph writes to the
   Ceph Storage Cluster. Ceph Object Storage objects are mapped to Ceph Storage
   Cluster objects. The S3 and Swift objects do not necessarily
   correspond in a 1:1 manner with an object stored in the storage cluster. It
   is possible for an S3 or Swift object to map to multiple Ceph objects.

See :ref:`object-gateway` for details.


.. index:: Ceph Block Device; block device; RBD; Rados Block Device

Ceph Block Device
-----------------

A Ceph Block Device stripes a block device image over multiple objects in the
Ceph Storage Cluster, where each object gets mapped to a placement group and
distributed, and the placement groups are spread across separate ``ceph-osd``
daemons throughout the cluster.

.. important:: Striping allows RBD block devices to perform better than a single
   server could!

Thin-provisioned snapshottable Ceph Block Devices are an attractive option for
virtualization and cloud computing. In virtual machine scenarios, people
typically deploy a Ceph Block Device with the ``rbd`` network storage driver in
QEMU/KVM, where the host machine uses ``librbd`` to provide a block device
service to the guest. Many cloud computing stacks use ``libvirt`` to integrate
with hypervisors. You can use thin-provisioned Ceph Block Devices with QEMU and
``libvirt`` to support OpenStack, OpenNebula and CloudStack
among other solutions.

While we do not provide ``librbd`` support with other hypervisors at this time,
you may also use Ceph Block Device kernel objects to provide a block device to a
client. Other virtualization technologies such as Xen can access the Ceph Block
Device kernel object(s). This is done with the  command-line tool ``rbd``.


.. index:: CephFS; Ceph File System; libcephfs; MDS; metadata server; ceph-mds

.. _arch-cephfs:

Ceph File System
----------------

The Ceph File System (CephFS) provides a POSIX-compliant filesystem as a
service that is layered on top of the object-based Ceph Storage Cluster.
CephFS files get mapped to objects that Ceph stores in the Ceph Storage
Cluster. Ceph Clients mount a CephFS filesystem as a kernel object or as
a Filesystem in User Space (FUSE).

.. ditaa::

            +-----------------------+  +------------------------+
            | CephFS Kernel Object  |  |      CephFS FUSE       |
            +-----------------------+  +------------------------+

            +---------------------------------------------------+
            |            CephFS Library (libcephfs)             |
            +---------------------------------------------------+

            +---------------------------------------------------+
            |      Ceph Storage Cluster Protocol (librados)     |
            +---------------------------------------------------+

            +---------------+ +---------------+ +---------------+
            |      OSDs     | |      MDSs     | |    Monitors   |
            +---------------+ +---------------+ +---------------+


The Ceph File System service includes the Ceph Metadata Server (MDS) deployed
with the Ceph Storage cluster. The purpose of the MDS is to store all the
filesystem metadata (directories, file ownership, access modes, etc) in
high-availability Ceph Metadata Servers where the metadata resides in memory.
The reason for the MDS (a daemon called ``ceph-mds``) is that simple filesystem
operations like listing a directory or changing a directory (``ls``, ``cd``)
would tax the Ceph OSD Daemons unnecessarily. So separating the metadata from
the data means that the Ceph File System can provide high performance services
without taxing the Ceph Storage Cluster.

CephFS separates the metadata from the data, storing the metadata in the MDS,
and storing the file data in one or more objects in the Ceph Storage Cluster.
The Ceph filesystem aims for POSIX compatibility. ``ceph-mds`` can run as a
single process, or it can be distributed out to multiple physical machines,
either for high availability or for scalability.

- **High Availability**: The extra ``ceph-mds`` instances can be `standby`,
  ready to take over the duties of any failed ``ceph-mds`` that was
  `active`. This is easy because all the data, including the journal, is
  stored on RADOS. The transition is triggered automatically by ``ceph-mon``.

- **Scalability**: Multiple ``ceph-mds`` instances can be `active`, and they
  will split the directory tree into subtrees (and shards of a single
  busy directory), effectively balancing the load amongst all `active`
  servers.

Combinations of `standby` and `active` etc are possible, for example
running 3 `active` ``ceph-mds`` instances for scaling, and one `standby`
instance for high availability.

.. _RESTful: https://en.wikipedia.org/wiki/RESTful
