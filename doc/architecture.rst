==============
 Architecture 
==============

Ceph provides an infinitely scalable object storage system. It is based 
upon :abbr:`RADOS (Reliable Autonomic Distributed Object Store)`, which
you can read about in 
`RADOS - A Scalable, Reliable Storage Service for Petabyte-scale Storage Clusters`_. 
Its high-level features include providing a native interface to the 
object storage system via ``librados``, and a number of service interfaces 
built on top of ``librados``. These include:

- **Block Devices:** The RADOS Block Device (RBD) service provides
  resizable, thin-provisioned block devices with snapshotting and 
  cloning. Ceph stripes a block device across the cluster for high
  performance. Ceph supports both kernel objects (KO) and a 
  QEMU hypervisor that uses ``librbd`` directly--avoiding the 
  kernel object overhead for virtualized systems.

- **RESTful Gateway:** The RADOS Gateway (RGW) service provides
  RESTful APIs with interfaces that are compatible with Amazon S3
  and OpenStack Swift. 
  
- **Ceph FS**: The Ceph Filesystem (CephFS) service provides 
  a POSIX compliant filesystem usable with ``mount`` or as 
  a filesytem in user space (FUSE). 
  
Ceph OSDs store all data--whether it comes through RBD, RGW, or 
CephFS--as objects in the object storage system. Ceph can run
additional instances of OSDs, MDSs, and monitors for scalability
and high availability. The following diagram depicts the 
high-level architecture. 

.. ditaa::  +--------+ +----------+ +-------+ +--------+ +------+
            | RBD KO | | QeMu RBD | |  RGW  | | CephFS | | FUSE |
            +--------+ +----------+ +-------+ +--------+ +------+
            +---------------------+           +-----------------+
            |       librbd        |           |    libcephfs    |
            +---------------------+           +-----------------+
            +---------------------------------------------------+
            |     librados (C, C++, Java, Python, PHP, etc.)    |
            +---------------------------------------------------+
            +---------------+ +---------------+ +---------------+
            |      OSDs     | |      MDSs     | |    Monitors   |
            +---------------+ +---------------+ +---------------+


.. _RADOS - A Scalable, Reliable Storage Service for Petabyte-scale Storage Clusters: http://ceph.com/papers/weil-rados-pdsw07.pdf


Removing Limitations
====================

Today's storage systems have demonstrated an ability to scale out, but with some
significant limitations: interfaces, session managers, and stateful sessions
with a centralized point of access often limit the scalability of today's
storage architectures. Furthermore, a centralized interface that dispatches
requests from clients to server nodes within a cluster and subsequently routes
responses from those server nodes back to clients will hit a scalability and/or
performance limitation.

Another problem for storage systems is the need to manually rebalance data when
increasing or decreasing the size of a data cluster. Manual rebalancing works
fine on small scales, but it is a nightmare at larger scales because hardware
additions are common and hardware failure becomes an expectation rather than an 
exception when operating at the petabyte scale and beyond. 

The operational challenges of managing legacy technologies with the burgeoning
growth in the demand for unstructured storage makes legacy technologies
inadequate for scaling into petabytes. Some legacy technologies (e.g., SAN) can
be considerably more expensive, and  more challenging to maintain when compared
to using commodity hardware. Ceph  uses commodity hardware, because it is
substantially less expensive to purchase (or to replace), and it only requires
standard system administration skills to  use it.

          
How Ceph Scales
===============

In traditional architectures, clients talk to a centralized component (e.g., a gateway, 
broker, API, facade, etc.), which acts as a single point of entry to a complex subsystem.
This imposes a limit to both performance and scalability, while introducing a single
point of failure (i.e., if the centralized component goes down, the whole system goes 
down, too).

Ceph uses a new and innovative approach. Ceph clients contact a Ceph monitor
and retrieve a copy of the cluster map. The :abbr:`CRUSH (Controlled Replication
Under Scalable Hashing)` algorithm allows a client to compute where data
*should* be stored, and enables the client to contact the primary OSD to store
or retrieve the data. The OSD also uses the CRUSH algorithm, but the OSD uses it
to compute where replicas of data should be stored (and for rebalancing). 
For a detailed discussion of CRUSH, see 
`CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data`_

The Ceph storage system supports the notion of 'Pools', which are logical
partitions for storing object data. Pools set ownership/access, the number of
object replicas, the number of placement groups, and the CRUSH rule set to use.
Each pool has a number of placement groups that are mapped dynamically to OSDs. 
When clients store data, CRUSH maps the object data to placement groups.
The following diagram depicts how CRUSH maps objects to placement groups, and
placement groups to OSDs.

.. ditaa:: 
           /-----\  /-----\  /-----\  /-----\  /-----\
           | obj |  | obj |  | obj |  | obj |  | obj |
           \-----/  \-----/  \-----/  \-----/  \-----/
              |        |        |        |        |
              +--------+--------+        +---+----+
              |                              |
              v                              v
   +-----------------------+      +-----------------------+
   |  Placement Group #1   |      |  Placement Group #2   |
   |                       |      |                       |
   +-----------------------+      +-----------------------+
               |                              |
               |      +-----------------------+---+
        +------+------+-------------+             |
        |             |             |             |
        v             v             v             v
   /----------\  /----------\  /----------\  /----------\ 
   |          |  |          |  |          |  |          |
   |  OSD #1  |  |  OSD #2  |  |  OSD #3  |  |  OSD #4  |
   |          |  |          |  |          |  |          |
   \----------/  \----------/  \----------/  \----------/  

Mapping objects to placement groups instead of directly to OSDs creates a layer
of indirection between the OSD and the client.  The cluster must be able to grow
(or shrink) and rebalance data dynamically. If the client "knew" which OSD had
the data, that would create a tight coupling between the client and the OSD.
Instead, the CRUSH algorithm maps the data to a placement group and then maps
the placement group to one or more OSDs. This layer of indirection allows Ceph
to rebalance dynamically when new OSDs come online. 

With a copy of the cluster map and the CRUSH algorithm, the client can compute
exactly which OSD to use when reading or writing a particular piece of data.

In a typical write scenario, a client uses the CRUSH algorithm to compute where
to store data, maps the data to a placement group, then looks at the CRUSH map
to identify the primary OSD for the placement group. Clients write data
to the identified placement group in the primary OSD. Then, the primary OSD with
its own copy of the CRUSH map identifies the secondary and tertiary OSDs for
replication purposes, and replicates the data to the appropriate placement
groups in the secondary and tertiary OSDs (as many OSDs as additional
replicas), and responds to the client once it has confirmed the data was
stored successfully.

.. ditaa:: +--------+     Write      +--------------+    Replica 1     +----------------+
           | Client |*-------------->| Primary OSD  |*---------------->| Secondary OSD  |
           |        |<--------------*|              |<----------------*|                |
           +--------+   Write  Ack   +--------------+  Replica 1 Ack   +----------------+
													    ^  *
                                           |  |        Replica 2       +----------------+
                                           |  +----------------------->|  Tertiary OSD  |
                                           +--------------------------*|                |
                                                     Replica 2 Ack     +----------------+


Since any network device has a limit to the number of concurrent connections it
can support, a centralized system has a low physical limit at high scales.  By
enabling clients to contact nodes directly, Ceph increases both performance and
total system capacity simultaneously, while removing a single point of failure.
Ceph clients can maintain a session when they need to, and with a particular
OSD instead of a centralized server.
          
.. _CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data: http://ceph.com/papers/weil-crush-sc06.pdf


Peer-Aware Nodes
================

Ceph's cluster map determines whether a node in a network is ``in`` the 
Ceph cluster or ``out`` of the Ceph cluster. 

.. ditaa:: +----------------+
           |                |
           |   Node ID In   |
           |                |
           +----------------+
                   ^
                   |
                   |
                   v
           +----------------+
           |                |
           |  Node ID Out   |
           |                |
           +----------------+

In many clustered architectures, the primary purpose of cluster membership
is so that a centralized interface knows which hosts it can access. Ceph
takes it a step further: Ceph's nodes are cluster aware. Each node knows 
about other nodes in the cluster. This enables Ceph's monitor, OSD, and 
metadata server daemons to interact directly with each other. One major 
benefit of this approach is that Ceph can utilize the CPU and RAM of its
nodes to easily perform tasks that would bog down a centralized server.

.. todo:: Explain OSD maps, Monitor Maps, MDS maps


Smart OSDs
==========

Ceph OSDs join a cluster and report on their status. At the lowest level, 
the OSD status is ``up`` or ``down`` reflecting whether or not it is 
running and able to service requests. If an OSD is ``down`` and ``in``
the cluster, this status may indicate the failure of the OSD. 

With peer awareness, OSDs can communicate with other OSDs and monitors
to perform tasks. OSDs take client requests to read data from or write
data to pools, which have placement groups. When a client makes a request
to write data to a primary OSD, the primary OSD knows how to determine 
which OSDs have the placement groups for the replica copies, and then
update those OSDs. This means that OSDs can also take requests from 
other OSDs. With multiple replicas of data across OSDs, OSDs can also 
"peer" to ensure that the placement groups are in sync. See 
`Placement Group States`_ and `Placement Group Concepts`_ for details.

If an OSD is not running (e.g., it crashes), the OSD cannot notify the monitor
that it is ``down``. The monitor can ping an OSD periodically to ensure that it
is running. However, Ceph also empowers OSDs to determine if a neighboring OSD
is ``down``, to update the cluster map and to report it to the monitor(s). When
an OSD is ``down``,  the data in the placement group is said to be ``degraded``.
If the OSD is ``down`` and ``in``, but subsequently taken ``out`` of the
cluster,  the OSDs receive an update to the cluster map and rebalance the
placement groups within the cluster automatically.

OSDs store all data as objects in a flat namespace (e.g., no hierarchy of
directories). An object has an identifier, binary data, and metadata consisting
of a set of name/value pairs. The semantics are completely up to the client. For
example, CephFS uses metadata to store file attributes such as the file owner,
created date, last modified date, and so forth.


.. ditaa:: /------+------------------------------+----------------\
           | ID   | Binary Data                  | Metadata       |
           +------+------------------------------+----------------+
           | 1234 | 0101010101010100110101010010 | name1 = value1 | 
           |      | 0101100001010100110101010010 | name2 = value2 |
           |      | 0101100001010100110101010010 | nameN = valueN |
           \------+------------------------------+----------------/

As part of maintaining data consistency and cleanliness, Ceph OSDs
can also scrub the data. That is, Ceph OSDs can compare object metadata
across replicas to catch OSD bugs or filesystem errors (daily). OSDs can 
also do deeper scrubbing by comparing data in objects bit-for-bit to find
bad sectors on a disk that weren't apparent in a light scrub (weekly).

.. todo:: explain "classes"

.. _Placement Group States: ../rados/operations/pg-states
.. _Placement Group Concepts: ../rados/operations/pg-concepts

Monitor Quorums
===============

Ceph's monitors maintain a master copy of the cluster map.  So Ceph daemons and
clients  merely contact the monitor periodically to ensure they have the most
recent  copy of the cluster map. Ceph monitors are light-weight processes, but
for added reliability and fault tolerance, Ceph supports distributed monitors.
Ceph must have agreement among various monitor instances regarding the state of
the cluster. To establish a consensus, Ceph always uses an odd number of
monitors (e.g., 1, 3, 5, 7, etc) and the `Paxos`_ algorithm in order to
establish a consensus.

.. _Paxos: http://en.wikipedia.org/wiki/Paxos_(computer_science)

MDS
===

The Ceph filesystem service is provided by a daemon called ``ceph-mds``. It uses
RADOS to store all the filesystem metadata (directories, file ownership, access
modes, etc), and directs clients to access RADOS directly for the file contents.
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
intance for high availability.


Client Interfaces
=================

Authentication and Authorization
--------------------------------

Ceph clients can authenticate their users with Ceph monitors, OSDs and metadata
servers. Authenticated users gain authorization to read, write and execute Ceph
commands. The Cephx authentication system is similar to Kerberos, but avoids a
single point of failure to ensure scalability and high availability.  For
details on Cephx, see `Ceph Authentication and Authorization`_.

.. _Ceph Authentication and Authorization: ../rados/operations/auth-intro/

librados
--------

.. todo:: Snapshotting, Import/Export, Backup
.. todo:: native APIs

RBD
---

RBD stripes a block device image over multiple objects in the cluster, where
each object gets mapped to a placement group and distributed, and the placement
groups are spread  across separate ``ceph-osd`` daemons throughout the cluster.

.. important:: Striping allows RBD block devices to perform better than a single server could!

RBD's thin-provisioned snapshottable block devices are an attractive option for
virtualization and cloud computing. In virtual machine scenarios, people
typically deploy RBD with the ``rbd`` network storage driver in Qemu/KVM, where
the host machine uses ``librbd`` to provide a block device service to the guest.
Many cloud computing stacks use ``libvirt`` to integrate with hypervisors. You
can use RBD thin-provisioned block devices with Qemu and libvirt to support
OpenStack and CloudStack among other solutions.

While we do not provide ``librbd`` support with other hypervisors at this time, you may 
also use RBD kernel objects to provide a block device to a client. Other virtualization
technologies such as Xen can access the RBD kernel object(s). This is done with the 
command-line tool ``rbd``.


RGW
---

The RADOS Gateway daemon, ``radosgw``, is a FastCGI service that provides a
RESTful_ HTTP API to store objects and metadata. It layers on top of RADOS with
its own data formats, and maintains its own user database, authentication, and
access control. The RADOS Gateway uses a unified namespace, which means you can
use either the OpenStack Swift-compatible API or the Amazon S3-compatible API.
For example, you can write data using the S3-comptable API with one application
and then read data using the Swift-compatible API with another application. 

See `RADOS Gateway`_ for details.

.. _RADOS Gateway: ../radosgw/
.. _RESTful: http://en.wikipedia.org/wiki/RESTful


.. index:: RBD, Rados Block Device



CephFS
------

.. todo:: cephfs, ceph-fuse