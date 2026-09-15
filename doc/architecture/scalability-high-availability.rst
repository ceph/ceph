.. index:: architecture; high availability, scalability

.. _arch_scalability_and_high_availability:

Scalability and High Availability
=================================

In traditional architectures, clients talk to a centralized component. This
centralized component might be a gateway, a broker, an API, or a facade. A
centralized component of this kind acts as a single point of entry to a complex
subsystem. Architectures that rely upon such a centralized component have a
single point of failure and incur limits to performance and scalability. If
the centralized component goes down, the whole system becomes unavailable.

Ceph eliminates this centralized component. This enables clients to interact
with Ceph OSDs directly. Ceph OSDs create object replicas on other Ceph Nodes
to ensure data safety and high availability. Ceph also uses a cluster of
monitors to ensure high availability. To eliminate centralization, Ceph uses an
algorithm called :abbr:`CRUSH (Controlled Replication Under Scalable Hashing)`.


.. index:: CRUSH; architecture

CRUSH Introduction
~~~~~~~~~~~~~~~~~~

Ceph Clients and Ceph OSD Daemons both use the :abbr:`CRUSH (Controlled
Replication Under Scalable Hashing)` algorithm to compute information about
object location instead of relying upon a central lookup table. CRUSH provides
a better data management mechanism than do older approaches, and CRUSH enables
massive scale by distributing the work to all the OSD daemons in the cluster
and all the clients that communicate with them. CRUSH uses intelligent data
replication to ensure resiliency, which is better suited to hyper-scale
storage. The following sections provide additional details on how CRUSH works.
For an in-depth, academic discussion of CRUSH, see `CRUSH - Controlled,
Scalable, Decentralized Placement of Replicated Data`_.

.. index:: architecture; cluster map

.. _architecture_cluster_map:

Cluster Map
~~~~~~~~~~~

In order for a Ceph cluster to function properly, Ceph Clients and Ceph OSDs
must have current information about the cluster's topology. Current information
is stored in the "Cluster Map", which is in fact a collection of five maps. The
five maps that constitute the cluster map are:

#. **The Monitor Map:** Contains the cluster ``fsid``, the position, the name,
   the address, and the TCP port of each monitor. The monitor map specifies the
   current epoch, the time of the monitor map's creation, and the time of the
   monitor map's last modification.  To view a monitor map, run ``ceph mon
   dump``.

#. **The OSD Map:** Contains the cluster ``fsid``, the time of the OSD map's
   creation, the time of the OSD map's last modification, a list of pools, a
   list of replica sizes, a list of PG numbers, and a list of OSDs and their
   statuses (for example, ``up``, ``in``). To view an OSD map, run ``ceph
   osd dump``.

#. **The PG Map:** Contains the PG version, its time stamp, the last OSD map
   epoch, the full ratios, and the details of each placement group. This
   includes the PG ID, the `Up Set`, the `Acting Set`, the state of the PG (for
   example, ``active + clean``), and data usage statistics for each pool.

#. **The CRUSH Map:** Contains a list of storage devices, the failure domain
   hierarchy (for example, ``device``, ``host``, ``rack``, ``row``, ``room``),
   and rules for traversing the hierarchy when storing data. To view a CRUSH
   map, run ``ceph osd getcrushmap -o {filename}`` and then decompile it by
   running ``crushtool -d {comp-crushmap-filename} -o
   {decomp-crushmap-filename}``. Use a text editor or ``cat`` to view the
   decompiled map.

#. **The MDS Map:** Contains the current MDS map epoch, when the map was
   created, and the last time it changed. It also contains the pool for
   storing metadata, a list of metadata servers, and which metadata servers
   are ``up`` and ``in``. To view an MDS map, execute ``ceph fs dump``.

Each map maintains a history of changes to its operating state. Ceph Monitors
maintain a master copy of the cluster map. This master copy includes the
cluster members, the state of the cluster, changes to the cluster, and
information recording the overall health of the Ceph Storage Cluster.

.. index:: high availability; monitor architecture

High Availability Monitors
~~~~~~~~~~~~~~~~~~~~~~~~~~

A Ceph Client must contact a Ceph Monitor and obtain a current copy of the
cluster map in order to read data from or to write data to the Ceph cluster.

It is possible for a Ceph cluster to function properly with only a single
monitor, but a Ceph cluster that has only a single monitor has a single point
of failure: if the monitor goes down, Ceph clients will be unable to read data
from or write data to the cluster.

Ceph leverages a cluster of monitors in order to increase reliability and fault
tolerance. When a cluster of monitors is used, however, one or more of the
monitors in the cluster can fall behind due to latency or other faults. Ceph
mitigates these negative effects by requiring multiple monitor instances to
agree about the state of the cluster. To establish consensus among the monitors
regarding the state of the cluster, Ceph uses the `Paxos`_ algorithm and a
majority of monitors (for example, one in a cluster that contains only one
monitor, two in a cluster that contains three monitors, three in a cluster that
contains five monitors, four in a cluster that contains six monitors, and so
on).

See the :ref:`monitor-config-reference` for more detail on configuring monitors.

.. index:: architecture; high availability authentication

.. _arch_high_availability_authentication:

High Availability Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CephX authentication system is used by Ceph to authenticate users and
daemons and to protect against man-in-the-middle attacks. Simultaneously, CephX
provides a mechanism to grant authorizations to clients for using Ceph
services. These authorizations are encoded as CephX Capabilities which are
interpreted by the respective services as to what they grant the client to do.
The authorizations take the form of time-limited tickets which must be
periodically refreshed by the Ceph client.

.. note:: The CephX protocol does not address data encryption in transport
   (for example, SSL/TLS) or encryption at rest.

CephX uses shared secret keys for authentication. This means that both the
client and the Monitors keep a copy of the client's secret key.

The CephX protocol makes it possible for each party to prove to the other
that it has a copy of the key without revealing it while preventing an
adversary to learn the key or replay messages to masquerade key ownership.

As stated in :ref:`Scalability and High Availability
<arch_scalability_and_high_availability>`, Ceph does not have any centralized
interface between clients and the Ceph object store. By avoiding such a
centralized interface, Ceph avoids the bottlenecks that attend such centralized
interfaces. However, this means that clients must interact directly with OSDs.
Direct interactions between Ceph clients and OSDs require authenticated
connections. The CephX authentication system establishes and sustains these
authenticated connections.

The CephX protocol operates in a manner similar to `Kerberos`_. A user invokes
a Ceph client which automatically contacts a Monitor. Like Kerberos, each
Monitor can independently authenticate users and distribute tickets to clients.
Consequently, there is no single point of failure and no bottleneck when using
CephX.

Like Kerberos, a Monitor responding to a client authentication request will
return an session key encrypted by the credential secret and a set of encrypted
tickets for use by the client to access Ceph services which its credential is
authorized to use. The session key and auth service ticket together can be used
for obtaining new or refreshed tickets for Ceph services. For each Ceph service
(AUTH, MON, MGR, OSD, MDS), a client might receive a ticket for each of the
rotating service keys (usually 3).

CephX uses rotating service keys which are securely distributed to each service
type, e.g. the CephFS Metadata Servers (MDS). The tickets encrypted with those
rotating service keys can then be decrypted by any service daemon to learn the
identity of the client (entity name), the client's global identifier or
incarnation, the time when the ticket was created and will expire, and all
capabilities (authorizations) the Client has with the service.

Like Kerberos tickets, CephX tickets expire. An attacker cannot use an
expired ticket or session key that has been obtained surreptitiously. This form
of authentication prevents attackers who have access to the communications
medium from creating bogus messages under another user's identity and prevents
attackers from altering another user's legitimate messages, as long as the
user's secret key is not divulged before it expires.

CephX also supports rotating a credential's secret key to address leaks
or scope changes when desired. It is expected to be routine in future
Ceph installations (2026+) that the service daemon keys (e.g. ``mgr.x``)
might be rotated whenever the daemon is relocated or even restarted.
The same can be done for any entity type, including clients. Finally, this 
mechanism can be used to effect key type upgrades as necessary when ciphers
are upgraded to improve security.

To use CephX, an administrator must set up each user/credential in advance. The
first credential routinely created for new Ceph installs is the
``client.admin`` key which can be used to administer the cluster. In the
following diagram, that ``client.admin`` user may invoke ``ceph auth
get-or-create-key`` from the command line to generate a new entity and secret
key. Ceph's ``auth`` subsystem generates the username and key, stores a copy on
all Monitors, and returns the new entity's secret back to administrator.

.. note:: The ``client.admin`` user must provide the user ID and
   secret key to the user in a secure manner.

.. ditaa::

           +---------+     +---------+
           | Client  |     | Monitor |
           +---------+     +---------+
                |  request to   |
                | create a user |
                |-------------->|----------+ create user
                |               |          | and
                |<--------------|<---------+ store key
                | transmit key  |
                |               |


Here is how a client authenticates with a Monitor with entity
``client.username``. The client passes the user name to the Monitor. The
Monitor generates an **auth session key** that is encrypted with the secret key
associated with ``client.username``. The Monitor transmits the encrypted
``auth`` ticket to the client. The client uses its principal's secret key to
decrypt the payload. The newly established **auth session key** now identifies
the user and will persist for the duration of its instance (incarnation).

When the client requests a service ticket, the Monitor will generate new
tickets with the latest rotating service secrets for the requested service
type. Within each ticket is a fresh **service session key** only for use with
that ticket. The **service session key** is encrypted twice: once using the
**auth session key** and a second time within the encrypted service ticket
which can only be decrypted by the service daemon. This new **service session
key** thereby allows the client and service daemon to mutually authenticate
each other when establishing a session.


.. ditaa::

           +---------+     +---------+
           | Client  |     | Monitor |
           +---------+     +---------+
                |  authenticate |
                |-------------->|----------+ generate and
                |               |          | encrypt
                |<--------------|<---------+ session key
                | transmit      |
                | encrypted     |
                | session key   |
                |               |
                |-----+ decrypt |
                |     | session |
                |<----+ key     |
                |               |
                |  req. ticket  |
                |-------------->|----------+ generate and
                |               |          | encrypt
                |<--------------|<---------+ ticket
                | recv. ticket  |
                |               |
                |-----+ decrypt |
                |     | ticket  |
                |<----+         |


The CephX protocol authenticates ongoing communications between the clients
and Ceph daemons. After initial authentication, each message sent between a
client and a daemon is signed using a ticket that can be verified by Monitors,
OSDs, and metadata daemons. This ticket is verified by using the secret shared
between the client and the daemon.

.. ditaa::

           +---------+     +---------+     +-------+     +-------+
           |  Client |     | Monitor |     |  MDS  |     |  OSD  |
           +---------+     +---------+     +-------+     +-------+
                |  request to   |              |             |
                | create a user |              |             |
                |-------------->| mon and      |             |
                |<--------------| client share |             |
                |    receive    | a secret.    |             |
                | shared secret |              |             |
                |               |<------------>|             |
                |               |<-------------+------------>|
                |               | mon, mds,    |             |
                | authenticate  | and osd      |             |
                |-------------->| share        |             |
                |<--------------| a secret     |             |
                |  session key  |              |             |
                |               |              |             |
                |  req. ticket  |              |             |
                |-------------->|              |             |
                |<--------------|              |             |
                | recv. ticket  |              |             |
                |               |              |             |
                |   make request (CephFS only) |             |
                |----------------------------->|             |
                |<-----------------------------|             |
                | receive response (CephFS only)             |
                |                                            |
                |                make request                |
                |------------------------------------------->|
                |<-------------------------------------------|
                               receive response

This authentication protects only the connections between Ceph clients and Ceph
daemons. The authentication is not extended beyond the Ceph client. If a user
accesses the Ceph client from a remote host, CephX authentication will not be
applied to the connection between the user's host and the client host.

See :ref:`rados-cephx-config-ref` for more on configuration and operational
details for CephX.

See :ref:`user-management` for more on user management and service
authorizations (CephX Capabilities).

See :ref:`A Detailed Description of the CephX Authentication Protocol
<cephx_2012_peter>` for more on the distinction between authorization and
authentication and for a step-by-step explanation of the setup of CephX
tickets and session keys.

.. index:: architecture; smart daemons and scalability

.. _arch_smart_daemons:

Smart Daemons Enable Hyperscale
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A feature of many storage clusters is a centralized interface that keeps track
of the nodes that clients are permitted to access. Such centralized
architectures provide services to clients by means of a double dispatch. At the
petabyte-to-exabyte scale, such double dispatches are a significant
bottleneck.

Ceph obviates this bottleneck: Ceph's OSD Daemons AND Ceph clients are
cluster-aware. Like Ceph clients, each Ceph OSD Daemon is aware of other Ceph
OSD Daemons in the cluster. This enables Ceph OSD Daemons to interact directly
with other Ceph OSD Daemons and to interact directly with Ceph Monitors.  Being
cluster-aware makes it possible for Ceph clients to interact directly with Ceph
OSD Daemons.

Because Ceph clients, Ceph monitors, and Ceph OSD daemons interact with one
another directly, Ceph OSD daemons can make use of the aggregate CPU and RAM
resources of the nodes in the Ceph cluster. This means that a Ceph cluster can
easily perform tasks that a cluster with a centralized interface would struggle
to perform. The ability of Ceph nodes to make use of the computing power of
the greater cluster provides several benefits:

#. **OSDs Service Clients Directly:** Network devices can support only a
   limited number of concurrent connections. Because Ceph clients contact
   Ceph OSD daemons directly without first connecting to a central interface,
   Ceph enjoys improved perfomance and increased system capacity relative to
   storage redundancy strategies that include a central interface. Ceph clients
   maintain sessions only when needed, and maintain those sessions with only
   particular Ceph OSD daemons, not with a centralized interface.

#. **OSD Membership and Status**: When Ceph OSD Daemons join a cluster, they
   report their status. At the lowest level, the Ceph OSD Daemon status is
   ``up`` or ``down``: this reflects whether the Ceph OSD daemon is running and
   able to service Ceph Client requests. If a Ceph OSD Daemon is ``down`` and
   ``in`` the Ceph Storage Cluster, this status may indicate the failure of the
   Ceph OSD Daemon. If a Ceph OSD Daemon is not running because it has crashed,
   the Ceph OSD Daemon cannot notify the Ceph Monitor that it is ``down``. The
   OSDs periodically send messages to the Ceph Monitor (in releases prior to
   Luminous, this was done by means of ``MPGStats``, and beginning with the
   Luminous release, this has been done with ``MOSDBeacon``). If the Ceph
   Monitors receive no such message after a configurable period of time,
   then they mark the OSD ``down``. This mechanism is a failsafe, however.
   Normally, Ceph OSD Daemons determine if a neighboring OSD is ``down`` and
   report it to the Ceph Monitors. This contributes to making Ceph Monitors
   lightweight processes. See `Monitoring OSDs`_ and `Heartbeats`_ for
   additional details.

#. **Data Scrubbing:** To maintain data consistency, Ceph OSD Daemons scrub
   RADOS objects. Ceph OSD Daemons compare the metadata of their own local
   objects against the metadata of the replicas of those objects, which are
   stored on other OSDs. Scrubbing occurs on a per-Placement-Group basis, finds
   mismatches in object size and finds metadata mismatches, and is usually
   performed daily. Ceph OSD Daemons perform deeper scrubbing by comparing the
   data in objects, bit-for-bit, against their checksums. Deep scrubbing finds
   bad sectors on drives that are not detectable with light scrubs. See :ref:`Data
   Scrubbing <rados_config_scrubbing>` for details on configuring scrubbing.

#. **Replication:** Data replication involves collaboration between Ceph
   Clients and Ceph OSD Daemons. Ceph OSD Daemons use the CRUSH algorithm to
   determine the storage location of object replicas. Ceph clients use the
   CRUSH algorithm to determine the storage location of an object, then the
   object is mapped to a pool and to a placement group, and then the client
   consults the CRUSH map to identify the placement group's primary OSD.

   After identifying the target placement group, the client writes the object
   to the identified placement group's primary OSD. The primary OSD then
   consults its own copy of the CRUSH map to identify secondary
   OSDS, replicates the object to the placement groups in those secondary
   OSDs, confirms that the object was stored successfully in the
   secondary OSDs, and reports to the client that the object
   was stored successfully.  We call these replication operations ``subops``.

.. ditaa::

             +----------+
             |  Client  |
             |          |
             +----------+
                 *  ^
      Write (1)  |  |  Ack (6)
                 |  |
                 v  *
            +-------------+
            | Primary OSD |
            |             |
            +-------------+
              *  ^   ^  *
    Write (2) |  |   |  |  Write (3)
       +------+  |   |  +------+
       |  +------+   +------+  |
       |  | Ack (4)  Ack (5)|  |
       v  *                 *  v
 +---------------+   +----------------+
 | Secondary OSD |   | Secondary OSD  |
 |               |   |                |
 +---------------+   +----------------+

By performing this data replication, Ceph OSD Daemons relieve Ceph
clients and their network interfaces of the burden of replicating data.


.. _Paxos: https://en.wikipedia.org/wiki/Paxos_(computer_science)
.. _Heartbeats: ../../rados/configuration/mon-osd-interaction
.. _Monitoring OSDs: ../../rados/operations/monitoring-osd-pg/#monitoring-osds
.. _Kerberos: https://en.wikipedia.org/wiki/Kerberos_(protocol)
.. _CRUSH - Controlled, Scalable, Decentralized Placement of Replicated Data: https://ceph.io/assets/pdfs/weil-crush-sc06.pdf
