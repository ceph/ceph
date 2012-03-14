======================================
Installing RADOS Processes and Daemons
======================================

When you start the Ceph service, the initialization process activates a series of daemons that run in the background. 
The hosts in a typical RADOS cluster run at least one of three processes: 

- RADOS (``ceph-osd``)
- Monitor (``ceph-mon``)
- Metadata Server (``ceph-mds``)

Each instance of a RADOS ``ceph-osd`` process performs a few essential tasks.

1. Each ``ceph-osd`` instance provides clients with an object interface to the OSD for read/write operations.
2. Each ``ceph-osd`` instance communicates and coordinates with other OSDs to store, replicate, redistribute and restore data.
3. Each ``ceph-osd`` instance communicates with monitors to retrieve and/or update the master copy of the cluster map.

Each instance of a monitor process performs a few essential tasks: 

1. Each ``ceph-mon`` instance communicates with other ``ceph-mon`` instances using PAXOS to establish consensus for distributed decision making.
2. Each ``ceph-mon`` instance serves as the first point of contact for clients, and provides clients with the topology and status of the cluster.
3. Each ``ceph-mon`` instance provides RADOS instances with a master copy of the cluster map and receives updates for the master copy of the cluster map.

A metadata server (MDS) process performs a few essential tasks: 

1. Each ``ceph-mds`` instance provides clients with metadata regarding the file system.
2. Each ``ceph-mds`` instance manage the file system namespace
3. Coordinate access to the shared OSD cluster.


Installing ``ceph-osd``
=======================
<placeholder>

Installing ``ceph-mon``
=======================
<placeholder>

Installing ``ceph-mds``
=======================
<placeholder>
