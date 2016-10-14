
Ceph filesystem client eviction
===============================

When a filesystem client is unresponsive or otherwise misbehaving, it
may be necessary to forcibly terminate its access to the filesystem.  This
process is called *eviction*.

This process is somewhat thorough in order to protect against data inconsistency
resulting from misbehaving clients.

OSD blacklisting
----------------

First, prevent the client from performing any more data operations by *blacklisting*
it at the RADOS level.  You may be familiar with this concept as *fencing* in other
storage systems.

Identify the client to evict from the MDS session list:

::

    # ceph daemon mds.a session ls
    [
        { "id": 4117,
          "num_leases": 0,
          "num_caps": 1,
          "state": "open",
          "replay_requests": 0,
          "reconnecting": false,
          "inst": "client.4117 172.16.79.251:0\/3271",
          "client_metadata": { "entity_id": "admin",
              "hostname": "fedoravm.localdomain",
              "mount_point": "\/home\/user\/mnt"}}]

In this case the 'fedoravm' client has address ``172.16.79.251:0/3271``, so we blacklist
it as follows:

::

    # ceph osd blacklist add 172.16.79.251:0/3271
    blacklisting 172.16.79.251:0/3271 until 2014-12-09 13:09:56.569368 (3600 sec)

OSD epoch barrier
-----------------

While the evicted client is now marked as blacklisted in the central (mon) copy of the OSD
map, it is now necessary to ensure that this OSD map update has propagated to all daemons
involved in subsequent filesystem I/O.  To do this, use the ``osdmap barrier`` MDS admin
socket command.

First read the latest OSD epoch:

::

    # ceph osd dump
    epoch 12
    fsid fd61ca96-53ff-4311-826c-f36b176d69ea
    created 2014-12-09 12:03:38.595844
    modified 2014-12-09 12:09:56.619957
    ...

In this case it is 12.  Now request the MDS to barrier on this epoch:

::

    # ceph daemon mds.a osdmap barrier 12

MDS session eviction
--------------------

Finally, it is safe to evict the client's MDS session, such that any capabilities it held
may be issued to other clients.  The ID here is the ``id`` attribute from the ``session ls``
output:

::

    # ceph daemon mds.a session evict 4117

That's it!  The client has now been evicted, and any resources it had locked will
now be available for other clients.

Background: OSD epoch barrier
-----------------------------

The purpose of the barrier is to ensure that when we hand out any
capabilities which might allow touching the same RADOS objects, the
clients we hand out the capabilities to must have a sufficiently recent
OSD map to not race with cancelled operations (from ENOSPC) or
blacklisted clients (from evictions)

More specifically, the cases where we set an epoch barrier are:

 * Client eviction (where the client is blacklisted and other clients
   must wait for a post-blacklist epoch to touch the same objects)
 * OSD map full flag handling in the client (where the client may
   cancel some OSD ops from a pre-full epoch, so other clients must
   wait until the full epoch or later before touching the same objects).
 * MDS startup, because we don't persist the barrier epoch, so must
   assume that latest OSD map is always required after a restart.

Note that this is a global value for simplicity: we could maintain this on
a per-inode basis.  We don't, because:

 * It would be more complicated
 * It would use an extra 4 bytes of memory for every inode
 * It would not be much more efficient as almost always everyone has the latest
   OSD map anyway, in most cases everyone will breeze through this barrier
   rather than waiting.
 * We only do this barrier in very rare cases, so any benefit from per-inode
   granularity would only very rarely be seen.

The epoch barrier is transmitted along with all capability messages, and
instructs the receiver of the message to avoid sending any more RADOS
operations to OSDs until it has seen this OSD epoch.  This mainly applies
to clients (doing their data writes directly to files), but also applies
to the MDS because things like file size probing and file deletion are
done directly from the MDS.

