CephFS Reclaim Interface
========================

Introduction
------------
NFS servers typically do not track ephemeral state on stable storage. If
the NFS server is restarted, then it will be resurrected with no
ephemeral state, and the NFS clients are expected to send requests to
reclaim what state they held during a grace period.

In order to support this use-case, libcephfs has grown several functions
that allow a client that has been stopped and restarted to destroy or
reclaim state held by a previous incarnation of itself. This allows the
client to reacquire state held by its previous incarnation, and to avoid
the long wait for the old session to time out before releasing the state
previously held.

As soon as an NFS server running over cephfs goes down, it's racing
against its MDS session timeout. If the Ceph session times out before
the NFS grace period is started, then conflicting state could be
acquired by another client. This mechanism also allows us to increase
the timeout for these clients, to ensure that the server has a long
window of time to be restarted.

Racing the MDS Session Timeout
------------------------------
An NFS server that exports a CephFS mount is a single CephFS client. It
holds capabilities and file locks for all of its NFS clients in one MDS
session. When the NFS server crashes, the MDS sees only a session that
has stopped renewing its capabilities, and it handles that session like
that of any other unresponsive client:

- After the file system's ``session_timeout``, 60 seconds by default,
  the MDS marks the session stale and revokes its capabilities.
- After the file system's ``session_autoclose``, 300 seconds by
  default, the MDS evicts the session and releases everything that it
  held, including its file locks.

The NFS clients of the crashed server still believe that they hold that
state, and they reclaim it from the restarted server during the NFS
grace period. The MDS must not release the old session's state until
the restarted server, and every other NFS server that exports the same
file system, is enforcing the grace period. Otherwise another client
can acquire conflicting state in the gap: for example, a lock that an
NFS client is about to reclaim. That NFS client then loses a lock that
it believes it has held all along, and the other client might have
changed the data that the lock protected.

The reclaim interface closes this gap from both sides:

- ``ceph_set_session_timeout()`` gives the server's session a timeout of
  its own. The MDS never marks such a session stale. It keeps the
  session's capabilities and locks until the session has gone that many
  seconds without renewing them, and then evicts it. A timeout of 0
  means that the MDS never times the session out.
- ``ceph_start_reclaim()`` with ``CEPH_RECLAIM_RESET`` lets the
  restarted server tear down its old session as soon as doing so is
  safe, instead of waiting for the timeout. It is safe once every NFS
  server that could hand out conflicting state is enforcing its grace
  period. With a single NFS server, that is the restarted server itself.
  With several, the servers coordinate through a shared grace database.
  See `Coordinating the Grace Period Across NFS Servers`_.

Setting the UUID
----------------
In order to properly reset or reclaim against the old session, we need a
way to identify the old session. This done by setting a unique opaque
value on the session using **ceph_set_uuid()**. The uuid value can be
any string and is treated as opaque by the client.

Setting the uuid directly can only be done on a new session, prior to
mounting. When reclaim is performed the current session will inherit the
old session's uuid.

Starting Reclaim
----------------
After calling ceph_create and ceph_init on the resulting struct
ceph_mount_info, the client should then issue ceph_start_reclaim,
passing in the uuid of the previous incarnation of the client with any
flags.

CEPH_RECLAIM_RESET
   This flag indicates that we do not intend to do any sort of reclaim
   against the old session indicated by the given uuid, and that it
   should just be discarded. Any state held by the previous client
   should be released immediately.

Finishing Reclaim
-----------------
After the Ceph client has completed all of its reclaim operations, the
client should issue ceph_finish_reclaim to indicate that the reclaim is
now complete.

Setting Session Timeout (Optional)
----------------------------------
When a client dies and is restarted, and we need to preserve its state,
we are effectively racing against the session expiration clock. In this
situation we generally want a longer timeout since we expect to
eventually kill off the old session manually.

Coordinating the Grace Period Across NFS Servers
------------------------------------------------
When several NFS-Ganesha servers export the same CephFS file system, it
is not enough for one server to enforce its own grace period: a client
of another server could acquire the state that the restarted server's
clients are about to reclaim. Every server must enforce a grace period
whenever any server needs one, and the grace period can end only when
no server needs it any longer.

NFS-Ganesha does this with its ``rados_cluster`` recovery backend. The
servers share a grace database: a RADOS object, named ``grace`` by
default, in a pool named ``nfs-ganesha`` by default. The object's data
holds two epoch serial numbers:

- The current epoch, under which the servers record new client state.
- The recovery epoch, from which clients may reclaim state. A recovery
  epoch of 0 means that no grace period is in effect.

The object's omap holds one record for each server, keyed by the
server's ``nodeid``, with two flags:

- Need grace, ``N``: the server has clients that need to reclaim state.
- Enforcing, ``E``: the server is enforcing the grace period, refusing
  requests from clients to acquire new state.

When a server restarts:

#. It starts a grace period, or joins the one in effect, by setting
   ``N`` and ``E`` in its own record. If no grace period is in effect,
   it makes the current epoch the recovery epoch and starts a new
   current epoch.
#. The other servers watch the grace object, so they are notified of
   the change. Each of them starts its own grace period and sets ``E``
   in its record.
#. The restarted server waits until every record in the database has
   ``E`` set, and only then loads its exports. When the NFS-Ganesha
   Ceph FSAL mounts an export, it sets a 300-second session timeout on
   the new session and calls ``ceph_start_reclaim()`` with
   ``CEPH_RECLAIM_RESET`` for the old session. The MDS therefore evicts
   the old session only after every server in the cluster has stopped
   handing out new state.
#. When a server's own clients have finished reclaiming, or its grace
   period expires, the server clears ``N`` in its record. When no
   record has ``N`` set, the grace period is lifted across the cluster:
   the recovery epoch goes back to 0, and each server clears ``E`` and
   resumes handing out new state.

Administrators manage the grace database with ``ganesha-rados-grace``.
A server must be added with ``ganesha-rados-grace add <nodeid>`` before
it can take part, and ``ganesha-rados-grace dump`` shows the epochs and
each server's flags. The NFS-Ganesha daemons that cephadm deploys use
the ``rados_cluster`` backend. cephadm adds each daemon's ``nodeid`` to
the grace database when it deploys the daemon, and removes the
``nodeid`` when the NFS service is scaled down and no longer needs it.
See the
`ganesha-rados-grace man page
<https://github.com/nfs-ganesha/nfs-ganesha/blob/next/src/doc/man/ganesha-rados-grace.rst>`_.

Example 1: Reset Old Session
----------------------------
This example just kills off the MDS session held by a previous instance
of itself. An NFS server can start a grace period and then ask the MDS
to tear down the old session. This allows clients to start reclaim
immediately.

(Note: error handling omitted for clarity)

.. code-block:: c

	struct ceph_mount_info *cmount;
	const char *uuid = "foobarbaz";

	/* Set up a new cephfs session, but don't mount it yet. */
	rc = ceph_create(&cmount);
	rc = ceph_init(&cmount);

	/*
	 * Set the timeout to 5 minutes to lengthen the window of time for
	 * the server to restart, should it crash.
	 */
	ceph_set_session_timeout(cmount, 300);

	/*
	 * Start reclaim vs. session with old uuid. Before calling this,
	 * all NFS servers that could acquire conflicting state _must_ be
	 * enforcing their grace period locally.
	 */
	rc = ceph_start_reclaim(cmount, uuid, CEPH_RECLAIM_RESET);

	/* Declare reclaim complete */
	rc = ceph_finish_reclaim(cmount);

	/* Set uuid held by new session */
	ceph_set_uuid(cmount, nodeid);

	/*
	 * Now mount up the file system and do normal open/lock operations to
	 * satisfy reclaim requests.
	 */
	ceph_mount(cmount, rootpath);
	...
