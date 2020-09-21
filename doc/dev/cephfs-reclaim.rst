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
