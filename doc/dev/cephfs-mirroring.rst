================
CephFS Mirroring
================

CephFS supports asynchronous replication of snapshots to a remote CephFS file
system via `cephfs-mirror` tool. Snapshots are synchronized by mirroring
snapshot data followed by creating a snapshot with the same name (for a given
directory on the remote file system) as the snapshot being synchronized.

Requirements
------------

The primary (local) and secondary (remote) Ceph clusters version should be
Pacific or later.

Key Idea
--------

For a given snapshot pair in a directory, `cephfs-mirror` daemon will rely on
readdir diff to identify changes in a directory tree. The diffs are applied to
directory in the remote file system thereby only synchronizing files that have
changed between two snapshots.

This feature is tracked here: https://tracker.ceph.com/issues/47034.

Currently, snapshot data is synchronized by bulk copying to the remote
filesystem.

.. note:: Synchronizing hardlinks is not supported -- hardlinked files get
   synchronized as separate files.

Creating Users
--------------

Start by creating a user (on the primary/local cluster) for the mirror daemon.
This user requires write capability on the metadata pool to create RADOS
objects (index objects) for watch/notify operation and read capability on the
data pool(s).

.. prompt:: bash $

   ceph auth get-or-create client.mirror mon 'profile cephfs-mirror' mds 'allow r' osd 'allow rw tag cephfs metadata=*, allow r tag cephfs data=*' mgr 'allow r'

Create a user for each file system peer (on the secondary/remote cluster). This user needs
to have full capabilities on the MDS (to take snapshots) and the OSDs::

  $ ceph fs authorize <fs_name> client.mirror_remote / rwps

This user should be used (as part of peer specification) when adding a peer.

Starting Mirror Daemon
----------------------

Mirror daemon should be spawned using `systemctl(1)` unit files::

  $ systemctl enable cephfs-mirror@mirror
  $ systemctl start cephfs-mirror@mirror

`cephfs-mirror` daemon can be run in foreground using::

  $ cephfs-mirror --id mirror --cluster site-a -f

.. note:: User used here is `mirror` as created in the `Creating Users` section.

Mirroring Design
----------------

CephFS supports asynchronous replication of snapshots to a remote CephFS file system
via `cephfs-mirror` tool. For a given directory, snapshots are synchronized by transferring
snapshot data to the remote file system and creating a snapshot with the same name as the
snapshot being synchronized.

Snapshot Synchronization Order
------------------------------

Although the order in which snapshots get chosen for synchronization does not matter,
snapshots are picked based on creation order (using snap-id).

Snapshot Incarnation
--------------------

A snapshot may be deleted and recreated (with the same name) with different contents.
An "old" snapshot could have been synchronized (earlier) and the recreation of the
snapshot could have been done when mirroring was disabled. Using snapshot names to
infer the point-of-continuation would result in the "new" snapshot (incarnation)
never getting picked up for synchronization.

Snapshots on the secondary file system stores the snap-id of the snapshot it was
synchronized from. This metadata is stored in `SnapInfo` structure on the MDS.

Interfaces
----------

`Mirroring` module (manager plugin) provides interfaces for managing directory snapshot
mirroring. Manager interfaces are (mostly) wrappers around monitor commands for managing
file system mirroring and is the recommended control interface.

Mirroring Module and Interface
------------------------------

Mirroring module provides interface for managing directory snapshot mirroring. The module
is implemented as a Ceph Manager plugin. Mirroring module does not manage spawning (and
terminating) the mirror daemons. Right now the preferred way would be to start/stop
mirror daemons via `systemctl(1)`. Going forward, deploying mirror daemons would be
managed by `cephadm` (Tracker: http://tracker.ceph.com/issues/47261).

The manager module is responsible for assigning directories to mirror daemons for
synchronization. Multiple mirror daemons can be spawned to achieve concurrency in
directory snapshot synchronization. When mirror daemons are spawned (or terminated)
, the mirroring module discovers the modified set of mirror daemons and rebalances
the directory assignment amongst the new set thus providing high-availability.

.. note:: Multiple mirror daemons is currently untested. Only a single mirror daemon
          is recommended.

Mirroring module is disabled by default. To enable mirroring use::

  $ ceph mgr module enable mirroring

Mirroring module provides a family of commands to control mirroring of directory
snapshots. To add or remove directories, mirroring needs to be enabled for a given
file system. To enable mirroring use::

  $ ceph fs snapshot mirror enable <fs_name>

.. note:: Mirroring module commands use `fs snapshot mirror` prefix as compared to
          the monitor commands which `fs mirror` prefix. Make sure to use module
          commands.

To disable mirroring, use::

  $ ceph fs snapshot mirror disable <fs_name>

Once mirroring is enabled, add a peer to which directory snapshots are to be mirrored.
Peers follow `<client>@<cluster>` specification and get assigned a unique-id (UUID)
when added. See `Creating Users` section on how to create Ceph users for mirroring.

To add a peer use::

  $ ceph fs snapshot mirror peer_add <fs_name> <remote_cluster_spec> [<remote_fs_name>] [<remote_mon_host>] [<cephx_key>]

`<remote_fs_name>` is optional, and default to `<fs_name>` (on the remote cluster).

This requires the remote cluster ceph configuration and user keyring to be available in
the primary cluster. See `Bootstrap Peers` section to avoid this. `peer_add` additionally
supports passing the remote cluster monitor address and the user key. However, bootstrapping
a peer is the recommended way to add a peer.

.. note:: Only a single peer is supported right now.

To remove a peer use::

  $ ceph fs snapshot mirror peer_remove <fs_name> <peer_uuid>

.. note:: See `Mirror Daemon Status` section on how to figure out Peer UUID.

To list file system mirror peers use::

  $ ceph fs snapshot mirror peer_list <fs_name>

To configure a directory for mirroring, use::

  $ ceph fs snapshot mirror add <fs_name> <path>

To stop a mirroring directory snapshots use::

  $ ceph fs snapshot mirror remove <fs_name> <path>

Only absolute directory paths are allowed. Also, paths are normalized by the mirroring
module, therefore, `/a/b/../b` is equivalent to `/a/b`.

  $ mkdir -p /d0/d1/d2
  $ ceph fs snapshot mirror add cephfs /d0/d1/d2
  {}
  $ ceph fs snapshot mirror add cephfs /d0/d1/../d1/d2
  Error EEXIST: directory /d0/d1/d2 is already tracked

Once a directory is added for mirroring, its subdirectory or ancestor directories are
disallowed to be added for mirroring::

  $ ceph fs snapshot mirror add cephfs /d0/d1
  Error EINVAL: /d0/d1 is a ancestor of tracked path /d0/d1/d2
  $ ceph fs snapshot mirror add cephfs /d0/d1/d2/d3
  Error EINVAL: /d0/d1/d2/d3 is a subtree of tracked path /d0/d1/d2

Commands to check directory mapping (to mirror daemons) and directory distribution are
detailed in `Mirror Daemon Status` section.

Bootstrap Peers
---------------

Adding a peer (via `peer_add`) requires the peer cluster configuration and user keyring
to be available in the primary cluster (manager host and hosts running the mirror daemon).
This can be avoided by bootstrapping and importing a peer token. Peer bootstrap involves
creating a bootstrap token on the peer cluster via::

  $ ceph fs snapshot mirror peer_bootstrap create <fs_name> <client_entity> <site-name>

e.g.::

  $ ceph fs snapshot mirror peer_bootstrap create backup_fs client.mirror_remote site-remote
  {"token": "eyJmc2lkIjogIjBkZjE3MjE3LWRmY2QtNDAzMC05MDc5LTM2Nzk4NTVkNDJlZiIsICJmaWxlc3lzdGVtIjogImJhY2t1cF9mcyIsICJ1c2VyIjogImNsaWVudC5taXJyb3JfcGVlcl9ib290c3RyYXAiLCAic2l0ZV9uYW1lIjogInNpdGUtcmVtb3RlIiwgImtleSI6ICJBUUFhcDBCZ0xtRmpOeEFBVnNyZXozai9YYUV0T2UrbUJEZlJDZz09IiwgIm1vbl9ob3N0IjogIlt2MjoxOTIuMTY4LjAuNTo0MDkxOCx2MToxOTIuMTY4LjAuNTo0MDkxOV0ifQ=="}

`site-name` refers to a user-defined string to identify the remote filesystem. In context
of `peer_add` interface, `site-name` is the passed in `cluster` name from `remote_cluster_spec`.

Import the bootstrap token in the primary cluster via::

  $ ceph fs snapshot mirror peer_bootstrap import <fs_name> <token>

e.g.::

  $ ceph fs snapshot mirror peer_bootstrap import cephfs eyJmc2lkIjogIjBkZjE3MjE3LWRmY2QtNDAzMC05MDc5LTM2Nzk4NTVkNDJlZiIsICJmaWxlc3lzdGVtIjogImJhY2t1cF9mcyIsICJ1c2VyIjogImNsaWVudC5taXJyb3JfcGVlcl9ib290c3RyYXAiLCAic2l0ZV9uYW1lIjogInNpdGUtcmVtb3RlIiwgImtleSI6ICJBUUFhcDBCZ0xtRmpOeEFBVnNyZXozai9YYUV0T2UrbUJEZlJDZz09IiwgIm1vbl9ob3N0IjogIlt2MjoxOTIuMTY4LjAuNTo0MDkxOCx2MToxOTIuMTY4LjAuNTo0MDkxOV0ifQ==

Mirror Daemon Status
--------------------

Mirror daemons get asynchronously notified about changes in file system mirroring status
and/or peer updates.

CephFS mirroring module provides `mirror daemon status` interface to check mirror daemon
status::

  $ ceph fs snapshot mirror daemon status

E.g::

  $ ceph fs snapshot mirror daemon status | jq
  [
    {
      "daemon_id": 284167,
      "filesystems": [
        {
          "filesystem_id": 1,
          "name": "a",
          "directory_count": 1,
          "peers": [
            {
              "uuid": "02117353-8cd1-44db-976b-eb20609aa160",
              "remote": {
                "client_name": "client.mirror_remote",
                "cluster_name": "ceph",
                "fs_name": "backup_fs"
              },
              "stats": {
                "failure_count": 1,
                "recovery_count": 0
              }
            }
          ]
        }
      ]
    }
  ]

An entry per mirror daemon instance is displayed along with information such as configured
peers and basic stats. For more detailed stats, use the admin socket interface as detailed
below.

CephFS mirror daemons provide admin socket commands for querying mirror status. To check
available commands for mirror status use::

  $ ceph --admin-daemon /path/to/mirror/daemon/admin/socket help
  {
      ....
      ....
      "fs mirror status cephfs@360": "get filesystem mirror status",
      ....
      ....
  }

Commands with `fs mirror status` prefix provide mirror status for mirror enabled
file systems. Note that `cephfs@360` is of format `filesystem-name@filesystem-id`.
This format is required since mirror daemons get asynchronously notified regarding
file system mirror status (A file system can be deleted and recreated with the same
name).

Right now, the command provides minimal information regarding mirror status::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror status cephfs@360
  {
    "rados_inst": "192.168.0.5:0/1476644347",
    "peers": {
        "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
            "remote": {
                "client_name": "client.mirror_remote",
                "cluster_name": "site-a",
                "fs_name": "backup_fs"
            }
        }
    },
    "snap_dirs": {
        "dir_count": 1
    }
  }

`Peers` section in the command output above shows the peer information such as unique
peer-id (UUID) and specification. The peer-id is required to remove an existing peer
as mentioned in the `Mirror Module and Interface` section.

Command with `fs mirror peer status` prefix provide peer synchronization status. This
command is of format `filesystem-name@filesystem-id peer-uuid`::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "/d0": {
        "state": "idle",
        "last_synced_snap": {
            "id": 120,
            "name": "snap1",
            "sync_duration": 0.079997898999999997,
            "sync_time_stamp": "274900.558797s"
        },
        "snaps_synced": 2,
        "snaps_deleted": 0,
        "snaps_renamed": 0
    }
  }

Synchronization stats such as `snaps_synced`, `snaps_deleted` and `snaps_renamed` are reset
on daemon restart and/or when a directory is reassigned to another mirror daemon (when
multiple mirror daemons are deployed).

A directory can be in one of the following states::

  - `idle`: The directory is currently not being synchronized
  - `syncing`: The directory is currently being synchronized
  - `failed`: The directory has hit upper limit of consecutive failures

When a directory hits a configured number of consecutive synchronization failures, the
mirror daemon marks it as `failed`. Synchronization for these directories are retried.
By default, the number of consecutive failures before a directory is marked as failed
is controlled by `cephfs_mirror_max_consecutive_failures_per_directory` configuration
option (default: 10) and the retry interval for failed directories is controlled via
`cephfs_mirror_retry_failed_directories_interval` configuration option (default: 60s).

E.g., adding a regular file for synchronization would result in failed status::

  $ ceph fs snapshot mirror add cephfs /f0
  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "/d0": {
        "state": "idle",
        "last_synced_snap": {
            "id": 120,
            "name": "snap1",
            "sync_duration": 0.079997898999999997,
            "sync_time_stamp": "274900.558797s"
        },
        "snaps_synced": 2,
        "snaps_deleted": 0,
        "snaps_renamed": 0
    },
    "/f0": {
        "state": "failed",
        "snaps_synced": 0,
        "snaps_deleted": 0,
        "snaps_renamed": 0
    }
  }

This allows a user to add a non-existent directory for synchronization. The mirror daemon
would mark the directory as failed and retry (less frequently). When the directory comes
to existence, the mirror daemons would unmark the failed state upon successful snapshot
synchronization.

When mirroring is disabled, the respective `fs mirror status` command for the file system
will not show up in command help.

Mirroring module provides a couple of commands to display directory mapping and distribution
information. To check which mirror daemon a directory has been mapped to use::

  $ ceph fs snapshot mirror dirmap cephfs /d0/d1/d2
  {
    "instance_id": "404148",
    "last_shuffled": 1601284516.10986,
    "state": "mapped"
  }

.. note:: `instance_id` is the RADOS instance-id associated with a mirror daemon.

Other information such as `state` and `last_shuffled` are interesting when running
multiple mirror daemons.

When no mirror daemons are running the above command shows::

  $ ceph fs snapshot mirror dirmap cephfs /d0/d1/d2
  {
    "reason": "no mirror daemons running",
    "state": "stalled"
  }

Signifying that no mirror daemons are running and mirroring is stalled.

Re-adding Peers
---------------

When re-adding (reassigning) a peer to a file system in another cluster, ensure that
all mirror daemons have stopped synchronization to the peer. This can be checked
via `fs mirror status` admin socket command (the `Peer UUID` should not show up
in the command output). Also, it is recommended to purge synchronized directories
from the peer  before re-adding it to another file system (especially those directories
which might exist in the new primary file system). This is not required if re-adding
a peer to the same primary file system it was earlier synchronized from.

Feature Status
--------------

`cephfs-mirror` daemon is built by default (follows `WITH_CEPHFS` CMake rule).
