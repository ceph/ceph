.. _cephfs-mirroring:

=========================
CephFS Snapshot Mirroring
=========================

CephFS supports asynchronous replication of snapshots to a remote CephFS file system via
the `cephfs-mirror` tool. Snapshots are synchronized by mirroring snapshot data followed by
creating a remote snapshot with the same name (for a given directory on the remote file system) as
the source snapshot.

Requirements
------------

The primary (local) and secondary (remote) Ceph clusters version should be Pacific or later.

.. _cephfs_mirroring_creating_users:

Creating Users
--------------

Start by creating a Ceph user (on the primary/local cluster) for the `cephfs-mirror` daemon. This user
requires write capability on the metadata pool to create RADOS objects (index objects)
for watch/notify operation and read capability on the data pool(s)::

  $ ceph auth get-or-create client.mirror mon 'profile cephfs-mirror' mds 'allow r' osd 'allow rw tag cephfs metadata=*, allow r tag cephfs data=*' mgr 'allow r'

Create a Ceph user for each file system peer (on the secondary/remote cluster). This user needs
to have full capabilities on the MDS (to take snapshots) and the OSDs::

  $ ceph fs authorize <fs_name> client.mirror_remote / rwps

This user will be supplied as part of the peer specification when adding a peer.

Starting Mirror Daemon
----------------------

The mirror daemon should be spawned using `systemctl(1)` unit files::

  $ systemctl enable cephfs-mirror@mirror
  $ systemctl start cephfs-mirror@mirror

`cephfs-mirror` daemon can be run in foreground using::

  $ cephfs-mirror --id mirror --cluster site-a -f

.. note:: The user specified here is `mirror`, the creation of which is
   described in the :ref:`Creating Users<cephfs_mirroring_creating_users>`
   section.

Multiple ``cephfs-mirror`` daemons may be deployed for concurrent
synchronization and high availability. Mirror daemons share the synchronization
load using a simple ``M/N`` policy, where ``M`` is the number of directories
and ``N`` is the number of ``cephfs-mirror`` daemons.

When ``cephadm`` is used to manage a Ceph cluster, ``cephfs-mirror`` daemons can be
deployed by running the following command:

.. prompt:: bash $

   ceph orch apply cephfs-mirror

To deploy multiple mirror daemons, run a command of the following form:

.. prompt:: bash $

   ceph orch apply cephfs-mirror --placement=<placement-spec>

For example, to deploy 3 `cephfs-mirror` daemons on different hosts, run a command of the following form:

.. prompt:: bash $

  $ ceph orch apply cephfs-mirror --placement="3 host1,host2,host3"

Interface
---------

The `Mirroring` module (manager plugin) provides interfaces for managing
directory snapshot mirroring. These are (mostly) wrappers around monitor
commands for managing file system mirroring and is the recommended control
interface.

Mirroring Module
----------------

The mirroring module is responsible for assigning directories to mirror daemons
for synchronization. Multiple mirror daemons can be spawned to achieve
concurrency in directory snapshot synchronization. When mirror daemons are
spawned (or terminated), the mirroring module discovers the modified set of
mirror daemons and rebalances directory assignments across the new set, thus
providing high-availability.

.. note:: Deploying a single mirror daemon is recommended. Running multiple
   daemons is untested.

The following file types are supported by the mirroring:

- Regular files (-)
- Directory files (d)
- Symbolic link file (l)

The other file types are ignored by the mirroring. So they won't be
available on a successfully synchronized peer.

The mirroring module is disabled by default. To enable the mirroring module,
run the following command:

.. prompt:: bash $

   ceph mgr module enable mirroring

The mirroring module provides a family of commands that can be used to control
the mirroring of directory snapshots. To add or remove directories, mirroring
must be enabled for a given file system. To enable mirroring for a given file
system, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror enable <fs_name>

.. note:: "Mirroring module" commands are prefixed with ``fs snapshot mirror``.
   This distinguishes them from "monitor commands", which are prefixed with ``fs
   mirror``. Be sure (in this context) to use module commands.

To disable mirroring for a given file system, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror disable <fs_name>

After mirroring is enabled, add a peer to which directory snapshots are to be
mirrored. Peers are specified by the ``<client>@<cluster>`` format, which is
referred to elsewhere in this document as the ``remote_cluster_spec``. Peers
are assigned a unique-id (UUID) when added. See the :ref:`Creating
Users<cephfs_mirroring_creating_users>` section for instructions that describe
how to create Ceph users for mirroring.

To add a peer, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror peer_add <fs_name> <remote_cluster_spec> [<remote_fs_name>] [<remote_mon_host>] [<cephx_key>]

``<remote_cluster_spec>`` is of the format ``client.<id>@<cluster_name>``.

``<remote_fs_name>`` is optional, and defaults to `<fs_name>` (on the remote
cluster).

For this command to succeed, the remote cluster's Ceph configuration and user
keyring must be available in the primary cluster. For example, if a user named
``client_mirror`` is created on the remote cluster which has ``rwps``
permissions for the remote file system named ``remote_fs`` (see `Creating
Users`) and the remote cluster is named ``remote_ceph`` (that is, the remote
cluster configuration file is named ``remote_ceph.conf`` on the primary
cluster), run the following command to add the remote filesystem as a peer to
the primary filesystem ``primary_fs``:

.. prompt:: bash $

   ceph fs snapshot mirror peer_add primary_fs client.mirror_remote@remote_ceph remote_fs

To avoid having to maintain the remote cluster configuration file and remote
ceph user keyring in the primary cluster, users can bootstrap a peer (which
stores the relevant remote cluster details in the monitor config store on the
primary cluster). See the :ref:`Bootstrap
Peers<cephfs_mirroring_bootstrap_peers>` section.

The ``peer_add`` command supports passing the remote cluster monitor address
and the user key. However, bootstrapping a peer is the recommended way to add a
peer.

.. note:: Only a single peer is currently supported.

To remove a peer, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror peer_remove <fs_name> <peer_uuid>

To list file system mirror peers, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror peer_list <fs_name>

To configure a directory for mirroring, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror add <fs_name> <path>

To stop mirroring directory snapshots, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror remove <fs_name> <path>

Only absolute directory paths are allowed. 

Paths are normalized by the mirroring module. This means that ``/a/b/../b`` is
equivalent to ``/a/b``. Paths always start from the CephFS file-system root and
not from the host system mount point.

For example::

  $ mkdir -p /d0/d1/d2
  $ ceph fs snapshot mirror add cephfs /d0/d1/d2
  {}
  $ ceph fs snapshot mirror add cephfs /d0/d1/../d1/d2
  Error EEXIST: directory /d0/d1/d2 is already tracked

After a directory is added for mirroring, the additional mirroring of
subdirectories or ancestor directories is disallowed::

  $ ceph fs snapshot mirror add cephfs /d0/d1
  Error EINVAL: /d0/d1 is a ancestor of tracked path /d0/d1/d2
  $ ceph fs snapshot mirror add cephfs /d0/d1/d2/d3
  Error EINVAL: /d0/d1/d2/d3 is a subtree of tracked path /d0/d1/d2

The :ref:`Mirroring Status<cephfs_mirroring_mirroring_status>` section contains
information about the commands for checking the directory mapping (to mirror
daemons) and for checking the directory distribution. 

.. _cephfs_mirroring_bootstrap_peers:

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


.. _cephfs_mirroring_mirroring_status:

Mirroring Status
----------------

CephFS mirroring module provides `mirror daemon status` interface to check mirror daemon status::

  $ ceph fs snapshot mirror daemon status
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

Commands prefixed with`fs mirror status` provide mirror status for mirror enabled
file systems. Note that `cephfs@360` is of format `filesystem-name@filesystem-id`.
This format is required since mirror daemons get asynchronously notified regarding
file system mirror status (A file system can be deleted and recreated with the same
name).

This command currently provides minimal information regarding mirror status::

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

The `Peers` section in the command output above shows the peer information including the unique
peer-id (UUID) and specification. The peer-id is required when removing an existing peer
as mentioned in the `Mirror Module and Interface` section.

Commands prefixed with `fs mirror peer status` provide peer synchronization status. This
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

Synchronization stats including `snaps_synced`, `snaps_deleted` and `snaps_renamed` are reset
on daemon restart and/or when a directory is reassigned to another mirror daemon (when
multiple mirror daemons are deployed).

A directory can be in one of the following states::

  - `idle`: The directory is currently not being synchronized
  - `syncing`: The directory is currently being synchronized
  - `failed`: The directory has hit upper limit of consecutive failures

When a directory experiences a configured number of consecutive synchronization failures, the
mirror daemon marks it as `failed`. Synchronization for these directories is retried.
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
will mark such a directory as failed and retry (less frequently). When the directory is
created, the mirror daemon will clear the failed state upon successful synchronization.

Adding a new snapshot or a new directory manually in the .snap directory of the remote filesystem would result in failed status of the corresponding configured directory. In the remote filesystem::

  $ ceph fs subvolume snapshot create cephfs subvol1 snap2 group1
  or
  $ mkdir /d0/.snap/snap2

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "/d0": {
        "state": "failed",
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

When the snapshot or the directory is removed from the remote filesystem, the mirror daemon will clear the failed state upon successful synchronization of the pending snapshots, if any.

When mirroring is disabled, the respective `fs mirror status` command for the file system
will not show up in command help.

Configuration Options
---------------------

.. confval:: cephfs_mirror_max_concurrent_directory_syncs
.. confval:: cephfs_mirror_action_update_interval
.. confval:: cephfs_mirror_restart_mirror_on_blocklist_interval
.. confval:: cephfs_mirror_max_snapshot_sync_per_cycle
.. confval:: cephfs_mirror_directory_scan_interval
.. confval:: cephfs_mirror_max_consecutive_failures_per_directory
.. confval:: cephfs_mirror_retry_failed_directories_interval
.. confval:: cephfs_mirror_restart_mirror_on_failure_interval
.. confval:: cephfs_mirror_mount_timeout

Re-adding Peers
---------------

When re-adding (reassigning) a peer to a file system in another cluster, ensure that
all mirror daemons have stopped synchronization to the peer. This can be checked
via `fs mirror status` admin socket command (the `Peer UUID` should not show up
in the command output). Also, it is recommended to purge synchronized directories
from the peer  before re-adding it to another file system (especially those directories
which might exist in the new primary file system). This is not required if re-adding
a peer to the same primary file system it was earlier synchronized from.
