.. _cephfs-mirroring:

=========================
CephFS Snapshot Mirroring
=========================

CephFS supports asynchronous push-based replication of snapshots to a remote CephFS file system via
the ``cephfs-mirror`` tool. Snapshots are synchronized by mirroring snapshot data followed by
creating a remote snapshot with the same name (for a given directory on the remote file system) as
the source snapshot.

Requirements
------------

The primary (local) and secondary (remote) Ceph cluster versions should be Pacific or later.

.. _cephfs_mirroring_creating_users:

Creating Users
--------------

Start by creating a Ceph user (on the primary/local cluster) for the ``cephfs-mirror`` daemon. This user
requires write capability on the metadata pool to create RADOS objects (index objects)
for watch/notify operation and read capability on the data pool(s)::

  $ ceph auth get-or-create client.mirror mon 'profile cephfs-mirror' mds 'allow r' osd 'allow rw tag cephfs metadata=*, allow r tag cephfs data=*' mgr 'allow r'

Create a Ceph user for each file system peer (on the secondary/remote cluster). This user needs
to have full capabilities on the MDS (to take snapshots) and the OSDs::

  $ ceph fs authorize <fs_name> client.mirror_remote / rwps

This user will be supplied as part of the peer specification when adding a peer.

Starting Mirror Daemon
----------------------

The mirror daemon is managed by ``systemd``, though in most cases it is best to use
the ``cephadm`` interface::

  $ systemctl enable cephfs-mirror@mirror
  $ systemctl start cephfs-mirror@mirror

``cephfs-mirror`` daemon can be run in foreground using::

  $ cephfs-mirror --id mirror --cluster site-a -f

.. note:: The user specified here is ``mirror``, the creation of which is
   described in the :ref:`Creating Users<cephfs_mirroring_creating_users>`
   section.

Multiple ``cephfs-mirror`` daemons may be deployed for concurrent
synchronization and high availability. Mirror daemons share the synchronization
load using a simple ``M/N`` policy, where ``M`` is the number of directories
and ``N`` is the number of ``cephfs-mirror`` daemons.

When cephadm is used to manage a Ceph cluster, ``cephfs-mirror`` daemons can be
deployed by running the following command:

.. prompt:: bash $

   ceph orch apply cephfs-mirror

To deploy multiple mirror daemons, run a command of the following form:

.. prompt:: bash $

   ceph orch apply cephfs-mirror --placement=<placement-spec>

For example, to deploy three ``cephfs-mirror`` daemons on different hosts, run a command of the following form:

.. prompt:: bash $

  $ ceph orch apply cephfs-mirror --placement="3 host1,host2,host3"

Interface
---------

The Manager ``mirroring`` module provides interfaces for managing
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
   mirror``. Enabling mirroring by using monitor commands will result in the mirror daemon
   entering the "failed" state due to the absence of the `cephfs_mirror` index object.
   So be sure (in this context) to use module commands.

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

``<remote_fs_name>`` is optional, and defaults to the same value as ``<fs_name>``
(on the remote cluster).

For this command to succeed, the remote cluster's Ceph configuration and user
keyring must be available in the primary cluster. For example, if a user named
``client_mirror`` is created on the remote cluster which has ``rwps``
permissions for the remote file system named ``remote_fs`` (see :ref:`Creating
Users<cephfs_mirroring_creating_users>`) and the remote cluster is named
``remote_ceph`` (that is, the remote cluster configuration file is named
``remote_ceph.conf`` on the primary cluster), run the following command to add
the remote filesystem as a peer to the primary filesystem ``primary_fs``:

.. prompt:: bash $

   ceph fs snapshot mirror peer_add primary_fs client.mirror_remote@remote_ceph remote_fs

To avoid having to maintain the remote cluster configuration file and remote
ceph user keyring in the primary cluster, users can bootstrap a peer (which
stores the relevant remote cluster details in the Monitor config store on the
primary cluster). See the :ref:`Bootstrap
Peers<cephfs_mirroring_bootstrap_peers>` section.

The ``peer_add`` command supports passing the remote cluster Monitor address
and the user key. However, bootstrapping a peer is the recommended way to add a
peer.

.. note:: Only a single peer is currently supported.
          The ``peer_add`` command is deprecated and will be removed in a future release.
          Use the ``peer_bootstrap`` command instead.

To remove a peer, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror peer_remove <fs_name> <peer_uuid>

To list file system mirror peers, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror peer_list <fs_name>

To configure a directory for mirroring, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror add <fs_name> <path>

To list the configured directories, run a command of the following form:

.. prompt:: bash $

   ceph fs snapshot mirror ls <fs_name>

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

Adding a peer via the ``peer_add`` subcommand requires the peer cluster configuration and
user keyring to be available in the primary cluster (Manager host and hosts running the
mirror daemon). This can be avoided by bootstrapping and importing a peer token. Peer
bootstrap involves creating a bootstrap token on the peer cluster via::

  $ ceph fs snapshot mirror peer_bootstrap create <fs_name> <client_entity> <site-name>

e.g.::

  $ ceph fs snapshot mirror peer_bootstrap create backup_fs client.mirror_remote site-remote
  {"token": "eyJmc2lkIjogIjBkZjE3MjE3LWRmY2QtNDAzMC05MDc5LTM2Nzk4NTVkNDJlZiIsICJmaWxlc3lzdGVtIjogImJhY2t1cF9mcyIsICJ1c2VyIjogImNsaWVudC5taXJyb3JfcGVlcl9ib290c3RyYXAiLCAic2l0ZV9uYW1lIjogInNpdGUtcmVtb3RlIiwgImtleSI6ICJBUUFhcDBCZ0xtRmpOeEFBVnNyZXozai9YYUV0T2UrbUJEZlJDZz09IiwgIm1vbl9ob3N0IjogIlt2MjoxOTIuMTY4LjAuNTo0MDkxOCx2MToxOTIuMTY4LjAuNTo0MDkxOV0ifQ=="}

``site-name`` refers to a user-defined string to identify the remote filesystem. In context
of the ``peer_add`` subcommand, ``site-name`` is that contained in the ``remote_cluster_spec``.

Import the bootstrap token in the primary cluster via::

  $ ceph fs snapshot mirror peer_bootstrap import <fs_name> <token>

e.g.::

  $ ceph fs snapshot mirror peer_bootstrap import cephfs eyJmc2lkIjogIjBkZjE3MjE3LWRmY2QtNDAzMC05MDc5LTM2Nzk4NTVkNDJlZiIsICJmaWxlc3lzdGVtIjogImJhY2t1cF9mcyIsICJ1c2VyIjogImNsaWVudC5taXJyb3JfcGVlcl9ib290c3RyYXAiLCAic2l0ZV9uYW1lIjogInNpdGUtcmVtb3RlIiwgImtleSI6ICJBUUFhcDBCZ0xtRmpOeEFBVnNyZXozai9YYUV0T2UrbUJEZlJDZz09IiwgIm1vbl9ob3N0IjogIlt2MjoxOTIuMTY4LjAuNTo0MDkxOCx2MToxOTIuMTY4LjAuNTo0MDkxOV0ifQ==


.. _cephfs_mirroring_mirroring_status:

Snapshot Mirroring
------------------

To initiate snapshot mirroring, create a snapshot of the configured directory in the primary cluster::

  $ mkdir -p /d0/d1/d2/.snap/snap1

Mirroring Status
----------------

CephFS mirroring module provides ``mirror daemon status`` interface to check mirror daemon status::

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
                "fs_name": "backup_fs",
                "mon_host": "[v2:192.168.64.5:40183,v1:192.168.64.5:40184]",
                "fsid": "5682c8e5-50cd-4cfd-b75c-5354dcdda487"
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
peers and basic stats. The peer information includes the remote file system name (``fs_name``),
cluster's Monitor addresses (``mon_host``) and cluster FSID (``fsid``). For more detailed
stats, use the admin socket interface as detailed below.

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

Commands beginning with ``fs mirror status`` provide mirror status for mirror enabled
file systems. Note that ``cephfs@360`` is of format ``filesystem-name@filesystem-id``.
This format is required since mirror daemons get asynchronously notified regarding
file system mirror status (a file system can be deleted and recreated with the same
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

The ``peers`` section in the command output above shows the peer information including the unique
peer-id (UUID) and specification. The peer-id is required when removing an existing peer
as mentioned in the *Mirroring Module* section.

Commands beginning with ``fs mirror peer status`` provide peer synchronization status. The
command parameter is of format ``filesystem-name@filesystem-id peer-uuid``::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "metrics": {
        "/d0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "idle",
                    "last_synced_snap": {
                        "id": 120,
                        "name": "snap1",
                        "crawl_duration": "2s",
                        "datasync_queue_wait_duration": "1s",
                        "sync_duration": "33s",
                        "sync_time_stamp": "274900.558797s",
                        "sync_bytes": "149.94 MiB",
                        "sync_files": 5000
                    },
                    "snaps_synced": 2,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        }
    }
  }

The per-directory fields are nested under ``metrics/<mirrored-dir-path>/peer/<peer-uuid>`` so
the same directory path can be reported for multiple peers without key collisions.

.. _cephfs_mirror_peer_status_formatting:

Value formatting
----------------

Several fields in the status output are formatted for readability rather than reported as raw
numbers. The subsections below describe each format; field tables later in this section refer
back to them.

.. _cephfs_mirror_status_durations:

Durations
---------

Fields: ``crawl_duration``, ``datasync_queue_wait_duration``, ``sync_duration``,
``crawl.duration``, ``datasync_queue_wait.duration``, and ``eta``.

Elapsed time is rounded to the nearest whole second and displayed as a combination of days,
hours, minutes, and seconds. The format adapts to the magnitude:

- ``<SS>s`` — seconds only, when less than one minute (for example, ``2s`` or ``33s``)
- ``<M>m <SS>s`` — minutes and seconds, when less than one hour (for example, ``5m 12s``)
- ``<H>h <MM>m <SS>s`` — hours, minutes, and seconds, when less than one day
  (for example, ``1h 05m 30s``)
- ``<D>d <HH>h <MM>m <SS>s`` — days, hours, minutes, and seconds, when one day or longer
  (for example, ``1d 02h 30m 45s``)

.. _cephfs_mirror_status_data_sizes:

Data sizes
----------

Fields: ``sync_bytes``, ``bytes.sync_bytes``, ``bytes.total_bytes``, and the byte counts in
``last_synced_snap``.

Byte counts use binary (IEC) units with two decimal places. The unit is chosen automatically
from ``B``, ``KiB``, ``MiB``, ``GiB``, ``TiB``, or ``PiB`` (powers of 1024). For example,
``149.94 MiB``.

.. _cephfs_mirror_status_throughput:

Throughput
----------

Fields: ``avg_read_throughput_bytes`` and ``avg_write_throughput_bytes``.

Average transfer rate in bytes per second, using the same binary units as :ref:`data sizes
<cephfs_mirror_status_data_sizes>` with a ``/s`` suffix. For example, ``13.03 MiB/s`` means
13.03 mebibytes per second.

.. _cephfs_mirror_status_percentages:

Percentages
-----------

Fields: ``bytes.sync_percent`` and ``files.sync_percent``.

Percentage complete with two decimal places and a ``%`` suffix (for example, ``40.29%``).

.. _cephfs_mirror_status_counts:

Counts
------

Fields: ``sync_files``, ``total_files``, ``snaps_synced``, ``snaps_deleted``, ``snaps_renamed``,
and snapshot ``id``.

Plain unsigned integers with no unit suffix.

.. _cephfs_mirror_status_timestamp:

Timestamp
---------

Field: ``sync_time_stamp``.

Monotonic clock time in seconds (since daemon startup) when the snapshot sync finished,
printed with sub-second precision and an ``s`` suffix (for example, ``274900.558797s``). This
is not a wall-clock or epoch timestamp.

Synchronization stats including ``snaps_synced``, ``snaps_deleted`` and ``snaps_renamed`` are reset
on daemon restart and/or when a directory is reassigned to another mirror daemon (when
multiple mirror daemons are deployed).

A directory can be in one of the following states:

- ``idle``: The directory is currently not being synchronized.
- ``syncing``: The directory is currently being synchronized.
- ``failed``: The directory has hit upper limit of consecutive failures.

When a directory is currently being synchronized, the mirror daemon marks it as ``syncing`` and
``fs mirror peer status`` shows the snapshot being synchronized under the ``current_syncing_snap``::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "metrics": {
        "/d0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "syncing",
                    "current_syncing_snap": {
                        "id": 121,
                        "name": "snap2",
                        "sync-mode": "full",
                        "avg_read_throughput_bytes": "13.03 MiB/s",
                        "avg_write_throughput_bytes": "24.24 MiB/s",
                        "crawl": {
                            "state": "completed",
                            "duration": "2s"
                        },
                        "datasync_queue_wait": {
                            "state": "complete",
                            "duration": "1s"
                        },
                        "bytes": {
                            "sync_bytes": "60.40 MiB",
                            "total_bytes": "149.94 MiB",
                            "sync_percent": "40.29%"
                        },
                        "files": {
                            "sync_files": 2013,
                            "total_files": 5000,
                            "sync_percent": "40.26%"
                        },
                        "eta": "7s"
                    },
                    "snaps_synced": 2,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        }
    }
  }

The mirror daemon marks it back to ``idle``, when the syncing completes.

When ``state`` is ``syncing``, ``current_syncing_snap`` includes the following
progress fields (see :ref:`cephfs_mirror_peer_status_formatting` for how values are
displayed):

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Field
     - Description
   * - ``sync-mode``
     - Whether the snapshot is synchronized with a full tree copy (``full``) or incremental snapdiff/blockdiff (``delta``).
   * - ``avg_read_throughput_bytes``
     - Average read rate from the primary filesystem for this snapshot sync. See
       :ref:`Throughput <cephfs_mirror_status_throughput>`.
   * - ``avg_write_throughput_bytes``
     - Average write rate to the remote peer for this snapshot sync. See
       :ref:`Throughput <cephfs_mirror_status_throughput>`.
   * - ``crawl.state``
     - Whether the directory tree walk is ``in-progress`` or ``completed``. While
       ``in-progress``, ``bytes.total_bytes`` and ``files.total_files`` reflect only what
       has been discovered so far and may keep increasing; once ``completed``, those totals
       are fully discovered for this snapshot sync.
   * - ``crawl.duration``
     - Elapsed crawl time so far, or total crawl time once ``crawl.state`` is ``completed``.
       See :ref:`Durations <cephfs_mirror_status_durations>`.
   * - ``datasync_queue_wait.state``
     - Whether the snapshot is still ``waiting`` in the data-sync queue or has started transfer (``complete``).
   * - ``datasync_queue_wait.duration``
     - Elapsed queue wait time so far, or total queue wait time once transfer has started.
       See :ref:`Durations <cephfs_mirror_status_durations>`.
   * - ``bytes.sync_bytes``
     - Amount of file data synchronized so far for this snapshot. See
       :ref:`Data sizes <cephfs_mirror_status_data_sizes>`.
   * - ``bytes.total_bytes``
     - Total file data discovered for this snapshot sync. See
       :ref:`Data sizes <cephfs_mirror_status_data_sizes>`. Increases during the crawl while
       ``crawl.state`` is ``in-progress``; final once ``crawl.state`` is ``completed``.
   * - ``bytes.sync_percent``
     - Percentage of ``total_bytes`` synchronized so far. See
       :ref:`Percentages <cephfs_mirror_status_percentages>`.
   * - ``files.sync_files``
     - Number of files synchronized so far for this snapshot. See
       :ref:`Counts <cephfs_mirror_status_counts>`.
   * - ``files.total_files``
     - Total number of files discovered for this snapshot sync. See
       :ref:`Counts <cephfs_mirror_status_counts>`. Increases during the crawl while
       ``crawl.state`` is ``in-progress``; final once ``crawl.state`` is ``completed``.
   * - ``files.sync_percent``
     - Percentage of ``total_files`` synchronized so far. See
       :ref:`Percentages <cephfs_mirror_status_percentages>`.
   * - ``eta``
     - Estimated time remaining to finish the snapshot sync, or ``calculating...`` until enough
       samples are collected. See :ref:`Durations <cephfs_mirror_status_durations>`.

``last_synced_snap`` includes these additional fields for the last completed snapshot sync
(see :ref:`cephfs_mirror_peer_status_formatting` for how values are displayed):

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Field
     - Description
   * - ``crawl_duration``
     - Total time spent walking the directory tree for that snapshot sync. See
       :ref:`Durations <cephfs_mirror_status_durations>`.
   * - ``datasync_queue_wait_duration``
     - Total time the snapshot waited in the data-sync queue before file transfer began. See
       :ref:`Durations <cephfs_mirror_status_durations>`.
   * - ``sync_duration``
     - Total elapsed time for the snapshot sync. See :ref:`Durations <cephfs_mirror_status_durations>`.
   * - ``sync_time_stamp``
     - When the sync finished. See :ref:`Timestamp <cephfs_mirror_status_timestamp>`.
   * - ``sync_bytes``
     - Total file data synchronized for that snapshot. See
       :ref:`Data sizes <cephfs_mirror_status_data_sizes>`.
   * - ``sync_files``
     - Number of files synchronized for that snapshot. See :ref:`Counts <cephfs_mirror_status_counts>`.

When a directory experiences a configured number of consecutive synchronization failures, the
mirror daemon marks it as ``failed``. Synchronization for these directories is retried.
By default, the number of consecutive failures before a directory is marked as failed
is controlled by ``cephfs_mirror_max_consecutive_failures_per_directory`` configuration
option (default: ``10``) and the retry interval for failed directories is controlled via
``cephfs_mirror_retry_failed_directories_interval`` configuration option (default: ``60`` seconds).

E.g., adding a regular file for synchronization would result in failed status::

  $ ceph fs snapshot mirror add cephfs /f0
  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "metrics": {
        "/d0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "idle",
                    "last_synced_snap": {
                        "id": 121,
                        "name": "snap2",
                        "crawl_duration": "2s",
                        "datasync_queue_wait_duration": "1s",
                        "sync_duration": "44s",
                        "sync_time_stamp": "500900.600797s",
                        "sync_bytes": "149.94 MiB",
                        "sync_files": 5000
                    },
                    "snaps_synced": 3,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        },
        "/f0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "failed",
                    "snaps_synced": 0,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        }
    }
  }

This allows a user to add a non-existent directory for synchronization. The mirror daemon
will mark such a directory as failed and retry (less frequently). When the directory is
created, the mirror daemon will clear the failed state upon successful synchronization.

Adding a new snapshot or a new directory in the ``.snap`` directory of the
remote filesystem will result in failed status of the corresponding configured directory.
In the remote filesystem::

  $ ceph fs subvolume snapshot create cephfs subvol1 snap2 group1
  or
  $ mkdir /d0/.snap/snap2

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok fs mirror peer status cephfs@360 a2dc7784-e7a1-4723-b103-03ee8d8768f8
  {
    "metrics": {
        "/d0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "failed",
                    "failure_reason": "snapshot 'snap2' has invalid metadata",
                    "last_synced_snap": {
                        "id": 120,
                        "name": "snap1",
                        "crawl_duration": "2s",
                        "datasync_queue_wait_duration": "1s",
                        "sync_duration": "33s",
                        "sync_time_stamp": "274900.558797s",
                        "sync_bytes": "149.94 MiB",
                        "sync_files": 5000
                    },
                    "snaps_synced": 2,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        },
        "/f0": {
            "peer": {
                "a2dc7784-e7a1-4723-b103-03ee8d8768f8": {
                    "state": "failed",
                    "snaps_synced": 0,
                    "snaps_deleted": 0,
                    "snaps_renamed": 0
                }
            }
        }
    }
  }

When the snapshot or the directory is removed from the remote filesystem, the mirror daemon will
clear the failed state upon successful synchronization of the pending snapshots, if any.

.. note:: Setting snap-schedule on the remote file system for directories that are being mirrored will
          cause the mirror daemon to report errors like ``invalid metadata``.

.. note:: Treat the remote filesystem as read-only. Nothing is inherently enforced by CephFS.
          But with the right MDS caps, users would not be able to snapshot directories in the
          remote file system.

When mirroring is disabled, the respective ``fs mirror status`` command for the file system
will not show up in command help.

Metrics
-------

CephFS exports mirroring metrics as :ref:`Labeled Perf Counters` for scraping by
monitoring tools (for example Prometheus via ``ceph-exporter``). Operators can inspect
them with the mirror daemon admin socket ``counter dump`` command (see
:ref:`cephfs_mirror_counter_dump`).

Three labeled counter groups are relevant for snapshot mirroring:

- ``cephfs_mirror_mirrored_filesystems`` — per primary file system on a mirror daemon
  (for example ``directory_count``, ``mirroring_peers``).
- ``cephfs_mirror_peers`` — aggregated across all mirrored directories for one peer
  on a file system.
- ``cephfs_mirror_directory`` — per mirrored directory path and peer (new; see
  :ref:`cephfs_mirror_directory_perf_counters`).

The JSON returned by ``fs mirror peer status`` and ``ceph fs snapshot mirror status``
describes the same synchronization state in human-readable form. The
``cephfs_mirror_directory`` counters expose that state as numeric gauges for monitoring
systems.

.. _cephfs_mirror_counter_dump:

Accessing perf counters
~~~~~~~~~~~~~~~~~~~~~~~

On a host running ``cephfs-mirror``::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok counter dump

Labeled groups appear as top-level JSON arrays (for example ``"cephfs_mirror_directory": [ ... ]``).
Each array element has a ``labels`` object and a ``counters`` object. Use
``counter schema`` on the same admin socket to list counter names and types.

.. list-table:: Mirror filesystem metrics (``cephfs_mirror_mirrored_filesystems``)
   :widths: 25 25 75
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - mirroring_peers
     - Gauge
     - The number of peers involved in mirroring
   * - directory_count
     - Gauge
     - The total number of directories being synchronized
   * - mirrored_filesystems
     - Gauge
     - The total number of filesystems which are mirrored
   * - mirror_enable_failures
     - Counter
     - Enable mirroring failures

.. list-table:: Peer replication metrics (``cephfs_mirror_peers``)
   :widths: 25 25 75
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - snaps_synced
     - Counter
     - Total snapshots synchronized for this peer on this file system (all directories combined)
   * - sync_bytes
     - Counter
     - The total bytes being synchronized
   * - sync_failures
     - Counter
     - The total number of failed snapshot synchronizations
   * - snaps_deleted
     - Counter
     - The total number of snapshots deleted
   * - snaps_renamed
     - Counter
     - The total number of snapshots renamed
   * - avg_sync_time
     - Gauge
     - The average time taken by all snapshot synchronizations
   * - last_synced_start
     - Gauge
     - The sync start time of the last synced snapshot
   * - last_synced_end
     - Gauge
     - The sync end time of the last synced snapshot
   * - last_synced_duration
     - Gauge
     - The time duration of the last synchronization
   * - last_synced_bytes
     - Counter
     - The total bytes being synchronized for the last synced snapshot

Peer-level counters are labeled with ``source_fscid``, ``source_filesystem``,
``peer_cluster_name``, and ``peer_cluster_filesystem``. They do not include
``peer_uuid`` or ``directory``; use ``cephfs_mirror_directory`` for per-path detail.

.. _cephfs_mirror_directory_perf_counters:

Per-directory replication metrics (``cephfs_mirror_directory``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``cephfs_mirror_directory`` labeled counter group reports snapshot mirror
progress for a single mirrored directory path toward a single peer. One perf counter
instance is created when a directory is added for mirroring (``fs snapshot mirror add``)
and removed when the directory is removed from the policy.

This matches the per-directory fields under ``metrics/<mirrored-dir-path>/peer/<peer-uuid>``
in ``fs mirror peer status`` (see :ref:`cephfs_mirroring_mgr_snapshot_status`), but uses
raw numeric values suitable for graphs and alerts (bytes per second, seconds, basis points).

**Labels**

Each ``cephfs_mirror_directory`` entry in ``counter dump`` includes:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Label
     - Description
   * - ``source_fscid``
     - File system cluster ID on the primary cluster
   * - ``source_filesystem``
     - File system name on the primary cluster
   * - ``peer_uuid``
     - Mirror peer UUID (disambiguates the same directory path mirrored to multiple peers)
   * - ``peer_cluster_name``
     - Remote cluster name of the peer
   * - ``peer_cluster_filesystem``
     - Remote file system name on the peer
   * - ``directory``
     - Mirrored directory path (for example ``/parent/d1``)

Example (one directory, one peer)::

  $ ceph --admin-daemon /var/run/ceph/cephfs-mirror.asok counter dump
  {
      "cephfs_mirror_directory": [
          {
              "labels": {
                  "source_fscid": "360",
                  "source_filesystem": "cephfs",
                  "peer_uuid": "8a85ab25-70f9-48e9-b82d-56324e75209b",
                  "peer_cluster_name": "site-a",
                  "peer_cluster_filesystem": "backup_fs",
                  "directory": "/parent/d1"
              },
              "counters": {
                  "dir_state": 0,
                  "current_snap_id": 0,
                  "snaps_synced": 1,
                  "last_snap_id": 3,
                  "last_sync_bytes": 157286400,
                  "last_sync_files": 5000,
                  ...
              }
          }
      ]
  }

When exported through ``ceph-exporter``, counter names are prefixed (for example
``ceph_cephfs_mirror_directory_current_sync_bytes``) with label dimensions attached.

**Update frequency**

Counters are not updated on every file read or write. Behavior differs by field group:

- **Current sync gauges** (``current_*``, ``crawl_*``, ``datasync_wait_*``, ``dir_state``
  while syncing): refreshed by the :ref:`per-peer tick thread <cephfs_mirror_tick_thread>`
  for each registered directory, on the interval configured by
  ``cephfs_mirror_tick_interval`` (default ``5`` seconds). Only directories that are
  actively registered for synchronization on this daemon are updated.
- **Last synced gauges** (``last_*``): updated when a snapshot sync completes and when
  a directory is added for mirroring.
- **Summary gauges** (``snaps_synced``, ``snaps_deleted``, ``snaps_renamed``): updated when
  the corresponding per-directory counters change (sync complete, snap delete, snap rename).

**Mapping to ``fs mirror peer status``**

.. list-table::
   :widths: 35 35 30
   :header-rows: 1

   * - Perf counter
     - Admin socket / mgr JSON field
     - Notes
   * - ``dir_state``
     - ``state``
     - ``0`` = idle, ``1`` = syncing, ``2`` = failed (not ``stale``; stale is mgr-only)
   * - ``current_snap_id``
     - ``current_syncing_snap.id``
     - ``0`` when idle or failed
   * - ``current_sync_mode``
     - ``current_syncing_snap.sync-mode``
     - ``0`` = full, ``1`` = delta
   * - ``current_read_bps`` / ``current_write_bps``
     - ``avg_read_throughput_bytes`` / ``avg_write_throughput_bytes``
     - Raw bytes per second (not human-readable strings)
   * - ``crawl_state`` / ``crawl_duration_seconds``
     - ``current_syncing_snap.crawl``
     - See crawl state table below
   * - ``datasync_wait_state`` / ``datasync_wait_duration_seconds``
     - ``current_syncing_snap.datasync_queue_wait``
     - See datasync wait state table below
   * - ``current_sync_bytes`` / ``current_total_bytes`` / ``current_sync_bytes_percent``
     - ``current_syncing_snap.bytes``
     - Percent is basis points (``4029`` = 40.29%)
   * - ``current_sync_files`` / ``current_total_files`` / ``current_sync_files_percent``
     - ``current_syncing_snap.files``
     - Percent is basis points
   * - ``current_eta_valid`` / ``current_eta_seconds``
     - ``current_syncing_snap.eta``
     - ``current_eta_valid``: ``0`` = calculating, ``1`` = ETA available
   * - ``last_snap_id`` and ``last_*_duration_seconds``, ``last_sync_bytes``, ``last_sync_files``
     - ``last_synced_snap``
     - Durations in seconds; bytes are raw counts
   * - ``last_sync_timestamp``
     - ``last_synced_snap.sync_time_stamp``
     - ``utime_t`` (seconds since epoch), not the monotonic string shown in admin JSON
   * - ``snaps_synced`` / ``snaps_deleted`` / ``snaps_renamed``
     - Same field names at directory level
     - Reset on daemon restart or directory reassignment (same as admin socket)

**Directory state (``dir_state``)**

.. list-table::
   :widths: 15 85
   :header-rows: 1

   * - Value
     - Meaning
   * - ``0``
     - Idle — no snapshot is currently being synchronized for this directory
   * - ``1``
     - Syncing — ``current_*`` gauges reflect in-progress snapshot sync
   * - ``2``
     - Failed — directory hit the consecutive failure limit; ``current_*`` gauges are cleared

When ``dir_state`` is ``0`` or ``2``, all ``current_*``, ``crawl_*``, and ``datasync_wait_*``
counters are set to zero.

**Current syncing snapshot counters**

Present while ``dir_state`` is ``1``. Corresponds to ``current_syncing_snap`` in
``fs mirror peer status``.

.. list-table::
   :widths: 30 15 55
   :header-rows: 1

   * - Counter
     - Type
     - Description
   * - ``current_snap_id``
     - Gauge
     - Snapshot ID being synchronized
   * - ``current_sync_mode``
     - Gauge
     - ``0`` = full sync, ``1`` = delta (snapdiff/blockdiff)
   * - ``current_read_bps``
     - Gauge
     - Average primary read throughput in bytes per second for this sync
   * - ``current_write_bps``
     - Gauge
     - Average remote write throughput in bytes per second for this sync
   * - ``crawl_state``
     - Gauge
     - ``0`` = not applicable, ``1`` = in progress, ``2`` = completed
   * - ``crawl_duration_seconds``
     - Gauge
     - Crawl duration in seconds (elapsed while in progress, total when completed)
   * - ``datasync_wait_state``
     - Gauge
     - ``0`` = none, ``1`` = waiting in data-sync queue, ``2`` = transfer started
   * - ``datasync_wait_duration_seconds``
     - Gauge
     - Time in the data-sync queue in seconds
   * - ``current_sync_bytes``
     - Gauge
     - Bytes synchronized so far for this snapshot
   * - ``current_total_bytes``
     - Gauge
     - Total bytes discovered for this snapshot sync
   * - ``current_sync_bytes_percent``
     - Gauge
     - Sync progress in **basis points** (``10000`` = 100.00%)
   * - ``current_sync_files``
     - Gauge
     - Files synchronized so far
   * - ``current_total_files``
     - Gauge
     - Total files discovered for this snapshot sync
   * - ``current_sync_files_percent``
     - Gauge
     - File progress in **basis points**
   * - ``current_eta_valid``
     - Gauge
     - ``0`` = ETA not yet available (``calculating...`` in admin socket), ``1`` = ETA available
   * - ``current_eta_seconds``
     - Gauge
     - Estimated seconds remaining when ``current_eta_valid`` is ``1``

**Last synced snapshot counters**

Updated after each successful snapshot synchronization. Corresponds to
``last_synced_snap`` in ``fs mirror peer status``.

.. list-table::
   :widths: 35 15 50
   :header-rows: 1

   * - Counter
     - Type
     - Description
   * - ``last_snap_id``
     - Gauge
     - Snapshot ID of the last successfully synchronized snapshot
   * - ``last_crawl_duration_seconds``
     - Gauge
     - Directory crawl duration for that sync
   * - ``last_datasync_wait_duration_seconds``
     - Gauge
     - Data-sync queue wait duration for that sync
   * - ``last_sync_duration_seconds``
     - Gauge
     - Total time to synchronize that snapshot
   * - ``last_sync_timestamp``
     - Time
     - Wall-clock time when that sync finished (``utime_t``)
   * - ``last_sync_bytes``
     - Gauge
     - Bytes synchronized for that snapshot
   * - ``last_sync_files``
     - Gauge
     - Files synchronized for that snapshot

**Per-directory snapshot summary counters**

.. list-table::
   :widths: 25 15 60
   :header-rows: 1

   * - Counter
     - Type
     - Description
   * - ``snaps_synced``
     - Gauge
     - Snapshots successfully synchronized since the last counter reset
   * - ``snaps_deleted``
     - Gauge
     - Snapshot deletes propagated to the peer since the last reset
   * - ``snaps_renamed``
     - Gauge
     - Snapshot renames propagated to the peer since the last reset

These counters reset when the mirror daemon restarts or when a directory is reassigned to
another mirror daemon, consistent with ``fs mirror peer status``.

.. _cephfs_mirror_tick_thread:

Per-peer tick thread
~~~~~~~~~~~~~~~~~~~~

Each mirror peer handled by a ``cephfs-mirror`` daemon runs a dedicated tick thread in its
``PeerReplayer``. The thread wakes every :confval:`cephfs_mirror_tick_interval` seconds
(default ``5``), re-reads that option on each iteration so configuration changes take effect
without restarting the daemon, and runs periodic mirroring work.

Currently, the tick thread refreshes the ``current_*``, ``crawl_*``, ``datasync_wait_*``, and
``dir_state`` fields of :ref:`cephfs_mirror_directory_perf_counters` for each directory that
is actively registered for synchronization on the daemon.

Example — set the tick interval to 10 seconds for the mirror daemon user::

  ceph config set client.mirror cephfs_mirror_tick_interval 10

Configuration Options
---------------------

.. confval:: cephfs_mirror_max_concurrent_directory_syncs
.. confval:: cephfs_mirror_max_datasync_threads
.. confval:: cephfs_mirror_distribute_datasync_threads
.. confval:: cephfs_mirror_datasync_files_per_batch
.. confval:: cephfs_mirror_action_update_interval
.. confval:: cephfs_mirror_restart_mirror_on_blocklist_interval
.. confval:: cephfs_mirror_max_snapshot_sync_per_cycle
.. confval:: cephfs_mirror_directory_scan_interval
.. confval:: cephfs_mirror_max_consecutive_failures_per_directory
.. confval:: cephfs_mirror_retry_failed_directories_interval
.. confval:: cephfs_mirror_restart_mirror_on_failure_interval
.. confval:: cephfs_mirror_mount_timeout
.. confval:: cephfs_mirror_perf_stats_prio
.. confval:: cephfs_mirror_tick_interval
.. confval:: cephfs_mirror_blockdiff_min_file_size

Re-adding Peers
---------------

When re-adding (reassigning) a peer to a file system in another cluster, ensure that
all mirror daemons have stopped synchronization to the peer. This can be checked
via the ``fs mirror status`` admin socket command (the *Peer UUID* should not show up
in the command output). Also, it is recommended to purge synchronized directories
from the peer  before re-adding it to another file system (especially those directories
which might exist in the new primary file system). This is not required if re-adding
a peer to the same primary file system it was earlier synchronized from.

Multi-threaded snapshot sync
----------------------------

CephFS mirroring now utilizes a multi-threaded architecture to improve synchronization
performance. The workload is split into two distinct thread pools: a crawler thread pool, which
manages snapshot crawl and a data synchronization thread pool, which handles concurrent file
transfers. Users can fine-tune these operations using configuration parameters:
- ``cephfs_mirror_max_concurrent_directory_syncs``: controls the number of concurrent snapshots being crawled.
- ``cephfs_mirror_max_datasync_threads``: controls the total threads available for data sync.
For more information, see https://tracker.ceph.com/issues/73452
