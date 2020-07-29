================
CephFS Mirroring
================

.. note:: CephFS mirroring feature is currently under development. Interfaces detailed in
          this document might change during development cycle.

CephFS will support asynchronous replication of snapshots to a remote CephFS filesystem via
`cephfs-mirror` tool. Snapshots are synchronized by mirroring snapshot data followed by
creating a snapshot with the same name (for a given directory on the remote filesystem) as
the snapshot being synchronized.

Key Idea
--------

For a given snapshot in a directory, `cephfs-mirror` daemon uses recursive stats to identify
changes in a directory tree. For a given directory, local rctime being larger than its
corresponding remote timestamp implies changes exist somewhere in the subtree. The subtree is
scanned further using the same rctime comparison principle to collect a set of changes for
propagation to remote filesystem.

Interfaces
----------

`Mirroring` module (manager plugin) provides interfaces for managing directory snapshot
mirroring. Manager interfaces are (mostly) wrappers around monitor commands for managing
filesystem mirroring and is the recommended control interface. See `Internal Interfaces`
(monitor commands) are detailed below.

.. note:: `Mirroring` module is work in progress and will be detailed when ready for use.

Internal Interfaces
-------------------

Mirroring needs be enabled for a Ceph filesystem for `cephfs-mirror` daemons to start
mirroring directory snapshots::

  $ ceph fs mirror enable <fsname>

Once mirroring is enabled, mirror peers can be added. `cephfs-mirror` daemon mirrors
directory snapshots to mirror peers. A mirror peer is represented as a client name
and cluster name tuple. To add a mirror peer::

  $ ceph fs mirror peer_add <fsname> <client@cluster> <remote_fsname>

Each peer is assigned a unique identifier which can be fetched via `ceph fs dump` or
`ceph fs get` commands as below::

  $ ceph fs get <fsname>
  ...
  ...
  [peers={uuid=e3739ebf-dbce-460a-bf9c-c66b57697c9a, remote_cluster={client_name=client.site-a, cluster_name=site-a, fs_name=backup}}]

To remove a mirror peer use the following::

  $ ceph fs mirror peer_remove <uuid>

Mirroring can be disabled for a Ceph filesystem with::

  $ ceph fs mirror disable <fsname>

Mirror status (enabled/disabled) and filesystem mirror peers are persisted in `FSMap`.
This enables any entity in a Ceph cluster to subscribe to `FSMap` updates and get
notified about changes in mirror status and/or peers. `cephfs-mirror` daemon subscribes
to `FSMap` and gets notified on mirror status and/or peer updates. Peer changes are
handled by starting or stopping mirroring to when a new peer is added or an existing peer
is removed.
