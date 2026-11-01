=======================================================
CephFS Subvolume Mirroring Design
=======================================================

.. contents::
   :depth: 3
   :local:

Overview
========

This document describes how CephFS snapshot mirroring should treat
**subvolumes** so that the secondary cluster has a usable subvolume, not
only a copied directory tree. It is intended for developers working on
``mgr/volumes``, ``mgr/mirroring``, and ``cephfs-mirror``.

**Tracker Issue**: https://tracker.ceph.com/issues/78584

Related work:

- Credential isolation / per-share peer:
  https://tracker.ceph.com/issues/75014
- Design for path-jailed keys and super-key handshake:
  https://github.com/ceph/ceph/pull/71686
- Subvolume templates (#81704) and adding a subvolume directory for
  mirroring (#81715) are implementation slices of this layout work.

This document owns **layout, snapshot-root selection, templates, and
CLI**. It does **not** redesign CephX, super-keys, or
``fs mirror peer_resolve_credentials``. Those stay in the credential
isolation design.

Problem and current behavior
============================

``cephfs-mirror`` replicates a directory path. A subvolume is more than
that path: a versioned tree, a ``.meta`` file, and Ceph vxattrs. Using
``ceph fs snapshot mirror add`` on a subvolume path therefore fails in
several ways.

``subvolume info`` fails on the secondary
-----------------------------------------

Mirroring ``/volumes/<group>/<subvol>`` copies files, but
``ceph fs subvolume info`` on the secondary fails because
``ceph.dir.layout.pool`` is missing on
``/volumes/<group>/<subvol>/<uuid>``. Recreating the subvolume on the
secondary writes those xattrs and "fixes" ``info``, at the cost of a
**new UUID**.

``cephfs-mirror`` does not copy Ceph vxattrs. If the remote path is
missing it only ``mkdirs`` a plain directory (mode ``0755``).
``info`` still needs ``.meta`` and vxattrs on the UUID path, so a
plain mkdir is not a subvolume.

v1 snapshots are missed
-----------------------

For v1, snapshots live **on the UUID directory**, not on the name
directory. Adding ``/volumes/<group>/<subvol>`` watches
``<name>/.snap/``, which does not contain user snapshots. Those snaps
sit under ``<name>/<uuid>/.snap/``.

v2 snapshots **are** on the name directory, and ``.meta`` is inside
each snap. v1 ``.meta`` is a sibling of the UUID dir and is **outside**
the snap.

Daemon path symmetry
--------------------

``cephfs-mirror`` assumes the primary and secondary paths are the same
string. ``subvolume create`` on the secondary always calls
``uuid.uuid4()``, so the live data path becomes a different UUID. The
daemon then cannot land v1 snaps (and v2 snap children) on the live
tree without path mapping.

``add_dir`` does not connect to the peer
----------------------------------------

``mgr/mirroring`` ``add_dir`` only updates the RADOS directory map and
notifies a local daemon. The lasting peer mount is in
``cephfs-mirror`` ``PeerReplayer``, created at **peer add**.
``mgr/mirroring`` opens the peer FS only briefly on ``peer_add`` /
``peer_remove`` (``ceph.mirror.info``).

Relationship to credential isolation (#75014)
=============================================

Issue #75014 replaces the filesystem-wide ``rwps`` peer key with
path-jailed per-subvolume keys in orchestrated environments.

A path-jailed key (``mds 'allow rwps path=/volumes/<g>/<sv>'``) **cannot
mkdir parents**. The secondary subvolume must exist **before** the
daemon syncs. That is the "target path creation dilemma" in PR 71686.

The two features share a **subvolume template**:

- #75014 needs a pre-created target so a jailed key can write.
- #78584 needs the same UUID, ``.meta``, version, and vxattrs so
  ``info`` works and snap roots match.

This document specifies templates and snap-root. PR 71686 specifies:

- Super-key (no MDS/OSD caps) and ``fs mirror peer_resolve_credentials``
- Path-jailed entity
  ``client.mirror.<peer_uuid>.<group_name>.<subvol_name>``
- ``libcephfs`` handle pooling
- Standalone fallback: one key scoped to ``/volumes`` (or ``/``)

``ceph-mgr`` must not mint CephX keys. Directory lifecycle on the
orchestrated track belongs to the control plane (CSI, Rook, Dashboard)
using volumes APIs. The standalone track may still have
``mgr/mirroring`` instantiate a skeleton **from a volumes template**
using the existing ``/volumes`` peer key.

Subvolume layout (v1 vs v2)
===========================

User data always lives at::

    /volumes/<group>/<subvol>/<uuid>/

The default group is ``_nogroup``. ``.meta`` always lives at::

    /volumes/<group>/<subvol>/.meta

``.meta`` is volumes' source of truth: ``version``, ``type``,
``path`` (UUID dir), ``state``. ``ceph fs subvolume ls`` is a readdir
of the group directory. ``ceph fs subvolume info`` stats the name dir,
reads ``.meta``, then reads vxattrs on the UUID path.

v1
--

- Snap root: **UUID directory**
- ``ceph.dir.subvolume=1`` on the UUID dir
- Snapshots: ``/volumes/<g>/<sv>/<uuid>/.snap/<snap>/``
- ``.meta`` is **not** inside the snapshot
- No ``--retain-snapshots``

v2
--

- Snap root: **name directory**
- ``ceph.dir.subvolume=1`` on the name dir
- Snapshots: ``/volumes/<g>/<sv>/.snap/<snap>/`` containing ``.meta``
  and ``<uuid>/``
- ``--retain-snapshots`` trash only the UUID dir; name dir and snaps
  remain. Recreate allocates a **new** live UUID; old snaps keep the
  old UUID.

Snap root vs ``snapshot getpath``
---------------------------------

``ceph fs subvolume snapshot getpath`` returns
``snapshot_data_path()``: user data **inside one snapshot**. That is
not the stable ``dir_root`` the daemon watches (``dir_root/.snap/``).

.. list-table::
   :header-rows: 1
   :widths: 12 36 36 36

   * - Version
     - Live ``getpath``
     - **Snap root** (mirror ``dir_root``)
     - ``snapshot getpath``
   * - v1
     - ``.../<sv>/<uuid>``
     - **UUID dir** (same)
     - ``.../<uuid>/.snap/<snap>``
   * - v2
     - ``.../<sv>/<uuid>``
     - **name dir** ``.../<sv>``
     - ``.../<sv>/.snap/<snap>/<uuid>``

UUID policy
-----------

**Preserve the primary UUID on the secondary.** The daemon stays
path-symmetric. Asymmetric source→dest mapping is a non-goal here.

v2 retain+recreate can mix UUIDs **inside snapshots**. The mirrored
``dir_root`` is still the name dir. The daemon applies snaps in snap-id
order; remote live becomes the latest snap's tree. That is existing
snapshot-mirror behavior, not a second UUID scheme. Tests must cover
mixed-UUID replay.

Templates
=========

``mgr/volumes`` owns how a subvolume is created. The template is an
opaque base64 JSON blob so CSI ``VolumeReplication``, Dashboard, and
shell wrappers do not have to parse internals.

Export
------

::

    ceph fs subvolume template export <vol_name> <subvol_name> \
        [--group_name <group>]

Typical payload (illustrative, not a wire freeze)::

    {
      "version": 2,
      "uuid": "<uuid>",
      "group": "_nogroup",
      "subvol": "<name>",
      "path": "/volumes/_nogroup/<name>/<uuid>",
      "snap_root": "/volumes/_nogroup/<name>",
      "type": "subvolume",
      "state": "complete",
      "uid": 0,
      "gid": 0,
      "mode": 493,
      "quota_bytes": 0,
      "snaps_visible": true,
      "data_pool": "<primary pool name, hint only>",
      "pool_namespace": null
    }

``data_pool`` is a **hint**. Secondary pool names often differ. Create
on the secondary must set ``ceph.dir.layout.pool`` to a pool that
**exists on the secondary** (explicit override, or ancestor/default of
the secondary ``/volumes`` tree). Copying the primary pool name will
fail ``info`` or layout set.

``snaps_visible`` is vxattr ``ceph.dir.subvolume.snaps.visible``.
Include it so failover hides ``.snap`` the same way. Do **not**
replicate ``client_respect_subvolume_snapshot_visibility`` (per-cluster
client config).

Create-from-template only writes the tree, ``.meta``, and vxattrs.

Create from template
--------------------

::

    ceph fs subvolume create <vol_name> <subvol_name> \
        --template <base64_token> [--group_name <group>] \
        [--pool_layout <secondary_pool>]

Runs on **whichever cluster receives the CLI**. On the secondary it
must:

1. Create group dir if needed.
2. ``mkdirs`` ``/volumes/<g>/<sv>/<uuid>`` using the **template UUID**.
3. Write ``.meta`` with that path and version.
4. Set vxattrs on the UUID dir: layout (secondary pool), quota,
   namespace, uid/gid/mode.
5. Set ``ceph.dir.subvolume=1`` on the **snap root** (UUID dir for v1,
   name dir for v2).
6. Set ``ceph.dir.subvolume.snaps.visible`` on the name dir, and on the
   UUID dir if quota is set (same as
   ``snapshot_visibility_set`` today).

Do not call ``uuid.uuid4()`` when a template supplies the UUID.

Internal helper: volumes should expose create-from-template against an
already-open ``libcephfs`` handle so standalone ``mgr/mirroring`` can
reuse it on the **peer** mount without going through remote
``mgr_command``.

Two tracks
==========

Track 1: orchestrated (CSI, Rook, Dashboard)
--------------------------------------------

Control plane, **before** data sync:

1. Primary: ``template export``.
2. Secondary: ``subvolume create --template`` (matching UUID).
3. Secondary MON: path-jailed key as in PR 71686.
4. Primary: ``fs subvolume snapshot mirror add`` (see below;
   ``instantiate: false``).

``mgr/mirroring`` does **not** mkdir on the peer. A jailed key could
not create parents anyway. ``cephfs-mirror`` resolves credentials via
the super-key and mounts the pre-created path.

Track 2: standalone (cephadm / CLI)
-----------------------------------

No external orchestrator. Peer bootstrap still uses a key that can
write under ``/volumes`` (today: ``/`` ``rwps``; isolation design may
narrow to ``/volumes``).

The same ``snapshot mirror add`` on the primary also instantiates the
secondary skeleton (``instantiate: true``):

1. Volumes resolves version, snap root, UUID, and builds a template.
2. Volumes hands the spec/template to mirroring
   (``self.remote('mirroring', ...)``).
3. Mirroring connects to the peer (same helper as
   ``verify_and_set_mirror_info``: ``mon_host`` + key from config-key).
4. Mirroring calls volumes' create-from-template on that **remote**
   handle (not an ad-hoc mkdir).
5. Disconnect.
6. Existing ``add_dir(fs, snap_root)``.

The daemon's ``PeerReplayer`` mount (from peer add) then copies data.
The mgr session is connect → skeleton → disconnect, like ``peer_add``.

Starting and stopping subvolume mirroring
=========================================

Both tracks use the volumes CLI on the primary (hides v1/v2 paths).
Track 1 calls it after the orchestrator has created the target and
keys; Track 2 uses it as the whole flow (instantiate then track)::

    ceph fs subvolume snapshot mirror add <vol> <group> <subvol>
    ceph fs subvolume snapshot mirror rm  <vol> <group> <subvol>

A filesystem peer must already be configured. ``peer_add`` and bootstrap
may change with credential isolation and per-share peering (#75014);
this document does not specify them.

``add``:

1. Open the local subvolume; refuse if missing / not complete.
2. Compute ``snap_root`` (v1 UUID dir, v2 name dir).
3. Build template.
4. ``self.remote('mirroring', '<new method>', spec)``.

The ``remote()`` mechanism already exists (Dashboard calls mirroring;
snap_schedule calls volumes). Volumes **does not** call mirroring
today. Add a mirroring method that accepts a spec dict, for example::

    {
      "fs_name": "cephfs",
      "group": "_nogroup",
      "subvol": "myvol",
      "version": 2,
      "snap_root": "/volumes/_nogroup/myvol",
      "data_path": "/volumes/_nogroup/myvol/<uuid>",
      "uuid": "<uuid>",
      "template": "<base64>",
      "instantiate": true
    }

``instantiate: true`` is the standalone fallback. Orchestrators set
``false`` after they have already created the target.

``add_dir`` remains ``(fs_name, path)`` only. It still does not mkdir
or rmdir.

``rm`` untracks ``snap_root``. It does not delete the secondary
subvolume or CephX keys (keys stay with the control plane per 71686).

Component interaction
---------------------

::

    Operator / CSI / Rook / Dashboard
            |
            |  template export  (primary volumes)
            |  create --template (secondary volumes)   [Track 1]
            |  path-jailed key                         [Track 1]
            v
    ┌───────────────────────────────────────────────┐
    │ mgr/volumes                                   │
    │  • snapshot mirror add|rm                     │
    │  • template export / create --template        │
    │  • snap_root = v1 UUID dir, v2 name dir       │
    └────────────────────┬──────────────────────────┘
                         | remote('mirroring', spec)
                         v
    ┌───────────────────────────────────────────────┐
    │ mgr/mirroring                                 │
    │  • Track 2: peer connect, create-from-template│
    │  • existing add_dir(snap_root)                │
    └────────────────────┬──────────────────────────┘
                         | RADOS notify (local)
                         v
    ┌───────────────────────────────────────────────┐
    │ cephfs-mirror (PeerReplayer)                  │
    │  • path-symmetric copy into snap_root         │
    │  • no subvolume layout logic                  │
    └───────────────────────────────────────────────┘

Daemon constraints
==================

Unchanged for this feature:

- ``dir_root/.snap/<snap>/`` → copy onto live ``dir_root`` → ``mksnap``
- Same path string on both clusters (UUID preserved)
- Regular files, directories, symlinks only
- Sets ``ceph.mirror.dirty_snap_id``; does **not** copy
  ``ceph.dir.layout.*``, quota, ``ceph.dir.subvolume``, or
  ``snaps.visible``
- v2: do **not** skip ``.meta`` (in-band)

Do not copy primary ``ceph.dir.layout.pool`` in the daemon.

Optional hardening: ``cephfs-mirror`` (and mirroring's peer mount)
should set ``client_respect_subvolume_snapshot_visibility=false`` so
hidden snaps on the primary still list. Mgr already does this; the
daemon currently relies on the default ``false``.

Remove and unmirror
===================

Local ``ceph fs subvolume rm`` / ``snapshot rm`` stay in volumes only
(trash ``/volumes/_deleting``, v2 retain, purge queue). They do not
notify mirroring today.

If the path is mirrored:

- Primary ``snapshot rm``: daemon later ``propagate_snap_deletes`` on
  the peer.
- Primary ``subvolume rm``: tracked path may vanish; secondary tree is
  not deleted.
- ``subvolume snapshot mirror rm``: untrack only.

Orchestrated key deletion stays with the control plane (71686). This
feature does not auto-delete path-jailed keys or the secondary
subvolume.
