.. _rgw-s3-files-api:

============================
 RGW S3 Files API (Design)
============================

.. warning::

   This document describes a design under active development. The
   schema, REST surface, and FoundationDB layout are **unstable**
   and may change without notice. There are no users yet; do not
   depend on anything described here.

Goals
=====

Add an opt-in REST API to ``radosgw`` (selectable via
``rgw_enable_apis``) that implements the AWS S3 Files API control
plane: ``Filesystem``, ``AccessPoint``, and ``MountTarget``
resources, plus the operations that manage them.

The API is **declarative**. RGW translates each call into state
persisted in FoundationDB. Per-zone reconciler microservices watch
that state and program the local NFS data plane (``nfs-ganesha``)
to match.

RGW itself never serves the file protocol. The data path stays in
``nfs-ganesha`` (and, eventually, Samba).

Non-goals (v1)
--------------

* Cross-zonegroup federation of Filesystems.
* SMB (Samba) data plane.
* Per-AccessPoint resource policies.
* Operator-supplied ``IpAddress`` on ``CreateMountTarget`` (the
  field, if AWS sends it, is accepted and ignored).
* Automatic VIP-pool allocation. Mount-target endpoints come from
  the cephadm-managed ``service: nfs`` for the zone.
* Cross-zone failover of MountTargets.
* DNS publishing.

Architecture
============

::

   +-----------+   REST    +---------+    FDB     +---------------+
   | S3 client +---------->+ radosgw +----------->+ FoundationDB  |
   +-----------+           +---------+            +-------+-------+
                                                          |
                                         watch (per-zone subspace)
                                                          v
                                                   +------+-------+
                                                   |  reconciler  |   one per zone
                                                   +------+-------+
                                                          |
                                                 programs Ganesha
                                                          v
                                          +---------------------------+
                                          |  cephadm service: nfs     |
                                          |  (Ganesha + keepalived    |
                                          |   + haproxy, virtual_ip)  |
                                          +---------------------------+

* RGW is the control-plane API; stateless w.r.t. file resources.
* FoundationDB is the source of truth.
* Reconcilers run **per zone**, watch only their own subspace, and
  program the cephadm-managed NFS service local to that zone.
* keepalived/haproxy/VIP plumbing is owned by cephadm's NFS
  service. The reconciler only programs Ganesha exports and reads
  the service's ``virtual_ip`` to populate MountTarget endpoints.

Resource model
==============

Three nested resource types, with progressively narrower scope.

Filesystem (zonegroup-scoped)
-----------------------------

* Bound 1:1 to an S3 bucket. The zonegroup is **derived** from the
  bucket's placement at create time, not caller-supplied.
* Spec: bucket reference, captured ``placement`` name (see
  `Files placements`_), optional default export attributes,
  lifecycle policy fields.
* The captured ``placement`` is immutable for the life of the
  Filesystem.
* Status: zonegroup-level phase only. The Filesystem resource
  itself programs nothing.

AccessPoint (zonegroup-scoped, parent = Filesystem)
---------------------------------------------------

* Holds the ``nfs-ganesha`` export shape: protocol, squash,
  read-only/read-write, sec_flavors, anonymous uid/gid, allowed
  clients (CIDRs), optional sub-path under the bucket.
* Every zone's reconciler in the parent zonegroup renders this
  into a local Ganesha export.
* Status is **per-zone** — one status sub-key per zone (see
  `FoundationDB layout`_).

MountTarget (zone-scoped, parent = AccessPoint)
-----------------------------------------------

* The zone-local network identity that exposes an AccessPoint.
* **Zone selection**: the AWS ``subnetId`` field is reinterpreted
  as a Ceph zone-id. ``availabilityZoneId``, if present in the AWS
  shape, is preferred over ``subnetId``. The literal zone-id is
  validated against the period.
* **IP address**: any caller-supplied ``IpAddress`` field is
  accepted and **ignored**. The mount endpoint is taken from the
  ``virtual_ip`` of the cephadm ``service: nfs`` deployed in the
  target zone, populated by the reconciler into ``status``.
* Lifecycle: created ``Pending``; transitions to ``Available``
  once the reconciler has programmed Ganesha and published the
  endpoint into ``status``.

All MountTargets in a given zone share that zone's
``virtual_ip``. They are distinguished by Ganesha export
pseudo-path, one per AccessPoint.

Scoping summary
---------------

+---------------+--------------+---------------------+
| Resource      | Spec scope   | Status fan-out      |
+===============+==============+=====================+
| Filesystem    | zonegroup    | none                |
+---------------+--------------+---------------------+
| AccessPoint   | zonegroup    | one per zone in zg  |
+---------------+--------------+---------------------+
| MountTarget   | zone         | one (this zone)     |
+---------------+--------------+---------------------+

Files placements
================

NFS service binding is managed through a placement system that
mirrors the existing bucket-placement machinery, with a separate
namespace and a different backend type.

Why a separate placement namespace
----------------------------------

Bucket placement encodes data-locality decisions (pool selection,
EC profile, storage class). Files placement encodes which
file-protocol service fronts a given Filesystem (the cephadm
``service: nfs`` name today; SMB later). The two are orthogonal:
a Filesystem on a "cold" bucket may legitimately be served by a
premium NFS cluster, and vice versa. They get separate placement
namespaces accordingly.

Schema
------

Mirrors bucket placement field-for-field, with a new type:

* **Zonegroup** (period config):

  - ``files_placement_targets`` — list of named placements.
  - ``files_default_placement`` — zonegroup-wide default name for
    new Filesystems.

* **Zone** (period config):

  - ``files_placement_services`` — per-placement-name binding to
    backend services. Structured value:
    ``{ nfs: <cephadm-service-name>, smb: <cephadm-service-name> }``.
    v1 populates only ``nfs``.

Defaults are zonegroup-scoped (matching bucket placement); the
per-zone-ness lives in the bindings, where it belongs — different
zones can map the same placement name to differently-named local
NFS services.

Capture and immutability
------------------------

* ``CreateFilesystem`` resolves the active
  ``files_default_placement`` (or an explicitly-supplied placement
  name) and records it as ``Filesystem.spec.placement``.
* The captured placement is **immutable** for the life of the
  Filesystem.
* Updating ``files_default_placement`` does not migrate existing
  Filesystems — new ones get the new default; old ones keep their
  captured name.

This is the bucket-placement migration story applied to NFS
service rotation: roll out a new NFS service, define a new
placement bound to it, set it as default, and new Filesystems
land on the new service while existing ones remain where they
are until deliberately migrated.

Resolution
----------

For a MountTarget in zone Z, with parent Filesystem ``F``:

1. Look up ``F.spec.placement`` (the captured placement name).
2. In zone Z's period config, look up
   ``files_placement_services[name].nfs`` → cephadm NFS service
   name.
3. Read the FDB NFS-service registration record for that service
   (cephadm-published) → ``virtual_ip``, port, etc.
4. Write the resolved endpoint into ``MountTarget.status``.

If step 2 yields nothing — zone Z has no binding for the placement
name — the MountTarget in Z stays ``Pending`` and surfaces
"placement '<name>' not bound to any NFS service in zone <Z>".
Operator-fixable.

Operator workflow
-----------------

Familiar from bucket placement, mirrored::

   radosgw-admin zonegroup files-placement add \
     --placement-id gen2
   radosgw-admin zonegroup files-placement default \
     --placement-id gen2
   radosgw-admin zone files-placement add \
     --placement-id gen2 \
     --nfs-service nfs.zone-a-prod

State backend
=============

FoundationDB. RGW Files is the second consumer in RGW after
``fdbd4n`` (PR ceph/ceph#65535). It builds on the
``src/rgw/fdb/`` wrapper introduced by that PR, and contributes
back a watch primitive, a thin tuple/directory abstraction, and
versionstamp helpers.

Why FoundationDB
----------------

* Strict serializability across the keyspace simplifies
  multi-resource mutations (e.g., create AccessPoint + bump every
  zone's wakeup key in one transaction).
* Watches drive the reconciler loop without polling.
* Global commit-version stamps replace ad-hoc generation counters.
* The deployment cost is amortized across other RGW FDB
  consumers (``fdbd4n``, future indexes, realm state).

Cluster scope
-------------

A single FDB cluster **per Ceph cluster**, matching the other RGW
FDB consumers (D4N, indexes), which want the FDB instance local
to RGW for latency. Zonegroup is a **keyspace prefix**, not a
separate FDB deployment. Cross-zonegroup federation is handled
by RGW multisite metadata sync over RADOS, not by FDB.

FoundationDB layout
===================

Keys use the FDB tuple/directory convention. Top-level subspace
proposed::

   ("rgw", "files", "v1", ...)

This sits alongside other RGW FDB users (e.g.
``("rgw", "fdbd4n", ...)``) under a shared ``("rgw", ...)``
prefix. The exact prefix convention is to be agreed with the
``fdbd4n`` author (see `Coordination`_).

Resource keys
-------------

::

   ("rgw","files","v1",zg,"fs",fs_id,"spec")
   ("rgw","files","v1",zg,"fs",fs_id,"status")
   ("rgw","files","v1",zg,"ap",ap_id,"spec")
   ("rgw","files","v1",zg,"ap",ap_id,"status","zone",zone)
   ("rgw","files","v1",zg,"zone",zone,"mt",mt_id,"spec")
   ("rgw","files","v1",zg,"zone",zone,"mt",mt_id,"status")

Per-zone control keys
---------------------

::

   ("rgw","files","v1",zg,"zone",zone,"wakeup")
   ("rgw","files","v1",zg,"zone",zone,"reconcilers",host)
   ("rgw","files","v1",zg,"zone",zone,"nfs-services",svc)

Conventions
-----------

* Spec and status are **sibling keys** (not nested in one value).
  RGW writes spec, reconcilers write status; they never CAS
  over each other.
* Each value carries an ``api_version`` field; readers refuse
  unknown versions.
* Value encoding TBD; candidates are zpp_bits (matches fdbd4n)
  and JSON (operator-readable). See `Open questions`_.
* The commit versionstamp is recorded on each spec write and
  read back as ``observed_versionstamp`` in status.

Reconcile model
===============

Wakeup-key watch
----------------

FDB watches are fire-once, must be re-armed, and have a
per-process budget. Rather than watching every spec key, each
zone has one ``wakeup`` key bumped (atomic increment) by RGW
inside the transaction that mutates any spec affecting that zone.

* The reconciler sets a watch on its zone's ``wakeup`` key.
* On fire, it re-lists its filtered subspace and reconciles
  whatever has drifted from its last observed state.
* It re-arms the watch from the read-version of its re-list.

Per-zone wakeup gives one watch per reconciler per zone — well
within FDB's watch budget for any plausible deployment, and
cheaper than per-resource watches.

Reconciler liveness
-------------------

Each reconciler writes a heartbeat row at
``("rgw","files","v1",zg,"zone",zone,"reconcilers",host)`` every
N seconds (default: 5). Stale rows are treated as offline. RGW
sweeps stale rows lazily on read. This replaces the etcd-style
lease pattern, since FDB has no native leases.

Spec/status writer contract
---------------------------

* RGW writes only ``spec`` keys, ``wakeup`` keys, and creates
  ``status`` keys with default values when a resource is created.
* Reconcilers write only the ``status/zone/<zone>`` sub-key for
  their own zone, the heartbeat row, and (during deletion) remove
  their finalizer entry from the spec.
* Neither writes the other's keys.

Lifecycle and deletion
======================

* Every spec write inside a single transaction also bumps the
  appropriate ``wakeup`` keys (one per zone in the zonegroup for
  AccessPoints; one for MountTargets).
* Spec writes record their commit versionstamp; reconciler status
  writes record an ``observed_versionstamp`` so the API layer can
  surface convergence ("Programmed in 2/3 zones").

Finalizers
----------

Each AccessPoint carries a finalizer set, with one entry per zone
in its zonegroup. Deletion is **two-phase**:

1. ``DeleteAccessPoint`` sets ``deletion_timestamp`` on the spec.
   The spec key is **not** removed.
2. Each zone's reconciler observes the deletion, tears down its
   local Ganesha export, and removes its own finalizer entry.
3. RGW reads the spec, sees an empty finalizer set, and removes
   the spec and status keys in one transaction.

This avoids orphan exports surviving after the FDB record is
gone. ``DeleteFilesystem`` cascades to AccessPoints;
``DeleteAccessPoint`` cascades to MountTargets.

REST API surface
================

Enabled via a new ``files`` entry in ``rgw_enable_apis`` (default
``off`` until stable). Routed through a new
``RGWRESTMgr_S3Files`` registered in
``rgw::AppMain::cond_init_apis()`` alongside the existing S3,
Swift, IAM, etc. managers.

Operations to be implemented in v1 cover create / describe /
update (where applicable) / delete / list for each of
``Filesystem``, ``AccessPoint``, and ``MountTarget``. Exact
request and response shapes track the AWS S3 Files API docs and
will be captured in a follow-on op-mapping table.

Authentication uses the existing S3 sigv4 path. Authorization
adds a new IAM action namespace
(e.g. ``s3:CreateFileAccessPoint``), wired through
``rgw_iam_policy*`` so existing policy machinery enforces it.

Configuration options (provisional)
-----------------------------------

New ``rgw.yaml.in`` entries (names provisional):

* ``rgw_files_fdb_cluster_file`` — path to the FDB cluster file
* ``rgw_files_fdb_prefix`` — top-level tuple prefix (default
  ``("rgw","files","v1")``)
* ``rgw_files_fdb_request_timeout_ms``

Per-zone NFS service bindings are **not** config knobs; they live
in zone period config under ``files_placement_services`` (see
`Files placements`_).

Reconciler service
==================

Out-of-tree-for-now microservice (proposed location:
``src/rgw/file_reconciler/`` or its own repo). Run as a
cephadm-managed service alongside the per-zone NFS cluster.

Responsibilities (zone Z):

* Watch
  ``("rgw","files","v1",zg,"zone",Z,"wakeup")``.
* On fire, reconcile:

  - For each AccessPoint in the zonegroup, render the local
    Ganesha export configuration; reload Ganesha if changed.
  - For each MountTarget bound to Z, ensure the export
    pseudo-path is configured and write the served endpoint into
    status.
  - Honor the finalizer protocol on deletion.

* Write the heartbeat row every N seconds.
* Never write to ``spec``.

The reconciler is NFS-only in v1. SMB is a follow-on.

Day-1 implementation note
-------------------------

Day 1 of the RGW-side work uses an **in-memory** ``Store``
implementation; the FDB-backed implementation is gated on PR
#65535 landing (or branch-and-co-develop on top of it). This
unblocks REST-handler and reconciler-contract development before
the FDB wrapper stabilizes.

Failure modes
=============

* **FDB unreachable from RGW.** Control-plane writes return
  503 with ``Retry-After``. Reads also 503; do not serve stale
  caches.
* **Reconciler offline in zone Z.** New work in Z stays
  ``Pending``; existing exports keep serving (Ganesha config is
  not removed). API surfaces "no live reconciler in zone Z".
* **NFS service in Z has no virtual_ip configured.** MountTarget
  stays ``Pending``; status surfaces the operator-fixable error.
* **Bucket moves zonegroups.** Filesystem becomes orphaned and
  surfaces an Error condition. v1 does not auto-migrate.
* **VIP collision.** Reconciler reports ``Error`` in MountTarget
  status. v1 does not reallocate.

Coordination
============

* Builds on PR ceph/ceph#65535 (chardan, draft) which adds
  ``src/rgw/fdb/`` and ``src/rgw/driver/fdbd4n/``.
* Files API contributes back: a watch primitive, a thin
  tuple/directory layer, and versionstamp helpers.
* Tuple-prefix convention to be agreed across FDB-in-RGW
  consumers; the ``("rgw","files","v1",...)`` shape proposed here
  assumes a peer prefix ``("rgw","fdbd4n",...)``.
* Schema-version policy (single global vs per-subsystem) to be
  agreed across consumers.
* Deployment of the FDB cluster itself is out of scope of this
  document and shared with the broader FDB-in-RGW workstream.
* Build flag is shared: ``-DWITH_RADOSGW_FDB=ON``. C++23 / GCC 14+
  baseline matches PR #65535.

Open questions
==============

* Tuple-prefix convention agreed with ``fdbd4n``?
* Schema-version policy: global, or per-subsystem?
* Value encoding: zpp_bits (matches fdbd4n) or JSON
  (operator-readable)?
* Do AccessPoints default to "every zone in the zonegroup", or
  must callers explicitly opt zones in?
* Does ``DescribeFilesystem`` synthesize aggregate availability
  from per-zone status, or surface raw per-zone breakdown?
* Multiple NFS services per zone — necessary in v1?
