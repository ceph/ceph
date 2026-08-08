.. _rgw-s3-files-api:

============================
 RGW S3 Files API (Design)
============================

.. warning::

   This document describes a design under active development. The
   schema, REST surface, and FoundationDB layout are **unstable**
   and may change without notice. There are no users yet; do not
   depend on anything described here.

Implementation status
=====================

This document describes the **target** architecture. Much of it is
design intent, not shipped code. As of this writing the in-tree
state is:

* **Store**: only an in-process, non-persistent ``MemoryStore`` is
  implemented. The FoundationDB-backed store and the entire FDB key
  layout described below are design only — they depend on a shared
  FDB client and generic ``rgw_fdb_*`` configuration tracked by
  ``ceph/ceph#65535``. Treat every FoundationDB reference in this
  document as design, not behavior.
* **Data-plane sink**: the ``Reconciler`` and a ``GaneshaSink``
  *interface* exist, exercised in unit tests via an in-process
  recording double. **No production sink ships.** The original
  D-Bus sink has been removed; the production transport will be
  gRPC and is future work. ``radosgw`` does **not** start the
  reconciler — the startup wiring is deferred with the gRPC sink.
* **REST surface**: implemented, and the primary reviewable
  surface. It reads/writes the active ``Store`` directly.

Net: this is a design + scaffolding drop. The only persistence is
in-memory, and the control plane's state is not yet programmed into
any NFS data plane.

Goals
=====

Add an opt-in REST API to ``radosgw`` (selectable via
``rgw_enable_apis``) that implements the AWS S3 Files API control
plane: ``FileSystem``, ``AccessPoint``, and ``MountTarget``
resources, plus the operations that manage them. The wire-level
protocol is REST + JSON (Smithy ``restJson1``), not S3's XML
shape.

The API is **declarative**. RGW translates each call into state
persisted in FoundationDB. Per-zone reconciler microservices watch
that state and program the local NFS data plane (``nfs-ganesha``)
to match.

RGW itself never serves the file protocol. The data path stays in
``nfs-ganesha`` (and, eventually, Samba).

Reference: the AWS Smithy model is vendored in-tree at
``src/rgw/spec/aws/s3files/<version>/s3files-<version>.json``
(Apache-2.0, see ``src/rgw/spec/aws/LICENSE`` and ``NOTICE``).
Source: ``aws/api-models-aws`` on GitHub, daily-updated. Currently
tracked version: ``2025-05-05``. Update process is documented in
``src/rgw/spec/aws/README.md``.

Non-goals (v1)
--------------

* Cross-zonegroup federation of FileSystems.
* SMB (Samba) data plane.
* Operator-supplied IP address on ``CreateMountTarget`` (the
  ``ipv4Address`` / ``ipv6Address`` fields are accepted and
  ignored).
* Automatic VIP-pool allocation. Mount-target endpoints come from
  the per-zone files-placement binding.
* Cross-zone failover of MountTargets.
* DNS publishing.
* KMS encryption enforcement (``kmsKeyId`` is captured in spec
  but not acted on).
* Synchronization configuration enforcement (config is captured
  in spec; the reconciler does not act on it because we serve the
  bucket directly).
* FileSystem resource policy enforcement at the NFS data plane —
  v1 enforces policies for API control-plane calls only, not for
  NFS-mount-time access decisions.

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
* FoundationDB (per-realm) is the source of truth.
* Reconcilers run **per zone**, watch only their own subspace, and
  program the cephadm-managed NFS service local to that zone.
* keepalived/haproxy/VIP plumbing is owned by cephadm's NFS
  service. The reconciler only programs Ganesha exports and reads
  the placement binding to populate MountTarget endpoints.

Resource model
==============

Three resource types, tracking the AWS S3 Files API.
``AccessPoint`` and ``MountTarget`` are **siblings**, both
children of ``FileSystem`` — not nested.

::

   FileSystem ─┬─ AccessPoint    (POSIX identity / path scope)
               └─ MountTarget    (network identity, per zone)

FileSystem (zonegroup-scoped)
-----------------------------

Bound 1:1 to an S3 bucket and an optional path prefix within it.
The zonegroup is **derived** from the bucket's placement at create
time; not caller-supplied.

Spec (AWS shape):

* ``bucket`` — S3 bucket ARN. Required.
* ``prefix`` — optional path prefix within the bucket; scopes the
  filesystem's view. Default empty (whole bucket).
* ``roleArn`` — IAM role ARN granting service permission to
  access the bucket. Required at create. v1 validates that the
  ARN names a Ceph IAM role that exists in the caller's account
  (RGW already supports account- and role-scoped IAM); wire-level
  role assumption / delegation is deferred.
* ``kmsKeyId`` — optional. Captured in spec; not enforced.
* ``placement`` — captured Files-placement name (see
  `Files placements`_). Resolved from the active
  ``files_default_placement`` at create time, or supplied
  explicitly. Immutable.
* ``acceptBucketWarning`` — boolean; controls whether warnings
  about bucket configuration are tolerated at create time.
* ``tags`` — list of key/value tag pairs.

Status / response fields:

* ``status`` — lifecycle state (see `Lifecycle states`_).
* ``creationTime`` — Unix epoch.
* Server-assigned: ``fileSystemId``, ``fileSystemArn``,
  ``ownerId``.
* ``name`` — convenience, derived from the ``Name`` tag if
  present (per AWS convention).

Immutability: AWS exposes no ``UpdateFileSystem``. The FileSystem
spec is **immutable** post-create. Resource policy,
synchronization configuration, and tags are managed via dedicated
ops.

AccessPoint (zonegroup-scoped, child of FileSystem)
---------------------------------------------------

A POSIX-identity / path-scope refinement on a FileSystem. AWS's
``CreateAccessPoint`` exposes a deliberately small surface:

Spec:

* ``fileSystemId`` — parent. Required.
* ``posixUser`` — uid, gid, secondary gid list. Optional.
* ``rootDirectory`` — path within the parent FileSystem (further
  narrowing the FileSystem's prefix); optional creation
  permissions for new directories.
* ``tags`` — tag list.

Notably absent from the AccessPoint API surface:
``squash``, ``ro/rw``, ``sec_flavors``, ``allowed_clients``,
``anon_uid/gid``. AWS does not expose these on the AP — they are
defaults of the managed service. We follow the same shape: those
NFS-export attributes live in **zone-level files-placement
defaults** (see `Files placements`_), not on the AccessPoint API.

Status: lifecycle state plus **per-zone** status sub-keys (one
per zone in the parent zonegroup) reflecting each zone's
reconciler progress.

Immutability: AWS exposes no ``UpdateAccessPoint``. The AP spec
is immutable post-create.

Every zone's reconciler in the parent zonegroup renders the AP
into a local Ganesha export — applying the placement's export
defaults plus the AP's posixUser and rootDirectory.

MountTarget (zone-scoped, child of FileSystem)
----------------------------------------------

The zone-local network identity that exposes a FileSystem.
Sibling to AccessPoints, **not** nested under them — at most one
MountTarget per (FileSystem, zone) pair, serving every
AccessPoint on the FileSystem through pseudo-path
discrimination.

Spec (request fields, mapping to AWS shape):

* ``fileSystemId`` — parent. Required.
* ``subnetId`` — required by AWS shape. **Carries a Ceph
  zone-id**, encoded as ``subnet-{zone_id}`` to satisfy the
  Smithy ``SubnetId`` pattern ``^subnet-[0-9a-f]{8,40}$``.
  RGW strips the ``subnet-`` prefix at the API layer to recover
  the zone-id and validates it against the active period. The
  zone-id must therefore be a hex string, 8..40 chars (Ceph
  zones use 32-char hex UUIDs, which fit naturally).
  ``availabilityZoneId`` is response-only in the AWS shape and
  is not accepted as input.
* ``ipv4Address`` / ``ipv6Address`` — accepted and ignored.
* ``ipAddressType`` — accepted; ``IPV4_ONLY`` is the default.
* ``securityGroups`` — accepted and ignored (no VPC SG model in
  Ceph).

Status / response fields:

* Server-assigned: ``mountTargetId``, ``ownerId``,
  ``networkInterfaceId`` (synthesized stub), ``vpcId``
  (synthesized stub).
* ``availabilityZoneId``, ``availabilityZoneName`` populated with
  the bare zone-id (no ``subnet-`` prefix), matching AWS's "this
  MT lives in AZ X" convention.
* ``ipv4Address`` populated by the reconciler from the resolved
  ``virtual_ip`` of the placement's NFS endpoint binding.
* ``status`` — lifecycle state (see `Lifecycle states`_).

Mutability: ``UpdateMountTarget`` is the only update operation in
the AWS surface. v1 accepts the call and updates spec; the
mutable fields (``securityGroups``, ``ipAddressType``) remain
effectively no-ops at the data plane.

All MountTargets serving a given (FileSystem, zone) pair share
the placement's ``virtual_ip``. AccessPoints under the FileSystem
are distinguished by Ganesha export pseudo-path (see
`Pseudo-path scheme`_).

Lifecycle states
================

All three resources expose the AWS lifecycle state enum::

   AVAILABLE | CREATING | DELETING | DELETED | ERROR | UPDATING

Transitions:

* On create: ``CREATING`` → ``AVAILABLE`` once the reconciler
  reports success in (for FS/AP) every zone in the zonegroup, or
  (for MT) the bound zone.
* On delete: ``AVAILABLE`` → ``DELETING`` → ``DELETED`` after
  finalizers clear (see `Lifecycle and deletion`_).
* On update (MT only): ``AVAILABLE`` → ``UPDATING`` →
  ``AVAILABLE``.
* On reconcile failure: ``ERROR`` with ``statusMessage``
  describing the operator-fixable cause.

Resource policies
=================

FileSystems carry an optional resource policy — IAM-policy-shaped
JSON, modeled on S3 bucket policies. Managed via:

* ``PutFileSystemPolicy`` — set or replace the policy on a FS.
* ``GetFileSystemPolicy``
* ``DeleteFileSystemPolicy``

v1 enforcement scope: **API control-plane calls only**. Policies
are evaluated against subsequent S3-Files-API operations against
the FileSystem (e.g., who can ``PutFileSystemPolicy`` or
``CreateAccessPoint`` on this FS, cross-account access, etc.).
They are **not** consulted at NFS mount time or for in-flight NFS
operations — the EFS-style mount helper auth flow (sigv4 over a
TLS-tunneled mount) is not implemented at the data plane in v1.
Wire-level policy enforcement is a follow-on.

Storage: policy JSON lives alongside the FS spec in FDB.

AccessPoints do not carry resource policies in AWS; we follow
that shape.

Synchronization configuration
=============================

AWS S3 Files exposes ``GetSynchronizationConfiguration`` and
``PutSynchronizationConfiguration`` to control sync between the
file system view and the underlying S3 bucket.

We serve the bucket directly through Ganesha — no separate file
layer maintains a synced replica — so most of this surface is
no-op semantically. We still:

* **Accept and store** the configuration in FDB alongside the FS
  spec.
* Return it verbatim from ``GetSynchronizationConfiguration``.
* Surface a status reflecting the spec, but do not implement any
  reconciler-side actions in v1.

This preserves API-shape compatibility for clients that issue the
calls, without committing to operational semantics we don't need.

Tagging
=======

Standard AWS tagging surface:

* ``ListTagsForResource`` (input: resource ARN or id)
* ``TagResource``
* ``UntagResource``

Per the AWS Smithy ``ResourceId`` pattern, tagging applies to
**FileSystem** and **AccessPoint** resources only. MountTargets
are not taggable in the AWS shape — their identifiers do not
match the ``ResourceId`` pattern — and we follow that contract.

Tags are key/value pairs stored alongside the resource spec in
FDB. The convention of deriving a resource's ``name`` from a tag
named ``Name`` (per AWS) is honored on ``Get*`` responses.

Pseudo-path scheme
==================

For an AccessPoint owned by account ``A`` with id ``ap-id``, the
Ganesha export pseudo-path is::

   /<account-id>/<ap-id>

Both components are server-assigned: the account is bound to the
authenticated principal at create time, and ``ap-id`` is generated
by RGW. Caller-supplied AccessPoint *names* never appear in the
pseudo-path — they live only in API responses for human
identification. Server-assigned ids guarantee global uniqueness,
which is what gives cross-account isolation under a shared VIP.

The corresponding mount string is::

   <virtual_ip>:/<account-id>/<ap-id>

This is a deliberate divergence from the AWS EFS mount helper
convention, in which the access point id is passed via the
``-o accesspoint=fsap-...`` mount option and the wire-level NFS
path is always ``/``. AWS achieves AP scoping via auth-layer
signaling on the EFS server; we achieve it via server-assigned
ids in the pseudo-path. Standard NFS clients work directly
against this scheme without a custom mount helper.

A future Ceph-side mount helper may translate::

   mount -t ceph-nfs -o accesspoint=<name> /mnt

into the underlying::

   mount -t nfs4 <virtual_ip>:/<account-id>/<ap-id> /mnt

for ergonomic parity with the EFS workflow. v1 documents the
underlying form directly; the helper is a follow-on.

The Ganesha-internal ``Export_Id`` (a ``uint16``) is unrelated to
``ap-id`` and is bookkept by the reconciler — typically a
per-zone monotonic counter or a hash of ``ap-id`` into the 16-bit
space, with collision retry. It is never exposed to clients.

Scoping summary
---------------

+---------------+--------------+------------------------+
| Resource      | Spec scope   | Status fan-out         |
+===============+==============+========================+
| FileSystem    | zonegroup    | one per zone in zg     |
+---------------+--------------+------------------------+
| AccessPoint   | zonegroup    | one per zone in zg     |
+---------------+--------------+------------------------+
| MountTarget   | zone         | one (this zone)        |
+---------------+--------------+------------------------+

Files placements
================

NFS service binding **and** export defaults are managed through a
placement system that mirrors the existing bucket-placement
machinery, with a separate namespace.

Why a separate placement namespace
----------------------------------

Bucket placement encodes data-locality decisions (pool selection,
EC profile, storage class). Files placement encodes which
file-protocol service fronts a given FileSystem (the cephadm
``service: nfs`` name today; SMB later) and the export defaults
to apply (squash, sec flavors, allowed clients, etc.). The two
are orthogonal: a FileSystem on a "cold" bucket may legitimately
be served by a premium NFS cluster, and vice versa. They get
separate placement namespaces accordingly.

Schema
------

Mirrors bucket placement field-for-field, with a new type:

* **Zonegroup** (period config):

  - ``files_placement_targets`` — list of named placements.
  - ``files_default_placement`` — zonegroup-wide default name for
    new FileSystems.

* **Zone** (period config):

  - ``files_placement_services`` — per-placement-name binding to
    a backend NFS endpoint **plus export defaults**::

       {
         # Endpoint (XOR-validated):
         virtual_ip:  "10.0.0.5",          # optional
         port:        2049,                # paired with virtual_ip
         nfs_service: "nfs.zone-a-prod",   # optional

         # Export defaults — applied by the reconciler to every
         # AccessPoint export rendered against this placement:
         export: {
           access_mode:     "rw",                 # rw | ro
           sec_flavors:     ["sys"],              # sys, krb5, krb5i, krb5p
           allowed_clients: ["10.0.0.0/8"],       # CIDR list
           squash:          "no_root_squash",     # root | no_root | all
           anon_uid:        65534,
           anon_gid:        65534
         }
       }

    Endpoint validation: **exactly one** of ``(virtual_ip + port)``
    or ``nfs_service`` per binding. Both forms are accepted; the
    reconciler resolves either to a concrete endpoint at MountTarget
    creation time. SMB will get its own sibling field once the SMB
    data plane lands.

    Export defaults form the per-export baseline. An
    AccessPoint's ``posixUser`` and ``rootDirectory`` further
    refine each rendered export.

Defaults are zonegroup-scoped (matching bucket placement); the
per-zone-ness lives in the bindings, where it belongs — different
zones can map the same placement name to differently-named local
NFS services or VIPs and to different export-default tunings.

Capture and immutability
------------------------

* ``CreateFileSystem`` resolves the active
  ``files_default_placement`` (or an explicitly-supplied placement
  name) and records it as ``FileSystem.spec.placement``.
* The captured placement is **immutable** for the life of the
  FileSystem.
* Updating ``files_default_placement`` does not migrate existing
  FileSystems — new ones get the new default; old ones keep their
  captured name.

This is the bucket-placement migration story applied to NFS
service rotation: roll out a new NFS service, define a new
placement bound to it, set it as default, and new FileSystems
land on the new service while existing ones remain where they
are until deliberately migrated.

Resolution
----------

For a MountTarget in zone Z, with parent FileSystem ``F``:

1. Look up ``F.spec.placement`` (the captured placement name).
2. In zone Z's period config, read
   ``files_placement_services[name]``. Branch on the endpoint
   form:

   - If ``virtual_ip`` and ``port`` are set, use them directly.
   - If ``nfs_service`` is set, query the local Ceph cluster's
     ``orch`` to resolve the named service to its ``virtual_ip``
     and port. This is a read-only orch query; cephadm itself is
     unmodified.

3. Write the resolved endpoint into ``MountTarget.status``.

If step 2 yields nothing — zone Z has no binding for the placement
name — the MountTarget in Z stays ``CREATING`` and surfaces
"placement '<name>' not bound in zone <Z>". Operator-fixable.

If step 2 names a service that the local orch can't resolve, the
MountTarget transitions to ``ERROR`` with
"unresolved nfs_service '<name>'".

Operator workflow
-----------------

Familiar from bucket placement, mirrored::

   radosgw-admin zonegroup files-placement add \
     --placement-id gen2
   radosgw-admin zonegroup files-placement default \
     --placement-id gen2

   # Zone binding takes one of two mutually-exclusive endpoint
   # forms, plus optional export-default flags:
   radosgw-admin zone files-placement add \
     --placement-id gen2 \
     <[--nfs-service NAME] | [--virtual-ip IP --port PORT]> \
     [--access-mode rw|ro] \
     [--sec-flavor sys,krb5,...] \
     [--allowed-clients CIDR,...] \
     [--squash root|no_root|all]

The CLI rejects bindings that supply both endpoint forms or
neither.

State backend
=============

FoundationDB. The Files API builds on the ``src/rgw/fdb/`` wrapper
from PR ceph/ceph#65535, and contributes back a watch primitive,
a thin tuple/directory abstraction, and versionstamp helpers.
Files API and ``fdbd4n`` share the wrapper and conventions but
run against **separate FDB instances** scoped to different
concerns (see `Cluster scope`_).

Why FoundationDB
----------------

* Strict serializability across the keyspace simplifies
  multi-resource mutations (e.g., create AccessPoint + bump every
  zone's wakeup key in one transaction).
* Watches drive the reconciler loop without polling.
* Global commit-version stamps replace ad-hoc generation counters.

Cluster scope
-------------

A single FDB cluster **per realm**. The realm-level scope gives a
single source of truth for zonegroup-scoped Files API state across
the entire realm — no cross-cluster replication needed for
control-plane writes, and cross-zonegroup federation is just a
prefix in the keyspace.

D4N and indexes use their own **per-Ceph-cluster** FDB instances,
sized for their hot-path latency requirements; the Files API does
not share or piggyback on those instances. The wrapper, tuple
conventions, and schema-versioning agreement are still shared
across consumers.

Eventually realm metadata itself (zonegroup definitions, period
state) is expected to lift from RADOS into FDB. Placing Files API
state at the realm level now aligns with that direction rather
than building something we'd later migrate.

The control-plane latency cost — RGW writes pay round-trip to the
realm-FDB cluster, which may not be local — is acceptable; Files
API CRUD is not hot-path.

FoundationDB layout
===================

Keys use the FDB tuple/directory convention. Top-level subspace
proposed::

   ("rgw", "s3files", "v1", ...)

This sits alongside other RGW FDB users (e.g.
``("rgw", "fdbd4n", ...)``) under a shared ``("rgw", ...)``
prefix. The exact prefix convention is to be agreed with the
``fdbd4n`` author (see `Coordination`_).

Resource keys
-------------

::

   ("rgw","s3files","v1",zg,"fs",fs_id,"spec")
   ("rgw","s3files","v1",zg,"fs",fs_id,"status")
   ("rgw","s3files","v1",zg,"fs",fs_id,"status","zone",zone)
   ("rgw","s3files","v1",zg,"fs",fs_id,"policy")
   ("rgw","s3files","v1",zg,"fs",fs_id,"sync-config")
   ("rgw","s3files","v1",zg,"fs",fs_id,"tags")

   ("rgw","s3files","v1",zg,"ap",ap_id,"spec")
   ("rgw","s3files","v1",zg,"ap",ap_id,"status")
   ("rgw","s3files","v1",zg,"ap",ap_id,"status","zone",zone)
   ("rgw","s3files","v1",zg,"ap",ap_id,"tags")

   ("rgw","s3files","v1",zg,"zone",zone,"mt",mt_id,"spec")
   ("rgw","s3files","v1",zg,"zone",zone,"mt",mt_id,"status")

Per-zone control keys
---------------------

::

   ("rgw","s3files","v1",zg,"zone",zone,"wakeup")
   ("rgw","s3files","v1",zg,"zone",zone,"reconcilers",host)

NFS endpoint information is **not** kept in FDB; it lives in zone
period config under ``files_placement_services`` (see `Files
placements`_), supplied by the operator via ``radosgw-admin``.

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
``("rgw","s3files","v1",zg,"zone",zone,"reconcilers",host)`` every
N seconds (default: 5). Stale rows are treated as offline. RGW
sweeps stale rows lazily on read. This replaces the etcd-style
lease pattern, since FDB has no native leases.

Spec/status writer contract
---------------------------

* RGW writes only ``spec`` keys, ``policy`` / ``sync-config`` /
  ``tags`` keys, ``wakeup`` keys, and creates ``status`` keys
  with default values when a resource is created.
* Reconcilers write only the ``status/zone/<zone>`` sub-key for
  their own zone, the heartbeat row, and (during deletion) remove
  their finalizer entry from the spec.
* Neither writes the other's keys.

Lifecycle and deletion
======================

* Every spec write inside a single transaction also bumps the
  appropriate ``wakeup`` keys (one per zone in the zonegroup for
  FileSystems and AccessPoints; the bound zone's wakeup for
  MountTargets).
* Spec writes record their commit versionstamp; reconciler status
  writes record an ``observed_versionstamp`` so the API layer can
  surface convergence ("Programmed in 2/3 zones").

Finalizers
----------

FileSystem and AccessPoint each carry a finalizer set, with one
entry per zone in the parent zonegroup. Deletion is **two-phase**:

1. ``DeleteFileSystem`` / ``DeleteAccessPoint`` sets
   ``deletion_timestamp`` on the spec and transitions ``status``
   to ``DELETING``. The spec key is **not** removed.
2. Each zone's reconciler observes the deletion, tears down its
   local Ganesha exports for the resource, and removes its own
   finalizer entry.
3. RGW reads the spec, sees an empty finalizer set, and removes
   the spec, status, and ancillary (policy / sync-config / tags)
   keys in one transaction.

This avoids orphan exports surviving after the FDB record is
gone.

MountTargets, being zone-scoped with no per-zone fan-out, use a
simpler one-finalizer pattern: their bound zone's reconciler
clears it after teardown.

REST API surface
================

Enabled via a new ``s3files`` entry in ``rgw_enable_apis`` (default
``off`` until stable). Routed through a new
``RGWRESTMgr_S3Files`` registered in
``rgw::AppMain::cond_init_apis()`` alongside the existing S3,
Swift, IAM, etc. managers.

Wire protocol is REST + JSON (Smithy ``restJson1``), authenticated
with sigv4. Operations to be implemented in v1 (verbatim AWS
shape):

FileSystem:
  ``CreateFileSystem``, ``GetFileSystem``, ``ListFileSystems``,
  ``DeleteFileSystem``, ``GetFileSystemPolicy``,
  ``PutFileSystemPolicy``, ``DeleteFileSystemPolicy``,
  ``GetSynchronizationConfiguration``,
  ``PutSynchronizationConfiguration``.

AccessPoint:
  ``CreateAccessPoint``, ``GetAccessPoint``, ``ListAccessPoints``,
  ``DeleteAccessPoint``.

MountTarget:
  ``CreateMountTarget``, ``GetMountTarget``, ``ListMountTargets``,
  ``UpdateMountTarget``, ``DeleteMountTarget``.

Tagging (any resource):
  ``ListTagsForResource``, ``TagResource``, ``UntagResource``.

Note the absence of ``UpdateFileSystem`` and
``UpdateAccessPoint``: those resources are immutable post-create
in the AWS shape; we follow that contract.

Authorization adds a new IAM action namespace (e.g.
``s3files:CreateFileSystem``, ``s3files:CreateAccessPoint``,
``s3files:CreateMountTarget``, etc.), wired through
``rgw_iam_policy*`` so existing policy machinery enforces it.
FileSystem resource policies are evaluated alongside identity
policies for control-plane authz.

Detailed op-by-op shapes (request/response fields, errors, paths,
divergence notes) live in the companion document
:doc:`s3_files_api_ops`.

Configuration options
---------------------

FDB connection options (cluster file, tuple prefix, request
timeout) are deferred to the FDB-integration PR
(``ceph/ceph#65535``) and will be defined as generic
``rgw_fdb_*`` knobs shared across s3files, the index layer, realm
metadata, and d4n — not as s3files-specific entries. Until that
lands, the s3files API uses an in-memory store; ``s3files`` is
opt-in via ``rgw_enable_apis``.

Per-zone NFS service bindings and export defaults are **not**
config knobs; they live in zone period config under
``files_placement_services`` (see `Files placements`_).

Mount-auth and role assumption
==============================

The control-plane API gates *who can manage Files API resources*.
The actual *data-plane* access decision — "can this NFS client
read or write these objects through this AccessPoint?" — happens
at mount time, evaluated by the Ganesha RGW FSAL. This section
documents the contract between the layers so a future contributor
can implement them without re-deriving the model.

Two policy AND-terms
--------------------

A successful NFS data-plane operation requires two separate
authorization steps to allow:

1. **Mount-auth (NFS-layer).** At the NFS mount handshake, before
   any export becomes usable to a client, the FSAL evaluates the
   mounting principal's permission against the s3files action
   namespace, AND-ing identity-based and resource-based policies:

   * **Principal IAM** — does the calling identity have the
     relevant ``s3files:Client*`` action on the FS / AccessPoint
     ARN?
   * **FileSystem resource policy** — what does the FS owner
     allow via ``PutFileSystemPolicy``?
   * (Bucket policy on the underlying S3 bucket is *not*
     evaluated at this step — it doesn't speak NFS-layer
     concepts. See term 2.)

   If either source denies, the mount fails and no NFS session
   opens.

2. **S3-layer (per-object).** Once mounted, every NFS read /
   write turns into one or more ``s3:GetObject`` / ``PutObject``
   / ``DeleteObject`` / ``ListBucket`` calls against the bucket.
   These go through the standard S3 IAM evaluation (role IAM ∩
   bucket policy). The FSAL doesn't need to know how this works
   — it just performs the S3 call as the assumed role and
   surfaces the S3 result back to NFS.

Mount-auth action namespace
---------------------------

Three actions, mirroring the EFS shape so operators authoring
mount policies don't have to relearn semantics:

* ``s3files:ClientMount`` — required to mount, **and** implies
  read access (no separate ``ClientRead`` action; the NFS read
  path is satisfied by ``ClientMount`` alone).
* ``s3files:ClientWrite`` — additive on ``ClientMount``; permits
  NFS write / modify ops (``write``, ``create``, ``mkdir``,
  ``unlink``, ``rename``, ``setattr``).
* ``s3files:ClientRootAccess`` — additive; bypasses the
  AccessPoint's ``posixUser`` squash (the principal can operate
  as any uid/gid). Does *not* alter the S3-layer permission set
  — it's NFS-layer uid mapping only.

These are registered in ``rgw_iam_policy.h`` /
``rgw_iam_policy.cc`` so policies can be authored against them
today, even though the mount-auth handshake that *invokes* the
evaluation is deferred to the FSAL workstream.

Resource ARN shape::

    arn:aws:s3files:<region>:<account>:file-system/fs-XXX
    arn:aws:s3files:<region>:<account>:file-system/fs-XXX/access-point/fsap-YYY

A tight policy looks like::

    {"Effect": "Allow",
     "Action": ["s3files:ClientMount", "s3files:ClientWrite"],
     "Resource": "arn:aws:s3files::*:file-system/fs-XXX/access-point/fsap-YYY"}

Role assumption: where the s3files-layer meets the S3-layer
------------------------------------------------------------

Every FileSystem carries a customer-supplied ``roleArn`` set at
``CreateFileSystem`` time. This role is the data plane's
maximum capability declaration on the underlying bucket — the
FSAL assumes it (per mount session) and uses the resulting temp
credentials to perform all S3 operations on behalf of NFS
clients of that FS.

The trust principal on the role must be the s3files data plane.
For Ceph this is the synthetic service principal
``s3files.ceph.io``::

    {"Version": "2012-10-17",
     "Statement": [{"Effect": "Allow",
                    "Principal": {"Service": "s3files.ceph.io"},
                    "Action": "sts:AssumeRole"}]}

The role's permission policy is the customer's choice and lives
outside the s3files API surface — the FSAL inherits whatever
the customer configured. AWS's pattern: usually a permissive
``s3:GetObject``/``PutObject``/``DeleteObject``/``ListBucket``
on the bucket+prefix scope.

Per-mount-session narrowing
---------------------------

An auto-role-per-AccessPoint pattern was considered and
rejected: AWS doesn't auto-create per-AP roles, and the FSAL
already has the context (AP scope + mounting principal) to
narrow the per-session capability via a synthesized session
policy passed to ``sts:AssumeRole``. The synthesis goes::

    granted = mount-auth(principal, FS resource policy, principal IAM)
            ⊆ {ClientMount, ClientWrite, ClientRootAccess}

    composed_prefix = fs_spec.prefix + ap_spec.rootDirectory.path
                      (with leading slash on rootDirectory stripped)

    session_policy = {
        "Version": "2012-10-17",
        "Statement": [
            ClientMount in granted ?
              {"Effect": "Allow",
               "Action": ["s3:GetObject"],
               "Resource":
                 "arn:aws:s3:::<bucket>/<composed_prefix>*"} : skip,
            ClientMount in granted ?
              {"Effect": "Allow",
               "Action": "s3:ListBucket",
               "Resource": "arn:aws:s3:::<bucket>",
               "Condition":
                 {"StringLike":
                    {"s3:prefix": ["<composed_prefix>*"]}}} : skip,
            ClientWrite in granted ?
              {"Effect": "Allow",
               "Action": ["s3:PutObject", "s3:DeleteObject"],
               "Resource":
                 "arn:aws:s3:::<bucket>/<composed_prefix>*"} : skip,
        ],
    }

(``ClientRootAccess`` doesn't appear here — it's NFS-layer uid
mapping, not an S3-layer capability.)

The effective S3 capability for the session is the
intersection of (role permission policy ∩ session policy ∩
bucket policy). Different principals mounting the same AP get
different session policies; the role itself is unchanged.

Layered responsibilities
------------------------

Translating the model into where each piece of work lives:

* **Control plane (this document, this codebase).** Persist FS
  state including ``roleArn``. Register
  ``s3files:Client{Mount,Write,RootAccess}`` so policies can
  reference them. Gate the 21 control-plane operations with
  ``s3files:*`` IAM checks (already in place).
* **Reflection layer (FdbStore).** FS / AP / MT spec records
  land in FDB; the FS record carries ``roleArn`` so the
  reconciler can read it.
* **Reconciler service.** Reads (FS, AP, MT) tuples from FDB,
  renders one Ganesha export per tuple. The export config
  carries: bucket, ``composed_prefix``, ``posixUser``,
  ``roleArn``, network endpoint. Signals Ganesha to reload.
* **Data plane (Ganesha + RGW FSAL — separate codebase).** Per
  mount session: authenticate principal, evaluate the two AND
  terms above, synthesize the session policy, call
  ``sts:AssumeRole`` on the export's ``roleArn``, use the temp
  creds for all S3 ops in the session.

Nothing in the s3files control plane synthesizes session
policies — that's the FSAL's job at mount handshake time.

Why no per-AP role
------------------

An earlier draft of this design proposed auto-creating an
immutable, service-linked IAM role per AccessPoint to declare
the AP's data-plane capability. After thinking through the
session-policy mechanism above, that pattern is unnecessary:
AWS's S3 Files doesn't do it, the per-session narrowing already
gives precisely the capability scoping the AP ought to have,
and a per-AP role would just be a stable structure shadowed by
the actual narrowing in session policy. Recording this here so
a future contributor doesn't repeat the exploration.

Error codes
===========

Smithy declares the six exception types and their HTTP status
codes (see `REST API surface`_), but the inner ``errorCode``
member of each exception is typed only as a non-empty string —
AWS does not publish canonical values. The single source of
truth for the values RGW returns is the C++ header
``src/rgw/rgw_s3files_errors.h``, with a Python mirror at
``src/test/rgw/s3files/errors.py`` for the conformance test
suite.

The catalog covers validation failures (invalid ARNs, bad
formats, malformed policy documents), not-found cases, conflicts
(duplicate FS for a bucket, MT-already-in-zone,
FS-has-children, placement-not-bound-in-zone,
unresolved-nfs_service), quota breaches, throttling, and
internal errors. Names are ``UPPER_SNAKE_CASE`` to keep them
visually distinct from the PascalCase exception types.

This is a Ceph-defined extension point. Clients should treat the
exception type as the primary classification and ``errorCode``
as an operator-debug identifier.

Reconciler service
==================

Out-of-tree-for-now microservice (proposed location:
``src/rgw/file_reconciler/`` or its own repo). Run as a
cephadm-managed service alongside the per-zone NFS cluster.

Responsibilities (zone Z):

* Watch
  ``("rgw","s3files","v1",zg,"zone",Z,"wakeup")``.
* On fire, reconcile:

  - For each AccessPoint in the zonegroup, render the local
    Ganesha export configuration (placement export defaults +
    AP's posixUser + AP's rootDirectory + parent FileSystem's
    prefix); reload Ganesha if changed.
  - For each MountTarget bound to Z, ensure the Ganesha exports
    are configured and write the resolved endpoint into status.
  - Honor the finalizer protocol on deletion.

* Write the heartbeat row every N seconds.
* Never write to ``spec``, ``policy``, ``sync-config``, or
  ``tags`` keys.

The reconciler is NFS-only in v1. SMB is a follow-on. The
reconciler does **not** implement synchronization configuration
(``sync-config`` is observed for visibility but no actions are
taken — we serve the bucket directly).

Day-1 implementation note
-------------------------

Day 1 of the RGW-side work uses an **in-memory** ``Store``
implementation; the FDB-backed implementation is gated on PR
#65535 landing (or branch-and-co-develop on top of it). This
unblocks REST-handler and reconciler-contract development before
the FDB wrapper stabilizes.

Failure modes
=============

* **Realm FDB unreachable from RGW.** Control-plane writes return
  503 with ``Retry-After``. Reads also 503; do not serve stale
  caches. Existing Ganesha exports keep serving — only the
  control plane is affected, and only realm-wide.
* **Reconciler offline in zone Z.** New work in Z stays
  ``CREATING``; existing exports keep serving (Ganesha config is
  not removed). API surfaces "no live reconciler in zone Z".
* **Placement not bound in zone Z.** MountTarget stays
  ``CREATING`` with "placement '<name>' not bound in zone <Z>".
  Operator-fixable via ``radosgw-admin zone files-placement add``.
* **nfs_service name doesn't resolve via local orch.**
  MountTarget transitions to ``ERROR`` with
  "unresolved nfs_service '<name>'".
* **roleArn names a non-existent role.** ``CreateFileSystem``
  returns 400 ``ValidationException`` synchronously.
* **Bucket moves zonegroups.** FileSystem becomes orphaned and
  surfaces an ``ERROR`` condition. v1 does not auto-migrate.

Coordination
============

* Builds on PR ceph/ceph#65535 (chardan, draft) which adds
  ``src/rgw/fdb/`` and ``src/rgw/driver/fdbd4n/``.
* Files API contributes back: a watch primitive, a thin
  tuple/directory layer, and versionstamp helpers — useful to
  ``fdbd4n`` even though the two run against different FDB
  instances.
* Tuple-prefix convention to be agreed across FDB-in-RGW
  consumers; the ``("rgw","s3files","v1",...)`` shape proposed here
  assumes a peer prefix ``("rgw","fdbd4n",...)`` in the per-cluster
  FDB.
* Schema-version policy (single global vs per-subsystem) to be
  agreed across consumers.
* Deployment of the realm FDB cluster itself is out of scope of
  this document and shared with the broader FDB-in-RGW workstream.
* Build flag is shared: ``-DWITH_RADOSGW_FDB=ON``. C++23 / GCC 14+
  baseline matches PR #65535.
* AWS Smithy model service version ``2025-05-05`` is vendored at
  ``src/rgw/spec/aws/s3files/2025-05-05/`` as the design target;
  updates pulled from ``aws/api-models-aws`` per the workflow in
  ``src/rgw/spec/aws/README.md``.

Conformance test strategy
=========================

The boto3 + pytest suite under ``src/test/rgw/s3files/`` exists
to keep the implementation honest against the vendored Smithy
model. It runs in two modes against the same code, distinguished
by an environment variable, and three orthogonal pytest markers
that describe what each test exercises rather than what target
it expects.

Run modes
---------

* ``S3FILES_TESTS_MODE=rgw`` (default): tests assert both the
  Smithy-modeled exception class and the RGW-defined
  ``errorCode`` string from ``src/test/rgw/s3files/errors.py``.
* ``S3FILES_TESTS_MODE=aws``: tests assert only the typed
  exception. The Smithy spec types ``errorCode`` as a non-empty
  string but doesn't fix the value — AWS does not publish
  theirs, so the value-level asserts cannot be portable.

The mode toggle lives in ``assert_errorcode()`` in
``src/test/rgw/s3files/__init__.py``; tests pass an expected
errorCode (or set of acceptable errorCodes) and the helper
no-ops the value check in AWS mode.

Pytest markers
--------------

* ``conformance`` — per-op AWS-shape tests. The Smithy model is
  the contract; assertions check shape, errors, and
  spec-defined behavior.
* ``divergence`` — exercises an intentional Ceph deviation. Tests
  in ``test_ceph_divergences.py`` carry only this mark; tests
  that *use* a divergence-dependent fixture
  (e.g. ``test_subnet_id`` carrying a Ceph zone-id rather than an
  EC2 subnet-id) carry both ``conformance`` and ``divergence``.
* ``read_after_write`` — multi-op state-transition tests across
  a sequence of mutations and reads, catching persistence
  failures that single-op shape conformance alone would miss.
* ``smoke`` — endpoint reachability / boto3 version sanity.

The matrix that matters in practice:

* Full local suite: ``pytest`` (no mark filter) — every test runs.
* AWS-portable subset: ``S3FILES_TESTS_MODE=aws pytest -m
  'conformance and not divergence'`` — drops Ceph-specific
  tests AND skips RGW-defined errorCode value checks.

Per-op file conventions
-----------------------

Each operation has a single ``test_<op_name>.py`` file. Inside,
tests are grouped under five comment-banner sections in this
order:

1. *positive* — minimum valid request, optional fields,
   idempotency
2. *validation* — Smithy ``@required`` / ``@length`` /
   ``@pattern`` / ``@range`` violations
3. *not-found* — typed ``ResourceNotFoundException`` per
   missing resource
4. *conflict* — typed ``ConflictException`` per Smithy-declared
   conflict
5. *(optional)* per-op extras (e.g., pagination on list ops,
   force-deletion on delete ops)

Positive tests round-trip every Smithy-modeled output member.
A test that creates a resource re-Gets it and value-compares
the create response against the Get response field-for-field;
list ops compare each entry against the per-id Get response.
ARNs are matched against their Smithy ``@pattern`` regex.

Tests that exercise a Smithy trait that boto3's client
validator already enforces (``@required`` on a missing field,
``@length`` on an empty list, ``@pattern`` on a malformed
string, ``@range`` on an out-of-bounds integer) accept either
``ValidationException`` (server-side) or ``ParamValidationError``
(client-side) via the ``validation_excs()`` helper. The
contract is enforced regardless of which side enforces it; a
future raw-HTTP test layer will exercise the server-side path
independently of boto3.

Audit log
---------

This section captures past audit waves so a future audit can
diff against the current state and pick up where the previous
one stopped.

**2026-05 audit and remediation.** Independent review of the
suite asked two questions: *did we weaken any test to make RGW
pass*, and *would the same suite pass against real AWS S3 Files*.
Findings:

* The ``errorCode`` value asserts were RGW-specific and would
  fail against AWS for ~40 tests.
* ~26 mount-target tests depended on the Ceph
  ``subnet-{zone_hex}`` reinterpretation of the ``subnetId``
  field but were tagged only ``conformance``.
* Many positive-path tests checked only field presence
  (``'field' in resp``) rather than value-comparing against the
  request input or against a follow-up Get.
* Several Smithy-modeled errorCodes
  (``INVALID_PREFIX``, ``INVALID_SYNC_RULES``,
  ``INVALID_MAX_RESULTS``) had constants in ``errors.py`` but no
  test asserted them.
* No List op exercised pagination via ``nextToken``.
* ``ListFileSystems(bucket=...)`` filter had no test.

Remediation landed in five focused commits on
``mmgaggle/wip-rgw-s3-files-api-design``:

* ``2315331d80a`` — ``assert_errorcode()`` helper + AWS run mode;
  ``missing_required_exc`` renamed to ``validation_excs`` with a
  scope-accurate docstring.
* ``608a86154c2`` — dual-mark Ceph subnet-divergence tests so the
  AWS-portable subset cleanly excludes them.
* ``674d2f28f79`` — positive-path round-trip tightening: ARN
  format checks, full posixUser / rootDirectory / 3-tag
  round-trip via Get, list-entry-vs-Get value compare.
* ``627683e9cc6`` — pagination + bucket-filter coverage; fixed
  two real bugs surfaced by the new tests:

  * ``MemoryStore::list_*`` ignored ``ListOptions`` entirely
    (every call returned every entry). Fixed via a generic
    ``apply_pagination()`` helper.
  * RGW capped ``maxResults`` at 1000 for ``ListFileSystems``
    and ``ListMountTargets``; the Smithy ``@range`` is 1..100
    for both. Fixed.

Passing counts after the wave: 123/123 conformance +
read_after_write + divergence; 27/27 MemoryStore gtests;
94/123 in AWS-portable mode (29 deselected by
``not divergence``).

Known gaps (deferred, not blocking the wave):

* ``ServiceQuotaExceededException`` paths
  (``TOO_MANY_FILE_SYSTEMS`` etc.) are untested; would require
  setting a quota knob first.
* ``ThrottlingException`` is untestable without rate-limit
  injection.
* ``DeleteFileSystem(forceDeletion=true)`` is in the Smithy
  input shape but the handler doesn't honor it yet — feature
  gap, not a test gap.
* Server-side validation of Smithy traits that boto3 also
  validates (``@required``, ``@length``, ``@pattern``,
  ``@range``) is exercised only through boto3 today; a
  raw-HTTP layer would let us assert the server-side path
  independently. Same for the documentation-only
  "tag key can't start with ``aws:``" rule, which Smithy
  cannot express.

Future audit waves should append a similar entry rather than
edit prior ones — the goal is a forward-only diff so a reader
can see what changed and when.

How to add a conformance test
-----------------------------

For an existing operation:

1. Open ``test_<op_name>.py``. Add the test under the matching
   comment-banner section.
2. For positive tests, value-compare the create/update response
   against a follow-up Get (or, on List ops, against the per-id
   Get) for every Smithy-modeled output member. ``'field' in
   resp`` checks are not enough.
3. For error tests, catch the typed exception declared on the
   Smithy operation (``ValidationException``,
   ``ResourceNotFoundException``, etc.). Use
   ``assert_errorcode(exc.value, errors.X)`` rather than reading
   ``errorCode`` directly so the test stays AWS-portable.
4. For traits boto3 enforces client-side (``@required``,
   ``@length``, ``@pattern``, ``@range``), wrap the assertion in
   ``pytest.raises(validation_excs(s3files_client))`` so the
   test passes whether the violation is caught client- or
   server-side.
5. If the test depends on a Ceph-specific fixture
   (``test_subnet_id``, ``test_mount_target``), add
   ``@pytest.mark.divergence`` next to ``@pytest.mark.conformance``.
6. Add or extend a ``read_after_write`` test if the new
   operation touches state observable from a different op.

For a new errorCode value:

1. Add the constant to ``src/rgw/rgw_s3files_errors.h`` (the
   C++ source of truth).
2. Mirror it in ``src/test/rgw/s3files/errors.py``.
3. Reference it in at least one test via
   ``assert_errorcode(exc.value, errors.NEW_CODE)``.

Open questions
==============

* Tuple-prefix convention agreed with ``fdbd4n``?
* Schema-version policy: global, or per-subsystem?
* Value encoding: zpp_bits (matches fdbd4n) or something else?
* Realm FDB deployment shape — operator-deployed standalone, or
  hosted on one Ceph cluster's mgr?
* ``DeleteFileSystem`` cascade semantics: refuse if children
  exist (require operator to delete APs/MTs first), or cascade
  with finalizer fan-out across all children?
* Does ``GetFileSystem`` synthesize aggregate availability from
  per-zone status, or surface raw per-zone breakdown?
* Multiple NFS services per zone — useful enough to support in v1?
* IAM action namespace: ``s3files:*`` (matches AWS service
  namespace) or fold under existing ``s3:*``?
