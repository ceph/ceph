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
   ("rgw","files","v1",zg,"fs",fs_id,"status","zone",zone)
   ("rgw","files","v1",zg,"fs",fs_id,"policy")
   ("rgw","files","v1",zg,"fs",fs_id,"sync-config")
   ("rgw","files","v1",zg,"fs",fs_id,"tags")

   ("rgw","files","v1",zg,"ap",ap_id,"spec")
   ("rgw","files","v1",zg,"ap",ap_id,"status")
   ("rgw","files","v1",zg,"ap",ap_id,"status","zone",zone)
   ("rgw","files","v1",zg,"ap",ap_id,"tags")

   ("rgw","files","v1",zg,"zone",zone,"mt",mt_id,"spec")
   ("rgw","files","v1",zg,"zone",zone,"mt",mt_id,"status")

Per-zone control keys
---------------------

::

   ("rgw","files","v1",zg,"zone",zone,"wakeup")
   ("rgw","files","v1",zg,"zone",zone,"reconcilers",host)

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
``("rgw","files","v1",zg,"zone",zone,"reconcilers",host)`` every
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

Enabled via a new ``files`` entry in ``rgw_enable_apis`` (default
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

The ``rgw.yaml.in`` entries:

* ``rgw_files_fdb_cluster_file`` — path to the FDB cluster file.
  Empty disables the FDB backend (in-memory store fallback used
  for development and testing).
* ``rgw_files_fdb_prefix`` — comma-separated tuple prefix.
  Default ``rgw,files,v1`` yields ``("rgw","files","v1")``.
* ``rgw_files_fdb_request_timeout_ms`` — default 5000.

Per-zone NFS service bindings and export defaults are **not**
config knobs; they live in zone period config under
``files_placement_services`` (see `Files placements`_).

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
  ``("rgw","files","v1",zg,"zone",Z,"wakeup")``.
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
  consumers; the ``("rgw","files","v1",...)`` shape proposed here
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

Open questions
==============

* Tuple-prefix convention agreed with ``fdbd4n``?
* Schema-version policy: global, or per-subsystem?
* Value encoding: zpp_bits (matches fdbd4n) or JSON
  (operator-readable)?
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
