=====================
RBD Orbital Snapshots
=====================

Orbital snapshots archive RBD snapshots to S3-compatible object storage
in a randomly-accessible, layered format. The archive supports
incremental push without retaining local snapshots, instant-live
restore through image live migration, and continuation of an image's
snapshot lineage from any cluster — none of the clusters that push to
or pull from the archive need to know about each other.

Motivation
==========

The target deployment is N per-AZ Ceph clusters sharing a single
regional object store, which is both more durable and cheaper per
gigabyte than replicated in-cluster capacity. The model is analogous to
AWS EBS snapshots and Google Persistent Disk snapshots: snapshots leave
the cluster and live in a regional tier, and a new image can be created
from any archived snapshot in any AZ, usable immediately while it
hydrates in the background.

This is not a low-RPO disaster recovery mechanism and does not compete
with ``rbd-mirror``; recovery points are snapshot-granularity. It
replaces deployments that run mirroring only because no shared snapshot
tier existed, and it replaces ``export`` / ``export-diff`` pipelines,
which produce sequential streams that cannot serve random-access reads
and therefore cannot back a live restore.

Concepts
========

lineage ("planet")
  A logical image, identified independently of any cluster. Cluster
  image ids do not survive cross-cluster restore, so lineages carry
  their own identity — the same reason ``rbd-mirror`` has a global
  image id. A lineage is a chain (or, after forks, a tree) of layers.

layer ("satellite")
  One archived snapshot: an immutable set of data objects plus a small
  metadata pair (header and bitmap). A layer references its parent
  layer; reads fall through the chain the way clone reads fall through
  to a parent image.

HEAD
  A per-lineage pointer object naming the current tip layer, updated
  with a compare-and-swap conditional PUT. This fences concurrent
  writers.

Object layout
=============

One archive object per RADOS data object, under deterministic keys::

  <prefix>/<planet>/HEAD
  <prefix>/<planet>/<satellite>/layer.json
  <prefix>/<planet>/<satellite>/bitmap
  <prefix>/<planet>/<satellite>/checksums.<shard>
  <prefix>/<planet>/<satellite>/<objectno as %016x>

Example::

  s3://bucket/rbd/ganymede-4f2a/HEAD
  s3://bucket/rbd/ganymede-4f2a/0000-vostok/layer.json
  s3://bucket/rbd/ganymede-4f2a/0000-vostok/bitmap
  s3://bucket/rbd/ganymede-4f2a/0000-vostok/0000000000000000
  s3://bucket/rbd/ganymede-4f2a/0000-vostok/0000000000000001
  s3://bucket/rbd/ganymede-4f2a/0001-saljut/layer.json
  s3://bucket/rbd/ganymede-4f2a/0001-saljut/bitmap
  s3://bucket/rbd/ganymede-4f2a/0001-saljut/0000000000000001

Metadata object names cannot collide with data objects, whose names are
always sixteen hex digits.

Naming
------

Planet and satellite names are generated at creation time and are
immutable. A planet name is drawn from a curated word list and suffixed
with short random hex to guarantee uniqueness (``ganymede-4f2a``). A
satellite name is prefixed with a monotonic sequence number
(``0004-vostok``) so that a plain bucket listing shows the chain in
push order. User-facing snapshot names are mutable in RBD
(``rbd snap rename``) and therefore appear only inside layer headers,
never in the keyspace. Headers additionally carry UUIDs as the true
identities of both lineage and layer.

Layer header
------------

``layer.json`` is a JSON document (human-inspectable with any HTTP
client) containing:

* format version
* lineage UUID and planet name
* layer UUID, satellite name, and sequence number
* user snapshot name
* parent satellite name, or null for a chain root
* image size in bytes at this snapshot
* object size and striping parameters (``stripe_unit`` /
  ``stripe_count``; v1 restricts to default striping)
* checksum sidecar parameters (algorithm, objects per shard), if
  present
* provenance: fsid, pool, image id, and image name of the pushing
  cluster (informational only — never used for addressing)
* an ``encrypted`` flag when the image data is a ciphertext passthrough

The header and bitmap are written last, after all data objects and
checksum shards; their presence is the commit point for the layer.

Bitmap
------

``bitmap`` is a ``ceph::BitVector<2>`` (the on-disk object-map
encoding), one entry per data object, indexed by object number:

========== ===== ======================================================
state      value meaning
========== ===== ======================================================
INHERITED  0     not present in this layer; fall through to the parent.
                 In a chain root, equivalent to ZEROED.
EXISTS     1     this layer contains the object at
                 ``<satellite>/<objectno>``
ZEROED     2     the object was discarded or zeroed at this snapshot;
                 the fall-through stops and reads return zeros
(reserved) 3
========== ===== ======================================================

ZEROED is what makes the layout sound: absence of a key alone cannot
distinguish "unchanged since the parent" from "discarded". The bitmap
is sized to the image size at this snapshot. At the default 4 MiB
object size, the bitmap for a 10 TiB image is roughly 650 KiB.

Checksum sidecar
----------------

``checksums.<shard>`` objects hold one fixed-width CRC32C per object
number, in shards of a fixed object-count range. Checksums are computed
server-side by the OSD CHECKSUM operation during push — object data
never round-trips to the client for checksumming. The sidecar is read
only by incremental push and by scrub; the restore path never touches
it.

Data-object uploads additionally declare the OSD-computed value as an
S3 additional checksum (``x-amz-checksum-crc32c``) where the endpoint
supports it. The S3 endpoint then independently verifies the uploaded
body on ingest — end-to-end integrity from OSD to bucket — and stores
the checksum, retrievable later via ``GetObjectAttributes`` without
reading data. Data objects are single-part uploads at RADOS object
sizes, so these are whole-object checksums, not multipart composites.
The sidecar remains authoritative and is required either way: bulk
comparison during incremental push needs all checksums in a few GETs,
which per-object metadata reads cannot provide at scale, and not every
S3 implementation supports additional checksums.

Semantics
=========

Read
----

To read object N at snapshot S: walk the chain starting at S's layer.
EXISTS → ranged GET against that layer's data object; ZEROED → return
zeros; INHERITED → continue to the parent. A GET beyond a short
object's length zero-fills to the object boundary, matching short RADOS
object semantics. The per-object diff between any two layers is a pure
bitmap comparison; no data reads are required.

Resize
------

Each header records the image size at its snapshot. When an image
shrinks and regrows between snapshots, the regrown range MUST be marked
ZEROED, not INHERITED — the data was logically discarded at shrink.
This matches ``export-diff`` semantics.

Push
----

Every archival operation reads from a snapshot that exists for at
least the duration of the operation. Whether that snapshot is a
transient one owned by the tool (``rbd orbital create``) or a
pre-existing one owned by the operator (``rbd orbital push``) is a
CLI-level concern (see CLI_); the layer produced is identical.

A full push (new lineage): create the planet, mark every existing
object EXISTS (seeded from the object map / ``diff_iterate`` with
``whole_object=true``), upload data objects and checksum shards, write
bitmap and header, then create HEAD.

An incremental push with a local parent snapshot: compute changed
objects with fast-diff against the parent snap, upload only those,
mark unchanged objects INHERITED and discarded ranges ZEROED.

An incremental push *without* a common snapshot — the normal case after
a cross-cluster restore, and the case ``export-diff`` cannot serve:
compare each local object's server-side CRC32C at the archived
snapshot against the tip layer's checksum sidecar, upload mismatches,
write the new layer. With a tool-owned transient snapshot, snapshot
lifetime is bounded by the duration of the push, so no snapshot space
overhead is carried between archive points.

Every push commits in the same order: data objects, checksum shards,
bitmap, header, then a conditional PUT of HEAD (If-Match on the
previously read ETag). A failed CAS means another writer advanced the
lineage; the layer is left un-referenced for the operator (or a retry)
to resolve. Forks — two layers naming the same parent — are
representable and detectable, like a clone tree; fencing exists to
prevent *silent* forks, not to forbid forks.

Restore
-------

Restore is an ``orbital`` format in ``librbd/migration``, beside the
``raw`` and ``qcow`` formats::

  rbd migration prepare --import-only \
    --source-spec '{
        "type": "orbital",
        "stream": {"type": "s3", "url": "...", ...},
        "planet": "ganymede-4f2a",
        "satellite": "0004-vostok"
      }' \
    <pool>/<image>

The image is usable immediately: the migration source is attached as
the destination's parent, so reads of unhydrated extents fall through
to S3 via the standard parent-read path and writes trigger normal
copyup. ``rbd migration execute`` hydrates in the background;
because ``deep_copy::ObjectCopyRequest`` begins each object with
``list_snaps``, and the orbital format answers ``list_snaps`` from
bitmap diffs alone, hydration fetches exactly the objects each
snapshot references. Archived snapshots up to the requested satellite
are recreated as real snapshots on the destination image.

Layer deletion and compaction
-----------------------------

Deleting an intermediate layer L is librbd flatten transplanted to S3:
for every object where L is EXISTS and a descendant INHERITs at that
position, server-side ``CopyObject`` into the child's prefix (copyup —
no data transits the client), flip the child's bitmap entry to EXISTS,
rewrite the child's bitmap and header as the commit point, then delete
L's prefix. Liveness is computed with bitwise operations over
descendant bitmaps and is evaluated per branch when the lineage has
forked. Storage is transiently duplicated for the copied objects until
the old prefix is deleted.

Scrub
-----

Scrub validates chain integrity (every parent reference resolves, every
EXISTS entry has a data object, bitmap sizes match recorded image
sizes) and verifies data object checksums against the sidecar. Against
endpoints that store S3 additional checksums, verification is
metadata-only — the sidecar is compared with S3-stored CRC32C values
via ``GetObjectAttributes``, with no data egress; a deep mode re-reads
object data and recomputes.

Encryption
----------

Encrypted images are archived as ciphertext passthrough: data objects
are uploaded as stored, without loading the encryption format. Restore
requires the same LUKS keys, and checksums remain stable across
push/restore cycles.

.. _CLI:

CLI
===

::

  rbd orbital create <pool>/<image> s3://bucket/prefix
  rbd orbital push <pool>/<image>@<snap> s3://bucket/prefix
  rbd orbital pull s3://bucket/prefix/<planet>/<satellite> <pool>/<image>
  rbd orbital ls s3://bucket/prefix[/<planet>]
  rbd orbital rm s3://bucket/prefix/<planet>/<satellite>
  rbd orbital scrub s3://bucket/prefix/<planet>

``create`` is the primary archival verb, following ``rbd snap create``
and the EBS / Persistent Disk model: it creates a tool-owned transient
snapshot, pushes it, and deletes it. Transient snapshots use a
recognizable name (``.orbital-<satellite>``) so that orphans from
interrupted runs are identifiable and reaped by a subsequent
invocation. A transient snapshot is crash-consistent only.

``push`` archives an existing, operator-owned snapshot — sugar that is
simpler than an ``rbd export | s3 cp`` pipeline, for deployments where
an in-cluster snapshot is still wanted (for example,
application-consistent snapshots orchestrated with quiesce hooks).
The tool never deletes a snapshot it did not create. Layers produced
by ``push`` record the user snapshot name in the header; layers
produced by ``create`` leave it empty, and the satellite codename is
the identity.

Both verbs produce identical layers and select full vs incremental
automatically from lineage state (the lineage id is stamped in image
metadata at first archive). ``pull`` is sugar for ``migration
prepare`` with an orbital source-spec. Configuration options live
under the ``rbd_orbital_*`` namespace.

Implementation plan
===================

1. Push to a local staging directory (object-per-file plus header and
   bitmap, fast-diff driven), with ``aws s3 sync`` or similar as the
   transport. No new dependencies.
2. The orbital migration format in librbd (read path), with a QA
   round-trip against RGW: push from cluster A, instantly-live image
   on cluster B.
3. Native S3 transport: aws-sdk-cpp for the write side, SigV4 signing
   in ``S3Stream``, HEAD fencing, checksum-based incrementals.
4. Lifecycle: layer deletion/compaction, ``ls``/inspect, scrub, fork
   tooling.

Dependency split: the ``rbd`` CLI links aws-sdk-cpp for the write side
(push, ``CopyObject`` compaction, scrub). The librbd read path keeps
the existing lightweight ``HttpClient`` / ``S3Stream``, upgraded from
Signature v2 to a small self-contained SigV4 signer, so the SDK never
enters librbd's link graph and QEMU is unaffected.

Prior art
=========

`Benji <https://github.com/elemental-lf/benji>`_ and
`backy2 <https://github.com/wamdam/backy2>`_ implement
chunk-deduplicated RBD backup to object storage with block-to-chunk
manifests, validating the layout; neither
offers restore without full rehydration. The instant-live restore via
``migration prepare`` is the novel piece. AWS EBS snapshots and Google
Persistent Disk snapshots are the closest analogies for the overall
shape: both store incremental block snapshots in a regional tier and
lazily hydrate new volumes in any zone.

Open questions
==============

* Should ``pull`` hydrate the full chain by default, or only the
  requested satellite's view with earlier snapshots elided?
* Sharding threshold for very large bitmaps (a 1 PiB image is a ~67 MiB
  bitmap — likely fine unsharded, but the format version should leave
  room).
* Whether HEAD should carry a small amount of lineage-wide metadata
  (e.g. fork records) or remain a bare pointer.
