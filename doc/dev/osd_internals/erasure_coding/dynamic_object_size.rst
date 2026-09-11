=====================================
EC Dynamic Per-Object Chunk Size
=====================================

Overview
========

Erasure-coded pools traditionally use a single, pool-wide chunk size for every
object.  The *dynamic object size* feature allows the OSD to choose a larger
chunk size for individual objects, based on a size hint supplied by the writer.
The chosen chunk size is stashed permanently in the object's ``object_info``
and honoured for all subsequent I/O to that object.

The primary motivation is to reduce I/O amplification for large objects in an
optimized EC pool.  When the pool-default chunk size is small (e.g. 64 KiB,
appropriate for small random I/O), a large object (say, a 4 MiB RBD image
segment) spans a large number of stripes and incurs proportionally more partial
reads and read-modify-write cycles on every overwrite.  Choosing a larger chunk
size for large objects – up to a configurable maximum (default 1 MiB) – allows
them to fit in a single stripe per shard and eliminates that amplification
entirely, while small objects continue to use the pool default and are
unaffected.

The feature is restricted to pools that have both ``allow_ec_overwrites`` and
``allow_ec_optimizations`` set; it has no effect on the legacy EC path.

Design Goals
============

#. **Zero impact on existing objects and non-dynamic pools.**  Any call site
   that does not supply a per-object chunk size behaves exactly as before.
   Encoding a zero chunk size in ``object_info`` means "use the pool default".

#. **Immutability.**  Once a chunk size is chosen and written to an object's
   ``object_info``, it cannot be changed.  Every subsequent write, read,
   recovery, and scrub operation is required to use the same value.

#. **Set-once pool flag.**  The ``dynamic_object_size`` pool flag can be
   enabled but never disabled.  This prevents the possibility of objects in
   the same pool having different interpretations of an absent ``ec_chunk_size``
   field.

#. **Backward compatibility.**  The ``object_info`` encoding is bumped from
   version 18 to 19, with a compat level that allows older OSDs to read objects
   that do not carry the new field (``ec_chunk_size == 0``).

Key Design Decisions
====================

1. Separating Pool-Invariant Geometry from Object-Scoped Geometry
-----------------------------------------------------------------

The pre-existing ``ECUtil::stripe_info_t`` class held both the pool-level
constants (k, m, plugin flags, chunk mapping) *and* the size-dependent
geometry (chunk size, stripe width, offset–shard translation).  To support
per-object chunk sizes cleanly, these two concerns are separated:

* ``stripe_info_base_t`` (new) holds only the pool-invariant state.  It
  deliberately exposes *no* offset-to-shard helpers; callers that need
  geometry must ask for an object-scoped view.

* ``stripe_info_t`` (renamed from the single class) is now the
  *object-scoped view*.  It is obtained cheaply via
  ``base.for_chunk_size(cs)`` or ``base.for_default()``, holds a reference
  to the base, and owns all the geometry helpers.

The long-lived ``sinfo`` members throughout the optimized EC stack
(``ECBackend``, ``ReadPipeline``, ``RMWPipeline``, ``RecoveryBackend``,
``ECExtentCache``) are changed to ``stripe_info_base_t``.  Every geometry
call site must now explicitly build a view, which surfaces all locations
that assumed a single pool-wide chunk size.  The initial conversion makes
all sites call ``for_default()``, preserving existing behaviour; the
``TODO(dynamic-object-size)`` marker identifies every site that is updated
in subsequent phases.

This design makes it a compile error to use pool-level geometry where
object-level geometry is needed, forcing correctness by construction.

2. The ``ec_chunk_size == 0`` Sentinel Convention
-------------------------------------------------

Rather than wrapping every call site in an ``if (dynamic)`` guard,
``stripe_info_base_t::for_object_chunk_size(cs)`` treats ``cs == 0`` as
"use the pool default".  This means:

* ``read_request_t``, ``WritePlanObj``, and the recovery path all carry a
  ``uint64_t chunk_size`` field initialised to zero.
* Call sites that have not yet been taught to supply a per-object chunk
  size automatically fall back to the pool default, so they produce
  bit-for-bit identical results for non-dynamic pools.
* The scrub path sources the chunk size from the object's OI
  (``auth_oi.ec_chunk_size``), falling back to
  ``get_default_chunk_size()`` when the field is zero.

3. Chunk Size Selection at Write Time
-------------------------------------

When the first write to an object arrives and no chunk size has been
stashed yet (``oi.ec_chunk_size == 0``), ``get_write_plan()`` selects one
via ``stripe_info_base_t::chunk_size_for_hint()``.  The hint is resolved
from three sources in priority order:

#. The ``expected_object_size`` carried in the current write's
   ``CEPH_OSD_OP_SETALLOCHINT`` op (if present).
#. The ``expected_object_size`` previously stored in the OI.
#. The furthest byte offset touched by the current write operation.

The selection algorithm keeps the object in a single stripe per shard for
as long as possible: the chosen chunk size is the smallest power-of-two
that is at least ``ceil(hint / k)`` bytes, clamped to the range
``[default_chunk_size, max_dynamic_chunk_size]``.  A zero hint yields the
pool default unchanged.

The selected chunk size is stored in ``WritePlanObj::chunk_size`` and
propagated to ``ECTransaction::Generate``, which uses it to build the
transaction geometry and, for dynamic pools, writes it back to both the
OBC-cached and transaction-encoded ``object_info``.  This stash happens in
``ECTransaction::Generate`` rather than in ``get_write_plan()`` because
that is the point at which the OI attribute is serialised into the
transaction.

.. _chunk-size-alignment:

Chunk Size Alignment: Design Alternatives
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The choice of which values are legal for a dynamically selected chunk
size is a significant design decision.  Three options were considered.

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Scheme
     - Pros
     - Cons
   * - **A. Power-of-two** *(implemented)*
     - Simple to reason about; pool operators can predict exactly which sizes
       will be chosen (4k → 8k → 16k → … → max).  Fewer distinct chunk
       sizes in a pool, so the scrub/recovery code encounters fewer
       combinations.  Efficient to compute: ``next_pow2(ceil(hint/k))``.
       Aligns naturally with typical block-device and page-cache granules.
     - Can over-allocate shard space significantly.  An object whose
       natural per-shard size is 65 KiB will be assigned a 128 KiB chunk
       (2× waste per shard in the worst case).  The discrete jumps mean a
       single extra byte of hint triggers a doubling of the chosen size.
   * - **B. Any multiple of 4 KiB** *(original brief)*
     - Minimal over-allocation: the chunk is the smallest 4k-multiple ≥
       ``ceil(hint/k)``, so waste is bounded by 4k per shard.  Matches the
       ``EC_ALIGN_SIZE`` invariant that the rest of the EC stack already
       uses for I/O alignment; no new alignment concept is introduced.
       The geometry arithmetic (``/``, ``%``) does not require power-of-two
       values and works correctly for any multiple of 4k.
     - Many more distinct chunk sizes are possible, complicating pool
       management (operators cannot easily enumerate the sizes in use).
       The ``chunk_size_for_hint()`` loop becomes a ceiling-to-4k-multiple
       instead of a power-of-two round-up, which is slightly more code.
       Does not improve the single-stripe goal as cleanly: a 65 KiB
       per-shard size still requires multiple stripes at the pool default.
   * - **C. Any multiple of default_chunk_size**
     - Guarantees ``stripe_width = k * chunk_size`` stays an exact multiple
       of the original stripe width; existing geometry helpers never see a
       sub-default stripe fragment.  Worst-case waste bounded by
       ``default_chunk_size`` per shard.
     - The default chunk size is not always power-of-two (e.g. ``k=3``,
       ``stripe_width=12k`` → ``default_chunk_size=4k``; but ``k=3``,
       ``stripe_width=9k`` → ``default_chunk_size=3k``).  Does not
       simplify the number of distinct sizes in a pool.  Adds an implicit
       coupling between the pool profile and the set of legal dynamic sizes
       that is hard to document.

**Why power-of-two was chosen (and where this could be revisited)**

Power-of-two was chosen because:

1. The pool default chunk size is itself always a multiple of 4k in
   practice (jerasure and ISA-L both pad to their plugin alignment, which
   is at least 4k), so ``default_chunk_size`` is already power-of-two for
   all common profiles.  Clamping the minimum to ``default_chunk_size``
   therefore guarantees the result is also a multiple of 4k.

2. Having a small, predictable set of bucket sizes (4k, 8k, 16k, …)
   greatly reduces the number of distinct on-disk layouts that can appear
   in a single pool, which simplifies scrub, recovery, and operator
   reasoning.

3. The ``max_dynamic_chunk_size`` pool option is rounded down to the
   largest power-of-two ≤ the configured value before use, so operators
   setting an arbitrary byte limit still get a power-of-two ceiling.

**If the original 4k-multiple brief is preferred** in a future revision,
the change is confined to ``chunk_size_for_hint()``: replace
``cs <<= 1`` with ``cs = p2roundup(per_shard, EC_ALIGN_SIZE)``.
The rest of the stack requires no changes, because the geometry arithmetic
already works for any multiple of 4k and the ``align_next()`` calls
throughout the stack already round shard extents up to ``EC_ALIGN_SIZE``.
The minimum would then be ``max(default_chunk_size,
p2roundup(ceil(hint/k), EC_ALIGN_SIZE))``.

**Minimum chunk size**

The minimum is always ``default_chunk_size`` (i.e. ``stripe_width / k``),
enforced by ``chunk_size_for_hint()``.  This guarantees that choosing a
dynamic chunk size never produces a geometry *smaller* than the pool was
configured for, which would violate assumptions elsewhere in the EC stack.
A zero hint – or an object small enough that ``ceil(hint/k)`` is less than
``default_chunk_size`` – silently falls back to the pool default, so
existing objects and small writes are entirely unaffected.

4. Immutability Enforcement
---------------------------

Once ``oi.ec_chunk_size`` is non-zero, ``get_write_plan()`` always honours
it and never calls ``chunk_size_for_hint()``.  The monitor enforces the
set-once semantics of the pool flag: attempting to set
``dynamic_object_size=false`` is rejected with an error.

The pool flag and the per-object field act in concert:

* Pool flag unset, field zero → normal EC, pool-default chunk size always.
* Pool flag set, field zero → object has not been written yet; the next
  write will choose and stash a chunk size.
* Pool flag set, field non-zero → the stashed value governs all I/O.

5. Propagation Through Read, Recovery, and Scrub
-------------------------------------------------

**Reads (RMW and normal):** ``ReadPipeline`` accepts a per-request chunk
size via ``read_request_t::chunk_size``.  The pipeline builds the view
with ``sinfo.for_object_chunk_size(chunk_size)`` and uses it to compute
shard extents.  For RMW operations the chunk size is sourced from the OI;
for normal client reads it is sourced from ``oi.ec_chunk_size`` at the
``PrimaryLogPG`` call site.

**Recovery:** ``continue_recovery_op()`` and the surrounding read
planning use ``op.recovery_info.oi.ec_chunk_size`` (falling back to
``op.obc->obs.oi.ec_chunk_size`` when an OBC is available).  The
``update_object_size_after_read()`` helper likewise uses the chunk size
from the recovery read request rather than re-deriving it from the pool
default.

**Scrub:** ``ScrubBackend::logical_to_ondisk_size()`` accepts a
``chunk_size`` parameter and builds the view accordingly.  All call sites
pass ``auth_oi.ec_chunk_size`` (or the shard ``oi.ec_chunk_size`` where
the auth OI is not yet resolved).

**Direct/split reads:** ``PGBackend::extent_to_shard_extent()`` and
``objects_readv_sync()`` gain a ``chunk_size`` parameter (default 0 →
pool default).  ``PrimaryLogPG`` passes ``oi.ec_chunk_size`` at these
call sites.  The ``objects_read_local()`` path (used for EC direct reads
on a non-primary shard) has the same gap and is addressed in section 7
below.

6. Known Limitations and Follow-up Work
-----------------------------------------

**Rollback visitors.**  ``PGBackend::RollbackVisitor`` (in
``PGBackend.cc``) is responsible for translating a ``pg_log_entry_t``
rollback description into an ``ObjectStore::Transaction``.  Two of its
visitor overrides compute per-shard sizes that are wrong when the object's
``oi.ec_chunk_size`` differs from the pool default.

* ``append()`` (~line 219) — calls
  ``object_size_to_shard_size(old_size, shard, /*chunk_size=*/0)``, where
  ``chunk_size=0`` resolves to the pool default via
  ``stripe_info_base_t::for_object_chunk_size(0)``.  The truncate
  issued by ``rollback_append()`` therefore truncates to the wrong shard
  offset for objects with a non-default chunk size.

* ``rollback_extents()`` (~line 304) — same call, same bug: shard
  boundaries and extent lengths are computed with pool-default geometry,
  causing ``rollback_extents()`` to clone the wrong byte range on each
  shard.

Both sites carry the comment ``TODO(dynamic-object-size)``.

The correct chunk size to use is the one the object had *before* the write
being rolled back, i.e. the value that was in ``oi.ec_chunk_size`` when
the write plan was prepared (``WritePlanObj::chunk_size``).  That value is
not currently stored in the rollback description.

The root cause is that ``ObjectModDesc::append()`` encodes only
``old_size`` (one ``uint64_t``), and the version-3
``ObjectModDesc::rollback_extents()`` encodes ``gen``, ``extents``,
``object_size``, and ``shards`` — but neither carries ``ec_chunk_size``.

Three remedies were considered (see also item 2 in
``EC_DYNAMIC_CHUNK_SIZE_TODO.md`` for the implementation details of each):

**Option A — Store ``ec_chunk_size`` in the mod_desc records.**
Bump ``APPEND`` from encoding version 1 to 2 and add a ``uint64_t
ec_chunk_size`` field.  Bump ``ROLLBACK_EXTENTS`` from version 3 to 4 and
add the same field.  At write time (``ECTransaction.cc`` lines 809 and
1138) pass the pre-write chunk size — ``sinfo.effective_chunk_size(obc->obs.oi)``
evaluated before ``plan.chunk_size`` overwrites it — to the respective
``mod_desc`` calls.  At rollback time the visitor receives the value
directly and passes it to ``object_size_to_shard_size()``.

*Pros:* Self-contained; rollback carries exactly what it needs.  No store
reads during rollback.  Backward-compatible: old records with the previous
version number decode ``ec_chunk_size = 0``, which falls back to the pool
default — correct for non-dynamic pools and for objects written before the
feature was enabled.

*Cons:* Adds 8 bytes to every ``APPEND`` and ``ROLLBACK_EXTENTS`` mod_desc
entry in the PG log, even for non-dynamic pools (unless guarded with
``allows_dynamic_object_size()``, adding a small coupling).  Requires an
on-wire / on-disk format bump for ``ObjectModDesc``.

**Option B — Extract ``ec_chunk_size`` from the co-recorded ``SETATTRS``
entry.**
When a write is planned, the existing OI (with the old ``ec_chunk_size``)
is captured in ``xattr_rollback`` and encoded into the mod_desc stream as
a ``SETATTRS`` record before the ``APPEND`` or ``ROLLBACK_EXTENTS`` record.
``RollbackVisitor`` can decode the old ``OI_ATTR`` during its
``setattrs()`` override, stash the ``ec_chunk_size`` in a member variable,
and then consume it in ``append()`` / ``rollback_extents()``.

*Pros:* No format change to ``ObjectModDesc`` or the PG log; no extra disk
storage.

*Cons:* Relies on the implicit ordering invariant that ``SETATTRS`` always
precedes ``APPEND`` / ``ROLLBACK_EXTENTS`` within the same ``mod_desc``
buffer.  This is true today but the coupling is undocumented and fragile.
Adds an OI-decode cost on every rollback.  If the write did not update the
OI (i.e. no ``SETATTRS`` was recorded before the ``APPEND``) the stash
would be zero and the bug would silently persist.

**Option C — Read the old OI from the stashed object on-disk.**
For ``rmobject`` / ``try_rmobject`` rollback paths the old object is
stashed as ``ghobject_t(hoid, old_version, shard)``; its OI xattr holds
the correct ``ec_chunk_size``.  A store read prior to issuing the
transaction could supply the value.

*Pros:* Ground-truth value; no dependency on mod_desc encoding.

*Cons:* Only applicable to the ``rmobject`` stash path — the ``APPEND``
rollback does not stash anything.  Adding a synchronous OSD-store read
inside a transaction-generation visitor is architecturally wrong and
expensive.  Not a viable general solution.

**Recommendation:** Option A is the correct fix.  It follows the same
precedent as the addition of ``object_size`` to ``ROLLBACK_EXTENTS`` (the
version 2 → 3 bump), is backward-compatible by construction, and avoids
any implicit ordering dependency.  Option B is a viable stopgap if the
encoding format must not be touched, but its ordering assumption should be
explicitly asserted.  Option C is not viable.

**Crimson OSD.**  The Crimson ``ec_backend`` passes ``chunk_size == 0``
(pool default) to ``objects_read_and_reconstruct``.  Crimson does not yet
source the per-object chunk size from the OI and does not support the
dynamic-object-size feature.  The omission is marked
``TODO(dynamic-object-size)`` in ``crimson/osd/ec_backend.cc``.

**Deep scrub stride.**  ``ECBackend::be_deep_scrub()`` aligns its read
stride to the pool-default chunk size rather than the per-object chunk
size; marked ``TODO(dynamic-object-size)`` in ``ECBackend.cc``.

**Sub-chunk encode path.**  The encode path in ``ECBackend`` that operates
on sub-chunks uses the pool-default chunk size; marked
``TODO(dynamic-object-size)`` in ``ECBackend.cc``.

7. Dynamic Chunk Size in Direct Reads and Split Reads
------------------------------------------------------

This section describes the design for teaching the ``ECSplitOp`` /
``EC_DIRECT_READ`` path to use per-object chunk sizes correctly.

Background
~~~~~~~~~~

When ``FLAG_CLIENT_SPLIT_READS`` is set on a pool, the Objecter may split
a client read across data shards in parallel (``ECSplitOp``) or route a
sub-chunk read directly to the owning shard without splitting
(``prepare_single_op``).  Both paths compute shard assignments and
translate logical offsets to shard offsets using a fixed chunk size derived
from the pool's ``stripe_width / data_chunk_count``.

On a ``FLAG_DYNAMIC_OBJECT_SIZE`` pool an individual object may have been
written with a chunk size larger than the pool default (stored in
``oi.ec_chunk_size``).  Using the pool-default geometry to decide which
shard holds a given byte range — and to translate the logical offset to a
shard-local offset — produces wrong results for those objects.

There are two distinct problems:

1. **Shard selection (Objecter side).**  ``ECSplitOp::init_read()``,
   ``prepare_single_op()``, and the ``ECStripeView``-based assembly
   helpers all use pool-default geometry to determine which raw shard
   contains a given byte range.  With a larger per-object chunk size the
   shard boundaries shift, so sub-reads may be sent to the wrong OSD.

2. **Offset translation (OSD side).**  ``objects_read_local()`` calls
   ``extent_to_shard_extent(off, len)`` with no ``chunk_size`` argument,
   defaulting to pool geometry.  Even when the sub-read arrives at the
   correct OSD shard the logical-to-shard-offset arithmetic produces a
   wrong file offset if ``oi.ec_chunk_size`` differs from the pool default.

Condition for enabling the optimized path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both problems only arise on pools that have ``FLAG_DYNAMIC_OBJECT_SIZE``
set.  If that flag is absent the current code is correct and unchanged.
``ECSplitOp::create()`` already checks ``FLAG_CLIENT_SPLIT_READS``; it
will additionally gate the dynamic-chunk logic on
``pi->allows_dynamic_object_size()``.

Object-size hint on the Op
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Objecter does not ordinarily know ``oi.ec_chunk_size`` before the
first reply arrives.  Rather than adding a round-trip STAT, callers that
already know the object size — notably RGW, which tracks content-length
for every object — can supply it as an *object-size hint* on the op.

A new field is added to ``Objecter::Op``::

    uint64_t ec_object_size_hint = 0;   // 0 = unknown

This is intentionally an *object size*, not a chunk size.  The Objecter
derives the effective chunk size from it using exactly the same algorithm
as the OSD write path: ``stripe_info_base_t::chunk_size_for_hint()``.
Because ``stripe_info_base_t`` is defined in ``osd/ECUtil.h``, which
``SplitOp.cc`` does not currently include, the algorithm is accessed by
adding an ``#include "osd/ECUtil.h"`` to ``SplitOp.cc`` (the compilation
unit already transitively depends on ``osd/osd_types.h`` and
``osd/ECTypes.h``) and constructing a temporary ``stripe_info_base_t``
from the pool pointer::

    // Inside SplitOp::create(), after the pool pointer is validated:
    uint64_t ec_chunk_size = 0;
    if (pi->allows_dynamic_object_size() && op->ec_object_size_hint != 0) {
        ECUtil::stripe_info_base_t tmp_sinfo(ec_impl, pi);
        ec_chunk_size = tmp_sinfo.chunk_size_for_hint(
            op->ec_object_size_hint,
            tmp_sinfo.get_max_dynamic_chunk_size());
    }

The derived ``ec_chunk_size`` is stored on the op and threaded into
``ECSplitOp``, ``ECStripeView``, and ``extent_to_shard_extent``.

When the hint is absent (``ec_object_size_hint == 0``) or the pool does
not have ``FLAG_DYNAMIC_OBJECT_SIZE``, the existing code path is followed
unchanged.

When the hint is unknown: design alternatives
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``ec_object_size_hint == 0`` and the pool has
``FLAG_DYNAMIC_OBJECT_SIZE``, the Objecter cannot determine the correct
shard assignment without first knowing the object's chunk size.  Four
strategies were considered.

**Option A — STAT first.**
Issue a ``STAT`` operation to the primary before the read to fetch the
object size (and therefore derive the chunk size).  The read is issued
only after the stat reply arrives.

*Pros:* Completely general; works for any caller regardless of whether it
knows the object size in advance.  The chunk size derived from the stat
will always be correct (the primary has the OI).

*Cons:* Adds a full round-trip latency to every read on a dynamic pool
when the hint is absent.  STAT itself is not free: it acquires the OBC on
the primary.  For the primary consumer (RGW) this extra trip is
unnecessary because the object size is already known.  The complexity of
making the Objecter issue a stat and then continue with the original op
is significant.

**Option B — Speculative split using pool-default geometry; use OSD
error handling to correct misrouted reads.**
Send the split op using pool-default chunk geometry.  For objects whose
actual chunk size equals the pool default this is correct.  For objects
with a larger chunk size, the sub-reads will be sent to wrong shards; the
OSD detects the mismatch and returns ``-EAGAIN`` with the correct chunk
size encoded in the reply.  The Objecter retries using the now-known
correct geometry.

*Pros:* No extra round-trip for the common case (objects written with
pool-default chunk size, or objects not yet written).  Naturally
self-correcting: after one retry the client has the correct chunk size
cached and subsequent reads to the same object use correct geometry
immediately.  Requires no new API for callers.

*Cons:* Objects upgraded from a non-dynamic pool (or written before the
flag was set) always have ``oi.ec_chunk_size == 0``, which the OSD treats
as "pool default" — so the pool-default guess is actually correct for
those objects.  The mismatch only fires for objects explicitly written with
a larger chunk size.  However, this option makes the first read to any
such object always pay one extra round-trip, which may be unacceptable in
latency-sensitive workloads.  It also increases OSD complexity (must
detect the mismatch and produce a meaningful ``-EAGAIN``).

**Option C — Send the read to all data shards.**
Issue a sub-read to every data shard covering the requested byte range,
regardless of which shard the geometry calculation suggests.  Each shard
reads what it has for the given logical offset range and returns it; the
client assembles whichever responses are non-empty.

*Pros:* Completely avoids any geometry calculation on the client side.
Correct regardless of per-object chunk size.  No new protocol fields
needed.

*Cons:* Wastes OSD resources: ``k`` OSDs are contacted even when only one
or two of them hold data for the requested range.  The assembly logic
becomes complex (the client must handle overlapping or gapped responses
from shards that each see the range differently).  May overload small
clusters.  Effectively degrades the performance advantage of split reads.

**Option D — Send to the primary (no split).**  *(chosen for first pass)*
When the hint is absent, skip the split optimisation entirely and route
the op to the primary OSD.  The primary has the OI and handles the read
via the standard async path with the correct per-object chunk size.
``SplitOp::create()`` returns ``false``, causing the Objecter to fall
through to its normal primary routing.

*Pros:* Trivially correct: no geometry calculation required, no extra
round-trip, no new OSD logic, no assembly complexity.  The primary path
is fully exercised and well-tested.

*Cons:* The performance benefit of split reads is lost for callers that
do not supply a size hint.  This is acceptable for the first pass because
the primary consumer (RGW) always knows the content-length and will
supply the hint; other callers without a hint would not have been able to
use split reads correctly anyway.

**Why Option D was chosen for the first implementation**

The primary intended consumer of ``FLAG_CLIENT_SPLIT_READS`` on a dynamic
pool is RGW.  RGW stores the object content-length as part of its object
metadata and can supply ``ec_object_size_hint`` on every read.  In that
configuration Option D never fires: the hint is always present and the
split proceeds with correct geometry.

For callers that do not know the object size, Options A and B are the
natural follow-ups.  Option B (speculative split with OSD-side
``-EAGAIN``) is the preferred next step: it requires no extra round-trip
in the common case and the ``-EAGAIN`` machinery is already present in
``SplitOp::assemble_rc()`` and ``op_post_split_op_complete()``.  It is
deferred because it requires OSD-side mismatch detection (see "Error
handling" below) and a caching mechanism on the op for the corrected chunk
size, which are non-trivial to implement safely.

Encoding the chunk size in the reply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regardless of which strategy is used for the ``hint == 0`` case, the OSD
must be able to communicate the actual per-object chunk size back to the
client to enable mismatch detection and to prime the hint cache for future
requests.

The OSD encodes ``oi.ec_chunk_size`` in every ``MOSDOpReply`` for
direct-read operations on a dynamic pool.  A new field is added to
``MOSDOpReply``::

    uint64_t ec_chunk_size = 0;   // 0 = pool default / not applicable

``HEAD_VERSION`` is bumped to 9; the field is only written when non-zero
so old clients and non-dynamic pools are unaffected.  The Objecter stores
the returned value in ``Op::ec_chunk_size`` (derived or confirmed) for use
on any retry.  This also enables future implementations of Option B: once
the Objecter has seen a reply for an object it knows the correct chunk
size and can cache it without any extra round-trips on subsequent reads.

Client API changes: librados and neorados
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The object-size hint mechanism described above requires callers to supply
the hint when building a read operation.  Neither the librados nor the
neorados API currently provides any way to do this.

**librados (C++ ``ObjectReadOperation`` / C ``rados_read_op_t``)**

``ObjectReadOperation`` has no method corresponding to
``ec_object_size_hint``.  ``set_alloc_hint`` / ``set_alloc_hint2`` exist
only on ``ObjectWriteOperation`` and serve a different purpose (advising
the OSD about storage allocation; they are not visible to the Objecter's
split-op path).  A new method must be added::

    // librados.hpp – ObjectReadOperation
    void set_ec_object_size_hint(uint64_t object_size);

    // librados.h – C API
    void rados_read_op_set_ec_object_size_hint(
        rados_read_op_t op, uint64_t object_size);

The hint is stored on the ``ObjectOperation`` and transferred to
``Objecter::Op::ec_object_size_hint`` when
``prepare_read_op()`` / ``read()`` is called.

**neorados (C++ ``ReadOp``)**

``neorados::ReadOp`` inherits from ``Op`` and exposes a fluent modifier
interface for per-operation flags.  A new modifier follows the same
pattern as the existing ``balance_reads()`` / ``localize_reads()``
modifiers::

    // neorados/RADOS.hpp – ReadOp
    ReadOp& ec_object_size_hint(uint64_t object_size) &;
    ReadOp&& ec_object_size_hint(uint64_t object_size) && {
        return std::move(ec_object_size_hint(object_size));
    }

The value is stored on the underlying ``Op`` and forwarded to
``Objecter::Op::ec_object_size_hint`` through the existing
``RADOS::execute()`` path.

**RGW usage pattern**

RGW is the primary consumer.  When issuing a read via neorados it already
knows the object content-length from its ``rgw_obj_size`` / manifest
metadata.  The expected call-site change is::

    // Before
    op.balance_reads().read(off, len, &bl);

    // After (when content-length is known)
    op.balance_reads()
      .ec_object_size_hint(obj_size)
      .read(off, len, &bl);

When the object size is not yet known (e.g. during listing or before the
manifest has been fetched) the hint is omitted and the op falls back to
primary routing as described above.

OSD-side fix: ``objects_read_local``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ECBackend::objects_read_local()`` is called on a non-primary shard when
the op carries ``CEPH_OSD_FLAG_EC_DIRECT_READ``.  It must translate the
logical ``(off, len)`` to a shard-local offset using the per-object chunk
size, which it currently does not do.

The fix threads ``ec_chunk_size`` through the call chain:

1. **``PGBackend::objects_read_local()``** (virtual, ``PGBackend.h``)
   gains a ``uint64_t ec_chunk_size = 0`` parameter.

2. **``ECBackend::objects_read_local()``** passes it to
   ``extent_to_shard_extent(off, len, ec_chunk_size)``.

3. **``PrimaryLogPG``** — the only caller — already has ``oi`` in scope
   at the ``ec_direct_read()`` branch; it passes ``oi.ec_chunk_size``::

       pgbackend->objects_read_local(
           soid, op.extent.offset, op.extent.length,
           op.flags, &osd_op.outdata, oi.ec_chunk_size);

4. **``ECSwitch::objects_read_local()``** (thin forwarder) forwards the
   new parameter unchanged.  The legacy ``ECBackendL`` and
   ``ReplicatedBackend`` implementations accept but ignore it.

Error handling: hint mismatch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A hint mismatch occurs when the ``ec_chunk_size`` derived by the Objecter
from the caller-supplied ``ec_object_size_hint`` differs from the object's
actual ``oi.ec_chunk_size``.  Causes include:

* The object was written before the pool gained ``FLAG_DYNAMIC_OBJECT_SIZE``
  (so ``oi.ec_chunk_size == 0``, i.e. pool default) but the hint suggests
  a larger size.
* The hint was stale — for example, the object was truncated and rewritten
  with a different size between the hint being recorded and the read being
  issued.
* The caller supplied a hint that was simply wrong.

The OSD detects the mismatch at the ``ec_direct_read()`` branch by
comparing the ``ec_chunk_size`` carried in the op (passed down from the
Objecter) against ``oi.ec_chunk_size``.  Four responses were considered.

**Option 1 — Return -EAGAIN to the client.**  *(chosen)*
The OSD returns ``-EAGAIN`` for the sub-read, encoding the correct
``oi.ec_chunk_size`` in the reply.  The Objecter's existing ``-EAGAIN``
handling in ``SplitOp::assemble_rc()`` and
``op_post_split_op_complete()`` retries the original op via the normal
(non-split) primary path.  On that retry the primary reads with the
correct chunk size.  The correct ``ec_chunk_size`` returned in the reply
primes the op's hint cache so that any subsequent split-read attempt uses
the right geometry directly.

*Pros:* Self-healing, at most one extra round-trip.  Reuses existing retry
infrastructure.  No partial-data assembly complexity.  The client sees a
correct result with no visible error.

*Cons:* Any read to a mismatched object pays one extra round-trip.  For
upgrades from non-dynamic pools this is likely the first read after the
pool flag is enabled; subsequent reads are correct because the chunk size
is now cached.

**Option 2 — Honour the read with corrected geometry on the OSD; complete
or partially complete in-place.**
The shard that receives the misrouted sub-read detects the mismatch,
re-translates ``(off, len)`` using the correct ``oi.ec_chunk_size``, and
returns whatever data it holds for that corrected range.  If the range
happens to be fully covered by this shard the read completes immediately;
otherwise the OSD signals to the Objecter which shards still need to be
read.

*Pros:* Can avoid a full retry in some cases — specifically when the
read was misrouted to a shard that happens to contain the correct data
under the new geometry.

*Cons:* Highly complex.  The Objecter sent the op to the wrong shard (the
shard selection used pool-default geometry); even if the shard can satisfy
the corrected offset translation, the assembly logic in the Objecter was
also using the wrong geometry.  Partial completion requires a new protocol
to express "I answered part of the request; you still need shards X and Y
for the rest", which does not currently exist.  The complexity is not
justified given that Option 1 already bounds the penalty to one extra
round-trip.

**Option 3 — Return an error to the client.**
The OSD returns an error code (e.g. ``-EINVAL``) to the client
application.

*Cons:* The client did nothing wrong; the mismatch is an internal
implementation detail of the dynamic chunk-size feature.  Surfacing it as
an application-visible error is unacceptable.

**Option 4 — Silently return wrong data.**
The OSD completes the read using the mismatched geometry, returning data
from the wrong shard offset.

*Cons:* Silent data corruption.  Unacceptable under any circumstances.

Pool Configuration
==================

Two new pool properties control the feature:

``dynamic_object_size`` (boolean, set-once)
    Enables the feature.  Requires ``allow_ec_overwrites`` and
    ``allow_ec_optimizations``.  Cannot be unset once enabled.

``ec_dynamic_max_chunk_size`` (integer, bytes)
    The maximum per-shard chunk size that may be chosen for any object.
    Default is 1 MiB (``1 << 20``).  The value is stored as the pool
    option ``pool_opts_t::EC_DYNAMIC_MAX_CHUNK_SIZE``.

Example::

    ceph osd pool set <pool> allow_ec_overwrites true
    ceph osd pool set <pool> allow_ec_optimizations true
    ceph osd pool set <pool> dynamic_object_size true
    ceph osd pool set <pool> ec_dynamic_max_chunk_size 1048576

On-disk Format Changes
======================

``object_info_t`` gains a new field ``uint64_t ec_chunk_size`` (default
0).  The encoding version is bumped to 19 with backward-compatibility
level 8, meaning an older OSD reading an object written by a newer OSD
silently treats the field as absent (i.e. uses the pool default), which is
correct for objects written before the feature was enabled.

No change is made to the on-shard data layout; the chunk size is recorded
in the OI attribute only.  The pool-level flag is stored in the OSDMap as
``pg_pool_t::FLAG_DYNAMIC_OBJECT_SIZE`` (bit 23) and survives standard
OSDMap encode/decode.

Testing Strategy
================

Unit tests
----------

* ``TestECUtil``: verifies the base/view split (``for_default``,
  ``for_chunk_size``), geometry correctness under a non-default chunk
  size, ``object_size_to_shard_size`` scaling, and
  ``chunk_size_for_hint`` boundary cases.
* ``osd/types``: ``object_info_t`` encode/decode round-trip for the
  ``ec_chunk_size`` field; pool flag and pool option round-trips.
* ``test_ec_transaction``: verifies that ``get_write_plan()`` chooses and
  records the correct chunk size for a large object, and that the object
  fits in a single stripe under that chunk size.
* ``test_extent_cache``: verifies that the per-object chunk size
  propagates through the extent cache to the backend read.

Standalone functional test
--------------------------

``qa/standalone/erasure-code/test-erasure-code-dynamic-object-size.sh``
exercises the full write/read/partial-overwrite/deep-scrub cycle on a
live cluster with objects of several sizes (4 KiB, ~200 KiB, 1 MiB, 5
MiB).  It also verifies the set-once semantics of the pool flag, and that
``ceph-objectstore-tool`` can dump ``ec_chunk_size > 0`` from the OI of
a large written object.

The recovery path was validated in a manual live-cluster test: stop an
OSD, write and partially overwrite dynamic-object-size objects while
degraded, restart and recover the OSD, then deep-scrub; no inconsistent
PGs or ``data_digest_mismatch`` errors were reported.

Relationship to Existing EC Features
=====================================

The dynamic object size feature requires and builds on:

* **EC overwrites** (``allow_ec_overwrites``): the object must be
  mutable for a persistent chunk-size choice to make sense.
* **EC optimizations** (``allow_ec_optimizations``): the optimized path
  is where the geometry view split and per-object chunk size threading
  live; the legacy ``ECUtilL`` path is unchanged.

The feature is orthogonal to direct reads, split reads, parity-delta
writes, and sub-chunk optimization.  Those features continue to use the
object's effective chunk size (pool default or stashed value) without
any semantic change.
