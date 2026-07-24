==============================
Fast EC: Failure Handling Design
==============================

:Author: Alex Ainscow
:Status: Draft

Overview
--------

Fast EC (``allow_ec_optimizations``) uses variable-size shards whose on-disk
size is derived from the object's logical size and stripe parameters.  A bug
in the transaction planning code (`tracker #77276`_) can leave shards on disk
that are smaller than this expected size — an *underrun*.  When such a shard
is later read during a reconstruct operation on a degraded PG, the EC decode
pipeline receives less data than it requested, returns a non-zero error code,
and the OSD asserts and crashes (`tracker #78400`_).  Because peering a
degraded PG itself triggers a reconstruct read, the OSD crash-loops and
cannot rejoin, with the crash migrating across the cluster as PGs remap.

The fix for #77276 prevents new underruns from being written, but shards
already on disk remain mis-sized.  This document describes the complementary
changes needed to make the system *tolerant* of mis-sized shards — whether
they originate from #77276, from legacy-format objects, or from any other
cause — rather than asserting on them.

The approach has three parts.  At **read time**, an underrun shard is
detected and treated as an I/O error, so the decode path never receives
inconsistent buffers.  In the **recovery write path**, a ``ceph_assert`` that
fires when shard size accounting is stale is converted to a logged error
return that reschedules recovery.  During **scrub with repair**, deep scrub's
existing detection of size-mismatched shards is extended so that those shards
are automatically corrected through the normal recovery mechanism, providing
a proactive way to repair shards that have never been read since the bug
occurred.


Background
----------

Fast EC and shard sizes
~~~~~~~~~~~~~~~~~~~~~~~

Fast EC computes the expected on-disk size for each shard from the object's
logical size and stripe parameters, distributing any remainder across the
first few shards and aligning to the chunk boundary.  This produces shards
that are generally *smaller* than under legacy EC, which padded every shard
to the next full chunk boundary regardless of the object size.

Tracker #77276: transaction planning bug writes under-sized shards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Tracker #77276`_ (fixed in main via PR #69663) describes a transaction
planning bug in the Fast EC write path.  Under certain rare combinations of
operations within a single transaction, the Fast EC transaction planner
produced on-disk shard files smaller than the size that
``object_size_to_shard_size`` would predict for the object's logical size.

The fix for #77276 corrects the planning logic so that new writes do not
produce under-sized shards.  However, objects that were already written
under the buggy code remain on disk with incorrect shard sizes.

Tracker #78400: under-sized shards cause OSD crash cascades
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Tracker #78400`_ documents a production outage on v20.2.1 and v20.2.2
triggered by exactly the shards that #77276 can produce (as well as by
legacy-format objects on pools that had ``allow_ec_optimizations`` enabled
retrospectively).

The crash signature is ``ceph_assert(r == 0)`` immediately after ``decode()``
in the reconstruct-read path, both in the recovery completion handler
(``ECCommon.cc:handle_recovery_read_complete``) and in the client-read
completion handler (``ClientReadCompleter::finish_single_request``).  A
related assertion fires via ``interval_map.h`` through
``complete_read_op`` → ``handle_sub_read_reply``, because the mismatched
buffer sizes violate the extent-map invariant on insertion.

**Why does decode return non-zero?**

When the Fast EC read pipeline issues a reconstruct read, it requests
*optimised-size* extents from each shard — the size that
``object_size_to_shard_size`` predicts.  If an on-disk shard is smaller
than this (due to #77276 or the legacy-format scenario), the remote OSD
returns exactly the bytes requested with no error, but those bytes are
shorter than expected.  The ``slice_iterator`` / ``decode_chunks`` call
inside ``shard_extent_map_t::_decode`` (``src/osd/ECUtil.cc:734``) then
returns a non-zero code because the available buffers do not consistently
cover the requested extents.  The callers assert on this code.

**Why does this cause an outage?**

Peering a degraded PG immediately issues a reconstruct read.  So an OSD
that holds a shard for a degraded PG crashes on startup, cannot rejoin,
and the PG remaps to a new primary — which may itself hold an affected
shard and crash in turn.  The cascade takes the pool inactive.  Standard
operational controls (``norecover``, ``pauserd/pausewr``, etc.) do not
prevent peering reads and therefore do not prevent the crash.

The reporter confirmed via ``ceph-objectstore-tool`` that all shards are
physically intact and version-consistent — the data is not lost and the
failure is entirely in the decode logic.

**What #77276's fix does not address**

The fix for #77276 prevents *new* under-sized shards from being written.
Shards already on disk with the wrong size are unchanged.  On a pool that
has never been degraded since the bad write occurred, those shards sit
silently waiting for the first time a PG loses a different shard and
triggers a reconstruct read — at which point the crash occurs.

The changes described in this document address that residual risk.


Improvement 1: Detect and Handle Shard Underruns Before Decode
--------------------------------------------------------------

Problem
~~~~~~~

In ``ECBackend::handle_sub_read_reply`` (``src/osd/ECBackend.cc:780``),
buffers received from a remote OSD are inserted into the read-complete map
without any validation that each shard returned the number of bytes that were
requested::

    // current code — no size validation
    for (auto &&[offset, buffer_list]: offset_buffer_map) {
        buffers_read.insert_in_shard(from.shard, offset, buffer_list);
    }

The requested extents for each shard are available in
``rop.to_read[hoid].shard_reads``.  The actual bytes received are in
``offset_buffer_map``.  These are never compared.

The mismatch reaches ``shard_extent_map_t::decode()`` →
``shard_extent_map_t::_decode()`` → ``ec_impl->decode_chunks()``, which
returns a non-zero error code.  The caller asserts on that code, crashing the
OSD.

Design
~~~~~~

When the reply arrives for a shard, compare the total bytes received against
the total bytes that were requested for that shard.  If they differ,
synthesise an -EIO error for that shard — exactly as the existing
error-injection path does when ``bluestore_debug_inject_read_err`` fires —
and discard the partial buffer::

    // pseudocode — details to be determined during implementation
    uint64_t expected = total_requested_bytes(rop, hoid, from.shard);
    uint64_t received = total_received_bytes(offset_buffer_map);
    if (received != expected) {
        dout(0) << __func__ << " shard underrun from " << from
                << " expected " << expected << " got " << received << dendl;
        complete.errors[from] = -EIO;
        complete.buffers_read.remove_shard(from.shard);
        rop.debug_log.emplace_back(ECUtil::SHARD_UNDERRUN, op.from);
        continue;  // skip normal buffer insertion
    }

**Special case: recovery reads with unknown object size**

On the first pass of a recovery operation, the object info (``obc``) may not
yet be available and the object size is therefore unknown.
``continue_recovery_op`` requests up to ``get_recovery_chunk_size()``
(rounded-up ``osd_recovery_max_chunk``, typically 4 MiB) in this case, and
shards for small objects will legitimately return fewer bytes than requested.
This is expected, correct behaviour handled via ``zeros_for_decode`` in
``update_object_size_after_read``.

The underrun check must therefore be suppressed when the read was issued with
an unknown object size.  The ``read_request_t::object_size`` field already
distinguishes these two cases: it is set to the true logical size when
``op.obc`` is available, and to ``get_recovery_chunk_size()`` otherwise.
The check should only fire when ``read_request_t::object_size`` reflects the
actual object size (i.e. not the fallback chunk size), or equivalently when
``rop.for_recovery`` is false (client reads always know the object size).

On the second and subsequent recovery passes, ``obc`` is populated and the
read is sized correctly, so the underrun check applies normally.  Any
shard that returns short data on a correctly-sized recovery read is
genuinely mis-sized and should be treated as -EIO.

The subsequent ``check_and_maybe_mark_unfound`` / minimum-to-decode check
determines whether the remaining shards are still sufficient to reconstruct
the object.  If not, the read pipeline returns -EIO to the caller.

For recovery operations, -EIO propagates to
``RecoveryReadCompleter::finish_single_request`` →
``_failed_push`` → ``on_failed_pull`` → ``force_object_missing`` on the
offending shard.  This is important: it is not sufficient merely to exclude
the short shard from the current decode.  The shard must also be added to the
missing set so that it is scheduled for recovery itself — i.e. reconstructed
from the remaining healthy shards and written back at the correct Fast EC
size.  Without this step, the mis-sized shard remains on disk and will cause
the same failure the next time that PG becomes degraded.

For non-recovery (client) reads with a degraded PG, the existing -EIO client
error path is unchanged.

This change prevents the ``ceph_assert(r == 0)`` in both
``handle_recovery_read_complete`` and ``ClientReadCompleter::finish_single_request``
from ever being reached for an underrun condition.

Affected files
~~~~~~~~~~~~~~

* ``src/osd/ECBackend.cc`` — ``handle_sub_read_reply()``: add size validation
  before buffer insertion
* ``src/osd/ECUtil.h`` — optionally add ``SHARD_UNDERRUN`` to the debug-log
  event enum


Improvement 2: Replace Recovery Assert with Error Handling
----------------------------------------------------------

Problem
~~~~~~~

In ``ECCommon::RecoveryBackend::handle_recovery_push``
(``src/osd/ECCommon.cc``), when recovery has finished writing a shard, the
code computes the expected on-disk size and truncates if needed::

    // src/osd/ECCommon.cc ~line 1240
    uint64_t shard_size = sinfo.object_size_to_shard_size(
        op.recovery_info.size, get_parent()->whoami_shard().shard);
    ceph_assert(shard_size >= tobj_size);  // <-- asserts if stale object_info
    if (shard_size != tobj_size) {
        m->t.truncate(coll, tobj, shard_size);
    }

If the ``object_info.size`` field is stale or inconsistent with the actual
shard content — which can occur for objects written under the #77276 bug, or
when objects written under legacy EC have their metadata partially updated —
the assertion fires, crashing the OSD.

This is a second crash site in the same overall failure mode: the first
(Improvement 1) is hit when *reading* from a source shard; this one is hit
when *writing* the recovered destination shard.

Design
~~~~~~

Replace the ``ceph_assert`` with a logged error return that cancels the
recovery operation and schedules a retry after re-fetching object metadata
from the auth peer::

    if (shard_size < tobj_size) {
        derr << __func__ << ": shard_size " << shard_size
             << " < tobj_size " << tobj_size << " for " << op.soid
             << "; object_info size may be stale, cancelling recovery" << dendl;
        _failed_push(op.hoid, /* synthesise read_result_t with r=-EIO */);
        return;
    }
    if (shard_size != tobj_size) {
        m->t.truncate(coll, tobj, shard_size);
    }

This converts a hard OSD crash into a recoverable situation.  If the
object_info is genuinely inconsistent, the object will eventually be marked
unfound and the operator can decide how to proceed.  If it was a transient
inconsistency, recovery succeeds on the next attempt.

Affected files
~~~~~~~~~~~~~~

* ``src/osd/ECCommon.cc`` — ``handle_recovery_push()``: replace
  ``ceph_assert(shard_size >= tobj_size)`` with a logged error return


Improvement 3: Scrub Repairs Mis-Sized EC Shards
-------------------------------------------------

Problem
~~~~~~~

Deep scrub already detects ``SHARD_EC_SIZE_MISMATCH`` in
``ScrubBackend::scan_object_for_errors``
(``src/osd/scrubber/scrub_backend.cc:640``).  When
``smap_obj.ec_size_mismatch`` is set, the shard is correctly excluded from
auth selection via ``dup_error_cond`` / ``set_ec_size_mismatch``.

However, the scrub repair path (``ScrubBackend::apply_repair``) iterates
``m_inconsistent`` and calls ``repair_object()`` only for objects that were
explicitly added to that set.  Objects whose only detected error is
``SHARD_EC_SIZE_MISMATCH`` may not be added to ``m_inconsistent``, so
``repair_object()`` is never called and the mis-sized on-disk shard persists.

Improvements 1 and 2 handle the crash when a mis-sized shard is *read*.  But
a shard can sit mis-sized indefinitely on a PG that has never become degraded
since the bad write.  Scrub repair is the only mechanism that can proactively
find and fix those shards before they cause an outage.

Design
~~~~~~

**Step 1 — Populate ``m_inconsistent`` for ``SHARD_EC_SIZE_MISMATCH``**

Where ``SHARD_EC_SIZE_MISMATCH`` is detected and the scrub is running in
repair mode (``m_repair == true``), add the affected shard to the
``m_inconsistent`` entry for that object alongside the auth peer's
``pg_shard_t``.  This mirrors the existing handling for ``SHARD_READ_ERR``
and ``SHARD_MISSING``.

**Step 2 — No change to ``repair_object()``**

``repair_object()`` already calls ``force_object_missing()`` on the bad
peers, which marks the shard as missing and triggers recovery::

    // src/osd/scrubber/scrub_backend.cc:425 — no changes required
    m_pg.force_object_missing(ScrubberPasskey{}, bad_peers, soid, oi.version);

For EC pools, recovery reconstructs the shard from the k healthy data shards
and writes it back with the correct Fast EC size, permanently fixing the
on-disk representation.

**Step 3 — Gate on ``m_repair``**

A regular deep scrub (without repair) should only detect and log the error,
consistent with the existing behaviour for all other shard errors.  Repair
is only performed when ``m_repair == true`` (i.e. ``ceph pg repair`` was
requested or the auto-repair threshold was crossed).

Affected files
~~~~~~~~~~~~~~

* ``src/osd/scrubber/scrub_backend.cc`` — logic that populates
  ``m_inconsistent`` (around the ``set_ec_size_mismatch`` call site,
  lines ~640–646)
* ``src/test/osd/test_scrubber_be.cc`` — new unit test for the EC shard
  size mismatch repair path


How the Three Improvements Work Together
-----------------------------------------

The three improvements form a complete lifecycle for mis-sized Fast EC shards:

**Scenario A: Shard mis-sized by #77276, PG later becomes degraded**

This is the primary scenario from tracker #78400.  With the fixes:

1. Recovery reads source shards.  A mis-sized shard returns fewer bytes than
   the optimised request; Improvement 1 detects the underrun and synthesises
   -EIO before the data reaches the decoder.
2. The underrun shard is excluded.  If enough healthy shards remain, decode
   succeeds and the missing shard is reconstructed at the correct Fast EC
   size, permanently correcting the on-disk representation for that shard.
3. If the mis-sized shard must itself serve as a source and fewer than k
   healthy shards remain, the object is marked unfound.  The operator must
   correct the shard sizes (e.g. by reading and rewriting the object) before
   recovery can proceed — but the OSD no longer crashes.
4. Improvement 2 ensures that even if ``object_info`` is stale during the
   recovery write, the OSD emits an error and retries rather than asserting.

**Scenario B: Shard mis-sized by #77276, PG never degraded**

Improvement 1 is not exercised (no reconstruct read is ever issued).
Running ``ceph pg repair`` triggers a deep scrub; Improvement 3 detects the
size mismatch, marks the shard missing, and recovery corrects it.  This is
the proactive path that clears #77276-induced corruption before it can
trigger an outage.

**Scenario C: Legacy-format shard on a pool upgraded to Fast EC**

Identical to Scenario A: the legacy shard is larger than the optimised size
expected, but the fast EC read pipeline requests fewer bytes, resulting in an
apparent underrun.  Improvements 1 and 3 handle this the same way.

The result is defence-in-depth:

* **Immediate**: underruns at read time become -EIO, preventing decode asserts
* **Recovery**: recovery writes are guarded against inconsistent metadata
* **Proactive**: periodic scrub with repair finds and corrects wrong-sized
  shards before they can cause an outage


Error Visibility
----------------

+-------------------------------+---------------------------------------------+
| Condition                     | Proposed action                             |
+===============================+=============================================+
| Shard underrun at read time   | ``dout(0)`` warning + synthesise -EIO;      |
|                               | for recovery ops: ``_failed_push`` →        |
|                               | ``on_failed_pull`` →                        |
|                               | ``force_object_missing``                    |
+-------------------------------+---------------------------------------------+
| ``shard_size < tobj_size``    | ``derr`` + cancel recovery op, reschedule   |
| during recovery push          | (instead of assert/crash)                   |
+-------------------------------+---------------------------------------------+
| ``SHARD_EC_SIZE_MISMATCH``    | Existing scrub log entry + (if              |
| at scrub time                 | ``m_repair``) ``clog.error`` +              |
|                               | ``repair_object()``                         |
+-------------------------------+---------------------------------------------+


Out of Scope
------------

* **The #77276 write-side fix**: the transaction planning bug is fixed by
  PR #69663 and is not re-described here.  This document covers only the
  tolerance and repair work for shards that are already mis-sized.

* **Legacy EC** (``ECBackendL``): the legacy code path pads all shards to
  the chunk boundary and its shard sizes are consistent with what the read
  pipeline requests.  These changes are specific to the Fast EC (optimised)
  code path.

* **Hash mismatches** (``SHARD_EC_HASH_MISMATCH``): data that is the correct
  size but has wrong content is handled by the existing deep-scrub
  decode-verification path and is not changed here.

* **Pool-level EIO flag** (``FLAG_EIO``): the -EIO treatment of underruns
  (Improvement 1) is at the per-shard level within a single PG operation
  and does not modify the pool-level flag behaviour.


Key Code Locations
------------------

+-------------------------------+-------------------------------------+---------------------+
| Function                      | File                                | Change              |
+===============================+=====================================+=====================+
| ``handle_sub_read_reply``     | ``src/osd/ECBackend.cc:780``        | Add shard size      |
|                               |                                     | validation before   |
|                               |                                     | buffer insertion    |
+-------------------------------+-------------------------------------+---------------------+
| ``shard_extent_map_t::        | ``src/osd/ECUtil.cc:639``           | No change; decode   |
| decode``                      |                                     | error is prevented  |
|                               |                                     | upstream            |
+-------------------------------+-------------------------------------+---------------------+
| ``handle_recovery_read_       | ``src/osd/ECCommon.cc:1350``        | No change; assert   |
| complete``                    |                                     | at line 1409 is     |
|                               |                                     | prevented upstream  |
+-------------------------------+-------------------------------------+---------------------+
| ``handle_recovery_push``      | ``src/osd/ECCommon.cc:1159``        | Replace assert with |
|                               |                                     | error return        |
+-------------------------------+-------------------------------------+---------------------+
| ``_failed_push``              | ``src/osd/ECCommon.cc:1142``        | No change           |
+-------------------------------+-------------------------------------+---------------------+
| ``repair_object``             | ``src/osd/scrubber/                 | No change           |
|                               | scrub_backend.cc:391``              |                     |
+-------------------------------+-------------------------------------+---------------------+
| ``scan_object_for_errors``    | ``src/osd/scrubber/                 | Populate            |
|                               | scrub_backend.cc:~620``             | ``m_inconsistent``  |
|                               |                                     | for size mismatch   |
|                               |                                     | when ``m_repair``   |
+-------------------------------+-------------------------------------+---------------------+
| ``object_size_to_shard_size`` | ``src/osd/ECUtil.h:614``            | No change;          |
|                               |                                     | reference for size  |
|                               |                                     | computation         |
+-------------------------------+-------------------------------------+---------------------+

.. _tracker #77276: https://tracker.ceph.com/issues/77276
.. _Tracker #77276: https://tracker.ceph.com/issues/77276
.. _tracker #78400: https://tracker.ceph.com/issues/78400
.. _Tracker #78400: https://tracker.ceph.com/issues/78400
