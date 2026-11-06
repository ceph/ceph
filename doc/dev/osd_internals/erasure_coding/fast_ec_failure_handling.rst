==============================
Fast EC: Failure Handling Design
==============================

:Author: Alex Ainscow
:Status: Draft

Overview
--------

Fast EC (``allow_ec_optimizations``) uses variable-size shards whose on-disk
size is derived from the object's logical size and stripe parameters.  A bug
in the EC write path (e.g. `tracker #77276`_) can leave shards on disk
that are smaller than this expected size — an *underrun*.  When such a shard
is later read during a reconstruct operation on a degraded PG, the EC decode
pipeline receives less data than it requested, returns a non-zero error code,
and the OSD asserts and crashes (e.g. `tracker #78400`_).  Because peering a
degraded PG itself triggers a reconstruct read, the OSD crash-loops and
cannot rejoin, with the crash migrating across the cluster as PGs remap.

The fix for #77276 prevents new underruns from being written, but shards
already on disk remain mis-sized.  This document describes the complementary
changes needed to make the system *tolerant* of mis-sized shards — whether
they originate from #77276, from legacy-format objects, or from any other
cause — rather than asserting on them.

The approach differs between client-driven reads and the recovery path.  For
**client operations** (reads and read-modify-writes), an underrun shard is
detected when the reply arrives and treated as an I/O error.  The shard is
excluded and the read retried on the remaining shards; if not enough healthy
shards remain, -EIO is returned to the client.  

For the **recovery path**,
reads do not reject short shards immediately — recovery is unique in that it
can loop over multiple read rounds.  Instead, after all reads for a recovery
pass are complete, any shard whose returned data is shorter than the expected
shard size is identified and added to the missing set; recovery then restarts,
this time treating the short shard as one that itself needs to be recovered.
If the result is that insufficient data remains to perform the reconstruction,
the object is marked unfound. 
 
During **scrub with repair**, scrub's existing detection of size-mismatched
shards is extended so that those shards are automatically corrected through the
existing replica-recovery mechanism, providing a proactive way to repair shards that have
never been involved in a degraded-PG recovery.


Background
----------

Fast EC and shard sizes
~~~~~~~~~~~~~~~~~~~~~~~

Fast EC optimises space by using variably-sized shards.  The size of each
shard is deterministically calculated from the object's logical size and the
pool's stripe parameters.  Any shard whose on-disk size differs from this
calculated value is considered mis-sized.

Improvement 1: Detect Underruns on Client-Op Reads
---------------------------------------------------

Problem
~~~~~~~

Client reads and read-modify-writes hit ``ceph_assert(r == 0)`` when the
decode of a reconstruct-read fails due to a mis-sized source shard.  The
fix is to detect the short buffer in ``handle_sub_read_reply`` before it
reaches the decoder, and treat it as an I/O error.

Additionally, if an object is genuinely the correct size on disk, the
object store read API guarantees that data will be returned for any
sub-read request.  A short return in that case indicates a store-level
failure and the shard should also be treated as -EIO.

Design
~~~~~~

A flag ``check_for_underruns`` is added to ``ReadOp``.  It is set for
client reads (and read-modify-writes) but not for recovery reads (which
handle short returns differently — see Improvement 2).

When the reply arrives for a shard and ``check_for_underruns`` is set,
the total bytes received are compared against the total bytes requested
for that shard.  If they differ, an -EIO error is synthesised for that
shard — exactly as the existing error-injection path does when
``bluestore_debug_inject_read_err`` fires — and the partial buffer is
discarded::

    // pseudocode — details to be determined during implementation
    if (rop.check_for_underruns) {
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
    }

The subsequent ``check_and_maybe_mark_unfound`` / minimum-to-decode check
determines whether the remaining shards are still sufficient to reconstruct
the object.  If not, -EIO is returned to the client.

This change prevents the ``ceph_assert(r == 0)`` in
``ClientReadCompleter::finish_single_request`` and in ``interval_map.h``
from being reached for an underrun condition on client reads.


Improvement 2: Detect and Correct Underruns on the Recovery Path
-----------------------------------------------------------------

Problem
~~~~~~~

The recovery path also calls ``decode()`` in ``handle_recovery_read_complete``
and asserts on failure.  Recovery cannot use the same simple per-shard -EIO
approach as Improvement 1 because on the first recovery pass the object size
may not yet be known — the read is sized to ``get_recovery_chunk_size()``
(rounded-up ``osd_recovery_max_chunk``, typically 4 MiB) — so shards for
small objects will legitimately return fewer bytes than requested.  This
expected short return is handled correctly via ``zeros_for_decode`` in
``update_object_size_after_read`` once the object info arrives with the
response.

Additionally, the ``ceph_assert`` in ``handle_recovery_push`` that fires
when a recovered shard's expected size is smaller than the current on-disk
size must be addressed as a second crash site in the same failure mode.

Design
~~~~~~

**Shard size validation before the final push**

Recovery reads do not set ``check_for_underruns`` (see Improvement 1).
Instead, shard sizes are validated at a higher level: after
``handle_recovery_read_complete`` has populated ``op.returned_data`` and
called ``decode()``, and on the final recovery pass when all data has been
gathered, the size of every source shard that contributed to the read is
checked against the expected shard size calculated from the now-known object
size.

If any source shard is found to have returned fewer bytes than its expected
shard size, the entire recovery operation is restarted with that shard added
to the missing set.  On the restarted pass, recovery reconstructs the
short shard from the remaining healthy shards and writes it back at the
correct size, permanently fixing the on-disk representation.

If adding the short shard to the missing set leaves fewer than k healthy
source shards available, the object is marked unfound.  No attempt is made
to wait for that shard to come back online; the operator must intervene.

**Removing the assert in ``handle_recovery_push``**

The ``ceph_assert(shard_size >= tobj_size)`` in ``handle_recovery_push``
is replaced with a logged error return that cancels and reschedules the
recovery operation::

    if (shard_size < tobj_size) {
        derr << __func__ << ": shard_size " << shard_size
             << " < tobj_size " << tobj_size << " for " << op.soid
             << dendl;
        _failed_push(op.hoid, /* synthesise read_result_t with r=-EIO */);
        return;
    }
    if (shard_size != tobj_size) {
        m->t.truncate(coll, tobj, shard_size);
    }

This eliminates the second assert crash site and converts the failure into
a recoverable situation.


Improvement 3: Scrub Repairs Mis-Sized EC Shards
-------------------------------------------------

Problem
~~~~~~~

The legacy EC backend (``ECBackendL``) has an existing size-mismatch
detection and repair pipeline, but it is gated on ``!allows_ecoverwrites()``.
In ``ECBackendL::be_deep_scrub``, when the pool does not have ``ec_overwrites``
enabled, the on-disk shard size (``pos.data_pos``) is compared against the
size stored in ``hinfo``; if they differ, ``ScrubMap::object::ec_size_mismatch``
is set.  ``scrub_backend.cc`` reads this flag via ``dup_error_cond`` →
``set_ec_size_mismatch()``, which causes ``errors != 0`` on the shard,
which causes ``cur_inconsistent.insert(srd)`` at line 1336, which causes
``repair_object()`` → ``force_object_missing()`` → recovery.  The full
pipeline works for legacy EC without overwrites.

Fast EC always has ``ec_overwrites`` enabled.  The legacy path's size check
is therefore never reached.  Additionally, the Fast EC backend
(``ECBackend::be_deep_scrub``) has no size check of its own — it reads the
shard, accumulates a hash, and returns.  ``ScrubMap::object::ec_size_mismatch``
is therefore never set for Fast EC objects, the repair pipeline is never
triggered, and mis-sized shards sit undetected indefinitely.

The fix is straightforward: extend the mechanism that already works for
legacy EC without overwrites so that it also works for Fast EC with
overwrites, using the variable-size shard size formula instead of
``hinfo``-based sizes.

Improvements 1 and 2 handle the crash when a mis-sized shard is encountered
during a degraded-PG operation.  But a shard can sit mis-sized indefinitely
on a PG that has never become degraded since the bad write.  Scrub repair is
the only mechanism that can proactively find and fix those shards before they
cause an outage.

Design
~~~~~~

Add a size check to ``ECBackend::be_deep_scrub``: after all strides have
been read and ``pos.data_pos`` reflects the total bytes on disk, compare
it against the expected shard size calculated from the object's logical
size using ``sinfo.object_size_to_shard_size(oi.size, whoami_shard)``.  If
they differ, set ``o.ec_size_mismatch = true``.

This is the only change required.  Once ``ec_size_mismatch`` is set in the
local scan map, the entire downstream pipeline already works correctly for
all pool types:

* ``scrub_backend.cc:641`` reads the flag → ``set_ec_size_mismatch()`` →
  ``errors != 0`` on the shard
* ``scrub_backend.cc:1334`` adds the shard to ``cur_inconsistent``
* ``inconsistents()`` populates ``m_inconsistent``
* ``scrub_process_inconsistent()`` calls ``repair_object()`` when
  ``m_repair == true``
* ``repair_object()`` calls ``force_object_missing()`` → recovery
  reconstructs the shard at the correct Fast EC size

A regular deep scrub (without repair) detects and logs the error without
attempting to fix it, consistent with all other shard error types.


Testing Requirements
--------------------

The overriding requirements across all scenarios are:

* **A mis-sized shard must never cause an OSD assert.**
* **A mis-sized shard must be corrected by recovery** when the PG becomes
  degraded and the shard is involved in a reconstruct operation.
* **A mis-sized shard must be detected and repaired by scrub** when
  ``ceph pg repair`` is run, without requiring the PG to become degraded
  first.

The following scenarios should be covered by
automated tests.  Unless otherwise stated, tests should use a Fast EC pool
with ``ec_overwrites`` enabled.  The error injection mechanism
(``bluestore_debug_inject_read_err`` or direct object store manipulation to
truncate a shard) should be used to create mis-sized shards.

**Improvement 1 — Client reads**

T1.1 Client read on a degraded PG (k+m pool, one shard offline, one shard
mis-sized among the remaining k+m−1 shards) where the mis-sized shard is
needed for the reconstruct.  Expected: the underrun is detected,
the mis-sized shard is excluded, decode succeeds using the remaining k−1
healthy shards plus the zeros padding, and the read returns correct data.

T1.2 Client read on a degraded PG where the mis-sized shard is the *only*
redundant copy available (i.e. the pool has exactly m=1 redundancy and
both the offline shard and the mis-sized shard are required for decoding).
Expected: -EIO returned to client; no OSD assert.

T1.3 Read-modify-write (RMW) on a degraded PG with a mis-sized source shard.
Expected: the underrun is detected, RMW fails with -EIO; no OSD assert.

T1.4 Client read on a healthy (non-degraded) PG with a mis-sized shard.
Expected: read succeeds without touching the mis-sized shard (no
reconstruct needed); mis-sized shard is not detected here.

**Improvement 2 — Recovery path**

T2.1 A shard is missing (taken offline).  Among the remaining shards used
as sources for recovery, one is mis-sized.  Expected: after the first
recovery pass completes, the mis-sized source shard is detected, added to
the missing set, and recovery restarts.  On the second pass, the mis-sized
shard is itself reconstructed from the remaining k−1 healthy shards.  Both
the originally missing shard and the mis-sized shard are correctly written
back.  The PG returns to ``active+clean``.

T2.2 A shard is missing and the only mis-sized shard is the one being
recovered (the destination).  Expected: ``handle_recovery_push`` detects the
size mismatch, does not assert, and recovery writes the shard at the correct
size.  PG returns to ``active+clean``.

T2.3 A shard is missing.  Exactly m source shards are mis-sized (leaving
fewer than k healthy sources after the mis-sized shards are added to the
missing set).  Expected: object is marked unfound; no OSD assert; cluster
health shows unfound objects.

T2.4 Recovery of a small object (smaller than ``osd_recovery_max_chunk``).
The first recovery pass requests more bytes than the object size; all shards
return short reads legitimately.  Expected: no spurious underrun detection;
``update_object_size_after_read`` correctly resets the zero masks; recovery
completes normally.

T2.5 OSD crash-loop regression: reproduce the exact scenario from
tracker #78400 — a Fast EC pool with legacy-format objects (written before
``allow_ec_optimizations`` was enabled) on a degraded PG.  Expected: OSDs
do not crash-loop; recovery eventually completes; no ``ceph_assert`` in logs.

**Improvement 3 — Scrub repair**

T3.1 Deep scrub (without repair) on a healthy PG where one shard is
mis-sized.  Expected: scrub reports ``SHARD_EC_SIZE_MISMATCH`` in the health
detail / log; no repair is attempted; PG remains ``active+clean``.

T3.2 ``ceph pg repair`` on a healthy PG where one shard is mis-sized.
Expected: scrub detects the mismatch, ``repair_object`` is called,
``force_object_missing`` marks the shard missing, recovery rewrites the shard
at the correct size, and the PG returns to ``active+clean`` with no
outstanding size-mismatch errors on the next scrub.

T3.3 ``ceph pg repair`` where multiple shards of the same object are
mis-sized but fewer than m (i.e. recovery can still reconstruct).  Expected:
all mis-sized shards are detected and repaired across successive repair
passes; PG returns to clean.

T3.4 ``ceph pg repair`` where the number of mis-sized shards equals m
(insufficient sources for recovery).  Expected: object is reported as
unfound; no OSD assert; remaining healthy shards are not disturbed.

T3.5 Deep scrub on a pool where ``allow_ec_optimizations`` was enabled
retrospectively on a pool with legacy-format objects.  Expected: legacy
objects whose shard sizes do not match the Fast EC formula are detected and
reported; repair corrects them via recovery.

**Cross-cutting**

T4.1 A mis-sized shard combined with a simultaneously offline shard such
that the total available healthy shards equals exactly k (minimum to
decode).  Expected: client reads and recovery succeed using exactly those k
shards; the mis-sized shard is excluded; no assert.

T4.2 Verify that ``bluestore_debug_inject_read_err`` (the existing short-read
injection mechanism) correctly triggers the new underrun detection path and
produces the expected ``SHARD_UNDERRUN`` log entry and -EIO behaviour.

T4.3 A mis-sized shard on a PG that is not degraded and is never read in a
reconstruct context.  The shard persists silently until scrub with repair
corrects it.  Verify via T3.2 that this path is complete.


Out of Scope
------------

* **Legacy EC** (``ECBackendL``): the legacy code path pads all shards to
  the chunk boundary and its shard sizes are consistent with what the read
  pipeline requests.  These changes are specific to the Fast EC (optimised)
  code path.

* **Hash mismatches** (``SHARD_EC_HASH_MISMATCH``): data that is the correct
  size but has wrong content is handled by the existing deep-scrub
  decode-verification path and is not changed here.  Note that today only
  the first parity shard can be checked, so it is not always possible to
  identify which specific shard has a hash mismatch.


.. _tracker #77276: https://tracker.ceph.com/issues/77276
.. _Tracker #77276: https://tracker.ceph.com/issues/77276
.. _tracker #78400: https://tracker.ceph.com/issues/78400
.. _Tracker #78400: https://tracker.ceph.com/issues/78400
