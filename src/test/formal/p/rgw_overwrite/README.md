# RGW overwrites, deletes, copies and multipart uploads: a P model

A model of writes to the keys of a non-versioned bucket that already hold
objects. A key is overwritten by PutObject, CopyObject or a multipart
completion, or removed by DeleteObject. Those operations race each other,
part re-uploads, aborts, lifecycle, dedup, bucket listings, a reshard and
GC. A copy within one pool, and dedup, share a tail through
`cls_refcount`.

The model follows the code on main as of `44d50f6abb9`, which includes
the completion-lock renewal of PR 67696 (tracker #75375). Line numbers
below refer to that commit. Tentacle has the same code in every place the
findings below point to; it lacks only PR 67696. It carries the
DeleteObject change behind finding 5 as `f28f0d9147d`.

## The properties

**HeadIntact.** No data object is deleted while a key's head references
it, and no head is written over an object already deleted. What each key
holds stays readable. Under `cls_refcount`, an object sent to GC under one
tag can rightly survive through another reference, so only deletion
counts.

**IndexMatchesHead.** At the end, after a listing has repaired the entries
with pending ops, each key's bucket index entry lists the object the head
holds, or nothing if there is no head.

**BucketStats.** At the end, the index header's stats count every listed
entry, in both namespaces, with its size.

**NoOrphans.** At the end, after GC has run every queued chain, every data
object is referenced by a head or by a live upload. Every entry in the
multipart namespace of the index belongs to a live upload.

**CompletionEtag.** A successful completion answers its object's ETag,
never an empty one.

**AllAnswered** (liveness). Every request is answered, unless its RGW
dies.

## What is modelled

- **The keys.** Key 1 starts with an object from an earlier PutObject, and
  so does key 2 in the copy scenarios. Uploads 1 and 2 complete to key 1;
  each has parts 1 and 2 uploaded at its base prefix. A part is one RADOS
  object: its head, which also names its index entry.
- **`Store`**: the RADOS state. Each handler is one atomic op.
  - Each key's head object. A write is guarded by `cmpxattr` on the ID
    tag that was read, or is an exclusive create. A removal
    (`cls_rgw_remove_obj`) may be guarded or not. Every head write or
    removal takes the PG's next version. A removal that finds no head
    answers the PG's last version, which is the floor the OSD sets on
    `-ENOENT`.
  - Each key's bucket index entry, as `rgw_bucket_prepare_op` and
    `rgw_bucket_complete_op` keep it: pending tags, the version check
    against the head's epoch, and `remove_objs`.
  - The multipart namespace of the index.
  - The data objects, with their `cls_refcount` references. None recorded
    means the implicit one; retired tags are kept, as `cls_rc_refcount_put`
    keeps them.
  - GC. A chain is queued under a tag. Later, GC drops that tag's
    reference from each object, falling back to the implicit one, and
    deletes an object once it has no references left. GC may run any
    queued chain before any op, and runs them all at the end.
  - The index header's stats, adjusted as `cls_rgw` adjusts them.
  - A listing's repair (`rgw_dir_suggest_changes`): it drops pending ops
    whose tag timeout has expired, and applies a suggestion only when none
    are left and no op has completed on the entry since the listing read
    it. At the end, a listing repairs every entry with pending ops.
  - Resharding with logrecord. In logrecord, index ops apply to the source
    and log the entries they touch. The inventory copies each source
    entry to the target. In progress, index ops answer
    `-ERR_BUSY_RESHARDING`. The incremental pass copies the logged entries
    again, taking a re-copied entry's old stats out first
    (`check_existing`). After the commit, the old shards still answer
    `-ERR_BUSY_RESHARDING`.
  - Each upload's meta object: its parts
    (`cls_rgw_mp_upload_part_info_update`, which carries past prefixes
    forward and bumps the `cls_version`), and the completion lock.
- **`Rgw`**: one S3 request, one RADOS op at a time.
  - `PutObject` (`AtomicObjectProcessor`, `write_meta`, `_do_write_meta`):
    - writes the tail first;
    - writes the head, first as an exclusive create, then as a guarded
      replace;
    - sends the replaced head's manifest to GC under its tail tag
      (`complete_atomic_modification`), then completes or cancels the
      index entry;
    - if it loses a race, deletes its own tail and still answers success.
  - `DeleteObject` on a non-versioned bucket (`Delete::delete_obj`):
    - reads the head and prepares the index;
    - removes the head, with no ID-tag guard since `55f5b762c67`;
    - completes the index `DEL`, then sends the manifest it read to GC.
  - Every write goes through `write_meta`. If its index completion fails
    after the head write, as the FIFO bilog flush can make it, it cancels
    the index op (which may fail too) and returns the error. PutObject
    then deletes its tail, a copy drops its references, and a completion
    returns before deleting the meta object.
  - Every index op names the layout generation its request read, and on
    `-ERR_BUSY_RESHARDING` waits for the commit and retries.
  - `CopyObject` within one pool (`copy_obj`):
    - reads the source;
    - takes a reference on each tail object under the new head's tag, and
      drops them if one fails;
    - writes the destination head, whose tail tag is that tag.
    - A copy onto itself instead rewrites the head with the manifest it
      read, `keep_tail`, and the tail tag kept.
  - `UploadPart` (`MultipartObjectProcessor`):
    - creates the part head exclusively, under a random prefix if the part
      was uploaded before;
    - records the part in the meta object, or removes what it wrote if the
      upload is gone.
  - `CompleteMultipartUpload` (`RGWCompleteMultipart::execute`,
    `RadosMultipartUpload::complete`):
    - takes the lock, with `check_previously_completed` when the meta
      object is gone;
    - checks the parts against the client's list;
    - sends each part's past prefixes to GC under the upload id
      (`cleanup_part_history`);
    - writes the head;
    - deletes the meta object under its `cls_version`, cleaning up parts
      that raced (`cleanup_orphaned_parts`) and retrying.
  - `AbortMultipartUpload` (`RGWAbortMultipart`) and lifecycle's
    AbortIncompleteMultipartUpload (`RGWLC::handle_multipart_expiration`).
    Both go through `RadosMultipartUpload::abort`; only the first takes
    the lock.
  - A bucket listing (`cls_bucket_list_ordered`, `check_disk_state`). An
    entry with pending ops, or not marked as existing, gets a suggestion
    from its head. For a multipart head, the listing also drops the
    parts' index entries.
  - Dedup (`rgw::dedup::Background::dedup_object`) of one key's object
    onto another's with the same bytes:
    - takes references on the source's tail under the target's ref tag;
    - rewrites the source head, then the target head, each under
      `cmpxattr` on its ETag and ref tag, leaving the ID tag alone;
    - frees the target's old tail at once, not through GC.
  - A reshard (`RGWBucketReshard::do_reshard`), step by step.
- **`Driver`**: runs a script of phases. The requests of a phase run
  concurrently.

| Scenario | Script |
|---|---|
| `SC_PUTS` | three PutObjects |
| `SC_PUT_VS_COMPLETE` | a PutObject and a completion of upload 1 |
| `SC_COMPLETES` | completions of uploads 1 and 2 |
| `SC_SAME_COMPLETES` | three concurrent completions of upload 1 |
| `SC_REUPLOAD` | a completion, and a re-upload of part 1 with the same bytes (an SDK retry) or other bytes |
| `SC_ABORT`, `SC_LC_ABORT` | a completion, and an AbortMultipartUpload or lifecycle's abort |
| `SC_RETRY` | a completion, then a retry of it |
| `SC_THEN_ABORT` | a completion, then an abort |
| `SC_PUT_THEN_RETRY` | a completion, then a PutObject, then a retry of the completion |
| `SC_DEL_VS_PUT` | a DeleteObject and a PutObject |
| `SC_DELS_AND_PUT` | two DeleteObjects and a PutObject |
| `SC_DEL_VS_COMPLETE` | a DeleteObject and a completion |
| `SC_COPY_VS_PUT_SRC`, `SC_COPY_VS_DEL_SRC` | a copy of key 1 to key 2, and a PutObject over, or DeleteObject of, key 1 |
| `SC_COPY_VS_PUT_DST` | a copy of key 1 to key 2 and a PutObject over key 2; then key 1 deleted |
| `SC_COPY_THEN_DELETES` | a copy of key 1 to key 2; then both keys deleted at once |
| `SC_COPY_SELF_VS_PUT` | a copy of key 1 onto itself, and a PutObject over key 1 |
| `SC_COPY_MPU` | a completion; a copy to key 2 and a PutObject over key 1; then key 2 deleted |
| `SC_CRASH_COPY_RETRY` | a completion; a copy to key 2; a retry of the completion; then key 2 deleted |
| `SC_PUT_ONE`, `SC_COPY_ONE` | one PutObject; or a copy to key 2, then key 1 deleted |
| `SC_LIST_VS_PUT`, `SC_LIST_VS_DEL`, `SC_LIST_VS_COMPLETE` | a listing, and a PutObject, DeleteObject or completion |
| `SC_DEDUP_*` | keys 1 and 2 hold the same bytes: dedup of key 2 onto key 1, alone or racing a PutObject over, DeleteObject of, or copy onto itself of either key |
| `SC_RESHARD_VS_PUTS`, `SC_RESHARD_VS_DEL`, `SC_RESHARD_VS_MPU` | a reshard, and two PutObjects; a DeleteObject and a PutObject; or a completion and a part re-upload |

## Environment and assumptions

- **A completion may leave its meta object behind** (`completeMayCrash`,
  `metaDeleteMayFail`). RGW may die after the head write and before
  deleting the meta object; the lock then expires. Or the delete may fail
  with an error other than `-ECANCELED`, which is only logged.
- **An index completion may fail before it reaches the OSD**
  (`ixCompleteMayFail`). On main, the FIFO bilog flush in `with_bilog`
  (`a67a233ccbd`, not in tentacle) runs first and can fail. The cancel
  that follows goes through the same path and may fail too.
- **A request completes its index op before its pending op expires**
  (`writersPrompt`). That is `rgw_pending_bucket_index_op_expiration`,
  120 s by default. *`tcAssumeWritersPrompt` breaks it.*
- **A listing repairs an entry right after it reads the head.** Its
  repair drops a pending op as expired only if the op's request had
  answered, or died, before the listing read the head, since the op has at
  least that long to run. An op left pending by a failed completion stays
  pending for the whole expiry, and a listing that read the head before
  that write cannot drop it.
- **A live holder keeps the completion lock** (`lockHeld`). The lock is
  renewed while its holder lives, and a failed renewal stops the
  completion only if it happens before the `is_locked()` check.
  *`tcAssumeLockHeld` breaks it.*
- **GC timing.** GC may run a queued chain at any point. On a cluster it
  waits `rgw_gc_obj_min_wait` (2 h). That delays the violations below but
  prevents none of them: each one references or deletes data that is never
  recovered.

## Configurations and results

`../run.sh rgw_overwrite [schedules]` checks each case against
`expect.txt`. Flags not named are as on main. Keep the default of 20,000
schedules: some counterexamples, such as `tcDelsAndPutIndex`'s, need a
rare order of three index completions, and 2,000 schedules can miss
them. Each of the 37 cases that
holds on main or with one proposed fix was also run for 100,000 schedules
under each of random, PCT and POS scheduling (`../deep.sh`), with no bug
found.

| Test case | Scenario | Changes | Result |
|---|---|---|---|
| `tcPutsSafe` | `SC_PUTS` | none | holds |
| `tcPutsIndex` | `SC_PUTS` | none | **violated**: IndexMatchesHead (finding 1) |
| `tcPutsCancelKeepsVer` | `SC_PUTS` | a skipped op leaves `entry.ver` alone (proposed) | holds |
| `tcBugNoIdTagGuard` | `SC_PUTS` | no `cmpxattr` on the ID tag | violated: a replaced write's tail leaks |
| `tcPutVsCompleteSafe` | `SC_PUT_VS_COMPLETE` | none | holds |
| `tcPutVsCompleteLeak` | `SC_PUT_VS_COMPLETE` | none | **violated**: NoOrphans (finding 4) |
| `tcPutVsCompleteLoserGc` | `SC_PUT_VS_COMPLETE` | a losing completion sends its parts to GC (proposed) | holds |
| `tcBugCancelSkipsRemoveObjs` | `SC_PUT_VS_COMPLETE` | that, and a cancel ignores `remove_objs` (before `8b27472bbd8a`) | violated: the parts' index entries are orphaned |
| `tcCompletesSafe` | `SC_COMPLETES` | none | holds |
| `tcCompletesLeak` | `SC_COMPLETES` | none | **violated**: NoOrphans (finding 4) |
| `tcCompletesLoserGc` | `SC_COMPLETES` | a losing completion sends its parts to GC (proposed) | holds |
| `tcSameCompletes` | `SC_SAME_COMPLETES` | none | holds |
| `tcBugNoLockRenewal` | `SC_SAME_COMPLETES` | the lock may lapse under a live holder (before PR 67696) | violated: a second completion GCs the parts the first one's head references (#75375) |
| `tcBugReplayNoEtag` | `SC_SAME_COMPLETES` | a replay answers no ETag (before `565077e2f1c`) | violated: CompletionEtag (#75999) |
| `tcReupload` | `SC_REUPLOAD` | none | holds |
| `tcBugNoMetaVersionCheck` | `SC_REUPLOAD` | the meta object deleted without `cls_version_check` (before `451b70dedb9`) | violated: the re-uploaded part leaks |
| `tcBugHistoryNoSkip` | `SC_REUPLOAD` | the orphan cleanup ignores `processed_prefixes` (part of `451b70dedb9`) | violated: the prefix the head uses is deleted |
| `tcAbortVsComplete` | `SC_ABORT` | none | holds |
| `tcBugAbortNoLock` | `SC_ABORT` | abort without the lock (before `bae9ed83edf`) | violated: HeadIntact |
| `tcAssumeLockHeld` | `SC_ABORT` | the assumption broken | violated: HeadIntact |
| `tcLcAbortVsComplete` | `SC_LC_ABORT` | none | **violated**: HeadIntact (finding 2) |
| `tcLcAbortTakesLock` | `SC_LC_ABORT` | lifecycle takes the lock (proposed) | holds |
| `tcCrashThenRetry` | `SC_RETRY` | RGW may die before the meta delete | **violated**: HeadIntact (finding 3) |
| `tcCrashThenAbort` | `SC_THEN_ABORT` | RGW may die before the meta delete | **violated**: HeadIntact (finding 3) |
| `tcMetaDeleteFailsThenRetry` | `SC_RETRY` | the meta delete may fail | **violated**: HeadIntact (finding 3) |
| `tcMetaDeleteFailsThenAbort` | `SC_THEN_ABORT` | the meta delete may fail | **violated**: HeadIntact (finding 3) |
| `tcSparesHeadCrashRetry` | `SC_RETRY` | GC spares what the head references (proposed) | holds |
| `tcSparesHeadCrashAbort` | `SC_THEN_ABORT` | the same | holds |
| `tcSparesHeadCrashPutRetry` | `SC_PUT_THEN_RETRY` | the same | violated: not a sufficient fix for finding 3 |
| `tcDelVsPutSafe` | `SC_DEL_VS_PUT` | none | holds |
| `tcDelVsPutLeak` | `SC_DEL_VS_PUT` | none | **violated**: NoOrphans (finding 5) |
| `tcDelVsPutGuard` | `SC_DEL_VS_PUT` | the removal guarded on the ID tag (as before `55f5b762c67`) | holds |
| `tcDelsAndPutSafe` | `SC_DELS_AND_PUT` | none | holds |
| `tcDelsAndPutIndex` | `SC_DELS_AND_PUT` | none | **violated**: IndexMatchesHead (finding 1) |
| `tcDelsCancelKeepsVer` | `SC_DELS_AND_PUT` | a skipped op leaves `entry.ver` alone (proposed) | holds |
| `tcDelVsCompleteSafe` | `SC_DEL_VS_COMPLETE` | none | holds |
| `tcDelVsCompleteLeak` | `SC_DEL_VS_COMPLETE` | none | **violated**: NoOrphans (finding 5) |
| `tcDelVsCompleteGuard` | `SC_DEL_VS_COMPLETE` | the guarded removal, and a losing completion GCs its parts | holds |
| `tcCopyVsPutSrc` | `SC_COPY_VS_PUT_SRC` | none | holds |
| `tcBugCopyNoRefs` | `SC_COPY_VS_PUT_SRC` | the copy shares the tail without references | violated: HeadIntact |
| `tcCopyVsDelSrc` | `SC_COPY_VS_DEL_SRC` | none | holds |
| `tcCopyVsPutDstSafe` | `SC_COPY_VS_PUT_DST` | none | holds |
| `tcCopyVsPutDstLeak` | `SC_COPY_VS_PUT_DST` | none | **violated**: NoOrphans (finding 6) |
| `tcCopyVsPutDstDropRefs` | `SC_COPY_VS_PUT_DST` | a losing copy drops its references (proposed) | holds |
| `tcCopyThenDeletes` | `SC_COPY_THEN_DELETES` | none | holds |
| `tcCopySelfVsPutLoss` | `SC_COPY_SELF_VS_PUT` | none | **violated**: HeadIntact (finding 7) |
| `tcCopySelfGuarded` | `SC_COPY_SELF_VS_PUT` | a copy onto itself writes only over the head it copied (proposed) | holds |
| `tcCopyMpu` | `SC_COPY_MPU` | none | holds |
| `tcCrashCopyRetry` | `SC_CRASH_COPY_RETRY` | RGW may die before the meta delete | **violated**: HeadIntact; a copy only delays finding 3 |
| `tcIxFailPutLoss` | `SC_PUT_ONE` | the index completion may fail | **violated**: HeadIntact (finding 8) |
| `tcIxFailPutIndex` | `SC_PUT_ONE` | the same | **violated**: IndexMatchesHead (finding 8) |
| `tcIxFailCopyLoss` | `SC_COPY_ONE` | the same | **violated**: HeadIntact (finding 8) |
| `tcIxFailRetryLoss` | `SC_RETRY` | the same | **violated**: HeadIntact (findings 8 and 3) |
| `tcIxKeepsWritePut`, `tcIxKeepsWriteCopy`, `tcIxKeepsWriteRetry` | `SC_PUT_ONE`, `SC_COPY_ONE`, `SC_RETRY` | the same, and a failed completion after the head write leaves the pending op, keeps the write and removes the entries it replaces (proposed) | holds |
| `tcListVsPut`, `tcListVsDel`, `tcListVsComplete` | `SC_LIST_VS_*` | none | holds |
| `tcAssumeWritersPrompt` | `SC_LIST_VS_PUT` | a request may stall past the pending-op expiry | violated: IndexMatchesHead (finding 11) |
| `tcDedupThenDeletes` | `SC_DEDUP_THEN_DELETES` | none | holds |
| `tcDedupVsPutTgtSafe` | `SC_DEDUP_VS_PUT_TGT` | none | holds |
| `tcDedupVsPutTgtLeak` | `SC_DEDUP_VS_PUT_TGT` | none | **violated**: NoOrphans (finding 9) |
| `tcDedupVsDelTgtLeak` | `SC_DEDUP_VS_DEL_TGT` | none | **violated**: NoOrphans (finding 9) |
| `tcDedupVsDelTgtGuarded` | `SC_DEDUP_VS_DEL_TGT` | the delete's guard restored (M5) | violated: M5 does not cover finding 9 |
| `tcDedupVsPutSrc` | `SC_DEDUP_VS_PUT_SRC` | none | holds |
| `tcDedupVsCopySelfLoss` | `SC_DEDUP_VS_COPY_SELF` | none | **violated**: HeadIntact (finding 10) |
| `tcDedupVsCopySelfGuarded` | `SC_DEDUP_VS_COPY_SELF` | a copy onto itself guarded on the tag it read (M7) | violated: M7 does not cover finding 10 |
| `tcReshardVsPuts`, `tcReshardVsDel`, `tcReshardVsMpu` | `SC_RESHARD_*` | none | holds |
| `tcBugReshardNoLog` | `SC_RESHARD_VS_PUTS` | no reshard log (before `55b404afeb6`) | violated: IndexMatchesHead |
| `tcBugReshardNoCheckExisting` | `SC_RESHARD_VS_PUTS` | the incremental pass adds re-copied entries' stats again | violated: BucketStats |
| `tcBugOldShardsOpen` | `SC_RESHARD_VS_PUTS` | the old shards accept ops after the commit | violated: IndexMatchesHead |
| `tcFixed<Scenario>` | each scenario | the seven proposed fixes together (`Fixed()`) | holds, except the dedup scenarios of findings 9 and 10 |
| `tcFixedIx<Scenario>` | each scenario | the same, and the index completion may fail | holds, except the same dedup scenarios |
| `tcStall<Scenario>` | `SC_LIST_*` | `Fixed()`, and a request may stall past the pending-op expiry | violated (finding 11) |
| `tcRelink<Scenario>` | `SC_LIST_*` | the same, and a writer whose pending op is gone re-links its entry (proposed) | holds |
| `tcMarkCrash<Scenario>` | `SC_RETRY`, `SC_THEN_ABORT`, `SC_PUT_THEN_RETRY`, `SC_CRASH_COPY_RETRY`, `SC_SAME_COMPLETES`, `SC_ABORT`, `SC_LC_ABORT` | `Fixed()`, a completion records its tag before the head write (proposed), and RGW may die | holds |
| `tcMarkMetaDel<Scenario>` | the same | the same, and the meta delete may fail instead | holds |
| `tcMarkLapse<Scenario>` | `SC_ABORT`, `SC_LC_ABORT`, `SC_SAME_COMPLETES` | the same, and the completion lock may lapse under a live holder | violated: a record cannot fence a holder whose lock lapsed before its head write |

## What the model finds on main

These are counterexamples the checker produced, traced by hand to the code
on main at `44d50f6abb9`; the line numbers below are from that commit. All
but finding 8 reproduce on a vstart cluster, with injection points that
hold one request while another runs (`rgw_inject.h`, in
[#72096](https://github.com/ceph/ceph/pull/72096)): finding 1 in `ceph_test_cls_rgw` ([#72097](https://github.com/ceph/ceph/pull/72097));
findings 4, 5, 6, 9 and 10 in `qa/workunits/rgw/test_rgw_overwrite_races.py`
([#72096](https://github.com/ceph/ceph/pull/72096)); findings 2, 3, 7 and 11 in s3-tests (`rgw_inject`
marker, [wip-rgw-overwrite-races](https://github.com/mmgaggle/s3-tests/tree/wip-rgw-overwrite-races)).
Finding 8 needs a FIFO bilog flush to fail.

| Finding | Tracker | Fix |
|---|---|---|
| 1 | [80894](https://tracker.ceph.com/issues/80894) | [#72097](https://github.com/ceph/ceph/pull/72097) |
| 2 | [80895](https://tracker.ceph.com/issues/80895) | [#72099](https://github.com/ceph/ceph/pull/72099) |
| 3 | [80896](https://tracker.ceph.com/issues/80896) | [#72103](https://github.com/ceph/ceph/pull/72103) |
| 4 | [80897](https://tracker.ceph.com/issues/80897) | [#72098](https://github.com/ceph/ceph/pull/72098) |
| 5 | [80898](https://tracker.ceph.com/issues/80898) | [#72100](https://github.com/ceph/ceph/pull/72100) |
| 6 | [80899](https://tracker.ceph.com/issues/80899) | [#72098](https://github.com/ceph/ceph/pull/72098) |
| 7 | [80900](https://tracker.ceph.com/issues/80900) | [#72101](https://github.com/ceph/ceph/pull/72101) |
| 8 | [80902](https://tracker.ceph.com/issues/80902) | [#72098](https://github.com/ceph/ceph/pull/72098) |
| 9, 10 | [80901](https://tracker.ceph.com/issues/80901) | open |
| 11 | [80903](https://tracker.ceph.com/issues/80903) | [#72102](https://github.com/ceph/ceph/pull/72102) |


1. **The bucket index can keep a stale entry for good.**
   `rgw_bucket_complete_op` skips a completion whose epoch is not newer
   than the entry's. It still sets `entry.ver = op.ver`
   (`cls_rgw.cc:1240`), lowering the entry's epoch, and a genuine cancel
   resets it the same way, to `{-1, 0}`. A later, older completion then
   passes the check and is applied. No pending op remains, so no listing
   checks the entry against the head again. `8b27472bbd8a` (2021) moved
   the assignment above the cancel branch; before it, a cancel returned
   first.
   - Three PutObjects whose head writes land in order A, B, C, with
     completions arriving C, A, B: the entry lists B while the head holds
     C (`tcPutsIndex`).
   - A DeleteObject, a PutObject and another DeleteObject, in turn, with
     completions arriving out of order: the entry lists the deleted PUT,
     so ListObjects shows a key that answers 404 (`tcDelsAndPutIndex`).

   Assigning the version only when the op is applied restores the
   property (`tcPutsCancelKeepsVer`, `tcDelsCancelKeepsVer`).
2. **Lifecycle's abort can delete a completing upload's data.**
   `handle_multipart_expiration` calls `RadosMultipartUpload::abort`
   without the completion lock (`rgw_lc.cc:1005`); `bae9ed83edf` gave the
   lock to AbortMultipartUpload only. Take an upload past its
   AbortIncompleteMultipartUpload age that is completed while lifecycle
   processes the bucket. Lifecycle sends the parts to GC, and the
   completion writes the head over them, or the other way round. Abort's
   `cls_version` check does not catch it, because a completion does not
   bump the meta object's version.
3. **A completion that leaves its meta object behind can lose the object's
   data later.** Once the head is written, a failure to delete the meta
   object is only logged (`rgw_op.cc:7879-7885`) and the completion
   answers success. RGW dying between the two steps has the same result.
   The meta object survives with its part list, and then:
   - an AbortMultipartUpload, or lifecycle's abort after the rule's age,
     sends the object's parts to GC;
   - a retried completion lists the same parts and writes the head again.
     `complete_atomic_modification` then sends the replaced head's
     manifest, which is the same parts, to GC (`rgw_rados.cc:6695`).
     `check_previously_completed` runs only when the meta object is gone.

   A copy of the object made in between only delays the loss: its
   reference keeps the parts until the copy is deleted
   (`tcCrashCopyRetry`). Keeping GC away from what the head references
   closes the abort and the plain retry, but not a retry after an
   overwrite (`tcSparesHeadCrashPutRetry`). A fix needs a durable record
   that the completion took effect.
4. **A completion that loses the head race leaks its parts.**
   `_do_write_meta` answers a lost race as success (`rgw_rados.cc:3733`),
   and `RadosMultipartUpload::complete` ignores `meta.canceled`. The
   completion then deletes the meta object, and nothing sends the parts to
   GC.
5. **DeleteObject no longer checks that it removes the head it read.**
   `55f5b762c67` (2025, "fix conditional Delete and MultiDelete")
   replaced the delete's `prepare_atomic_modification` call, which added
   `cmpxattr` on the ID tag, with `check_preconditions`, which adds
   nothing to the op (`rgw_rados.cc:7241`). A delete that races an
   overwrite removes the new head but sends the manifest it read to GC
   (`rgw_rados.cc:7268`). The new object's tail, or a completion's parts,
   is never collected.
6. **A copy that loses the head race leaks the source's tail.**
   `copy_obj` takes a reference on each tail object (`rgw_rados.cc:5488`)
   and rolls them back only when `write_meta` returns an error
   (`rgw_rados.cc:5549`). A lost race returns 0, so the references stay.
   Once the source is deleted or overwritten, its tail survives, held by a
   tag no head carries.
7. **A copy onto itself can resurrect a deleted tail.** A metadata-only
   copy (`copy_itself`) reads the manifest, then writes it back with
   `keep_tail` and the old tail tag (`rgw_rados.cc:5547`). The destination
   has its own `RGWObjectCtx`, so `write_meta` reads the head afresh and
   guards on whatever tag it finds then. An overwrite between the two
   steps replaces the head and sends the old tail to GC; the copy then
   writes that tail back into the head. The overwrite's tail leaks, and
   GC deletes the tail the head now names. Guarding the rewrite on the
   tag the copy read closes it (`tcCopySelfGuarded`).

8. **A failed index completion undoes a write that already happened.**
   `_do_write_meta` treats any error from `index_op->complete` like a lost
   race: it cancels the index op and returns the error
   (`rgw_rados.cc:3670-3678`; the flush at `rgw_rados.cc:11296`). On main since `a67a233ccbd` (March 2026), a
   FIFO-bilog bucket flushes its bilog batch before the index op, so a
   flush error surfaces there, after the head is written. PutObject's
   writer then deletes the tail its new head names. A copy drops its
   references, and the source's deletion takes the tail. A completion
   returns before deleting the meta object, which leads into finding 3. If
   the cancel succeeds, the index also keeps the old object for good.
   Leaving the pending op for a listing to repair, and keeping the write,
   holds (`tcIxKeepsWrite*`).
9. **A writer that read a head before dedup rewrote it leaks the source's
   tail.** Dedup changes the target's manifest but not its ID tag, which
   every writer guards on. A PutObject, DeleteObject or completion that
   read the target before dedup still passes its guard. It then sends the
   stale manifest to GC, which dedup had already freed. The references
   dedup took on the source's tail, under the target's tag, are never
   dropped.
10. **Dedup and a copy onto itself can delete the object's data.** Dedup
    frees the target's old tail at once, not through GC. A copy onto
    itself that read the head before dedup writes that tail back into the
    head. Guarding the copy on the tag it read (M7) does not help,
    because dedup leaves the ID tag alone.
11. **A write that stalls past the pending-op expiry is lost from the
    index.** A listing that finds the write's op pending for more than
    `rgw_pending_bucket_index_op_expiration` (120 s) drops the pending op,
    and rewrites the entry from the head it reads, which may still be the
    old one. The write's completion then fails with `-EINVAL`, and that
    error is ignored because the completion is asynchronous. It needs a
    request to stall for two minutes between its index prepare and
    complete.

The seven proposed fixes hold together (`tcFixed*`), with index
completions that may fail too (`tcFixedIx*`), apart from dedup. Checking
them together showed that keeping a write whose index completion failed
must still remove the entries it replaces, such as a completion's parts:
the listing that repairs the entry reads the head it finds, and a later
completion may have replaced this one by then.

Proposed fixes for findings 3 and 11, checked with the seven fixes on:

- **Finding 3: a completion record** (`completionMark`). Before its head
  write, a completion records its tag, which is the ID tag its head will
  carry, in the meta object. A later completion or abort of the upload
  that finds a record reads the head. If the head carries the tag, the
  completion took effect: a retry answers success and deletes the meta
  object, and an abort deletes the meta object without touching the
  parts. If it does not, the head was never written or was replaced: a
  retry refuses, and an abort proceeds as before. This holds with a crash
  or a failed meta delete in every multipart scenario (`tcMarkCrash*`,
  `tcMarkMetaDel*`). It does not help a holder whose lock lapses before
  its head write (`tcMarkLapse*`); that still rests on the lock being
  renewed while its holder lives. The cost: a retry after a crash between
  the record and the head write is refused, and the upload has to be
  aborted and uploaded again.
- **Finding 11: a re-link** (`lateCompleteRelinks`). A writer whose
  index completion finds its pending op gone runs a fresh index
  transaction for its key: it prepares a new pending op, reads the head,
  and completes from it, with the head's epoch if there is a head, its
  own delete's epoch if it deleted it, or a cancel. It passes its
  `remove_objs` along. A racing delete either sees the new pending op or
  is ordered by epoch, as for any write. The re-link is assumed prompt:
  its own op does not expire. It holds in every listing scenario with
  writers that stall (`tcRelink*`); without it they all fail
  (`tcStall*`). Applying a late completion in `cls_rgw` whenever its
  epoch is newer does not work: an entry that a delete has removed keeps
  no epoch, so the late completion would bring the deleted object back.

With both on, together with the seven fixes, every scenario holds except
dedup, with and without failing index completions (checked; not in
`expect.txt`).

Resharding holds under concurrent PutObject, DeleteObject and multipart
traffic. Each of its mechanisms is needed: without the reshard log, the
index keeps the pre-reshard object; without `check_existing`, the stats
count re-copied entries twice; if the old shards accepted ops after the
commit, a write would land in an index nobody reads.

## Not modelled

- Versioned buckets, OLH, and conditional writes (`If-Match`,
  `If-None-Match`, conditional delete).
- Multi-object delete, and a copy across pools or placements
  (`copy_obj_data`, which writes a fresh tail like a PutObject).
- GET. HeadIntact stands in for "a GET of the current object succeeds".
- Tail stripes, and a head that carries data. A PutObject is one tail
  object and a part is one object.
- Multisite sync and the bilog's contents. The bilog appears only as a
  source of index completion failures.
- `-ETIMEDOUT` handling.
- Dedup's split-head mode, and its table and scan beyond the two
  records it acts on.
- More than one shard per generation. A reshard moves every entry from
  one shard to another.
- More than 3 retries of the meta object's delete (15 on main).
