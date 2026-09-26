/*
 * RGW writes to the keys of a non-versioned bucket: PutObject over an
 * existing object, DeleteObject, CopyObject (sharing the source's tail
 * through cls_refcount), and multipart uploads - their parts, re-uploaded
 * parts, completion, abort and lifecycle's abort - completing over key 1.
 * PutObject, a completion and DeleteObject may carry a condition
 * (If-Match, If-None-Match: *).
 *
 * The RADOS state is one Store machine: each key's head object and bucket
 * index entry, the multipart namespace of the index, the data objects that
 * manifests name with their cls_refcount references, each upload's meta
 * object, and the GC queue. Each Store handler is one atomic RADOS op. Each
 * Rgw machine serves one S3 request, one op at a time, as RGW does.
 */

type tCfg = (
  // mechanisms on main (true is main; false is before the commit named)
  idTagGuard: bool,            // the head write is guarded by cmpxattr on the ID tag it read
  cancelRemovesObjs: bool,     // an index cancel still removes remove_objs (8b27472bbd8a)
  metaVersionCheck: bool,      // the meta object is deleted under cls_version_check,
                               // and parts that raced the completion are GC'd (451b70dedb9)
  historySkipsProcessed: bool, // part history cleanup skips prefixes already processed (451b70dedb9)
  abortTakesLock: bool,        // AbortMultipartUpload takes the completion lock (bae9ed83edf)
  replayAnswersEtag: bool,     // a replayed completion answers the object's ETag (565077e2f1c)
  copyTakesRefs: bool,         // a copy that shares the source's tail takes a cls_refcount
                               // reference on each tail object
  reshardLogs: bool,           // entries changed while resharding in logrecord are logged, and the
                               // incremental pass copies them again (55b404afeb6)
  reshardCheckExisting: bool,  // the incremental pass takes a re-copied entry's old stats out
  oldShardsBlocked: bool,      // after the commit, the old shards still answer -ERR_BUSY_RESHARDING
  // proposed (false is main)
  cancelKeepsVer: bool,        // an index cancel leaves the entry's version alone
  lcTakesLock: bool,           // lifecycle's abort takes the completion lock too
  loserGcsParts: bool,         // a completion that loses the head race sends its parts to GC
  gcSparesHead: bool,          // neither a head write nor an abort sends to GC an object
                               // that the head it writes, or finds, references
  deleteGuard: bool,           // the head removal is guarded by cmpxattr on the ID tag
                               // (as before 55f5b762c67)
  copyLoserDropsRefs: bool,    // a copy that loses the head race drops the references it took
  copySelfGuardsSource: bool,  // a copy onto itself writes only over the head it copied
  ixFailKeepsWrite: bool,      // an index completion that fails after the head write neither
                               // cancels the index op nor undoes the write; it removes the
                               // entries it replaces with a cancel without a tag
  lateCompleteRelinks: bool,   // a writer whose index completion finds its pending op gone
                               // runs a fresh index transaction for its key: prepare, read
                               // the head, complete from it
  completionMark: bool,        // a completion records its tag in the meta object before the
                               // head write; a later completion or abort of the upload that
                               // finds the record checks it against the head's ID tag
  condLossFails: bool,         // a conditional write or delete that loses the head race is
                               // answered an error, not success
  refusedKeepsParts: bool,     // a write refused after it lost the head race cancels its index
                               // op without remove_objs, so a live upload's parts stay listed
  // environment
  completeMayCrash: bool,      // RGW may die after a completion's head write, before it
                               // deletes the meta object; the lock then expires
  metaDeleteMayFail: bool,     // deleting the meta object may fail other than with -ECANCELED
  ixCompleteMayFail: bool,     // an index completion may fail before it reaches the OSD, as
                               // the FIFO bilog flush can (a67a233ccbd); so may the cancel
  // assumption
  lockHeld: bool,              // a live holder keeps the completion lock (renewal succeeds)
  writersPrompt: bool          // a request completes its index op before its pending op
                               // expires (rgw_pending_bucket_index_op_expiration, 120 s)
);

enum tRc { OK, EEXIST, ECANCELED, ENOENT, EBUSY, EIO, EINVAL }

// What a key's head object holds. writer is the request that wrote it;
// tailTag is the tag its tail is referenced by (RGW_ATTR_TAIL_TAG).
// size is the object's size in the model's units; ver is the head
// object's RADOS version, set by the Store.
type tHead = (present: bool, tag: int, tailTag: int, manifest: set[int], writer: int, etag: int, upload: int,
              size: int, ver: int);

// a key's bucket index entry: listed = entry.exists; ver = (pool, epoch);
// iver = entry.index_ver, bumped whenever an op completes on the entry
type tIx = (present: bool, listed: bool, writer: int, size: int, pool: int, epoch: int, pending: set[int],
            iver: int);

// an uploaded part as the meta object records it (RGWUploadPartInfo)
type tPart = (prefix: int, etag: int, past: set[int]);

enum tKind { R_PUT, R_DELETE, R_COPY, R_UPLOAD_PART, R_COMPLETE, R_ABORT, R_LC_ABORT, R_LIST, R_DEDUP, R_RESHARD }
// A request's condition on the head of its key: If-Match with an ETag,
// If-Match: *, or If-None-Match: * on a PutObject or completion; If-Match
// with an ETag on a DeleteObject.
enum tCondKind { C_NONE, C_IF_MATCH, C_IF_MATCH_ANY, C_IF_NONE_MATCH_ANY }
type tCond = (kind: tCondKind, etag: int);
// what a request is answered: success, 412 PreconditionFailed, 404
// NoSuchKey, or another error
enum tAns { A_OK, A_PRECOND, A_NOTFOUND, A_ERR }
// key: the key written or deleted, or a copy's destination; src: a copy's source
type tSpec = (kind: tKind, key: int, src: int, upload: int, num: int, etag: int, list: map[int, int],
              cond: tCond);

// Ids. rid < 100. An upload's base prefix is its id (1..9); a
// re-uploaded part takes a random prefix, 100 + rid. A part is one
// object, its head, which also names its index entry. Multipart uploads
// complete to key 1.
fun MPKEY(): int { return 1; }
fun OBJ(prefix: int, num: int): int { return prefix * 10 + num; }
fun TAIL(rid: int): int { return 5000 + rid; }
fun METAKEY(upload: int): int { return 7000 + upload; }
fun RANDPREFIX(rid: int): int { return 100 + rid; }
fun OLDTAIL(key: int): int { return 9000 + key; }
fun OLDWRITER(key: int): int { return 90 + key; }
fun PARTETAG(upload: int, num: int): int { return 10 * upload + num; }
// the GC tag of an upload's part cleanup: its upload id
fun UPLOADTAG(upload: int): int { return 900 + upload; }
// cls_refcount's implicit reference
fun WILD(): int { return 0; }
// the tag of a writer's re-link index transaction: RELINK() + its rid
fun RELINK(): int { return 100000; }
// an entry's size in the multipart namespace: a part 1, the meta object 0
fun MPSIZE(key: int): int {
  if (key >= 7000) {
    return 0;
  }
  return 1;
}
// a multipart object's ETag is derived from its parts' ETags, in order
fun MPETAG(list: map[int, int]): int {
  var acc: int;
  var n: int;
  n = 1;
  while (n <= 3) {
    if (n in list) {
      acc = acc * 100 + list[n];
    }
    n = n + 1;
  }
  return 1000000 + acc;
}

// driver -> rgw -> driver
event eDone: (rid: int, crashed: bool);

// head objects
event eReadHead: (from: machine, key: int);
event eHeadRead: tHead;
event eHeadWrite: (from: machine, key: int, guard: bool, expectTag: int, excl: bool, head: tHead);
event eHeadRemove: (from: machine, key: int, guard: bool, expectTag: int, rid: int);
// dedup's head update: cmpxattr on the ETag and ref (tail) tag, then
// setxattr of SHARE_MANIFEST and, on the target, of the manifest
event eHeadRewrite: (from: machine, key: int, etag: int, tailTag: int, setManifest: bool, manifest: set[int]);
event eHeadWritten: (rc: tRc, epoch: int);
// data objects
event eWriteData: (from: machine, objs: set[int]);    // write_full of fresh tail objects
event eCreateExcl: (from: machine, obj: int);         // a part head's exclusive create
event eDeleteInline: (from: machine, objs: set[int]); // ~RadosWriter removing what it wrote
event eSendGc: (from: machine, tag: int, objs: set[int]); // a chain sent to GC
event eRefGet: (from: machine, obj: int, tag: int);   // cls_refcount_get
event eRefPut: (from: machine, obj: int, tag: int);   // cls_refcount_put
event eDataDone: tRc;
// the bucket index: each key's entry, and the multipart namespace
enum tIxOp { IX_ADD, IX_DEL, IX_CANCEL }
// every index op names the layout generation its RGW read
event eIndexPrepare: (from: machine, gen: int, key: int, tag: int);
event eIndexComplete: (from: machine, gen: int, key: int, op: tIxOp, tag: int, pool: int, epoch: int,
                       writer: int, size: int, removeKeys: set[int]);
// a listing: an entry as listed, and check_disk_state's suggestion
event eListEntry: (from: machine, key: int);
event eIxEntry: tIx;
event eSuggest: (from: machine, gen: int, key: int, remove: bool, head: tHead, iverSeen: int, readTick: int);
event eClock: machine;                          // how many requests have been answered
event eClockIs: int;
// the bucket's index layout: its generation, now or once a reshard in
// progress commits
event eGetLayout: machine;
event eWaitLayout: machine;
event eLayout: int;
// a reshard: start (logrecord), list the source, copy an entry, block
// (in progress), the incremental pass, commit
event eReshardStart: machine;
event eReshardList: machine;
event eReshardEntries: (ix: map[int, tIx], mp: set[int]);
event eReshardPut: (from: machine, isMain: bool, id: int, e: tIx);
event eReshardBlock: machine;
event eReshardInc: machine;
event eReshardCommit: machine;
event eMpIndexAdd: (from: machine, gen: int, key: int);   // a part head's own index entry
event eMpIndexDel: (from: machine, gen: int, key: int);
event eIndexDone: tRc;
// an upload's meta object
event eTryLock: (from: machine, upload: int, rid: int);
event eUnlock: (from: machine, upload: int, rid: int);
event eIsLocked: (from: machine, upload: int, rid: int);
event eLocked: bool;
event eGetAttrs: (from: machine, upload: int);
event eListParts: (from: machine, upload: int);
event ePartUpdate: (from: machine, upload: int, num: int, part: tPart);
event eMetaDelete: (from: machine, gen: int, upload: int, checkVer: int, removeKeys: set[int]);
event eMetaRc: (rc: tRc, ver: int);
event eMetaMark: (from: machine, upload: int, mark: int);  // the completion's tag, in the meta object
event eGetMark: (from: machine, upload: int);
event eMark: (rc: tRc, mark: int);
event eParts: (rc: tRc, parts: map[int, tPart]);
event eCrashed: int;                            // a request's RGW died
event eFinished: int;                           // a request was answered
event eQuiesce: machine;
event eQuiesced;

// what the specs see at the end, after GC has run to completion
type tFinal = (heads: map[int, tHead], ixs: map[int, tIx],
               live: set[int], referenced: set[int],
               mpIndex: set[int], validKeys: set[int],
               statCount: int, statSize: int);

// monitor-only
event mHead: (key: int, writer: int, manifest: set[int]);  // a head written, or removed (empty)
event mDeleted: int;          // a data object deleted
event mCreated: int;          // a data object created again after it was deleted
event mFinal: tFinal;
event mStarted: int;
event mAnswered: (rid: int, ok: bool);
event mCompleted: (rid: int, etag: int, want: int);  // a completion answered success
event mCrashed: int;
// a conditional request starts: the key, its condition, and what its own
// op leaves there (del: no head; else a head with this ETag)
event mRequest: (rid: int, key: int, cond: tCond, del: bool, etag: int);
// a key's head written (present) or removed by request by (0: set up)
event mHeadState: (key: int, by: int, present: bool, etag: int);
event mReply: (rid: int, ans: tAns);
// n index entries removed through a request's remove_objs
event mIxRemoved: (by: int, n: int);
