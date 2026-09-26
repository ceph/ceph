/*
 * RGW serving one S3 request, one RADOS op at a time:
 * - PutObject (RGWPutObj, AtomicObjectProcessor);
 * - DeleteObject on a non-versioned bucket
 *   (RGWRados::Object::Delete::delete_obj);
 * - CopyObject within one pool (RGWRados::copy_obj), which shares the
 *   source's tail;
 * - UploadPart (RGWPutObj, MultipartObjectProcessor);
 * - CompleteMultipartUpload (RGWCompleteMultipart,
 *   RadosMultipartUpload::complete);
 * - AbortMultipartUpload (RGWAbortMultipart) and lifecycle's
 *   AbortIncompleteMultipartUpload (RGWLC::handle_multipart_expiration),
 *   both through RadosMultipartUpload::abort;
 * - a bucket listing's repair of entries with pending ops
 *   (cls_bucket_list_ordered, check_disk_state);
 * - dedup of one key's object onto another's with the same bytes
 *   (rgw::dedup::Background::dedup_object);
 * - a bucket reshard (RGWBucketReshard::execute).
 * A request reads the bucket's index layout when it starts. An index op
 * answered -ERR_BUSY_RESHARDING waits for the reshard to commit and is
 * retried on the new layout (UpdateIndex::guard_reshard).
 * A request's tag, and the writer recorded in what it writes, is its rid.
 * write_meta's outcome is WRITTEN, LOST (a lost race, answered as
 * success) or FAILED (the head was written but the index completion
 * failed, answered as an error).
 */
machine Rgw {
  var cfg: tCfg;
  var store: machine;
  var driver: machine;
  var rid: int;
  var gen: int;

  start state Serve {
    entry (p: (cfg: tCfg, store: machine, driver: machine, rid: int, req: tSpec)) {
      cfg = p.cfg;
      store = p.store;
      driver = p.driver;
      rid = p.rid;
      announce mStarted, rid;
      gen = GetLayout();
      if (p.req.kind == R_PUT) {
        PutObject(p.req.key);
      } else if (p.req.kind == R_DELETE) {
        DeleteObject(p.req.key);
      } else if (p.req.kind == R_COPY) {
        CopyObject(p.req.src, p.req.key);
      } else if (p.req.kind == R_UPLOAD_PART) {
        UploadPart(p.req.upload, p.req.num, p.req.etag);
      } else if (p.req.kind == R_COMPLETE) {
        CompleteMultipart(p.req.upload, p.req.list);
      } else if (p.req.kind == R_ABORT) {
        Abort(p.req.upload, cfg.abortTakesLock);
      } else if (p.req.kind == R_LIST) {
        ListBucket();
      } else if (p.req.kind == R_DEDUP) {
        Dedup(p.req.src, p.req.key);
      } else if (p.req.kind == R_RESHARD) {
        Reshard();
      } else {
        Abort(p.req.upload, cfg.lcTakesLock);
      }
    }
  }

  // AtomicObjectProcessor: the tail first, then the head (holding the
  // first chunk) in write_meta
  fun PutObject(key: int) {
    var tail: set[int];
    var w: int;
    tail += (TAIL(rid));
    WriteData(tail);
    w = WriteMeta(key, tail, rid, 0, default(set[int]), rid, false, 1);
    if (w != WRITTEN()) {
      // lost a race, or failed: ~RadosWriter removes the tail it wrote
      DeleteInline(tail);
    }
    Answer(w != FAILED());
  }

  // RGWRados::Object::Delete::delete_obj on a non-versioned bucket
  fun DeleteObject(key: int) {
    var st: tHead;
    var r: (rc: tRc, epoch: int);
    st = ReadHead(key);
    if (!st.present) {
      Answer(true);  // -ENOENT, answered 204
      return;
    }
    IndexPrepare(key);
    // cls_rgw_remove_obj; on main without the ID tag check (55f5b762c67)
    r = HeadRemove(key, cfg.deleteGuard, st.tag);
    if (r.rc == OK || r.rc == ENOENT) {
      if (cfg.ixCompleteMayFail && $) {
        // complete_del fails before the op reaches the OSD: the pending
        // op stays, and the error is answered after the tail goes to GC
        SendGc(st.tailTag, st.manifest);
        Answer(false);
        return;
      }
      if (IndexComplete(key, IX_DEL, 1, r.epoch, 0, default(set[int])) == EINVAL && cfg.lateCompleteRelinks) {
        Relink(key, r.epoch, true, default(set[int]));
      }
      // complete_atomic_modification: the head it read goes to GC
      SendGc(st.tailTag, st.manifest);
      Answer(true);
      return;
    }
    IndexComplete(key, IX_CANCEL, -1, 0, 0, default(set[int]));
    Answer(false);
  }

  // RGWRados::copy_obj within one pool. The destination shares the
  // source's tail: each tail object gets a reference under the new head's
  // tag, which is also the new head's tail tag.
  fun CopyObject(src: int, dst: int) {
    var s: tHead;
    var got: set[int];
    var o: int;
    var g: int;
    var rc: tRc;
    var canceled: bool;
    var w: int;
    s = ReadHead(src);
    if (!s.present) {
      Answer(false);  // NoSuchKey
      return;
    }
    if (src == dst) {
      // copy_itself: the manifest read above, keep_tail, the tail tag
      // kept. The write reads the head afresh (the destination has its own
      // RGWObjectCtx), so it guards on whatever head is there by then.
      if (cfg.copySelfGuardsSource) {
        canceled = WriteHeadOver(dst, s, s.manifest, s.etag, s.upload, s.tailTag, s.size);
        Answer(true);
      } else {
        w = WriteMeta(dst, s.manifest, s.etag, s.upload, default(set[int]), s.tailTag, true, s.size);
        Answer(w != FAILED());
      }
      return;
    }
    if (cfg.copyTakesRefs) {
      foreach (o in s.manifest) {
        rc = RefGet(o, rid);
        if (rc != OK) {
          // done_ret: drop the references taken
          foreach (g in got) {
            rc = RefPut(g, rid);
          }
          Answer(false);
          return;
        }
        got += (o);
      }
    }
    w = WriteMeta(dst, s.manifest, s.etag, 0, default(set[int]), rid, false, s.size);
    // done_ret drops the references on an error; a lost race keeps them
    if (w == FAILED() || (w == LOST() && cfg.copyLoserDropsRefs)) {
      foreach (g in got) {
        rc = RefPut(g, rid);
      }
    }
    Answer(w != FAILED());
  }

  // RGWRados::Object::Write::write_meta and _do_write_meta
  fun WriteMeta(key: int, manifest: set[int], etag: int, upload: int, removeKeys: set[int],
                tailTag: int, keepTail: bool, size: int): int {
    var st: tHead;
    var nh: tHead;
    var r: (rc: tRc, epoch: int);
    var assumeNoent: bool;
    var prepared: bool;
    var attempts: int;
    var old: set[int];
    var o: int;
    var ixrc: tRc;
    nh = (present = true, tag = rid, tailTag = tailTag, manifest = manifest, writer = rid, etag = etag,
          upload = upload, size = size, ver = 0);
    // first without reading the head, as an exclusive create; on
    // -EEXIST, read it and replace it
    assumeNoent = true;
    while (attempts < 2) {
      attempts = attempts + 1;
      if (assumeNoent) {
        st = default(tHead);
      } else {
        st = ReadHead(key);
      }
      if (!prepared) {
        IndexPrepare(key);
        prepared = true;
      }
      // prepare_atomic_modification: cmpxattr on the ID tag read, and an
      // exclusive create if there was no head
      r = HeadWrite(key, st.present && cfg.idTagGuard, st.tag, !st.present, nh);
      if (!(r.rc == EEXIST && assumeNoent)) {
        attempts = 2;
      }
      assumeNoent = false;
    }
    if (r.rc != OK) {
      // done_cancel: -ECANCELED, -ENOENT or -EEXIST, answered as success
      IndexComplete(key, IX_CANCEL, -1, 0, 0, removeKeys);
      return LOST();
    }
    // complete_atomic_modification: the replaced head's manifest goes to
    // GC under its tail tag, unless keep_tail
    if (st.present && !keepTail) {
      foreach (o in st.manifest) {
        if (!(cfg.gcSparesHead && o in manifest)) {
          old += (o);
        }
      }
      SendGc(st.tailTag, old);
    }
    if (cfg.ixCompleteMayFail && $) {
      // UpdateIndex::complete fails before the op reaches the OSD
      if (cfg.ixFailKeepsWrite) {
        // the pending op stays for a listing to repair. The entries the
        // write replaces are its own to remove (complete_remove_objs); this
        // cleanup is assumed to get through, or they would stay for a
        // bucket check
        if (sizeof(removeKeys) > 0) {
          IndexRemoveObjs(key, removeKeys);
        }
        return WRITTEN();
      }
      // done_cancel: the cancel may fail the same way; the caller then
      // undoes its write
      if ($) {
        IndexComplete(key, IX_CANCEL, -1, 0, 0, removeKeys);
      }
      return FAILED();
    }
    ixrc = IndexComplete(key, IX_ADD, 1, r.epoch, size, removeKeys);
    if (ixrc == EINVAL && cfg.lateCompleteRelinks) {
      Relink(key, r.epoch, false, removeKeys);
    }
    return WRITTEN();
  }

  // proposed: the pending op of this write is gone, dropped as expired
  // by a listing that may have read the head before the write. Run a
  // fresh index transaction for the key: prepare, read the head, and
  // complete from what it holds now. Like any write, a racing delete
  // either sees this pending op or is ordered by epoch. A re-link is
  // prompt: its own op does not expire.
  fun Relink(key: int, ownEpoch: int, isDel: bool, removeKeys: set[int]) {
    var h: tHead;
    var tag: int;
    tag = RELINK() + rid;
    IndexPrepareTag(key, tag);
    h = ReadHead(key);
    if (h.present) {
      IndexCompleteAs(key, IX_ADD, 1, h.ver, h.writer, h.size, removeKeys, tag);
    } else if (isDel) {
      IndexCompleteAs(key, IX_DEL, 1, ownEpoch, rid, 0, removeKeys, tag);
    } else {
      IndexCompleteAs(key, IX_CANCEL, -1, 0, rid, 0, removeKeys, tag);
    }
  }

  // Background::dedup_object: the target comes to share the source's
  // tail. The records are the scan's: each head's ETag, manifest and ref
  // tag (the tail tag).
  fun Dedup(src: int, tgt: int) {
    var s: tHead;
    var t: tHead;
    var got: set[int];
    var o: int;
    var g: int;
    var rc: tRc;
    s = ReadHead(src);
    t = ReadHead(tgt);
    if (!s.present || !t.present || s.etag != t.etag) {
      Answer(false);
      return;
    }
    // inc_ref_count_by_manifest: the source's tail, under the target's tag
    foreach (o in s.manifest) {
      rc = RefGet(o, t.tailTag);
      if (rc != OK) {
        foreach (g in got) {
          rc = RefPut(g, t.tailTag);
        }
        Answer(false);
        return;
      }
      got += (o);
    }
    // the source head, then the target head, each under cmpxattr on its
    // ETag and ref tag; a failure rolls the references back
    rc = HeadRewrite(src, s.etag, s.tailTag, false, default(set[int]));
    if (rc == OK) {
      rc = HeadRewrite(tgt, t.etag, t.tailTag, true, s.manifest);
    }
    if (rc != OK) {
      foreach (g in got) {
        rc = RefPut(g, t.tailTag);
      }
      Answer(false);
      return;
    }
    // free_tail_objs_by_manifest: the target's old tail, at once, not
    // through GC
    foreach (o in t.manifest) {
      rc = RefPut(o, t.tailTag);
    }
    Answer(true);
  }

  fun HeadRewrite(key: int, etag: int, tailTag: int, setManifest: bool, manifest: set[int]): tRc {
    var r: tRc;
    send store, eHeadRewrite, (from = this, key = key, etag = etag, tailTag = tailTag, setManifest = setManifest,
                               manifest = manifest);
    receive {
      case eDataDone: (x: tRc) { r = x; }
    }
    return r;
  }

  // RGWBucketReshard::do_reshard, with logrecord
  fun Reshard() {
    var src: (ix: map[int, tIx], mp: set[int]);
    var k: int;
    var o: int;
    ReshardOp(0);
    // the inventory: each source entry as listed, copied to the target
    src = ReshardList();
    foreach (k in keys(src.ix)) {
      if (src.ix[k].present) {
        ReshardPut(true, k, src.ix[k]);
      }
    }
    foreach (o in src.mp) {
      ReshardPut(false, o, default(tIx));
    }
    ReshardOp(1);  // in progress: client index ops block
    ReshardOp(2);  // the incremental pass over the logged entries
    ReshardOp(3);  // commit
    Answer(true);
  }

  fun ReshardOp(step: int) {
    if (step == 0) {
      send store, eReshardStart, this;
    } else if (step == 1) {
      send store, eReshardBlock, this;
    } else if (step == 2) {
      send store, eReshardInc, this;
    } else {
      send store, eReshardCommit, this;
    }
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun ReshardList(): (ix: map[int, tIx], mp: set[int]) {
    var r: (ix: map[int, tIx], mp: set[int]);
    send store, eReshardList, this;
    receive {
      case eReshardEntries: (x: (ix: map[int, tIx], mp: set[int])) { r = x; }
    }
    return r;
  }

  fun ReshardPut(isMain: bool, id: int, e: tIx) {
    send store, eReshardPut, (from = this, isMain = isMain, id = id, e = e);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun GetLayout(): int {
    var g: int;
    send store, eGetLayout, this;
    receive {
      case eLayout: (x: int) { g = x; }
    }
    return g;
  }

  // -ERR_BUSY_RESHARDING: block_while_resharding, then the new layout
  fun Rewait() {
    send store, eWaitLayout, this;
    receive {
      case eLayout: (x: int) { gen = x; }
    }
  }

  fun WRITTEN(): int { return 0; }
  fun LOST(): int { return 1; }
  fun FAILED(): int { return 2; }

  // cls_bucket_list_ordered: an entry with pending ops, or not marked as
  // existing, goes through check_disk_state, which reads the head and
  // suggests an update from it, or a removal
  fun ListBucket() {
    var tick: int;
    var k: int;
    var e: tIx;
    var h: tHead;
    var o: int;
    k = 1;
    while (k <= 2) {
      e = ListEntry(k);
      if (e.present && (!e.listed || sizeof(e.pending) > 0)) {
        tick = Clock();
        h = ReadHead(k);
        if (h.present && h.upload != 0) {
          // a multipart head's parts leave the multipart namespace
          foreach (o in h.manifest) {
            MpIndexDel(o);
          }
        }
        Suggest(k, !h.present, h, e.iver, tick);
      }
      k = k + 1;
    }
    Answer(true);
  }

  // proposed: a keep_tail rewrite guarded on the head it read before, so
  // it lands only over that head
  fun WriteHeadOver(key: int, st: tHead, manifest: set[int], etag: int, upload: int, tailTag: int,
                     size: int): bool {
    var nh: tHead;
    var r: (rc: tRc, epoch: int);
    nh = (present = true, tag = rid, tailTag = tailTag, manifest = manifest, writer = rid, etag = etag,
          upload = upload, size = size, ver = 0);
    IndexPrepare(key);
    r = HeadWrite(key, true, st.tag, false, nh);
    if (r.rc != OK) {
      IndexComplete(key, IX_CANCEL, -1, 0, 0, default(set[int]));
      return true;
    }
    IndexComplete(key, IX_ADD, 1, r.epoch, size, default(set[int]));
    return false;
  }

  // MultipartObjectProcessor
  fun UploadPart(u: int, num: int, etag: int) {
    var prefix: int;
    var obj: int;
    var rc: tRc;
    var written: set[int];
    // process_first_chunk: the part head is created exclusively; if the
    // part was uploaded before, under a random prefix
    prefix = u;
    rc = CreateExcl(OBJ(prefix, num));
    if (rc == EEXIST) {
      prefix = RANDPREFIX(rid);
      rc = CreateExcl(OBJ(prefix, num));
      assert rc == OK, "a random part prefix collided";
    }
    obj = OBJ(prefix, num);
    written += (obj);
    // the part head's write_meta: its entry in the multipart namespace
    MpIndexAdd(obj);
    rc = PartUpdate(u, num, (prefix = prefix, etag = etag, past = default(set[int])));
    if (rc != OK) {
      // -ERR_NO_SUCH_UPLOAD: ~RadosWriter removes the part head, through
      // the index
      DeleteInline(written);
      MpIndexDel(obj);
      Answer(false);
      return;
    }
    Answer(true);
  }

  // RGWCompleteMultipart::execute
  fun CompleteMultipart(u: int, list: map[int, int]) {
    var m: (rc: tRc, ver: int);
    var lp: (rc: tRc, parts: map[int, tPart]);
    var h: tHead;
    var ver: int;
    var num: int;
    var pt: tPart;
    var done: set[int];
    var processed: map[int, set[int]];
    var hist: (chain: set[int], ixKeys: set[int], done: set[int]);
    var manifest: set[int];
    var removeKeys: set[int];
    var chain: set[int];
    var w: int;
    var i: int;
    var rc: tRc;
    var k: int;
    var mk: (rc: tRc, mark: int);

    // the lock keeps racing completions and aborts off the parts
    m = TryLock(u);
    if (m.rc == ENOENT) {
      // check_previously_completed: the head's ETag against the list's
      h = ReadHead(MPKEY());
      if (h.present && h.etag == MPETAG(list)) {
        if (cfg.replayAnswersEtag) {
          announce mCompleted, (rid = rid, etag = h.etag, want = MPETAG(list));
        } else {
          announce mCompleted, (rid = rid, etag = 0, want = MPETAG(list));
        }
        Answer(true);
        return;
      }
      Answer(false);
      return;
    }
    if (m.rc != OK) {
      Answer(false);  // "This multipart completion is already in progress"
      return;
    }
    m = GetAttrs(u);
    if (m.rc != OK) {
      Finish(u, false);
      return;
    }
    ver = m.ver;
    if (!IsLocked(u)) {
      Finish(u, false);  // lock renewal failed
      return;
    }
    if (cfg.completionMark) {
      mk = GetMark(u);
      if (mk.rc != OK) {
        Finish(u, false);
        return;
      }
      if (mk.mark != 0) {
        // an earlier completion recorded its tag. If the head carries
        // it, that completion took effect: only its meta object is left.
        // Otherwise its head was never written, or was replaced, and
        // either way the parts cannot be trusted
        h = ReadHead(MPKEY());
        if (!(h.present && h.tag == mk.mark)) {
          Finish(u, false);
          return;
        }
        lp = ListParts(u);
        if (lp.rc == OK) {
          foreach (num in keys(lp.parts)) {
            removeKeys += (OBJ(lp.parts[num].prefix, num));
          }
        }
        MetaDelete(u, VerCheck(ver), removeKeys);
        if (h.etag == MPETAG(list)) {
          announce mCompleted, (rid = rid, etag = h.etag, want = MPETAG(list));
          Finish(u, true);
          return;
        }
        Finish(u, false);
        return;
      }
    }

    // RadosMultipartUpload::complete: the parts must be the list's
    lp = ListParts(u);
    if (lp.rc != OK) {
      Finish(u, false);
      return;
    }
    if (sizeof(lp.parts) != sizeof(list)) {
      Finish(u, false);
      return;
    }
    foreach (num in keys(list)) {
      if (!(num in lp.parts) || lp.parts[num].etag != list[num]) {
        Finish(u, false);  // -ERR_INVALID_PART
        return;
      }
    }
    foreach (num in keys(lp.parts)) {
      pt = lp.parts[num];
      manifest += (OBJ(pt.prefix, num));
      removeKeys += (OBJ(pt.prefix, num));
      done = default(set[int]);
      done += (pt.prefix);
      hist = History(num, pt, done);
      SendGc(UPLOADTAG(u), hist.chain);
      foreach (k in hist.ixKeys) {
        removeKeys += (k);
      }
      processed[num] = hist.done;
    }
    if (cfg.completionMark) {
      // record this completion's tag, the ID tag its head will carry
      if (MetaMark(u, rid) != OK) {
        Finish(u, false);
        return;
      }
      if (cfg.completeMayCrash && $) {
        Crash();  // after the record, before the head write
        return;
      }
    }
    w = WriteMeta(MPKEY(), manifest, MPETAG(list), u, removeKeys, rid, false, sizeof(manifest));
    if (w == FAILED()) {
      // RadosMultipartUpload::complete returns the error: the meta
      // object stays, and complete() releases the lock
      Finish(u, false);
      return;
    }
    if (w == LOST() && cfg.loserGcsParts) {
      SendGc(UPLOADTAG(u), manifest);
    }
    if (cfg.completeMayCrash && $) {
      Crash();
      return;
    }

    // delete the meta object, which releases the lock. A part upload
    // that raced the completion makes that -ECANCELED: GC its part and
    // try again.
    removeKeys = default(set[int]);
    i = 0;
    while (i < 3) {
      if (cfg.metaDeleteMayFail && $) {
        rc = EIO;
      } else {
        rc = MetaDelete(u, VerCheck(ver), removeKeys);
      }
      if (rc != ECANCELED || i == 2) {
        break;  // any error is only logged
      }
      m = GetAttrs(u);
      if (m.rc != OK) {
        break;
      }
      ver = m.ver;
      // cleanup_orphaned_parts
      lp = ListParts(u);
      if (lp.rc == OK) {
        chain = default(set[int]);
        foreach (num in keys(lp.parts)) {
          pt = lp.parts[num];
          done = default(set[int]);
          if (num in processed) {
            done = processed[num];
          }
          if (!(pt.prefix in done)) {
            chain += (OBJ(pt.prefix, num));
            removeKeys += (OBJ(pt.prefix, num));
          }
          hist = History(num, pt, done);
          SendGc(UPLOADTAG(u), hist.chain);
          foreach (k in hist.ixKeys) {
            removeKeys += (k);
          }
          processed[num] = hist.done;
        }
        SendGc(UPLOADTAG(u), chain);
      }
      i = i + 1;
    }
    // the ETag set on the object's attrs by upload->complete()
    announce mCompleted, (rid = rid, etag = MPETAG(list), want = MPETAG(list));
    Finish(u, true);
  }

  // cleanup_part_history: a part's past prefixes go to GC and their
  // entries out of the index, skipping those processed already
  fun History(num: int, pt: tPart, done: set[int]): (chain: set[int], ixKeys: set[int], done: set[int]) {
    var r: (chain: set[int], ixKeys: set[int], done: set[int]);
    var pp: int;
    r.done = done;
    foreach (pp in pt.past) {
      if (!(pp in r.done) || !cfg.historySkipsProcessed) {
        r.done += (pp);
        r.ixKeys += (OBJ(pp, num));
        r.chain += (OBJ(pp, num));
      }
    }
    return r;
  }

  // RGWAbortMultipart::execute and the lifecycle's abort, around
  // RadosMultipartUpload::abort
  fun Abort(u: int, takeLock: bool) {
    var m: (rc: tRc, ver: int);
    var lp: (rc: tRc, parts: map[int, tPart]);
    var h: tHead;
    var num: int;
    var pt: tPart;
    var done: set[int];
    var processed: map[int, set[int]];
    var hist: (chain: set[int], ixKeys: set[int], done: set[int]);
    var removeKeys: set[int];
    var chain: set[int];
    var i: int;
    var rc: tRc;
    var k: int;
    var mk: (rc: tRc, mark: int);
    if (takeLock) {
      m = TryLock(u);
      if (m.rc != OK) {
        Answer(false);
        return;
      }
    }
    rc = ENOENT;
    i = 0;
    while (i < 3) {
      m = GetAttrs(u);
      if (m.rc != OK) {
        break;
      }
      lp = ListParts(u);
      if (lp.rc != OK) {
        break;
      }
      if (cfg.completionMark) {
        mk = GetMark(u);
        if (mk.rc == OK && mk.mark != 0) {
          h = ReadHead(MPKEY());
          if (h.present && h.tag == mk.mark) {
            // the upload was completed: its parts are the head's
            foreach (num in keys(lp.parts)) {
              removeKeys += (OBJ(lp.parts[num].prefix, num));
            }
            rc = MetaDelete(u, VerCheck(m.ver), removeKeys);
            break;
          }
        }
      }
      if (cfg.gcSparesHead) {
        h = ReadHead(MPKEY());
      }
      chain = default(set[int]);
      foreach (num in keys(lp.parts)) {
        pt = lp.parts[num];
        done = default(set[int]);
        if (num in processed) {
          done = processed[num];
        }
        if (!(pt.prefix in done)) {
          done += (pt.prefix);
          if (!(cfg.gcSparesHead && h.present && OBJ(pt.prefix, num) in h.manifest)) {
            chain += (OBJ(pt.prefix, num));
          }
          removeKeys += (OBJ(pt.prefix, num));
          hist = History(num, pt, done);
          SendGc(UPLOADTAG(u), hist.chain);
          foreach (k in hist.ixKeys) {
            removeKeys += (k);
          }
          done = hist.done;
        }
        processed[num] = done;
      }
      SendGc(UPLOADTAG(u), chain);
      rc = MetaDelete(u, VerCheck(m.ver), removeKeys);
      if (rc != ECANCELED) {
        break;
      }
      i = i + 1;
    }
    if (takeLock) {
      Unlock(u);
    }
    Answer(rc == OK);
  }

  fun VerCheck(ver: int): int {
    if (cfg.metaVersionCheck) {
      return ver;
    }
    return -1;
  }

  // RGWCompleteMultipart::complete: unlock if still held, and answer
  fun Finish(u: int, ok: bool) {
    Unlock(u);
    Answer(ok);
  }

  fun Answer(ok: bool) {
    send store, eFinished, rid;
    announce mAnswered, (rid = rid, ok = ok);
    send driver, eDone, (rid = rid, crashed = false);
  }

  // RGW dies: no answer, and the lock it holds expires
  fun Crash() {
    send store, eCrashed, rid;
    announce mCrashed, rid;
    send driver, eDone, (rid = rid, crashed = true);
  }

  // RADOS ops

  fun ReadHead(key: int): tHead {
    var h: tHead;
    send store, eReadHead, (from = this, key = key);
    receive {
      case eHeadRead: (x: tHead) { h = x; }
    }
    return h;
  }

  fun HeadWrite(key: int, guard: bool, expectTag: int, excl: bool, nh: tHead): (rc: tRc, epoch: int) {
    var r: (rc: tRc, epoch: int);
    send store, eHeadWrite, (from = this, key = key, guard = guard, expectTag = expectTag, excl = excl, head = nh);
    receive {
      case eHeadWritten: (x: (rc: tRc, epoch: int)) { r = x; }
    }
    return r;
  }

  fun HeadRemove(key: int, guard: bool, expectTag: int): (rc: tRc, epoch: int) {
    var r: (rc: tRc, epoch: int);
    send store, eHeadRemove, (from = this, key = key, guard = guard, expectTag = expectTag);
    receive {
      case eHeadWritten: (x: (rc: tRc, epoch: int)) { r = x; }
    }
    return r;
  }

  fun WriteData(objs: set[int]) {
    send store, eWriteData, (from = this, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun CreateExcl(obj: int): tRc {
    var r: tRc;
    send store, eCreateExcl, (from = this, obj = obj);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun DeleteInline(objs: set[int]) {
    send store, eDeleteInline, (from = this, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun SendGc(tag: int, objs: set[int]) {
    if (sizeof(objs) == 0) {
      return;
    }
    send store, eSendGc, (from = this, tag = tag, objs = objs);
    receive {
      case eDataDone: (rc: tRc) { }
    }
  }

  fun RefGet(obj: int, tag: int): tRc {
    var r: tRc;
    send store, eRefGet, (from = this, obj = obj, tag = tag);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun RefPut(obj: int, tag: int): tRc {
    var r: tRc;
    send store, eRefPut, (from = this, obj = obj, tag = tag);
    receive {
      case eDataDone: (rc: tRc) { r = rc; }
    }
    return r;
  }

  fun IndexPrepare(key: int) {
    IndexPrepareTag(key, rid);
  }

  fun IndexPrepareTag(key: int, tag: int) {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eIndexPrepare, (from = this, gen = gen, key = key, tag = tag);
      receive {
        case eIndexDone: (rc: tRc) { r = rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
  }

  // UpdateIndex::complete_remove_objs: a cancel without a tag
  fun IndexRemoveObjs(key: int, removeKeys: set[int]) {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eIndexComplete, (from = this, gen = gen, key = key, op = IX_CANCEL, tag = 0, pool = -1,
                                   epoch = 0, writer = rid, size = 0, removeKeys = removeKeys);
      receive {
        case eIndexDone: (rc: tRc) { r = rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
  }

  fun IndexComplete(key: int, op: tIxOp, pool: int, epoch: int, size: int, removeKeys: set[int]): tRc {
    return IndexCompleteAs(key, op, pool, epoch, rid, size, removeKeys, rid);
  }

  fun IndexCompleteAs(key: int, op: tIxOp, pool: int, epoch: int, writer: int, size: int,
                      removeKeys: set[int], tag: int): tRc {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eIndexComplete, (from = this, gen = gen, key = key, op = op, tag = tag, pool = pool,
                                   epoch = epoch, writer = writer, size = size, removeKeys = removeKeys);
      receive {
        case eIndexDone: (rc: tRc) { r = rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
    return r;
  }

  fun ListEntry(key: int): tIx {
    var e: tIx;
    send store, eListEntry, (from = this, key = key);
    receive {
      case eIxEntry: (x: tIx) { e = x; }
    }
    return e;
  }

  // cls_rgw_suggest_changes is sent and not retried
  fun Clock(): int {
    var c: int;
    send store, eClock, this;
    receive {
      case eClockIs: (n: int) { c = n; }
    }
    return c;
  }

  fun Suggest(key: int, remove: bool, h: tHead, iverSeen: int, readTick: int) {
    send store, eSuggest, (from = this, gen = gen, key = key, remove = remove, head = h, iverSeen = iverSeen,
                           readTick = readTick);
    receive {
      case eIndexDone: (rc: tRc) { }
    }
  }

  fun MpIndexAdd(key: int) {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eMpIndexAdd, (from = this, gen = gen, key = key);
      receive {
        case eIndexDone: (rc: tRc) { r = rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
  }

  fun MpIndexDel(key: int) {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eMpIndexDel, (from = this, gen = gen, key = key);
      receive {
        case eIndexDone: (rc: tRc) { r = rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
  }

  fun TryLock(u: int): (rc: tRc, ver: int) {
    var r: (rc: tRc, ver: int);
    send store, eTryLock, (from = this, upload = u, rid = rid);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x; }
    }
    return r;
  }

  fun Unlock(u: int) {
    send store, eUnlock, (from = this, upload = u, rid = rid);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { }
    }
  }

  fun IsLocked(u: int): bool {
    var r: bool;
    send store, eIsLocked, (from = this, upload = u, rid = rid);
    receive {
      case eLocked: (x: bool) { r = x; }
    }
    return r;
  }

  fun GetAttrs(u: int): (rc: tRc, ver: int) {
    var r: (rc: tRc, ver: int);
    send store, eGetAttrs, (from = this, upload = u);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x; }
    }
    return r;
  }

  fun ListParts(u: int): (rc: tRc, parts: map[int, tPart]) {
    var r: (rc: tRc, parts: map[int, tPart]);
    send store, eListParts, (from = this, upload = u);
    receive {
      case eParts: (x: (rc: tRc, parts: map[int, tPart])) { r = x; }
    }
    return r;
  }

  fun PartUpdate(u: int, num: int, part: tPart): tRc {
    var r: tRc;
    send store, ePartUpdate, (from = this, upload = u, num = num, part = part);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x.rc; }
    }
    return r;
  }

  fun MetaMark(u: int, mark: int): tRc {
    var r: tRc;
    send store, eMetaMark, (from = this, upload = u, mark = mark);
    receive {
      case eMetaRc: (x: (rc: tRc, ver: int)) { r = x.rc; }
    }
    return r;
  }

  fun GetMark(u: int): (rc: tRc, mark: int) {
    var r: (rc: tRc, mark: int);
    send store, eGetMark, (from = this, upload = u);
    receive {
      case eMark: (x: (rc: tRc, mark: int)) { r = x; }
    }
    return r;
  }

  fun MetaDelete(u: int, checkVer: int, removeKeys: set[int]): tRc {
    var r: tRc;
    r = EBUSY;
    while (r == EBUSY) {
      send store, eMetaDelete, (from = this, gen = gen, upload = u, checkVer = checkVer, removeKeys = removeKeys);
      receive {
        case eMetaRc: (x: (rc: tRc, ver: int)) { r = x.rc; }
      }
      if (r == EBUSY) {
        Rewait();
      }
    }
    return r;
  }
}
