/*
 * The RADOS state. Each handler is one atomic op.
 *
 * - Each key's head object: a replace guarded by cmpxattr on the ID tag
 *   that was read, or an exclusive create; a removal (cls_rgw_remove_obj),
 *   guarded or not. Every head write or removal takes the PG's next
 *   version; a removal that finds no head answers the PG's last version,
 *   the floor the OSD sets on -ENOENT.
 * - Each key's bucket index entry, kept as cls_rgw keeps it: prepare adds
 *   a pending tag; complete (rgw_bucket_complete_op) drops it and applies
 *   the op unless its epoch is not newer than the entry's. The entry of a
 *   part head or meta object lives in the multipart namespace, removed
 *   through remove_objs.
 * - The data objects that manifests name, with their cls_refcount
 *   references: none recorded means the implicit one. GC queues chains
 *   under a tag, and later drops that tag's reference on each object
 *   (cls_refcount_put, implicit_ref); an object with none left is deleted.
 *   GC may run a queued chain before any op, in any order; at the end it
 *   runs every chain.
 * - Each upload's meta object: its parts
 *   (cls_rgw_mp_upload_part_info_update, which bumps the cls_version),
 *   and the completion lock, which lapses once its holder is dead.
 * - The index header's stats, adjusted as cls_rgw adjusts them, and the
 *   listing's repair: rgw_dir_suggest_changes drops expired pending ops,
 *   and applies a suggestion only when none are left and no op has
 *   completed on the entry since the listing read it. At the end, a
 *   listing repairs whatever is still pending.
 * - Resharding (RGWBucketReshard::do_reshard with logrecord): in
 *   logrecord, index ops apply to the source and log the entries they
 *   touch; the inventory copies each source entry to the target; in
 *   progress, index ops answer -ERR_BUSY_RESHARDING; the incremental pass
 *   copies the logged entries again, taking a re-copied entry's old stats
 *   out first (check_existing); the commit makes the target current.
 *   After it, the old shards still answer -ERR_BUSY_RESHARDING, and an
 *   RGW that sees that waits for the commit and retries on the new
 *   generation.
 */
machine Store {
  var cfg: tCfg;
  var heads: map[int, tHead];
  var epoch: int;
  var ixs: map[int, tIx];
  var mpIndex: set[int];
  var live: set[int];
  var gone: set[int];
  var refs: map[int, set[int]];
  var retired: map[int, set[int]];
  var gcTags: seq[int];
  var gcObjs: seq[set[int]];
  var metas: set[int];
  var metaVer: map[int, int];
  var metaMark: map[int, int];
  var parts: map[int, map[int, tPart]];
  var lockOwner: map[int, int];
  var dead: set[int];
  var finished: set[int];
  // requests answered (or dead) so far, and when each was
  var clock: int;
  var finishedAt: map[int, int];
  var hver: int;
  var statCount: int;
  var statSize: int;
  var gen: int;
  var rstate: int;          // 0 none, 1 logrecord, 2 in progress
  var rlog: set[int];
  var tix: map[int, tIx];
  var tmp: set[int];
  var tCount: int;
  var tSize: int;
  var waiters: seq[machine];

  start state Serve {
    entry (p: (cfg: tCfg, objects: set[int], twins: bool, uploads: set[int])) {
      var k: int;
      var u: int;
      var num: int;
      var h: tHead;
      var ps: map[int, tPart];
      cfg = p.cfg;
      k = 1;
      while (k <= 2) {
        heads[k] = default(tHead);
        ixs[k] = default(tIx);
        k = k + 1;
      }
      // an object at each of these keys, PUT earlier
      foreach (k in p.objects) {
        epoch = epoch + 1;
        h = (present = true, tag = OLDWRITER(k), tailTag = OLDWRITER(k), manifest = default(set[int]),
             writer = OLDWRITER(k), etag = OLDWRITER(k), upload = 0, size = 1, ver = epoch);
        if (p.twins) {
          h.etag = OLDWRITER(1);  // the keys hold the same bytes
        }
        h.manifest += (OLDTAIL(k));
        heads[k] = h;
        live += (OLDTAIL(k));
        ixs[k] = (present = true, listed = true, writer = OLDWRITER(k), size = 1, pool = 1, epoch = epoch,
                  pending = default(set[int]), iver = 0);
        Account(1);
        announce mHead, (key = k, writer = h.writer, manifest = h.manifest);
        announce mHeadState, (key = k, by = 0, present = true, etag = h.etag);
      }
      // uploads to key 1 with parts 1 and 2 uploaded once, at the base prefix
      foreach (u in p.uploads) {
        ps = default(map[int, tPart]);
        num = 1;
        while (num <= 2) {
          ps[num] = (prefix = u, etag = PARTETAG(u, num), past = default(set[int]));
          live += (OBJ(u, num));
          mpIndex += (OBJ(u, num));
          Account(MPSIZE(OBJ(u, num)));
          num = num + 1;
        }
        parts[u] = ps;
        metas += (u);
        metaVer[u] = 1;
        lockOwner[u] = 0;
        mpIndex += (METAKEY(u));
        Account(MPSIZE(METAKEY(u)));
      }
    }

    on eReadHead do (p: (from: machine, key: int)) {
      MaybeGc();
      send p.from, eHeadRead, heads[p.key];
    }

    on eHeadWrite do (w: (from: machine, key: int, guard: bool, expectTag: int, excl: bool, head: tHead)) {
      var h: tHead;
      MaybeGc();
      h = heads[w.key];
      if (w.guard && !h.present) {
        send w.from, eHeadWritten, (rc = ENOENT, epoch = 0);
        return;
      }
      if (w.guard && h.tag != w.expectTag) {
        send w.from, eHeadWritten, (rc = ECANCELED, epoch = 0);
        return;
      }
      if (w.excl && h.present) {
        send w.from, eHeadWritten, (rc = EEXIST, epoch = 0);
        return;
      }
      epoch = epoch + 1;
      h = w.head;
      h.ver = epoch;
      heads[w.key] = h;
      announce mHead, (key = w.key, writer = h.writer, manifest = h.manifest);
      announce mHeadState, (key = w.key, by = h.writer, present = true, etag = h.etag);
      send w.from, eHeadWritten, (rc = OK, epoch = epoch);
    }

    on eHeadRemove do (w: (from: machine, key: int, guard: bool, expectTag: int, rid: int)) {
      var h: tHead;
      MaybeGc();
      h = heads[w.key];
      if (!h.present) {
        send w.from, eHeadWritten, (rc = ENOENT, epoch = epoch);
        return;
      }
      if (w.guard && h.tag != w.expectTag) {
        send w.from, eHeadWritten, (rc = ECANCELED, epoch = 0);
        return;
      }
      heads[w.key] = default(tHead);
      epoch = epoch + 1;
      announce mHead, (key = w.key, writer = 0, manifest = default(set[int]));
      announce mHeadState, (key = w.key, by = w.rid, present = false, etag = 0);
      send w.from, eHeadWritten, (rc = OK, epoch = epoch);
    }

    // dedup's guarded setxattr: the ID tag is left as it is
    on eHeadRewrite do (p: (from: machine, key: int, etag: int, tailTag: int, setManifest: bool,
                            manifest: set[int])) {
      var h: tHead;
      MaybeGc();
      h = heads[p.key];
      if (!h.present) {
        send p.from, eDataDone, ENOENT;
        return;
      }
      if (h.etag != p.etag || h.tailTag != p.tailTag) {
        send p.from, eDataDone, ECANCELED;
        return;
      }
      epoch = epoch + 1;
      h.ver = epoch;
      if (p.setManifest) {
        h.manifest = p.manifest;
      }
      heads[p.key] = h;
      if (p.setManifest) {
        announce mHead, (key = p.key, writer = h.writer, manifest = h.manifest);
      }
      send p.from, eDataDone, OK;
    }

    on eWriteData do (p: (from: machine, objs: set[int])) {
      var o: int;
      MaybeGc();
      foreach (o in p.objs) {
        live += (o);
      }
      send p.from, eDataDone, OK;
    }

    on eCreateExcl do (p: (from: machine, obj: int)) {
      MaybeGc();
      if (p.obj in live) {
        send p.from, eDataDone, EEXIST;
        return;
      }
      live += (p.obj);
      if (p.obj in gone) {
        gone -= (p.obj);
        announce mCreated, p.obj;
      }
      send p.from, eDataDone, OK;
    }

    on eDeleteInline do (p: (from: machine, objs: set[int])) {
      var o: int;
      MaybeGc();
      foreach (o in p.objs) {
        Remove(o);
      }
      send p.from, eDataDone, OK;
    }

    on eSendGc do (p: (from: machine, tag: int, objs: set[int])) {
      MaybeGc();
      gcTags += (sizeof(gcTags), p.tag);
      gcObjs += (sizeof(gcObjs), p.objs);
      send p.from, eDataDone, OK;
    }

    // cls_refcount_get with implicit_ref: fails on a missing object
    on eRefGet do (p: (from: machine, obj: int, tag: int)) {
      var r: set[int];
      MaybeGc();
      if (!(p.obj in live)) {
        send p.from, eDataDone, ENOENT;
        return;
      }
      r = RefsOf(p.obj);
      r += (p.tag);
      refs[p.obj] = r;
      send p.from, eDataDone, OK;
    }

    on eRefPut do (p: (from: machine, obj: int, tag: int)) {
      var rc: tRc;
      MaybeGc();
      rc = Put(p.obj, p.tag);
      send p.from, eDataDone, rc;
    }

    // rgw_bucket_prepare_op
    on eIndexPrepare do (p: (from: machine, gen: int, key: int, tag: int)) {
      var e: tIx;
      MaybeGc();
      if (!GenOk(p.gen)) {
        send p.from, eIndexDone, Busy(p.gen);
        return;
      }
      Touch(p.key);
      e = ixs[p.key];
      if (!e.present) {
        e = (present = true, listed = false, writer = 0, size = 0, pool = -1, epoch = 0, pending = default(set[int]),
             iver = hver);
      }
      e.pending += (p.tag);
      ixs[p.key] = e;
      send p.from, eIndexDone, OK;
    }

    // rgw_bucket_complete_op
    on eIndexComplete do (c: (from: machine, gen: int, key: int, op: tIxOp, tag: int, pool: int, epoch: int,
                              writer: int, size: int, removeKeys: set[int])) {
      var e: tIx;
      var op: tIxOp;
      MaybeGc();
      if (!GenOk(c.gen)) {
        send c.from, eIndexDone, Busy(c.gen);
        return;
      }
      // a cancel without a tag touches no entry and no pending op: it
      // only removes the remove_objs entries
      if (c.tag == 0) {
        RemoveObjs(c.writer, c.removeKeys);
        send c.from, eIndexDone, OK;
        return;
      }
      Touch(c.key);
      e = ixs[c.key];
      if (!e.present || !(c.tag in e.pending)) {
        send c.from, eIndexDone, EINVAL;
        return;
      }
      e.pending -= (c.tag);
      op = c.op;
      if (op != IX_CANCEL && c.pool == e.pool && c.epoch != 0 && c.epoch <= e.epoch) {
        op = IX_CANCEL;  // "skipping request, old epoch"
      }
      // entry.ver = op.ver - on main for a cancel too, whose ver is
      // {-1, 0}, or the stale op's
      if (op != IX_CANCEL || !cfg.cancelKeepsVer) {
        e.pool = c.pool;
        e.epoch = c.epoch;
      }
      if (op == IX_CANCEL) {
        if (!e.listed && sizeof(e.pending) == 0) {
          e.present = false;
        }
      } else if (op == IX_DEL) {
        if (e.listed) {
          Unaccount(e.size);
        }
        if (sizeof(e.pending) == 0) {
          e.present = false;
        } else {
          e.listed = false;
        }
      } else {
        if (e.listed) {
          Unaccount(e.size);
        }
        Account(c.size);
        e.listed = true;
        e.writer = c.writer;
        e.size = c.size;
      }
      hver = hver + 1;
      e.iver = hver;
      ixs[c.key] = e;
      if (op != IX_CANCEL || cfg.cancelRemovesObjs) {
        RemoveObjs(c.writer, c.removeKeys);
      }
      send c.from, eIndexDone, OK;
    }

    on eMpIndexAdd do (p: (from: machine, gen: int, key: int)) {
      MaybeGc();
      if (!GenOk(p.gen)) {
        send p.from, eIndexDone, Busy(p.gen);
        return;
      }
      if (!(p.key in mpIndex)) {
        Touch(p.key);
        mpIndex += (p.key);
        Account(MPSIZE(p.key));
      }
      send p.from, eIndexDone, OK;
    }

    on eMpIndexDel do (p: (from: machine, gen: int, key: int)) {
      MaybeGc();
      if (!GenOk(p.gen)) {
        send p.from, eIndexDone, Busy(p.gen);
        return;
      }
      MpRemove(p.key);
      send p.from, eIndexDone, OK;
    }

    on eGetLayout do (from: machine) {
      send from, eLayout, gen;
    }

    // RGWRados::block_while_resharding: wait out a reshard in progress
    on eWaitLayout do (from: machine) {
      if (rstate == 2) {
        waiters += (sizeof(waiters), from);
        return;
      }
      send from, eLayout, gen;
    }

    on eReshardStart do (from: machine) {
      MaybeGc();
      rstate = 1;
      rlog = default(set[int]);
      tix = default(map[int, tIx]);
      tmp = default(set[int]);
      tCount = 0;
      tSize = 0;
      send from, eIndexDone, OK;
    }

    // bi_list of the source shards
    on eReshardList do (from: machine) {
      MaybeGc();
      send from, eReshardEntries, (ix = ixs, mp = mpIndex);
    }

    // the inventory's bi_put and stats update on the target
    on eReshardPut do (p: (from: machine, isMain: bool, id: int, e: tIx)) {
      MaybeGc();
      if (p.isMain) {
        tix[p.id] = p.e;
        if (p.e.present && p.e.listed) {
          tCount = tCount + 1;
          tSize = tSize + p.e.size;
        }
      } else if (!(p.id in tmp)) {
        tmp += (p.id);
        tCount = tCount + 1;
        tSize = tSize + MPSIZE(p.id);
      }
      send p.from, eIndexDone, OK;
    }

    on eReshardBlock do (from: machine) {
      MaybeGc();
      rstate = 2;
      send from, eIndexDone, OK;
    }

    // the incremental pass: each logged entry, as it is now, over the target
    on eReshardInc do (from: machine) {
      var id: int;
      var t: tIx;
      MaybeGc();
      foreach (id in rlog) {
        if (id <= 2) {
          if (id in tix) {
            t = tix[id];
            if (cfg.reshardCheckExisting && t.present && t.listed) {
              tCount = tCount - 1;
              tSize = tSize - t.size;
            }
          }
          tix[id] = ixs[id];
          if (ixs[id].present && ixs[id].listed) {
            tCount = tCount + 1;
            tSize = tSize + ixs[id].size;
          }
        } else {
          if (id in tmp) {
            if (cfg.reshardCheckExisting) {
              tCount = tCount - 1;
              tSize = tSize - MPSIZE(id);
            }
            tmp -= (id);
          }
          if (id in mpIndex) {
            tmp += (id);
            tCount = tCount + 1;
            tSize = tSize + MPSIZE(id);
          }
        }
      }
      send from, eIndexDone, OK;
    }

    on eReshardCommit do (from: machine) {
      var k: int;
      var w: machine;
      MaybeGc();
      k = 1;
      while (k <= 2) {
        if (!(k in tix)) {
          tix[k] = default(tIx);
        }
        k = k + 1;
      }
      ixs = tix;
      mpIndex = tmp;
      statCount = tCount;
      statSize = tSize;
      gen = gen + 1;
      rstate = 0;
      foreach (w in waiters) {
        send w, eLayout, gen;
      }
      waiters = default(seq[machine]);
      send from, eIndexDone, OK;
    }

    on eListEntry do (p: (from: machine, key: int)) {
      MaybeGc();
      send p.from, eIxEntry, ixs[p.key];
    }

    // rgw_dir_suggest_changes
    on eSuggest do (p: (from: machine, gen: int, key: int, remove: bool, head: tHead, iverSeen: int,
                        readTick: int)) {
      var e: tIx;
      var pend: set[int];
      var t: int;
      MaybeGc();
      if (!GenOk(p.gen)) {
        send p.from, eIndexDone, Busy(p.gen);
        return;
      }
      e = ixs[p.key];
      if (e.present) {
        // pending ops whose tag timeout has expired are dropped
        foreach (t in e.pending) {
          if (!Expired(t, p.readTick)) {
            pend += (t);
          }
        }
        // an op completed since the listing read the entry: skip
        if (p.iverSeen >= e.iver && sizeof(pend) == 0) {
          ApplySuggestion(p.key, p.remove, p.head);
        }
      }
      send p.from, eIndexDone, OK;
    }

    on eFinished do (rid: int) {
      finished += (rid);
      clock = clock + 1;
      finishedAt[rid] = clock;
    }

    on eClock do (from: machine) {
      send from, eClockIs, clock;
    }

    on eTryLock do (p: (from: machine, upload: int, rid: int)) {
      var owner: int;
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      owner = lockOwner[p.upload];
      if (owner != 0 && owner != p.rid && !(owner in dead) && (cfg.lockHeld || $)) {
        send p.from, eMetaRc, (rc = EBUSY, ver = 0);
        return;
      }
      lockOwner[p.upload] = p.rid;
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eUnlock do (p: (from: machine, upload: int, rid: int)) {
      MaybeGc();
      if (p.upload in metas && lockOwner[p.upload] == p.rid) {
        lockOwner[p.upload] = 0;
      }
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eIsLocked do (p: (from: machine, upload: int, rid: int)) {
      MaybeGc();
      send p.from, eLocked, (p.upload in metas && lockOwner[p.upload] == p.rid);
    }

    on eMetaMark do (p: (from: machine, upload: int, mark: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      metaMark[p.upload] = p.mark;
      send p.from, eMetaRc, (rc = OK, ver = metaVer[p.upload]);
    }

    on eGetMark do (p: (from: machine, upload: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMark, (rc = ENOENT, mark = 0);
        return;
      }
      if (p.upload in metaMark) {
        send p.from, eMark, (rc = OK, mark = metaMark[p.upload]);
        return;
      }
      send p.from, eMark, (rc = OK, mark = 0);
    }

    on eGetAttrs do (p: (from: machine, upload: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      send p.from, eMetaRc, (rc = OK, ver = metaVer[p.upload]);
    }

    on eListParts do (p: (from: machine, upload: int)) {
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eParts, (rc = ENOENT, parts = default(map[int, tPart]));
        return;
      }
      send p.from, eParts, (rc = OK, parts = parts[p.upload]);
    }

    // assert_exists + cls_rgw_mp_upload_part_info_update + cls_version_inc
    on ePartUpdate do (p: (from: machine, upload: int, num: int, part: tPart)) {
      var info: tPart;
      var stored: tPart;
      var ps: map[int, tPart];
      var x: int;
      MaybeGc();
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      info = p.part;
      ps = parts[p.upload];
      if (p.num in ps) {
        // carry the stored part's prefixes forward
        stored = ps[p.num];
        info.past += (stored.prefix);
        foreach (x in stored.past) {
          info.past += (x);
        }
      }
      if (info.prefix in info.past) {
        send p.from, eMetaRc, (rc = EEXIST, ver = 0);
        return;
      }
      ps[p.num] = info;
      parts[p.upload] = ps;
      metaVer[p.upload] = metaVer[p.upload] + 1;
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    // the meta object's delete_obj, with cls_version_check if checkVer >= 0
    on eMetaDelete do (p: (from: machine, gen: int, upload: int, checkVer: int, removeKeys: set[int])) {
      var k: int;
      MaybeGc();
      if (!GenOk(p.gen)) {
        send p.from, eMetaRc, (rc = Busy(p.gen), ver = 0);
        return;
      }
      if (!(p.upload in metas)) {
        send p.from, eMetaRc, (rc = ENOENT, ver = 0);
        return;
      }
      if (p.checkVer >= 0 && metaVer[p.upload] != p.checkVer) {
        // the index transaction is canceled; the cancel applies remove_objs
        if (cfg.cancelRemovesObjs) {
          foreach (k in p.removeKeys) {
            MpRemove(k);
          }
        }
        send p.from, eMetaRc, (rc = ECANCELED, ver = 0);
        return;
      }
      metas -= (p.upload);
      if (p.upload in metaMark) {
        metaMark -= (p.upload);
      }
      lockOwner[p.upload] = 0;
      MpRemove(METAKEY(p.upload));
      foreach (k in p.removeKeys) {
        MpRemove(k);
      }
      send p.from, eMetaRc, (rc = OK, ver = 0);
    }

    on eCrashed do (rid: int) {
      MaybeGc();
      dead += (rid);
      clock = clock + 1;
      finishedAt[rid] = clock;
    }

    // radosgw-admin gc process --include-all, then the specs look
    on eQuiesce do (from: machine) {
      var k: int;
      var u: int;
      var num: int;
      var pp: int;
      var pt: tPart;
      var o: int;
      var referenced: set[int];
      var validKeys: set[int];
      while (sizeof(gcTags) > 0) {
        GcEntry(0);
      }
      // a listing: every request has finished, so every pending op left
      // has expired, and check_disk_state repairs each entry that needs it
      foreach (k in keys(ixs)) {
        if (ixs[k].present && (!ixs[k].listed || sizeof(ixs[k].pending) > 0)) {
          if (heads[k].present && heads[k].upload != 0) {
            foreach (o in heads[k].manifest) {
              MpRemove(o);
            }
          }
          ApplySuggestion(k, !heads[k].present, heads[k]);
        }
      }
      foreach (k in keys(heads)) {
        if (heads[k].present) {
          foreach (o in heads[k].manifest) {
            referenced += (o);
          }
        }
      }
      // a live upload's parts, current and past, and their index entries
      foreach (u in metas) {
        validKeys += (METAKEY(u));
        foreach (num in keys(parts[u])) {
          pt = parts[u][num];
          referenced += (OBJ(pt.prefix, num));
          validKeys += (OBJ(pt.prefix, num));
          foreach (pp in pt.past) {
            referenced += (OBJ(pp, num));
            validKeys += (OBJ(pp, num));
          }
        }
      }
      announce mFinal, (heads = heads, ixs = ixs, live = live, referenced = referenced,
                        mpIndex = mpIndex, validKeys = validKeys, statCount = statCount, statSize = statSize);
      send from, eQuiesced;
    }
  }

  fun Account(size: int) {
    statCount = statCount + 1;
    statSize = statSize + size;
  }

  fun Unaccount(size: int) {
    statCount = statCount - 1;
    statSize = statSize - size;
  }

  // an index op may go ahead: its generation is current, and the
  // current shards are not blocked by a reshard in progress. On an old
  // generation, the op lands on the old shards, which are blocked unless
  // oldShardsBlocked is off.
  fun GenOk(g: int): bool {
    if (g == gen) {
      return rstate != 2;
    }
    return false;
  }

  // what an op refused by GenOk answers: an op on the old shards that no
  // longer block is applied there, where nobody reads it (OK, no effect)
  fun Busy(g: int): tRc {
    if (g != gen && !cfg.oldShardsBlocked) {
      return OK;
    }
    return EBUSY;
  }

  // in logrecord, an index write logs the entry it touches
  fun Touch(id: int) {
    if (rstate == 1 && cfg.reshardLogs) {
      rlog += (id);
    }
  }

  // an index op's remove_objs (complete_remove_obj), for request by
  fun RemoveObjs(by: int, removeKeys: set[int]) {
    var k: int;
    var n: int;
    foreach (k in removeKeys) {
      if (k in mpIndex) {
        n = n + 1;
      }
      MpRemove(k);
    }
    if (n > 0) {
      announce mIxRemoved, (by = by, n = n);
    }
  }

  // a multipart-namespace entry removed (complete_remove_obj, or a DEL)
  fun MpRemove(k: int) {
    if (k in mpIndex) {
      Touch(k);
      mpIndex -= (k);
      Unaccount(MPSIZE(k));
    }
  }

  // a pending op's tag timeout has run out, as a listing that read the
  // head at readTick sees it. A listing sends its repair right after its
  // read, so only the op of a request that was answered (or died) before
  // that read can have expired by then, unless requests may stall past
  // the timeout
  fun Expired(t: int, readTick: int): bool {
    var r: int;
    r = t;
    if (t >= RELINK()) {
      r = t - RELINK();
    }
    if ((r in finished || r in dead) && finishedAt[r] <= readTick) {
      return $;
    }
    if (t >= RELINK()) {
      return false;  // a re-link is prompt: prepare, read, complete
    }
    return !cfg.writersPrompt && $;
  }

  // CEPH_RGW_UPDATE from the head, or CEPH_RGW_REMOVE
  fun ApplySuggestion(k: int, remove: bool, h: tHead) {
    var e: tIx;
    Touch(k);
    e = ixs[k];
    if (e.listed) {
      Unaccount(e.size);
    }
    if (remove) {
      e.present = false;
      e.listed = false;
      e.pending = default(set[int]);
    } else {
      Account(h.size);
      hver = hver + 1;
      e = (present = true, listed = true, writer = h.writer, size = h.size, pool = 1, epoch = h.ver,
           pending = default(set[int]), iver = hver);
    }
    ixs[k] = e;
  }

  // an object's cls_refcount references: the implicit one if none recorded
  fun RefsOf(o: int): set[int] {
    var r: set[int];
    if (o in refs) {
      return refs[o];
    }
    r += (WILD());
    return r;
  }

  // cls_refcount_put with implicit_ref
  fun Put(o: int, tag: int): tRc {
    var r: set[int];
    var ret: set[int];
    var found: int;
    if (!(o in live)) {
      return ENOENT;
    }
    r = RefsOf(o);
    if (o in retired) {
      ret = retired[o];
    }
    if (sizeof(r) == 0) {
      return EINVAL;
    }
    if (tag in r) {
      found = tag;
    } else if (WILD() in r) {
      found = WILD();
    } else {
      return OK;
    }
    if (tag in ret) {
      return OK;
    }
    ret += (tag);
    r -= (found);
    if (sizeof(r) == 0) {
      Remove(o);
      return OK;
    }
    refs[o] = r;
    retired[o] = ret;
    return OK;
  }

  fun Remove(o: int) {
    if (!(o in live)) {
      return;
    }
    live -= (o);
    if (o in refs) {
      refs -= (o);
    }
    if (o in retired) {
      retired -= (o);
    }
    gone += (o);
    announce mDeleted, o;
  }

  // RGWGC::process for one queued chain
  fun GcEntry(i: int) {
    var tag: int;
    var objs: set[int];
    var o: int;
    var rc: tRc;
    tag = gcTags[i];
    objs = gcObjs[i];
    gcTags -= (i);
    gcObjs -= (i);
    foreach (o in objs) {
      rc = Put(o, tag);
    }
  }

  // GC may run queued chains, in any order, before any op
  fun MaybeGc() {
    while (sizeof(gcTags) > 0 && $) {
      GcEntry(choose(sizeof(gcTags)));
    }
  }
}
