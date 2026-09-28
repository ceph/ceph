// the code on main
fun Main(): tCfg {
  return (idTagGuard = true, cancelRemovesObjs = true, metaVersionCheck = true,
          historySkipsProcessed = true, abortTakesLock = true, replayAnswersEtag = true,
          copyTakesRefs = true, reshardLogs = true, reshardCheckExisting = true, oldShardsBlocked = true,
          cancelKeepsVer = false, lcTakesLock = false, loserGcsParts = false, gcSparesHead = false,
          deleteGuard = false, copyLoserDropsRefs = false, copySelfGuardsSource = false, copySelfRetries = false,
          ixFailKeepsWrite = false, lateCompleteRelinks = false, completionMark = false,
          condLossFails = false, refusedKeepsParts = false, condDelNoKey = false, historyAfterHead = false,
          markVersioned = MV_SKIP(), srcShards = 1, srcOrdered = false, dstShards = 1,
          dstOrdered = false, reshardByIndexName = false, versioning = VER_OFF(), completeMayCrash = false, metaDeleteMayFail = false, ixCompleteMayFail = false,
          lockHeld = true, writersPrompt = true);
}

fun Req(kind: tKind, key: int, src: int, upload: int): tSpec {
  return (kind = kind, key = key, src = src, upload = upload, num = 0, etag = 0, list = default(map[int, int]),
          cond = default(tCond));
}
fun Put(key: int): tSpec { return Req(R_PUT, key, 0, 0); }
fun Del(key: int): tSpec { return Req(R_DELETE, key, 0, 0); }
fun Copy(src: int, dst: int): tSpec { return Req(R_COPY, dst, src, 0); }
fun SetTags(key: int): tSpec { return Req(R_SET_TAGS, key, 0, 0); }
fun List(): tSpec { return Req(R_LIST, 0, 0, 0); }
fun Dedup(src: int, tgt: int): tSpec { return Req(R_DEDUP, tgt, src, 0); }
fun Reshard(): tSpec { return Req(R_RESHARD, 0, 0, 0); }
fun AbortMpu(u: int): tSpec { return Req(R_ABORT, MPKEY(), 0, u); }
fun LcAbortMpu(u: int): tSpec { return Req(R_LC_ABORT, MPKEY(), 0, u); }
// complete upload u with the ETags its parts were first uploaded with
fun Complete(u: int): tSpec {
  var l: map[int, int];
  l[1] = PARTETAG(u, 1);
  l[2] = PARTETAG(u, 2);
  return (kind = R_COMPLETE, key = MPKEY(), src = 0, upload = u, num = 0, etag = 0, list = l,
          cond = default(tCond));
}
// complete upload u with part 1's ETag e1 and part 2's e2
fun CompleteList(u: int, e1: int, e2: int): tSpec {
  var r: tSpec;
  r = Complete(u);
  r.list[1] = e1;
  r.list[2] = e2;
  return r;
}
fun Reupload(u: int, num: int, etag: int): tSpec {
  return (kind = R_UPLOAD_PART, key = MPKEY(), src = 0, upload = u, num = num, etag = etag,
          list = default(map[int, int]), cond = default(tCond));
}
// conditions, and requests that carry one
fun IfMatch(etag: int): tCond { return (kind = C_IF_MATCH, etag = etag); }
fun IfMatchAny(): tCond { return (kind = C_IF_MATCH_ANY, etag = 0); }
fun IfNoneMatchAny(): tCond { return (kind = C_IF_NONE_MATCH_ANY, etag = 0); }
fun With(r: tSpec, c: tCond): tSpec {
  r.cond = c;
  return r;
}
fun One(a: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  return s;
}
fun Two(a: tSpec, b: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  s += (1, b);
  return s;
}
fun Three(a: tSpec, b: tSpec, c: tSpec): seq[tSpec] {
  var s: seq[tSpec];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  return s;
}

enum tScenario {
  SC_PUTS,              // three PutObjects over an existing object
  SC_PUT_VS_COMPLETE,   // a PutObject and a completion over it
  SC_COMPLETES,         // two uploads' completions over it
  SC_SAME_COMPLETES,    // three concurrent completions of one upload
  SC_REUPLOAD,          // a completion, and a re-upload of part 1 (same or other bytes)
  SC_ABORT,             // a completion and an AbortMultipartUpload
  SC_LC_ABORT,          // a completion and lifecycle's abort of the upload
  SC_RETRY,             // a completion, then the client retries it
  SC_THEN_ABORT,        // a completion, then an abort of the upload
  SC_PUT_THEN_RETRY,    // a completion, then a PutObject, then a retry of the completion
  SC_DEL_VS_PUT,        // a DeleteObject and a PutObject on the key
  SC_DELS_AND_PUT,      // two DeleteObjects and a PutObject on the key
  SC_DEL_VS_COMPLETE,   // a DeleteObject and a completion on the key
  SC_COPY_VS_PUT_SRC,   // a copy of key 1 to key 2, and a PutObject over key 1
  SC_COPY_VS_DEL_SRC,   // a copy of key 1 to key 2, and a DeleteObject of key 1
  SC_COPY_VS_PUT_DST,   // a copy of key 1 to key 2 and a PutObject over key 2; then key 1 deleted
  SC_COPY_THEN_DELETES, // a copy of key 1 to key 2; then both keys deleted at once
  SC_COPY_SELF_VS_PUT,  // a copy of key 1 onto itself, and a PutObject over key 1
  SC_COPY_MPU,          // a completion; a copy to key 2 and a PutObject over key 1; key 2 deleted
  SC_CRASH_COPY_RETRY,  // a completion; a copy to key 2; a retry of the completion; key 2 deleted
  SC_PUT_ONE,           // one PutObject over an existing object
  SC_COPY_ONE,          // a copy of key 1 to key 2; then key 1 deleted
  SC_LIST_VS_PUT,       // a PutObject and a bucket listing
  SC_LIST_VS_DEL,       // a DeleteObject and a bucket listing
  SC_LIST_VS_COMPLETE,  // a completion and a bucket listing
  SC_DEDUP_THEN_DELETES, // keys 1 and 2 hold the same bytes: dedup of 2 onto 1; then both deleted
  SC_DEDUP_VS_PUT_TGT,  // dedup of 2 onto 1 and a PutObject over key 2; then key 1 deleted
  SC_DEDUP_VS_DEL_TGT,  // dedup of 2 onto 1 and a DeleteObject of key 2; then key 1 deleted
  SC_DEDUP_VS_PUT_SRC,  // dedup of 2 onto 1 and a PutObject over key 1; then key 2 deleted
  SC_DEDUP_VS_COPY_SELF, // dedup of 2 onto 1 and a copy of key 2 onto itself; then key 1 deleted
  SC_RESHARD_VS_PUTS,   // a reshard, and two PutObjects over key 1
  SC_RESHARD_VS_DEL,    // a reshard, a DeleteObject and a PutObject on key 1
  SC_RESHARD_VS_MPU,    // a reshard, a completion, and a re-upload of part 1
  SC_LIST_PUT_DEL,      // a PutObject, a DeleteObject and a bucket listing
  SC_LIST_NEW_PUT,      // key 1 deleted; then a PutObject of a new object and a bucket listing
  // conditional requests. The ETag named is that of key 1's object
  SC_CREATES,           // key 1 empty: two PutObjects with If-None-Match: *
  SC_CREATE_VS_COMPLETE, // key 1 empty: a PutObject and a completion, both with If-None-Match: *
  SC_IF_MATCH_VS_PUT,   // a PutObject with If-Match, and a PutObject
  SC_MATCH_ANY_VS_MATCH, // a PutObject with If-Match: *, and one with If-Match
  SC_COND_DEL_VS_PUT,   // a DeleteObject with If-Match, and a PutObject
  SC_COND_DEL_VS_MATCH, // a DeleteObject and a PutObject, both with If-Match
  SC_COND_COMPLETE_VS_PUT, // a completion with If-Match, and a PutObject
  SC_COND_DELS,         // two DeleteObjects with If-Match on the same ETag
  // an SDK retry of part 1; a completion refused for part 2's ETag, after
  // part 1's history; then part 1 uploaded again
  SC_INVALID_THEN_REUPLOAD,
  SC_DEL_AFTER_RETRY,   // a completion (request 2), then a retry of it; then request 2's version deleted
  SC_PUT_THEN_ABORT,    // a completion, then a PutObject, then an abort of the upload
  SC_RESHARD_THEN_COMPLETE, // a reshard; then a completion of upload 1
  SC_RESHARD_THEN_ABORT,    // a reshard; then an abort of upload 1
  SC_COPY_SELF_VS_TAG,      // a copy of key 1 onto itself, and a tagging update of key 1
  SC_TAG_VS_PUT,            // a tagging update of key 1, and a PutObject over it
  SC_TAG_THEN_COPY_SELF     // a tagging update of key 1; then a copy of key 1 onto itself
}

// Key 1 starts with an object, except in the If-None-Match scenarios, and
// key 2 too in the copy scenarios. Uploads 1 and 2 (to key 1) have parts 1
// and 2 uploaded.
machine Scenario {
  start state Init {
    entry (p: (cfg: tCfg, sc: tScenario)) {
      var script: seq[seq[tSpec]];
      var objects: set[int];
      var uploads: set[int];
      var etag: int;
      var twins: bool;
      objects += (1);
      if (p.sc == SC_PUTS) {
        script += (0, Three(Put(1), Put(1), Put(1)));
      } else if (p.sc == SC_PUT_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Put(1), Complete(1)));
      } else if (p.sc == SC_COMPLETES) {
        uploads += (1);
        uploads += (2);
        script += (0, Two(Complete(1), Complete(2)));
      } else if (p.sc == SC_SAME_COMPLETES) {
        uploads += (1);
        script += (0, Three(Complete(1), Complete(1), Complete(1)));
      } else if (p.sc == SC_REUPLOAD) {
        uploads += (1);
        etag = PARTETAG(1, 1);
        if ($) {
          etag = 19;
        }
        script += (0, Two(Complete(1), Reupload(1, 1, etag)));
      } else if (p.sc == SC_ABORT) {
        uploads += (1);
        script += (0, Two(Complete(1), AbortMpu(1)));
      } else if (p.sc == SC_LC_ABORT) {
        uploads += (1);
        script += (0, Two(Complete(1), LcAbortMpu(1)));
      } else if (p.sc == SC_RETRY) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Complete(1)));
      } else if (p.sc == SC_THEN_ABORT) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(AbortMpu(1)));
      } else if (p.sc == SC_PUT_THEN_RETRY) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Put(1)));
        script += (2, One(Complete(1)));
      } else if (p.sc == SC_DEL_VS_PUT) {
        script += (0, Two(Del(1), Put(1)));
      } else if (p.sc == SC_DELS_AND_PUT) {
        script += (0, Three(Del(1), Del(1), Put(1)));
      } else if (p.sc == SC_DEL_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Del(1), Complete(1)));
      } else if (p.sc == SC_COPY_VS_PUT_SRC) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Put(1)));
      } else if (p.sc == SC_COPY_VS_DEL_SRC) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Del(1)));
      } else if (p.sc == SC_COPY_VS_PUT_DST) {
        objects += (2);
        script += (0, Two(Copy(1, 2), Put(2)));
        script += (1, One(Del(1)));
      } else if (p.sc == SC_COPY_THEN_DELETES) {
        objects += (2);
        script += (0, One(Copy(1, 2)));
        script += (1, Two(Del(1), Del(2)));
      } else if (p.sc == SC_COPY_SELF_VS_PUT) {
        script += (0, Two(Copy(1, 1), Put(1)));
      } else if (p.sc == SC_COPY_MPU) {
        objects += (2);
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, Two(Copy(1, 2), Put(1)));
        script += (2, One(Del(2)));
      } else if (p.sc == SC_CRASH_COPY_RETRY) {
        objects += (2);
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Copy(1, 2)));
        script += (2, One(Complete(1)));
        script += (3, One(Del(2)));
      } else if (p.sc == SC_PUT_ONE) {
        script += (0, One(Put(1)));
      } else if (p.sc == SC_COPY_ONE) {
        objects += (2);
        script += (0, One(Copy(1, 2)));
        script += (1, One(Del(1)));
      } else if (p.sc == SC_LIST_VS_PUT) {
        script += (0, Two(Put(1), List()));
      } else if (p.sc == SC_LIST_VS_DEL) {
        script += (0, Two(Del(1), List()));
      } else if (p.sc == SC_LIST_VS_COMPLETE) {
        uploads += (1);
        script += (0, Two(Complete(1), List()));
      } else if (p.sc == SC_LIST_PUT_DEL) {
        script += (0, Three(Put(1), Del(1), List()));
      } else if (p.sc == SC_LIST_NEW_PUT) {
        script += (0, One(Del(1)));
        script += (1, Two(Put(1), List()));
      } else if (p.sc == SC_DEL_AFTER_RETRY) {
        // DeleteObject with the version id of request 2's completion: the
        // first request of a script is request 2
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Complete(1)));
        script += (2, One(Del(VKEY(1, 2))));
      } else if (p.sc == SC_PUT_THEN_ABORT) {
        uploads += (1);
        script += (0, One(Complete(1)));
        script += (1, One(Put(1)));
        script += (2, One(AbortMpu(1)));
      } else if (p.sc == SC_RESHARD_THEN_COMPLETE) {
        uploads += (1);
        script += (0, One(Reshard()));
        script += (1, One(Complete(1)));
      } else if (p.sc == SC_RESHARD_THEN_ABORT) {
        uploads += (1);
        script += (0, One(Reshard()));
        script += (1, One(AbortMpu(1)));
      } else if (p.sc == SC_COPY_SELF_VS_TAG) {
        script += (0, Two(Copy(1, 1), SetTags(1)));
      } else if (p.sc == SC_TAG_VS_PUT) {
        script += (0, Two(SetTags(1), Put(1)));
      } else if (p.sc == SC_TAG_THEN_COPY_SELF) {
        script += (0, One(SetTags(1)));
        script += (1, One(Copy(1, 1)));
      } else if (p.sc == SC_RESHARD_VS_PUTS) {
        script += (0, Three(Reshard(), Put(1), Put(1)));
      } else if (p.sc == SC_RESHARD_VS_DEL) {
        script += (0, Three(Reshard(), Del(1), Put(1)));
      } else if (p.sc == SC_RESHARD_VS_MPU) {
        uploads += (1);
        script += (0, Three(Reshard(), Complete(1), Reupload(1, 1, PARTETAG(1, 1))));
      } else if (p.sc == SC_CREATES) {
        objects -= (1);
        script += (0, Two(With(Put(1), IfNoneMatchAny()), With(Put(1), IfNoneMatchAny())));
      } else if (p.sc == SC_CREATE_VS_COMPLETE) {
        objects -= (1);
        uploads += (1);
        script += (0, Two(With(Put(1), IfNoneMatchAny()), With(Complete(1), IfNoneMatchAny())));
      } else if (p.sc == SC_IF_MATCH_VS_PUT) {
        script += (0, Two(With(Put(1), IfMatch(OLDWRITER(1))), Put(1)));
      } else if (p.sc == SC_MATCH_ANY_VS_MATCH) {
        script += (0, Two(With(Put(1), IfMatchAny()), With(Put(1), IfMatch(OLDWRITER(1)))));
      } else if (p.sc == SC_COND_DEL_VS_PUT) {
        script += (0, Two(With(Del(1), IfMatch(OLDWRITER(1))), Put(1)));
      } else if (p.sc == SC_COND_DEL_VS_MATCH) {
        script += (0, Two(With(Del(1), IfMatch(OLDWRITER(1))), With(Put(1), IfMatch(OLDWRITER(1)))));
      } else if (p.sc == SC_COND_COMPLETE_VS_PUT) {
        uploads += (1);
        script += (0, Two(With(Complete(1), IfMatch(OLDWRITER(1))), Put(1)));
      } else if (p.sc == SC_COND_DELS) {
        script += (0, Two(With(Del(1), IfMatch(OLDWRITER(1))), With(Del(1), IfMatch(OLDWRITER(1)))));
      } else if (p.sc == SC_INVALID_THEN_REUPLOAD) {
        uploads += (1);
        script += (0, One(Reupload(1, 1, PARTETAG(1, 1))));
        script += (1, One(CompleteList(1, PARTETAG(1, 1), 29)));
        script += (2, One(Reupload(1, 1, PARTETAG(1, 1))));
      } else {
        objects += (2);
        twins = true;
        if (p.sc == SC_DEDUP_THEN_DELETES) {
          script += (0, One(Dedup(1, 2)));
          script += (1, Two(Del(1), Del(2)));
        } else if (p.sc == SC_DEDUP_VS_PUT_TGT) {
          script += (0, Two(Dedup(1, 2), Put(2)));
          script += (1, One(Del(1)));
        } else if (p.sc == SC_DEDUP_VS_DEL_TGT) {
          script += (0, Two(Dedup(1, 2), Del(2)));
          script += (1, One(Del(1)));
        } else if (p.sc == SC_DEDUP_VS_PUT_SRC) {
          script += (0, Two(Dedup(1, 2), Put(1)));
          script += (1, One(Del(2)));
        } else {
          script += (0, Two(Dedup(1, 2), Copy(2, 2)));
          script += (1, One(Del(1)));
        }
      }
      new Driver((cfg = p.cfg, objects = objects, twins = twins, uploads = uploads, script = script));
    }
  }
}

// PutObject over PutObject
machine TestPuts { start state Init { entry { new Scenario((cfg = Main(), sc = SC_PUTS)); } } }
machine TestPutsCancelKeepsVer {
  start state Init { entry { var c: tCfg; c = Main(); c.cancelKeepsVer = true; new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestPutsNoIdTagGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.idTagGuard = false; new Scenario((cfg = c, sc = SC_PUTS)); } }
}

// a completion over the key, racing PutObject or another completion
machine TestPutVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_PUT_VS_COMPLETE)); } } }
machine TestPutVsCompleteLoserGc {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestPutVsCompleteCancelSkipsRemoveObjs {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; c.cancelRemovesObjs = false; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestCompletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COMPLETES)); } } }
machine TestCompletesLoserGc {
  start state Init { entry { var c: tCfg; c = Main(); c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}

// one upload completed three times at once
machine TestSameCompletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_SAME_COMPLETES)); } } }
machine TestSameCompletesLockLapses {
  start state Init { entry { var c: tCfg; c = Main(); c.lockHeld = false; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestSameCompletesReplayNoEtag {
  start state Init { entry { var c: tCfg; c = Main(); c.replayAnswersEtag = false; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}

// a part re-uploaded during the completion
machine TestReupload { start state Init { entry { new Scenario((cfg = Main(), sc = SC_REUPLOAD)); } } }
machine TestReuploadNoMetaVersionCheck {
  start state Init { entry { var c: tCfg; c = Main(); c.metaVersionCheck = false; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestReuploadHistoryNoSkip {
  start state Init { entry { var c: tCfg; c = Main(); c.historySkipsProcessed = false; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}

// an abort during the completion
machine TestAbort { start state Init { entry { new Scenario((cfg = Main(), sc = SC_ABORT)); } } }
machine TestAbortNoLock {
  start state Init { entry { var c: tCfg; c = Main(); c.abortTakesLock = false; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestAbortLockLapses {
  start state Init { entry { var c: tCfg; c = Main(); c.lockHeld = false; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestLcAbort { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LC_ABORT)); } } }
machine TestLcAbortTakesLock {
  start state Init { entry { var c: tCfg; c = Main(); c.lcTakesLock = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}

// a completion that leaves its meta object behind, then a retry or an abort
machine TestCrashThenRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestMetaDeleteFailsThenRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestMetaDeleteFailsThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestCrashThenRetrySparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestCrashThenAbortSparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestCrashPutThenRetrySparesHead {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; c.gcSparesHead = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}

// DeleteObject
machine TestDelVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEL_VS_PUT)); } } }
machine TestDelVsPutGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestDelsAndPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DELS_AND_PUT)); } } }
machine TestDelsAndPutCancelKeepsVer {
  start state Init { entry { var c: tCfg; c = Main(); c.cancelKeepsVer = true; new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestDelVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEL_VS_COMPLETE)); } } }
machine TestDelVsCompleteGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; c.loserGcsParts = true; new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}

// CopyObject sharing the source's tail
machine TestCopyVsPutSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_PUT_SRC)); } } }
machine TestCopyVsPutSrcNoRefs {
  start state Init { entry { var c: tCfg; c = Main(); c.copyTakesRefs = false; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_SRC)); } }
}
machine TestCopyVsDelSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_DEL_SRC)); } } }
machine TestCopyVsPutDst { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_VS_PUT_DST)); } } }
machine TestCopyVsPutDstDropRefs {
  start state Init { entry { var c: tCfg; c = Main(); c.copyLoserDropsRefs = true; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_DST)); } }
}
machine TestCopyThenDeletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_THEN_DELETES)); } } }
machine TestCopySelfVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_SELF_VS_PUT)); } } }
machine TestCopySelfVsPutGuarded {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestCopyMpu { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COPY_MPU)); } } }
machine TestCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}

// an index completion that fails after the head write
machine TestIxFailPut {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestIxFailCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestIxFailRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestIxKeepsWritePut {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestIxKeepsWriteCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestIxKeepsWriteRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; c.ixFailKeepsWrite = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}

// a bucket listing's repair
machine TestListVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_PUT)); } } }
machine TestListVsDel { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_DEL)); } } }
machine TestListVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_LIST_VS_COMPLETE)); } } }
machine TestListVsPutSlowWriter {
  start state Init { entry { var c: tCfg; c = Main(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}

// dedup of key 2's object onto key 1's, which holds the same bytes
machine TestDedupThenDeletes { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_THEN_DELETES)); } } }
machine TestDedupVsPutTgt { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_PUT_TGT)); } } }
machine TestDedupVsDelTgt { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_DEL_TGT)); } } }
machine TestDedupVsDelTgtGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_DEL_TGT)); } }
}
machine TestDedupVsPutSrc { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_PUT_SRC)); } } }
machine TestDedupVsCopySelf { start state Init { entry { new Scenario((cfg = Main(), sc = SC_DEDUP_VS_COPY_SELF)); } } }
machine TestDedupVsCopySelfGuarded {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_COPY_SELF)); } }
}

// a bucket reshard racing writes
machine TestReshardVsPuts { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_PUTS)); } } }
machine TestReshardVsDel { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_DEL)); } } }
machine TestReshardVsMpu { start state Init { entry { new Scenario((cfg = Main(), sc = SC_RESHARD_VS_MPU)); } } }
machine TestReshardNoLog {
  start state Init { entry { var c: tCfg; c = Main(); c.reshardLogs = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestReshardNoCheckExisting {
  start state Init { entry { var c: tCfg; c = Main(); c.reshardCheckExisting = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestReshardOldShardsOpen {
  start state Init { entry { var c: tCfg; c = Main(); c.oldShardsBlocked = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}

// conditional requests
machine TestCreates { start state Init { entry { new Scenario((cfg = Main(), sc = SC_CREATES)); } } }
machine TestCreateVsComplete { start state Init { entry { new Scenario((cfg = Main(), sc = SC_CREATE_VS_COMPLETE)); } } }
machine TestCreateVsCompleteKeepsParts {
  start state Init { entry { var c: tCfg; c = Main(); c.refusedKeepsParts = true; new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestIfMatchVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_IF_MATCH_VS_PUT)); } } }
machine TestIfMatchVsPutNoIdTagGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.idTagGuard = false; new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestMatchAnyVsMatch { start state Init { entry { new Scenario((cfg = Main(), sc = SC_MATCH_ANY_VS_MATCH)); } } }
machine TestMatchAnyVsMatchLossFails {
  start state Init { entry { var c: tCfg; c = Main(); c.condLossFails = true; new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestCondDelVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COND_DEL_VS_PUT)); } } }
machine TestCondDelVsPutGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestCondDelVsMatch { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COND_DEL_VS_MATCH)); } } }
machine TestCondDelVsMatchGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestCondDelVsMatchLossFails {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; c.condLossFails = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestCondCompleteVsPut { start state Init { entry { new Scenario((cfg = Main(), sc = SC_COND_COMPLETE_VS_PUT)); } } }

// the seven fixes proposed upstream (see the README), together: every scenario, and every scenario
// with index completions that may fail
fun Fixed(): tCfg {
  var c: tCfg;
  c = Main();
  c.cancelKeepsVer = true;
  c.lcTakesLock = true;
  c.loserGcsParts = true;
  c.deleteGuard = true;
  c.copyLoserDropsRefs = true;
  c.copySelfGuardsSource = true;
  c.ixFailKeepsWrite = true;
  return c;
}
machine TestFixedPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestFixedPutOne {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestFixedPutVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestFixedCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}
machine TestFixedSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestFixedReupload {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestFixedAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestFixedLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestFixedRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestFixedThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestFixedPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestFixedDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestFixedDelsAndPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestFixedDelVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}
machine TestFixedCopyOne {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestFixedCopyVsPutSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_VS_PUT_SRC)); } }
}
machine TestFixedCopyVsDelSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_VS_DEL_SRC)); } }
}
machine TestFixedCopyVsPutDst {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_VS_PUT_DST)); } }
}
machine TestFixedCopyThenDeletes {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_THEN_DELETES)); } }
}
machine TestFixedCopySelfVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestFixedCopyMpu {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COPY_MPU)); } }
}
machine TestFixedCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}
machine TestFixedListVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}
machine TestFixedListVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_LIST_VS_DEL)); } }
}
machine TestFixedListVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_LIST_VS_COMPLETE)); } }
}
machine TestFixedDedupThenDeletes {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEDUP_THEN_DELETES)); } }
}
machine TestFixedDedupVsPutTgt {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEDUP_VS_PUT_TGT)); } }
}
machine TestFixedDedupVsDelTgt {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEDUP_VS_DEL_TGT)); } }
}
machine TestFixedDedupVsPutSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEDUP_VS_PUT_SRC)); } }
}
machine TestFixedDedupVsCopySelf {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_DEDUP_VS_COPY_SELF)); } }
}
machine TestFixedReshardVsPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestFixedReshardVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestFixedReshardVsMpu {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestFixedIxPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestFixedIxPutOne {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestFixedIxPutVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestFixedIxCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}
machine TestFixedIxSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestFixedIxReupload {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestFixedIxAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestFixedIxLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestFixedIxRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestFixedIxThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestFixedIxPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestFixedIxDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestFixedIxDelsAndPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestFixedIxDelVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}
machine TestFixedIxCopyOne {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_ONE)); } }
}
machine TestFixedIxCopyVsPutSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_SRC)); } }
}
machine TestFixedIxCopyVsDelSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_VS_DEL_SRC)); } }
}
machine TestFixedIxCopyVsPutDst {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_VS_PUT_DST)); } }
}
machine TestFixedIxCopyThenDeletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_THEN_DELETES)); } }
}
machine TestFixedIxCopySelfVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestFixedIxCopyMpu {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COPY_MPU)); } }
}
machine TestFixedIxCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}
machine TestFixedIxListVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}
machine TestFixedIxListVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_LIST_VS_DEL)); } }
}
machine TestFixedIxListVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_LIST_VS_COMPLETE)); } }
}
machine TestFixedIxDedupThenDeletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEDUP_THEN_DELETES)); } }
}
machine TestFixedIxDedupVsPutTgt {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_PUT_TGT)); } }
}
machine TestFixedIxDedupVsDelTgt {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_DEL_TGT)); } }
}
machine TestFixedIxDedupVsPutSrc {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_PUT_SRC)); } }
}
machine TestFixedIxDedupVsCopySelf {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_DEDUP_VS_COPY_SELF)); } }
}
machine TestFixedIxReshardVsPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestFixedIxReshardVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestFixedIxReshardVsMpu {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}

// proposed fixes for findings 3 and 11, with the seven fixes on
machine TestStallListVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}
machine TestStallListVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_VS_DEL)); } }
}
machine TestStallListVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_VS_COMPLETE)); } }
}
machine TestStallListPutDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_PUT_DEL)); } }
}
machine TestStallListNewPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; new Scenario((cfg = c, sc = SC_LIST_NEW_PUT)); } }
}
machine TestRelinkListVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; c.lateCompleteRelinks = true; new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}
machine TestRelinkListVsDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; c.lateCompleteRelinks = true; new Scenario((cfg = c, sc = SC_LIST_VS_DEL)); } }
}
machine TestRelinkListVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; c.lateCompleteRelinks = true; new Scenario((cfg = c, sc = SC_LIST_VS_COMPLETE)); } }
}
machine TestRelinkListPutDel {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; c.lateCompleteRelinks = true; new Scenario((cfg = c, sc = SC_LIST_PUT_DEL)); } }
}
machine TestRelinkListNewPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.writersPrompt = false; c.lateCompleteRelinks = true; new Scenario((cfg = c, sc = SC_LIST_NEW_PUT)); } }
}
machine TestMarkCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestMarkCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestMarkCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestMarkCrashCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}
machine TestMarkCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestMarkCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestMarkCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestMarkMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestMarkMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestMarkMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestMarkMetaDelCrashCopyRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_CRASH_COPY_RETRY)); } }
}
machine TestMarkMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestMarkMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestMarkMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestMarkLapseAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.lockHeld = false; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestMarkLapseLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.lockHeld = false; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestMarkLapseSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.lockHeld = false; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestFixedCreates {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestFixedIxCreates {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestFixedCreateVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestFixedIxCreateVsComplete {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestFixedIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestFixedIxIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestFixedMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestFixedIxMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestFixedCondDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestFixedIxCondDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestFixedCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestFixedIxCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestFixedCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestFixedIxCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
// the seven fixes proposed upstream, and the two for conditional requests
fun FixedCond(): tCfg {
  var c: tCfg;
  c = Fixed();
  c.condLossFails = true;
  c.refusedKeepsParts = true;
  return c;
}
machine TestCondFixedCreates {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestCondFixedIxCreates {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestCondFixedCreateVsComplete {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestCondFixedIxCreateVsComplete {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestCondFixedIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestCondFixedIxIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestCondFixedMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestCondFixedIxMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestCondFixedCondDelVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestCondFixedIxCondDelVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestCondFixedCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestCondFixedIxCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestCondFixedCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestCondFixedIxCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = FixedCond(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}

// S3 answers (S3Answers): on main, with fixes alone, and with every
// fix: the nine of FixedCond(), the completion record, and the two
// proposed for S3's answers
fun AnsFixed(): tCfg {
  var c: tCfg;
  c = FixedCond();
  c.condDelNoKey = true;
  c.historyAfterHead = true;
  return c;
}
machine TestAnsPuts {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestAnsPutVsComplete {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestAnsCompletes {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}
machine TestAnsSameCompletes {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestAnsReupload {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestAnsAbort {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestAnsLcAbort {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestAnsRetry {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestAnsThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestAnsPutThenRetry {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestAnsDelVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestAnsDelsAndPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestAnsDelVsComplete {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}
machine TestAnsCopyVsPutSrc {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_VS_PUT_SRC)); } }
}
machine TestAnsCopyVsDelSrc {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_VS_DEL_SRC)); } }
}
machine TestAnsCopySelfVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestAnsCopyMpu {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_MPU)); } }
}
machine TestAnsListVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_LIST_VS_PUT)); } }
}
machine TestAnsCreates {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestAnsCreateVsComplete {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestAnsIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestAnsMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestAnsCondDelVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestAnsCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestAnsCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestAnsCondDels {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COND_DELS)); } }
}
machine TestAnsInvalidThenReupload {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_INVALID_THEN_REUPLOAD)); } }
}
machine TestAnsIxFailPut {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_ONE)); } }
}
machine TestAnsIxFailRetry {
  start state Init { entry { var c: tCfg; c = Main(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestAnsGuardCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = Main(); c.deleteGuard = true; new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestAnsTakesLockLcAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.lcTakesLock = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestAnsLossFailsIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.condLossFails = true; new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestAnsLossFailsCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.condLossFails = true; new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestAnsNoKeyCondDels {
  start state Init { entry { var c: tCfg; c = Main(); c.condDelNoKey = true; new Scenario((cfg = c, sc = SC_COND_DELS)); } }
}
machine TestAnsHistoryInvalidThenReupload {
  start state Init { entry { var c: tCfg; c = Main(); c.historyAfterHead = true; new Scenario((cfg = c, sc = SC_INVALID_THEN_REUPLOAD)); } }
}
machine TestAnsFixedPuts {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestAnsFixedPutVsComplete {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestAnsFixedCompletes {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}
machine TestAnsFixedSameCompletes {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestAnsFixedReupload {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestAnsFixedAbort {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestAnsFixedLcAbort {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestAnsFixedRetry {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestAnsFixedThenAbort {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestAnsFixedPutThenRetry {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestAnsFixedDelVsPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestAnsFixedDelsAndPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_DELS_AND_PUT)); } }
}
machine TestAnsFixedDelVsComplete {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_DEL_VS_COMPLETE)); } }
}
machine TestAnsFixedCopyVsDelSrc {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COPY_VS_DEL_SRC)); } }
}
machine TestAnsFixedCopyMpu {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COPY_MPU)); } }
}
machine TestAnsFixedReshardVsMpu {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestAnsFixedCreates {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_CREATES)); } }
}
machine TestAnsFixedCreateVsComplete {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestAnsFixedIfMatchVsPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_IF_MATCH_VS_PUT)); } }
}
machine TestAnsFixedMatchAnyVsMatch {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_MATCH_ANY_VS_MATCH)); } }
}
machine TestAnsFixedCondDelVsPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_PUT)); } }
}
machine TestAnsFixedCondDelVsMatch {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COND_DEL_VS_MATCH)); } }
}
machine TestAnsFixedCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestAnsFixedCondDels {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_COND_DELS)); } }
}
machine TestAnsFixedInvalidThenReupload {
  start state Init { entry { var c: tCfg; c = AnsFixed(); new Scenario((cfg = c, sc = SC_INVALID_THEN_REUPLOAD)); } }
}
machine TestAnsFixedIxPutVsComplete {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_VS_COMPLETE)); } }
}
machine TestAnsFixedIxCompletes {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COMPLETES)); } }
}
machine TestAnsFixedIxSameCompletes {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestAnsFixedIxReupload {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_REUPLOAD)); } }
}
machine TestAnsFixedIxRetry {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestAnsFixedIxCreateVsComplete {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_CREATE_VS_COMPLETE)); } }
}
machine TestAnsFixedIxCondCompleteVsPut {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_COMPLETE_VS_PUT)); } }
}
machine TestAnsFixedIxCondDels {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_COND_DELS)); } }
}
machine TestAnsFixedIxInvalidThenReupload {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.ixCompleteMayFail = true; new Scenario((cfg = c, sc = SC_INVALID_THEN_REUPLOAD)); } }
}
machine TestAnsFixedMarkCrashRetry {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestAnsFixedMarkCrashThenAbort {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestAnsFixedMarkCrashInvalidThenReupload {
  start state Init { entry { var c: tCfg; c = AnsFixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_INVALID_THEN_REUPLOAD)); } }
}

// versioned buckets: the completion record (finding 3) with versioning
// enabled (VE) or suspended (VS); the record skipped as in the PR (Pr),
// checked against the current version (Cur), or naming its instance (Inst)
machine TestVEPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestVEDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestVERetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVEDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVEPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSPuts {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_PUTS)); } }
}
machine TestVSDelVsPut {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_DEL_VS_PUT)); } }
}
machine TestVSRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestMarkCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestMarkCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestMarkMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestMarkMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.completionMark = true; c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVEPrCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVEPrCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVEPrCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVEPrCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVEPrCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVEPrCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVEPrCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVEPrCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVEPrMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVEPrMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVEPrMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVEPrMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVEPrMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVEPrMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVEPrMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVEPrMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVECurCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVECurCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVECurCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVECurCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVECurCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVECurCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVECurCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVECurCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVECurMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVECurMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVECurMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVECurMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVECurMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVECurMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVECurMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVECurMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVEInstCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVEInstCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVEInstCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVEInstCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVEInstCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVEInstCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVEInstCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVEInstCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVEInstMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVEInstMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVEInstMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVEInstMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVEInstMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVEInstMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVEInstMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVEInstMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_ENABLED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSPrCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSPrCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSPrCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSPrCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSPrCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSPrCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSPrCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSPrCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSPrMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSPrMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSPrMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSPrMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSPrMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSPrMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSPrMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSPrMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_SKIP(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSCurCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSCurCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSCurCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSCurCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSCurCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSCurCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSCurCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSCurCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSCurMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSCurMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSCurMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSCurMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSCurMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSCurMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSCurMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSCurMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_CURRENT(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSInstCrashRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSInstCrashThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSInstCrashPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSInstCrashSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSInstCrashAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSInstCrashLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSInstCrashDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSInstCrashPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.completeMayCrash = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}
machine TestVSInstMetaDelRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_RETRY)); } }
}
machine TestVSInstMetaDelThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_THEN_ABORT)); } }
}
machine TestVSInstMetaDelPutThenRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_RETRY)); } }
}
machine TestVSInstMetaDelSameCompletes {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_SAME_COMPLETES)); } }
}
machine TestVSInstMetaDelAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_ABORT)); } }
}
machine TestVSInstMetaDelLcAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_LC_ABORT)); } }
}
machine TestVSInstMetaDelDelAfterRetry {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_DEL_AFTER_RETRY)); } }
}
machine TestVSInstMetaDelPutThenAbort {
  start state Init { entry { var c: tCfg; c = Fixed(); c.versioning = VER_SUSPENDED(); c.completionMark = true; c.markVersioned = MV_INSTANCE(); c.metaDeleteMayFail = true; new Scenario((cfg = c, sc = SC_PUT_THEN_ABORT)); } }
}

// sharding: a reshard between layouts of one or two shards, hashed (H) or
// ordered (O); entries copied by their object's name (Obj) or by their index
// key name (Idx, PR 70053)
machine TestShH1H2ObjVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2ObjVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH1H2ObjVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH1H2ObjThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2ObjThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH1H2ObjVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2ObjVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2ObjVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2ObjVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2IdxVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2IdxVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH1H2IdxVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH1H2IdxThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2IdxThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH1H2IdxVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2IdxVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2IdxVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1H2IdxVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2ObjVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2ObjVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH2H2ObjVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH2H2ObjThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2ObjThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH2H2ObjVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2ObjVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2ObjVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2ObjVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2IdxVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2IdxVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH2H2IdxVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH2H2IdxThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2IdxThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH2H2IdxVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2IdxVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2H2IdxVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2H2IdxVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2ObjVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2ObjVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH2O2ObjVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH2O2ObjThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2ObjThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH2O2ObjVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2ObjVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2ObjVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2ObjVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2IdxVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2IdxVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH2O2IdxVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH2O2IdxThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2IdxThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH2O2IdxVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2IdxVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH2O2IdxVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH2O2IdxVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2ObjVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2ObjVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShO2H2ObjVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShO2H2ObjThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2ObjThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShO2H2ObjVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2ObjVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2ObjVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2ObjVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2IdxVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2IdxVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShO2H2IdxVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShO2H2IdxThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2IdxThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShO2H2IdxVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2IdxVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShO2H2IdxVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShO2H2IdxVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 2; c.srcOrdered = true; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2ObjVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2ObjVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH1O2ObjVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH1O2ObjThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2ObjThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH1O2ObjVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2ObjVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2ObjVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2ObjVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = false; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2IdxVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2IdxVsDel {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}
machine TestShH1O2IdxVsMpu {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_MPU)); } }
}
machine TestShH1O2IdxThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2IdxThenAbort {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; new Scenario((cfg = c, sc = SC_RESHARD_THEN_ABORT)); } }
}
machine TestShH1O2IdxVEVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2IdxVEThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_ENABLED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1O2IdxVSVsPuts {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_VS_PUTS)); } }
}
machine TestShH1O2IdxVSThenComplete {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = true; c.reshardByIndexName = true; c.versioning = VER_SUSPENDED(); new Scenario((cfg = c, sc = SC_RESHARD_THEN_COMPLETE)); } }
}
machine TestShH1H2ObjVsDelGuard {
  start state Init { entry { var c: tCfg; c = Main(); c.srcShards = 1; c.srcOrdered = false; c.dstShards = 2; c.dstOrdered = false; c.reshardByIndexName = false; c.deleteGuard = true; new Scenario((cfg = c, sc = SC_RESHARD_VS_DEL)); } }
}

// attribute updates: a copy onto itself, and a tagging update (set_attrs), on main
// (Main), with the copy guarded on the head it read (Guard, PR 72101), and with
// the guarded copy retried (Retry, proposed)
machine TestAttrMainVsTag {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_SELF_VS_TAG)); } }
}
machine TestAttrMainTagVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_TAG_VS_PUT)); } }
}
machine TestAttrMainTagThenCopy {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_TAG_THEN_COPY_SELF)); } }
}
machine TestAttrMainVsPut {
  start state Init { entry { var c: tCfg; c = Main(); new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestAttrGuardVsTag {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_TAG)); } }
}
machine TestAttrGuardTagVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_TAG_VS_PUT)); } }
}
machine TestAttrGuardTagThenCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_TAG_THEN_COPY_SELF)); } }
}
machine TestAttrGuardVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
machine TestAttrRetryVsTag {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; c.copySelfRetries = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_TAG)); } }
}
machine TestAttrRetryTagVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; c.copySelfRetries = true; new Scenario((cfg = c, sc = SC_TAG_VS_PUT)); } }
}
machine TestAttrRetryTagThenCopy {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; c.copySelfRetries = true; new Scenario((cfg = c, sc = SC_TAG_THEN_COPY_SELF)); } }
}
machine TestAttrRetryVsPut {
  start state Init { entry { var c: tCfg; c = Main(); c.copySelfGuardsSource = true; c.copySelfRetries = true; new Scenario((cfg = c, sc = SC_COPY_SELF_VS_PUT)); } }
}
