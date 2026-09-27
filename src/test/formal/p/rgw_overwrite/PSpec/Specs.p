// No data object is deleted while a key's head references it, and no head
// is written over an object already deleted: what each key holds stays
// readable. (With cls_refcount, an object sent to GC under one tag can
// rightly survive through another reference; only its deletion counts.)
spec HeadIntact observes mHead, mDeleted, mCreated {
  var heads: map[int, set[int]];
  var writers: map[int, int];
  var deleted: set[int];
  start state Watch {
    on mHead do (h: (key: int, writer: int, manifest: set[int])) {
      var o: int;
      foreach (o in h.manifest) {
        assert !(o in deleted),
          format("request {0} wrote the head of key {1} over object {2}, already deleted", h.writer, h.key, o);
      }
      heads[h.key] = h.manifest;
      writers[h.key] = h.writer;
    }
    on mDeleted do (o: int) {
      var k: int;
      foreach (k in keys(heads)) {
        assert !(o in heads[k]),
          format("object {0} was deleted while the head of key {1} (request {2}'s) references it", o, k, writers[k]);
      }
      deleted += (o);
    }
    on mCreated do (o: int) {
      deleted -= (o);
    }
  }
}

// At the end, after a listing has repaired the entries with pending ops,
// each key's bucket index entry lists the object its head holds, or
// nothing if there is no head.
spec IndexMatchesHead observes mFinal {
  start state Watch {
    on mFinal do (f: tFinal) {
      var k: int;
      var e: tIx;
      var h: tHead;
      foreach (k in keys(f.heads)) {
        h = f.heads[k];
        e = f.ixs[k];
        if (h.present) {
          assert e.present && e.listed && e.writer == h.writer,
            format("the bucket index lists request {0}'s object at key {1} (listed: {2}), but the head holds request {3}'s",
                   e.writer, k, e.present && e.listed, h.writer);
        } else {
          assert !(e.present && e.listed),
            format("the bucket index lists request {0}'s object at key {1}, but there is no head", e.writer, k);
        }
      }
    }
  }
}

// At the end, the index header's stats count every listed entry, in both
// namespaces, with its size.
spec BucketStats observes mFinal {
  start state Watch {
    on mFinal do (f: tFinal) {
      var k: int;
      var count: int;
      var size: int;
      foreach (k in keys(f.ixs)) {
        if (f.ixs[k].present && f.ixs[k].listed) {
          count = count + 1;
          size = size + f.ixs[k].size;
        }
      }
      foreach (k in f.mpIndex) {
        count = count + 1;
        size = size + MPSIZE(k);
      }
      assert f.statCount == count && f.statSize == size,
        format("the bucket stats say {0} entries of total size {1}, but the index lists {2} of size {3}",
               f.statCount, f.statSize, count, size);
    }
  }
}

// At the end, after GC has run every queued chain, every data object is
// referenced by a head or by a live upload; and every multipart index
// entry belongs to a live upload.
spec NoOrphans observes mFinal {
  start state Watch {
    on mFinal do (f: tFinal) {
      var o: int;
      foreach (o in f.live) {
        assert o in f.referenced,
          format("object {0} leaked: nothing references it, and GC will never delete it", o);
      }
      foreach (o in f.mpIndex) {
        assert o in f.validKeys, format("index entry {0} orphaned in the multipart namespace", o);
      }
    }
  }
}

// a successful completion answers the object's ETag, never an empty one
spec CompletionEtag observes mCompleted {
  start state Watch {
    on mCompleted do (c: (rid: int, etag: int, want: int)) {
      assert c.etag == c.want,
        format("completion {0} answered success with ETag {1} (empty if 0), not {2}", c.rid, c.etag, c.want);
    }
  }
}

// A conditional request's answer is one that some order of the requests on
// its key would give:
// - one that wrote or removed the head found a head that met its
//   condition;
// - one answered success without changing the head met its condition at
//   some point while it ran, and can be ordered just before the write that
//   replaced that head, whose own condition still holds after it (a
//   conditional delete also meets its condition where there is no head,
//   and is then answered 204 with nothing to do);
// - one answered PreconditionFailed found, while it ran, a head that
//   failed its condition, and one answered NoSuchKey found no head; neither
//   changed anything, a head or an index entry.
// Unconditional requests are not checked, except as the writes that
// replace a head.
type tHeadState = (present: bool, etag: int);
type tSeen = (present: bool, etag: int, by: int);
type tCondReq = (key: int, cond: tCond, del: bool, etag: int);

// whether a request's condition holds over a head (present, with etag)
fun CondMet(cond: tCond, del: bool, present: bool, etag: int): bool {
  if (cond.kind == C_NONE) {
    return true;
  }
  if (del && !present) {
    return true;
  }
  if (cond.kind == C_IF_MATCH) {
    return present && etag == cond.etag;
  }
  if (cond.kind == C_IF_MATCH_ANY) {
    return present;
  }
  return !present;
}

fun AnsName(a: tAns): string {
  if (a == A_OK) {
    return "success";
  }
  if (a == A_PRECOND) {
    return "412 PreconditionFailed";
  }
  if (a == A_NOTFOUND) {
    return "404 NoSuchKey";
  }
  return "an error";
}

spec CondSemantics observes mRequest, mHeadState, mReply, mCrashed, mIxRemoved {
  var cur: map[int, tHeadState];
  var reqs: map[int, tCondReq];
  // each open request's heads so far, each with the request that replaced
  // it (0 while it is current)
  var seen: map[int, seq[tSeen]];
  // the head a request's own op replaced
  var took: map[int, tHeadState];
  var removedIx: set[int];

  start state Watch {
    on mRequest do (r: (rid: int, key: int, cond: tCond, del: bool, etag: int)) {
      var s: tHeadState;
      var l: seq[tSeen];
      if (r.key in cur) {
        s = cur[r.key];
      }
      reqs[r.rid] = (key = r.key, cond = r.cond, del = r.del, etag = r.etag);
      l += (0, (present = s.present, etag = s.etag, by = 0));
      seen[r.rid] = l;
    }

    on mHeadState do (h: (key: int, by: int, present: bool, etag: int)) {
      var r: int;
      var l: seq[tSeen];
      var last: tSeen;
      foreach (r in keys(seen)) {
        if (reqs[r].key == h.key) {
          l = seen[r];
          last = l[sizeof(l) - 1];
          if (h.by == 0) {
            // the set-up, which a request may announce itself before: the
            // head it will find, not a change
            l[sizeof(l) - 1] = (present = h.present, etag = h.etag, by = 0);
          } else {
            if (h.by == r) {
              took[r] = (present = last.present, etag = last.etag);
            }
            last.by = h.by;
            l[sizeof(l) - 1] = last;
            l += (sizeof(l), (present = h.present, etag = h.etag, by = 0));
          }
          seen[r] = l;
        }
      }
      cur[h.key] = (present = h.present, etag = h.etag);
    }

    on mIxRemoved do (x: (by: int, n: int)) {
      removedIx += (x.by);
    }

    on mCrashed do (r: int) {
      if (r in seen) {
        seen -= (r);
      }
    }

    on mReply do (a: (rid: int, ans: tAns)) {
      var q: tCondReq;
      var s: tSeen;
      var ok: bool;
      if (!(a.rid in seen)) {
        return;
      }
      q = reqs[a.rid];
      if (a.rid in took) {
        assert CondMet(q.cond, q.del, took[a.rid].present, took[a.rid].etag),
          format("request {0} changed the head of key {1} over one that fails its condition (present: {2}, ETag {3})",
                 a.rid, q.key, took[a.rid].present, took[a.rid].etag);
        assert a.ans == A_OK || a.ans == A_ERR,
          format("request {0} changed the head of key {1}, but was answered {2}", a.rid, q.key, AnsName(a.ans));
      } else if (a.ans == A_OK) {
        foreach (s in seen[a.rid]) {
          if (CondMet(q.cond, q.del, s.present, s.etag)) {
            if (q.del && !s.present) {
              ok = true;
            } else if (s.by != 0 && Allows(s.by, !q.del, q.etag)) {
              ok = true;
            }
          }
        }
        assert ok,
          format("request {0} was answered success without changing the head of key {1}, and no order of the requests explains it",
                 a.rid, q.key);
      }
      if (a.ans == A_PRECOND || a.ans == A_NOTFOUND) {
        assert !(a.rid in removedIx),
          format("request {0} was answered {1}, but removed index entries", a.rid, AnsName(a.ans));
        foreach (s in seen[a.rid]) {
          if ((a.ans == A_PRECOND && !CondMet(q.cond, q.del, s.present, s.etag)) ||
              (a.ans == A_NOTFOUND && !s.present)) {
            ok = true;
          }
        }
        assert ok,
          format("request {0} was answered {1}, but every head of key {2} while it ran met its condition",
                 a.rid, AnsName(a.ans), q.key);
      }
      seen -= (a.rid);
    }
  }

  // whether request by's condition holds over the head a request would
  // have left just before it (present, with etag)
  fun Allows(by: int, present: bool, etag: int): bool {
    if (!(by in reqs)) {
      return true;
    }
    return CondMet(reqs[by].cond, reqs[by].del, present, etag);
  }
}

// every request is answered, unless its RGW dies
spec AllAnswered observes mStarted, mAnswered, mCrashed {
  var open: set[int];
  start cold state Idle {
    on mStarted do (r: int) {
      open += (r);
      goto Busy;
    }
    ignore mAnswered, mCrashed;
  }
  hot state Busy {
    on mStarted do (r: int) {
      open += (r);
    }
    on mAnswered do (a: (rid: int, ok: bool)) {
      open -= (a.rid);
      if (sizeof(open) == 0) {
        goto Idle;
      }
    }
    on mCrashed do (r: int) {
      open -= (r);
      if (sizeof(open) == 0) {
        goto Idle;
      }
    }
  }
}

// Each request is answered as the S3 API allows for its operation. The
// contract is AWS's Smithy model of S3 (aws/api-models-aws,
// models/s3/service/2006-03-01/s3-2006-03-01.json at f9dcea01): each
// operation's success status (its http trait's code), and the errors its
// model and documentation name, with their statuses. For conditional
// requests it is also the S3 User Guide's pages on conditional writes and
// deletes.
// - Success carries the operation's status: 204 for DeleteObject and
//   AbortMultipartUpload, 200 for the others.
// - Each error is one the operation's contract names, with S3's status
//   for it. Those are:
//   - PutObject: 412 PreconditionFailed and 409 ConditionalRequestConflict
//     under If-Match or If-None-Match, and 404 NoSuchKey under If-Match.
//   - DeleteObject: the same three under If-Match.
//   - CopyObject: 404 NoSuchKey for a missing source (S3's error list;
//     the Smithy model names none).
//   - UploadPart: 404 NoSuchUpload.
//   - CompleteMultipartUpload: 400 InvalidPart and 404 NoSuchUpload, and
//     PutObject's under a condition. Also 500 InternalError, which its
//     documentation tells clients to retry.
//   - AbortMultipartUpload: 404 NoSuchUpload.
//   - Any of them: 503 ServiceUnavailable, which clients retry.
//   A request that met an injected fault may be answered any 5xx.
//   UnknownError is not an S3 error code.
// - A conditional DeleteObject answered 204 removed an object. S3
//   answers one that finds no object 404, and one that loses a race 409
//   or 404 (the User Guide's "How to perform conditional deletes").
// Lifecycle's abort, dedup and resharding are not S3 requests.
spec S3Answers observes mResponse {
  start state Watch {
    on mResponse do (r: (rid: int, kind: tKind, cond: tCond, changed: bool, fault: bool, ans: tAnswer)) {
      if (r.kind == R_LC_ABORT || r.kind == R_DEDUP || r.kind == R_RESHARD) {
        return;
      }
      if (r.ans.status < 300) {
        assert r.ans.status == SuccessStatus(r.kind),
          format("request {0} ({1}) succeeded with status {2}, where S3 answers {3}",
                 r.rid, OpName(r.kind), r.ans.status, SuccessStatus(r.kind));
        if (r.kind == R_DELETE && r.cond.kind != C_NONE && !r.fault) {
          assert r.changed,
            format("request {0} (a conditional DeleteObject) was answered 204 but removed no object; S3 answers 404, or 409 on a conflict",
                   r.rid);
        }
        return;
      }
      if (r.fault && r.ans.status >= 500) {
        return;
      }
      assert r.ans.code != S3_UNKNOWN_ERROR,
        format("request {0} ({1}) was answered {2} UnknownError, an error S3 does not define",
               r.rid, OpName(r.kind), r.ans.status);
      assert r.ans.status == S3Status(r.ans.code),
        format("request {0} ({1}) was answered {2} {3}, where S3's status for {3} is {4}",
               r.rid, OpName(r.kind), r.ans.status, CodeName(r.ans.code), S3Status(r.ans.code));
      assert Allowed(r.kind, r.cond, r.ans.code),
        format("request {0} ({1}) was answered {2} {3}, an error S3 does not give {1}{4}",
               r.rid, OpName(r.kind), r.ans.status, CodeName(r.ans.code), CondName(r.cond));
    }
  }
}

fun SuccessStatus(kind: tKind): int {
  if (kind == R_DELETE || kind == R_ABORT) {
    return 204;
  }
  return 200;
}

// S3's status for each error code: the httpError trait of the Smithy
// model's error shapes (NoSuchKey, NoSuchUpload, BucketAlreadyExists),
// and the documentation for the others
fun S3Status(code: tS3Code): int {
  if (code == S3_PRECONDITION_FAILED) {
    return 412;
  }
  if (code == S3_NO_SUCH_KEY || code == S3_NO_SUCH_UPLOAD) {
    return 404;
  }
  if (code == S3_CONDITIONAL_REQUEST_CONFLICT || code == S3_BUCKET_ALREADY_EXISTS) {
    return 409;
  }
  if (code == S3_INVALID_PART || code == S3_INVALID_ARGUMENT) {
    return 400;
  }
  if (code == S3_SERVICE_UNAVAILABLE) {
    return 503;
  }
  return 500;
}

// the errors S3 gives each operation, under its condition
fun Allowed(kind: tKind, cond: tCond, code: tS3Code): bool {
  if (code == S3_SERVICE_UNAVAILABLE) {
    return true;
  }
  if (cond.kind != C_NONE && (kind == R_PUT || kind == R_DELETE || kind == R_COMPLETE)) {
    if (code == S3_PRECONDITION_FAILED || code == S3_CONDITIONAL_REQUEST_CONFLICT) {
      return true;
    }
    if (code == S3_NO_SUCH_KEY && (kind == R_DELETE || cond.kind == C_IF_MATCH || cond.kind == C_IF_MATCH_ANY)) {
      return true;
    }
  }
  if (kind == R_COPY) {
    return code == S3_NO_SUCH_KEY;
  }
  if (kind == R_UPLOAD_PART || kind == R_ABORT) {
    return code == S3_NO_SUCH_UPLOAD;
  }
  if (kind == R_COMPLETE) {
    return code == S3_INVALID_PART || code == S3_NO_SUCH_UPLOAD || code == S3_INTERNAL_ERROR;
  }
  return false;
}

fun OpName(kind: tKind): string {
  if (kind == R_PUT) {
    return "PutObject";
  }
  if (kind == R_DELETE) {
    return "DeleteObject";
  }
  if (kind == R_COPY) {
    return "CopyObject";
  }
  if (kind == R_UPLOAD_PART) {
    return "UploadPart";
  }
  if (kind == R_COMPLETE) {
    return "CompleteMultipartUpload";
  }
  if (kind == R_ABORT) {
    return "AbortMultipartUpload";
  }
  return "ListObjects";
}

fun CodeName(code: tS3Code): string {
  if (code == S3_PRECONDITION_FAILED) {
    return "PreconditionFailed";
  }
  if (code == S3_NO_SUCH_KEY) {
    return "NoSuchKey";
  }
  if (code == S3_NO_SUCH_UPLOAD) {
    return "NoSuchUpload";
  }
  if (code == S3_INVALID_PART) {
    return "InvalidPart";
  }
  if (code == S3_CONDITIONAL_REQUEST_CONFLICT) {
    return "ConditionalRequestConflict";
  }
  if (code == S3_BUCKET_ALREADY_EXISTS) {
    return "BucketAlreadyExists";
  }
  if (code == S3_INVALID_ARGUMENT) {
    return "InvalidArgument";
  }
  if (code == S3_INTERNAL_ERROR) {
    return "InternalError";
  }
  if (code == S3_SERVICE_UNAVAILABLE) {
    return "ServiceUnavailable";
  }
  return "UnknownError";
}

fun CondName(c: tCond): string {
  if (c.kind == C_IF_MATCH) {
    return " under If-Match";
  }
  if (c.kind == C_IF_MATCH_ANY) {
    return " under If-Match: *";
  }
  if (c.kind == C_IF_NONE_MATCH_ANY) {
    return " under If-None-Match: *";
  }
  return "";
}
