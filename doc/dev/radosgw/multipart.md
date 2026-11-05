# Hardening Multipart Uploads

## Motivation

CompleteMultipartUpload and AbortMultipartUpload rely on cls_lock for mutual exclusion, but the locks may expire while the locker is still working. After expiration, a racing Complete/Abort request can acquire the lock and clobber an already-completed upload and garbage collect its tail objects.

These locks expire to accomodate crash recovery. If the original locker crashes, another writer should be able to take over the lock and recover. However, neither Complete nor Abort try to detect whether a previous Complete/Abort request took effect: that is, whether Complete wrote a head object, or Abort started deleting tail objects. We should treat either of these cases as an upload that already completed, and just finish the cleanup.

## Design

To harden multipart uploads against these issues, we start with the design from https://github.com/ceph/ceph/pull/72103: Complete uses an atomic write on the multipart meta object to enter a "pending state", by storing the ID_TAG and VersionId of the head object it's going to write. Once set, other Complete/Abort requests must be rejected. They can look for a head object with that ID_TAG and VersionId, if it still exists, as an indicator that the upload already completed.

The missing piece of this design is crash recovery. Once a Complete request enters this pending state, other requests that observe that state cannot attempt recovery because they can't prove the original request won't eventually succeed. Without the ability to recover, a crash during multipart upload must leak its part objects forever.

### Recovery

To complete this design for recovery, we need two additional building blocks:

#### 1. Enforcing Mutual Exclusion

When a Complete or Abort request finds the upload in this pending state, it cannot attempt recovery unless it can prove that the original operation will not eventually succeed.

Complete can address this by guarding its head object write with an assertion that the lock has not expired. As long as the osd rejects the write after its lease has expired, mutual exclusion is preserved.

The assert_locked() method from cls_lock can't be used for this, because the lock is held on a different rados object. What we can do is pass the lock's current expiration timestamp with the request, and have the osd check that before applying the write. This involves comparing a timestamp generated on radosgw against the osd's current time, so is subject to clock skew. We'll want to set some reasonable limit on acceptable skew (10 seconds?), and subtract that from the expiration values we send.

Rados doesn't provide an existing primitive for current time comparisons, so we'll need to introduce a new cls method for this. cls_rgw_obj_check_mtime() from cls_rgw is a good example to copy. Until osds upgrade to a release that supports this new method, calls will fail with EOPNOTSUPP errors. Radosgw should handle these errors by retrying the head object write without this guard.

By rejecting this head object write that commits the multipart upload, we can guarantee that other requests that successfully acquire the lock will be able to detect its completion. Abort should use the same guard for its write operations, at least on the first part objects consulted for completion.

ETIMEDOUT errors from rados_osd_op_timeout need special handling for CompleteMultipartUpload. In other error cases, this multipart lock is released before sending the http response. But because ETIMEDOUT indicates that the osd may still apply the write operation later, at least up to the expiration time we sent, we can't release this lock early and must let it expire naturally. This prevents a later Complete/Abort request from acquiring the lock and racing with that earlier write operation.

#### 2. Detecting Completion

Whenever a Complete/Abort request takes over exclusive access from a previous writer, it must first determine whether the upload already completed or aborted: either that head object still exists, it did exist but was deleted/overwritten, or abort started garbage collecting parts. In any of these cases, treat the request as already completed - just finish cleanup (calling cleanup_orphaned_parts() and deleting the multipart meta object) before returning.

Determining whether the head object still exists is trivial, given the ID_TAG and VersionId stored in the multipart meta object.

Handling the other cases involves looking for evidence of garbage collection. For this, we can consult cls_refcount_read() on the first part object. The upload was removed if:
- ENOENT error indicates that the tail object no longer exists,
- the object no longer contains a wildcard reference (gc already took effect, but the upload was copied first),
- the part object is present in the corresponding gc queue, with a tag that qualifies as the wildcard (it should either match the stored ID_TAG, or the multipart upload id)

Scanning the gc queue is potentially expensive, especially after moving to unbounded fifo queues. We select the corresponding gc queue using a hash of its gc tag, so changes to rgw_gc_max_objs would make this check unreliable.
