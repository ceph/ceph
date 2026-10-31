# Hardening Multipart Uploads

## Motivation

CompleteMultipartUpload and AbortMultipartUpload rely on cls_lock for mutual exclusion, but the locks may expire while the locker is still working. After expiration, a racing Complete/Abort request can acquire the lock and clobber an already-completed upload and garbage collect its tail objects.

These locks expire to accomodate crash recovery. If the original locker crashes, another writer should be able to take over the lock and recover. However, neither Complete nor Abort try to detect whether a previous Complete/Abort request took effect: that is, whether Complete wrote a head object, or Abort started deleting tail objects. We should treat either of these cases as an upload that already completed, and just finish the cleanup.

## Design

To harden multipart uploads against these issues, we start with the design from https://github.com/ceph/ceph/pull/72103: Complete uses an atomic write on the multipart meta object to enter a "pending state", by storing the ID_TAG and VersionId of the head object it's going to write. Once set, other Complete/Abort requests must be rejected. They can look for a head object with that ID_TAG and VersionId, if it still exists, as an indicator that the upload already completed.

The missing piece of this design is crash recovery. Once a Complete request enters this pending state, other requests that observe that state cannot attempt recovery because they can't prove the original request won't eventually succeed. Without the ability to recover, a crash during multipart upload must leak its part objects forever.

### Recovery

To complete this design for recovery, we need two additional building blocks:

#### 1. Detecting Liveness

When a Complete or Abort request finds the upload in this pending state, it cannot attempt recovery unless it can prove that the original operation will not eventually succeed.

This can be accomplished with the Watch/Notify primitive: if the exclusive writer holds a Watch on the multipart meta object, other candidate writers can send a Notify to check whether they're still watching.

Before trying to enter this pending state, candidate writers establish a watch on the multipart meta object. If they fail to acquire exclusive access to the upload by setting this pending state (another writer already has exclusive access), they send a notify message.

If any watcher fails to respond before timeout, the result is inconclusive so we assume they're still working. If the exclusive writer responds, they're still working. Otherwise, the exclusive writer is no longer watcher watching so we can attempt to take over exclusive access by atomically changing their ID_TAG/VersionId to our own.

If we instead tried to establish the watch only after successfully acquiring exclusive access, racing notify messages would assume that we've crashed. So we must establish the watch first, meaning that the object will have watchers that aren't exclusive writers. To disambiguate these, each watcher replies to these notify messages with their own ID_TAG and the notifier compares that against what it read from the multipart meta object.

#### 2. Detecting Completion

Whenever a Complete/Abort request takes over exclusive access from a previous writer, it must first determine whether the upload already completed or aborted: either that head object still exists, it did exist but was deleted/overwritten, or abort started garbage collecting parts. In any of these cases, treat the request as already completed - just finish cleanup (calling cleanup_orphaned_parts() and deleting the multipart meta object) before returning.

Determining whether the head object still exists is trivial, given the ID_TAG and VersionId stored in the multipart meta object.

Handling the other cases involves looking for evidence of garbage collection. For this, we can consult cls_refcount_read() on the first part object. The upload was removed if:
- ENOENT error indicates that the tail object no longer exists,
- the object no longer contains a wildcard reference (gc already took effect, but the upload was copied first),
- the part object is present in the corresponding gc queue, with a tag that qualifies as the wildcard (it should either match the stored ID_TAG, or the multipart upload id)

Scanning the gc queue is potentially expensive, especially after moving to unbounded fifo queues. We select the corresponding gc queue using a hash of its gc tag, so changes to rgw_gc_max_objs would make this check unreliable.

### Rolling Upgrade

Before upgrade, old radosgw instances still rely on cls_lock so upgraded radosgws have to continue taking it. We can remove the use of cls lock (MPSerializer) after two major releases. After the upgrade completes, the admin can safely disable this lock by setting rgw_mp_lock_max_time=0.
