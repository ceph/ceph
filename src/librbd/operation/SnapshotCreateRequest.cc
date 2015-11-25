// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotCreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotCreateRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotCreateRequest<I>::State& state) {
  switch(state) {
  case SnapshotCreateRequest<I>::STATE_SUSPEND_REQUESTS:
    os << "SUSPEND_REQUESTS";
    break;
  case SnapshotCreateRequest<I>::STATE_SUSPEND_AIO:
    os << "SUSPEND_AIO";
    break;
  case SnapshotCreateRequest<I>::STATE_ALLOCATE_SNAP_ID:
    os << "ALLOCATE_SNAP_ID";
    break;
  case SnapshotCreateRequest<I>::STATE_CREATE_SNAP:
    os << "CREATE_SNAP";
    break;
  case SnapshotCreateRequest<I>::STATE_CREATE_OBJECT_MAP:
    os << "CREATE_OBJECT_MAP";
    break;
  case SnapshotCreateRequest<I>::STATE_RELEASE_SNAP_ID:
    os << "RELEASE_SNAP_ID";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

template <typename I>
SnapshotCreateRequest<I>::SnapshotCreateRequest(I &image_ctx,
                                                Context *on_finish,
                                                const std::string &snap_name)
  : Request<I>(image_ctx, on_finish), m_snap_name(snap_name), m_ret_val(0),
    m_aio_suspended(false), m_requests_suspended(false),
    m_snap_id(CEPH_NOSNAP), m_snap_created(false) {
}

template <typename I>
void SnapshotCreateRequest<I>::send_op() {
  send_suspend_requests();
}

template <typename I>
bool SnapshotCreateRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  int orig_result = r;
  r = filter_state_return_code(r);
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  if (m_ret_val < 0) {
    return should_complete_error();
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_SUSPEND_REQUESTS:
    send_suspend_aio();
    break;
  case STATE_SUSPEND_AIO:
    send_allocate_snap_id();
    break;
  case STATE_ALLOCATE_SNAP_ID:
    send_create_snap();
    break;
  case STATE_CREATE_SNAP:
    if (orig_result == 0) {
      update_snap_context();
      finished = send_create_object_map();
    } else {
      assert(orig_result == -ESTALE);
      send_allocate_snap_id();
    }
    break;
  case STATE_CREATE_OBJECT_MAP:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }

  if (finished) {
    resume_aio();
    resume_requests();
  }
  return finished;
}

template <typename I>
bool SnapshotCreateRequest<I>::should_complete_error() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  lderr(cct) << this << " " << __func__ << ": "
             << "ret_val=" << m_ret_val << dendl;

  // only valid exit points during error recovery
  bool finished = true;
  if (m_state != STATE_RELEASE_SNAP_ID) {
    finished = send_release_snap_id();
  }

  if (finished) {
    resume_aio();
    resume_requests();
  }
  return finished;
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_requests() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  // TODO suspend (shrink) resize to ensure consistent RBD mirror
  send_suspend_aio();
}

template <typename I>
void SnapshotCreateRequest<I>::send_suspend_aio() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_SUSPEND_AIO;
  m_aio_suspended = true;

  // can issue a re-entrant callback if no IO in-progress
  image_ctx.aio_work_queue->block_writes(this->create_async_callback_context());
}

template <typename I>
void SnapshotCreateRequest<I>::send_allocate_snap_id() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_ALLOCATE_SNAP_ID;

  // TODO create an async version of selfmanaged_snap_create
  int r = image_ctx.md_ctx.selfmanaged_snap_create(&m_snap_id);
  this->async_complete(r);
}

template <typename I>
void SnapshotCreateRequest<I>::send_create_snap() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  RWLock::RLocker snap_locker(image_ctx.snap_lock);
  RWLock::RLocker parent_locker(image_ctx.parent_lock);

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_CREATE_SNAP;

  // should have been canceled prior to releasing lock
  assert(!image_ctx.image_watcher->is_lock_supported(image_ctx.snap_lock) ||
         image_ctx.image_watcher->is_lock_owner());

  // save current size / parent info for creating snapshot record in ImageCtx
  m_size = image_ctx.size;
  m_parent_info = image_ctx.parent_md;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_add(&op, m_snap_id, m_snap_name);
  } else {
    if (image_ctx.image_watcher->is_lock_owner()) {
      image_ctx.image_watcher->assert_header_locked(&op);
    }
    cls_client::snapshot_add(&op, m_snap_id, m_snap_name);
  }

  librados::AioCompletion *rados_completion =
    this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
bool SnapshotCreateRequest<I>::send_create_object_map() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker object_map_lock(image_ctx.object_map_lock);
    if (image_ctx.object_map.enabled(image_ctx.object_map_lock)) {
      CephContext *cct = image_ctx.cct;
      ldout(cct, 5) << this << " " << __func__ << dendl;
      m_state = STATE_CREATE_OBJECT_MAP;

      image_ctx.object_map.snapshot_add(m_snap_id,
                                        this->create_callback_context());
      return false;
    }
  }
  return true;
}

template <typename I>
bool SnapshotCreateRequest<I>::send_release_snap_id() {
  I &image_ctx = this->m_image_ctx;
  if (m_snap_id != CEPH_NOSNAP && !m_snap_created) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << ": snap_id=" << m_snap_id
                  << dendl;
    m_state = STATE_RELEASE_SNAP_ID;

    // TODO add async version of selfmanaged_snap_remove
    int r = image_ctx.md_ctx.selfmanaged_snap_remove(m_snap_id);
    m_snap_id = CEPH_NOSNAP;

    this->async_complete(r);
    return false;
  }
  return true;
}

template <typename I>
void SnapshotCreateRequest<I>::resume_aio() {
  I &image_ctx = this->m_image_ctx;
  if (m_aio_suspended) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

    image_ctx.aio_work_queue->unblock_writes();
    m_aio_suspended = false;
  }
}

template <typename I>
void SnapshotCreateRequest<I>::resume_requests() {
  I &image_ctx = this->m_image_ctx;
  if (m_requests_suspended) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

    // TODO
    m_requests_suspended = false;
  }
}

template <typename I>
void SnapshotCreateRequest<I>::update_snap_context() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  m_snap_created = true;

  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  if (image_ctx.old_format) {
    return;
  }

  if (image_ctx.get_snap_info(m_snap_id) != NULL) {
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // should have been canceled prior to releasing lock
  assert(!image_ctx.image_watcher->is_lock_supported(image_ctx.snap_lock) ||
         image_ctx.image_watcher->is_lock_owner());

  // immediately add a reference to the new snapshot
  image_ctx.add_snap(m_snap_name, m_snap_id, m_size, m_parent_info,
                     RBD_PROTECTION_STATUS_UNPROTECTED, 0);

  // immediately start using the new snap context if we
  // own the exclusive lock
  std::vector<snapid_t> snaps;
  snaps.push_back(m_snap_id);
  snaps.insert(snaps.end(), image_ctx.snapc.snaps.begin(),
               image_ctx.snapc.snaps.end());

  image_ctx.snapc.seq = m_snap_id;
  image_ctx.snapc.snaps.swap(snaps);
  image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(
    image_ctx.snapc.seq, image_ctx.snaps);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotCreateRequest<librbd::ImageCtx>;
