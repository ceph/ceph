// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotCreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotCreateRequest: "

namespace librbd {
namespace operation {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotCreateRequest::State& state) {
  switch(state) {
  case SnapshotCreateRequest::STATE_SUSPEND_REQUESTS:
    os << "SUSPEND_REQUESTS";
    break;
  case SnapshotCreateRequest::STATE_SUSPEND_AIO:
    os << "SUSPEND_AIO";
    break;
  case SnapshotCreateRequest::STATE_FLUSH_AIO:
    os << "FLUSH_AIO";
    break;
  case SnapshotCreateRequest::STATE_ALLOCATE_SNAP_ID:
    os << "ALLOCATE_SNAP_ID";
    break;
  case SnapshotCreateRequest::STATE_CREATE_SNAP:
    os << "CREATE_SNAP";
    break;
  case SnapshotCreateRequest::STATE_CREATE_OBJECT_MAP:
    os << "CREATE_OBJECT_MAP";
    break;
  case SnapshotCreateRequest::STATE_RELEASE_SNAP_ID:
    os << "RELEASE_SNAP_ID";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

SnapshotCreateRequest::SnapshotCreateRequest(ImageCtx &image_ctx,
                                             Context *on_finish,
                                             const std::string &snap_name)
  : Request(image_ctx, on_finish), m_snap_name(snap_name), m_ret_val(0),
    m_aio_suspended(false), m_requests_suspended(false),
    m_snap_id(CEPH_NOSNAP), m_snap_created(false) {
}

void SnapshotCreateRequest::send_op() {
  send_suspend_requests();
}

bool SnapshotCreateRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
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

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_SUSPEND_REQUESTS:
    send_suspend_aio();
    break;
  case STATE_SUSPEND_AIO:
    send_flush_aio();
    break;
  case STATE_FLUSH_AIO:
    send_allocate_snap_id();
    break;
  case STATE_ALLOCATE_SNAP_ID:
    send_create_snap();
    break;
  case STATE_CREATE_SNAP:
    if (r == 0) {
      m_snap_created = true;
      finished = send_create_object_map();
    } else {
      assert(r == -ESTALE);
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
    update_snap_context();
    resume_aio();
    resume_requests();
  }
  return finished;
}

bool SnapshotCreateRequest::should_complete_error() {
  CephContext *cct = m_image_ctx.cct;
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

void SnapshotCreateRequest::send_suspend_requests() {
  assert(m_image_ctx.owner_lock.is_locked());

  // TODO suspend (shrink) resize to ensure consistent RBD mirror
  send_suspend_aio();
}

void SnapshotCreateRequest::send_suspend_aio() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_SUSPEND_AIO;
  m_aio_suspended = true;

  // can issue a re-entrant callback if no IO in-progress
  m_image_ctx.aio_work_queue->block_writes(create_async_callback_context());
}

void SnapshotCreateRequest::send_flush_aio() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_FLUSH_AIO;

  // can issue a re-entrant callback if no IO to flush
  m_image_ctx.flush(create_async_callback_context());
}

void SnapshotCreateRequest::send_allocate_snap_id() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_ALLOCATE_SNAP_ID;

  // TODO create an async version of selfmanaged_snap_create
  int r = m_image_ctx.md_ctx.selfmanaged_snap_create(&m_snap_id);
  async_complete(r);
}

void SnapshotCreateRequest::send_create_snap() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_CREATE_SNAP;

  // should have been canceled prior to releasing lock
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
         m_image_ctx.image_watcher->is_lock_owner());

  librados::ObjectWriteOperation op;
  if (m_image_ctx.old_format) {
    cls_client::old_snapshot_add(&op, m_snap_id, m_snap_name);
  } else {
    if (m_image_ctx.image_watcher->is_lock_owner()) {
      m_image_ctx.image_watcher->assert_header_locked(&op);
    }
    cls_client::snapshot_add(&op, m_snap_id, m_snap_name);
  }

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

bool SnapshotCreateRequest::send_create_object_map() {
  // TODO add object map support
  return true;
}

bool SnapshotCreateRequest::send_release_snap_id() {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_snap_id != CEPH_NOSNAP && !m_snap_created) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << ": snap_id=" << m_snap_id
                  << dendl;
    m_state = STATE_RELEASE_SNAP_ID;

    // TODO add async version of selfmanaged_snap_remove
    int r = m_image_ctx.data_ctx.selfmanaged_snap_remove(m_snap_id);
    m_snap_id = CEPH_NOSNAP;

    async_complete(r);
    return false;
  }
  return true;
}

void SnapshotCreateRequest::resume_aio() {
  if (m_aio_suspended) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

    m_image_ctx.aio_work_queue->unblock_writes();
    m_aio_suspended = false;
  }
}

void SnapshotCreateRequest::resume_requests() {
  if (m_requests_suspended) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

    // TODO
    m_requests_suspended = false;
  }
}

void SnapshotCreateRequest::update_snap_context() {
  assert(m_image_ctx.owner_lock.is_locked());

  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  if (m_image_ctx.old_format || !m_snap_created) {
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  // should have been canceled prior to releasing lock
  assert(!m_image_ctx.image_watcher->is_lock_supported(m_image_ctx.snap_lock) ||
         m_image_ctx.image_watcher->is_lock_owner());

  // immediately start using the new snap context if we
  // own the exclusive lock
  std::vector<snapid_t> snaps;
  snaps.push_back(m_snap_id);
  snaps.insert(snaps.end(), m_image_ctx.snapc.snaps.begin(),
               m_image_ctx.snapc.snaps.end());

  m_image_ctx.snapc.seq = m_snap_id;
  m_image_ctx.snapc.snaps.swap(snaps);
  m_image_ctx.data_ctx.selfmanaged_snap_set_write_ctx(
    m_image_ctx.snapc.seq, m_image_ctx.snaps);
}

} // namespace operation
} // namespace librbd
