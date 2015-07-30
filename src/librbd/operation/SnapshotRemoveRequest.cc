// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRemoveRequest: "

namespace librbd {
namespace operation {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotRemoveRequest::State& state) {
  switch(state) {
  case SnapshotRemoveRequest::STATE_REMOVE_OBJECT_MAP:
    os << "REMOVE_OBJECT_MAP";
    break;
  case SnapshotRemoveRequest::STATE_REMOVE_CHILD:
    os << "REMOVE_CHILD";
    break;
  case SnapshotRemoveRequest::STATE_REMOVE_SNAP:
    os << "REMOVE_SNAP";
    break;
  case SnapshotRemoveRequest::STATE_RELEASE_SNAP_ID:
    os << "RELEASE_SNAP_ID";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

SnapshotRemoveRequest::SnapshotRemoveRequest(ImageCtx &image_ctx,
                                             Context *on_finish,
                                             const std::string &snap_name,
                                             uint64_t snap_id)
  : Request(image_ctx, on_finish), m_snap_name(snap_name),
    m_snap_id(snap_id) {
}

void SnapshotRemoveRequest::send_op() {
  send_remove_object_map();
}

bool SnapshotRemoveRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  r = filter_state_return_code(r);
  if (r < 0) {
    return true;
  }

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_REMOVE_OBJECT_MAP:
    send_remove_child();
    break;
  case STATE_REMOVE_CHILD:
    send_remove_snap();
    break;
  case STATE_REMOVE_SNAP:
    remove_snap_context();
    send_release_snap_id();
    break;
  case STATE_RELEASE_SNAP_ID:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }

  return finished;
}

void SnapshotRemoveRequest::send_remove_object_map() {
  assert(m_image_ctx.owner_lock.is_locked());

  // TODO add object map support
  send_remove_child();
}

void SnapshotRemoveRequest::send_remove_child() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);

    parent_spec our_pspec;
    int r = m_image_ctx.get_parent_spec(m_snap_id, &our_pspec);
    if (r < 0) {
      lderr(cct) << "failed to retrieve parent spec" << dendl;
      async_complete(r);
      return;
    }

    if (m_image_ctx.parent_md.spec != our_pspec &&
        (scan_for_parents(our_pspec) == -ENOENT)) {
      // no other references to the parent image
      ldout(cct, 5) << this << " " << __func__ << dendl;
      m_state = STATE_REMOVE_CHILD;

      librados::ObjectWriteOperation op;
      cls_client::remove_child(&op, our_pspec, m_image_ctx.id);

      librados::AioCompletion *rados_completion = create_callback_completion();
      r = m_image_ctx.md_ctx.aio_operate(RBD_CHILDREN, rados_completion, &op);
      assert(r == 0);
      rados_completion->release();
      return;
    }
  }

  // HEAD image or other snapshots still associated with parent
  send_remove_snap();
}

void SnapshotRemoveRequest::send_remove_snap() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_REMOVE_SNAP;

  librados::ObjectWriteOperation op;
  if (m_image_ctx.old_format) {
    cls_client::old_snapshot_remove(&op, m_snap_name);
  } else {
    if (m_image_ctx.image_watcher->is_lock_owner()) {
      m_image_ctx.image_watcher->assert_header_locked(&op);
    }
    cls_client::snapshot_remove(&op, m_snap_id);
  }

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::send_release_snap_id() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_name=" << m_snap_name << ", "
                << "snap_id=" << m_snap_id << dendl;
  m_state = STATE_RELEASE_SNAP_ID;

  // TODO add async version of selfmanaged_snap_remove
  m_image_ctx.data_ctx.selfmanaged_snap_remove(m_snap_id);
  async_complete(0);
}

void SnapshotRemoveRequest::remove_snap_context() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  m_image_ctx.rm_snap(m_snap_name, m_snap_id);
}

int SnapshotRemoveRequest::scan_for_parents(parent_spec &pspec) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.parent_lock.is_locked());

  if (pspec.pool_id != -1) {
    map<uint64_t, SnapInfo>::iterator it;
    for (it = m_image_ctx.snap_info.begin();
         it != m_image_ctx.snap_info.end(); ++it) {
      // skip our snap id (if checking base image, CEPH_NOSNAP won't match)
      if (it->first == m_snap_id) {
        continue;
      }
      if (it->second.parent.spec == pspec) {
        break;
      }
    }
    if (it == m_image_ctx.snap_info.end()) {
      return -ENOENT;
    }
  }
  return 0;
}

} // namespace operation
} // namespace librbd
