// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRemoveRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotRemoveRequest<I>::State& state) {
  switch(state) {
  case SnapshotRemoveRequest<I>::STATE_REMOVE_OBJECT_MAP:
    os << "REMOVE_OBJECT_MAP";
    break;
  case SnapshotRemoveRequest<I>::STATE_REMOVE_CHILD:
    os << "REMOVE_CHILD";
    break;
  case SnapshotRemoveRequest<I>::STATE_REMOVE_SNAP:
    os << "REMOVE_SNAP";
    break;
  case SnapshotRemoveRequest<I>::STATE_RELEASE_SNAP_ID:
    os << "RELEASE_SNAP_ID";
    break;
  case SnapshotRemoveRequest<I>::STATE_ERROR:
    os << "STATE_ERROR";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

template <typename I>
SnapshotRemoveRequest<I>::SnapshotRemoveRequest(I &image_ctx,
						Context *on_finish,
						const std::string &snap_name,
						uint64_t snap_id)
  : Request<I>(image_ctx, on_finish), m_snap_name(snap_name),
    m_snap_id(snap_id) {
}

template <typename I>
void SnapshotRemoveRequest<I>::send_op() {
  send_remove_object_map();
}

template <typename I>
bool SnapshotRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  r = filter_state_return_code(r);
  if (r < 0) {
    return true;
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
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

template <typename I>
void SnapshotRemoveRequest<I>::send_remove_object_map() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker object_map_locker(image_ctx.object_map_lock);
    if (image_ctx.object_map != nullptr) {
      CephContext *cct = image_ctx.cct;
      ldout(cct, 5) << this << " " << __func__ << dendl;
      m_state = STATE_REMOVE_OBJECT_MAP;

      image_ctx.object_map->snapshot_remove(
        m_snap_id, this->create_callback_context());
      return;
    }
  }
  send_remove_child();
}

template <typename I>
void SnapshotRemoveRequest<I>::send_remove_child() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker parent_locker(image_ctx.parent_lock);

    parent_spec our_pspec;
    int r = image_ctx.get_parent_spec(m_snap_id, &our_pspec);
    if (r < 0) {
      lderr(cct) << "failed to retrieve parent spec" << dendl;
      m_state = STATE_ERROR;

      this->async_complete(r);
      return;
    }

    if (image_ctx.parent_md.spec != our_pspec &&
        (scan_for_parents(our_pspec) == -ENOENT)) {
      // no other references to the parent image
      ldout(cct, 5) << this << " " << __func__ << dendl;
      m_state = STATE_REMOVE_CHILD;

      librados::ObjectWriteOperation op;
      cls_client::remove_child(&op, our_pspec, image_ctx.id);

      librados::AioCompletion *rados_completion = this->create_callback_completion();
      r = image_ctx.md_ctx.aio_operate(RBD_CHILDREN, rados_completion, &op);
      assert(r == 0);
      rados_completion->release();
      return;
    }
  }

  // HEAD image or other snapshots still associated with parent
  send_remove_snap();
}

template <typename I>
void SnapshotRemoveRequest<I>::send_remove_snap() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_REMOVE_SNAP;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_remove(&op, m_snap_name);
  } else {
    if (image_ctx.exclusive_lock != nullptr &&
        image_ctx.exclusive_lock->is_lock_owner()) {
      image_ctx.exclusive_lock->assert_header_locked(&op);
    }
    cls_client::snapshot_remove(&op, m_snap_id);
  }

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
                                       rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void SnapshotRemoveRequest<I>::send_release_snap_id() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": "
                << "snap_name=" << m_snap_name << ", "
                << "snap_id=" << m_snap_id << dendl;
  m_state = STATE_RELEASE_SNAP_ID;

  // TODO add async version of selfmanaged_snap_remove
  int r = image_ctx.md_ctx.selfmanaged_snap_remove(m_snap_id);
  this->async_complete(r);
}

template <typename I>
void SnapshotRemoveRequest<I>::remove_snap_context() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  image_ctx.rm_snap(m_snap_name, m_snap_id);
}

template <typename I>
int SnapshotRemoveRequest<I>::scan_for_parents(parent_spec &pspec) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.snap_lock.is_locked());
  assert(image_ctx.parent_lock.is_locked());

  if (pspec.pool_id != -1) {
    map<uint64_t, SnapInfo>::iterator it;
    for (it = image_ctx.snap_info.begin();
         it != image_ctx.snap_info.end(); ++it) {
      // skip our snap id (if checking base image, CEPH_NOSNAP won't match)
      if (it->first == m_snap_id) {
        continue;
      }
      if (it->second.parent.spec == pspec) {
        break;
      }
    }
    if (it == image_ctx.snap_info.end()) {
      return -ENOENT;
    }
  }
  return 0;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRemoveRequest<librbd::ImageCtx>;
