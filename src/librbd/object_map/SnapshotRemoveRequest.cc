// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/SnapshotRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::SnapshotRemoveRequest: "

namespace librbd {
namespace object_map {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotRemoveRequest::State& state) {
  switch(state) {
  case SnapshotRemoveRequest::STATE_LOAD_MAP:
    os << "LOAD_MAP";
    break;
  case SnapshotRemoveRequest::STATE_REMOVE_SNAPSHOT:
    os << "REMOVE_SNAPSHOT";
    break;
  case SnapshotRemoveRequest::STATE_INVALIDATE_NEXT_MAP:
    os << "INVALIDATE_NEXT_MAP";
    break;
  case SnapshotRemoveRequest::STATE_REMOVE_MAP:
    os << "REMOVE_MAP";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

void SnapshotRemoveRequest::send() {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_wlocked());

  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    compute_next_snap_id();

    uint64_t flags;
    int r = m_image_ctx.get_flags(m_snap_id, &flags);
    assert(r == 0);

    if ((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
      send_invalidate_next_map();
    } else {
      send_load_map();
    }
  } else {
    send_remove_map();
  }
}

bool SnapshotRemoveRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_LOAD_MAP:
    if (r == -ENOENT) {
      finished = true;
      break;
    }

    if (r == 0) {
      bufferlist::iterator it = m_out_bl.begin();
      r = cls_client::object_map_load_finish(&it, &m_snap_object_map);
    }
    if (r < 0) {
      RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
      send_invalidate_next_map();
    } else {
      send_remove_snapshot();
    }
    break;
  case STATE_REMOVE_SNAPSHOT:
    if (r < 0 && r != -ENOENT) {
      RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
      send_invalidate_next_map();
    } else {
      update_object_map();
      send_remove_map();
    }
    break;
  case STATE_INVALIDATE_NEXT_MAP:
    send_remove_map();
    break;
  case STATE_REMOVE_MAP:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }
  return finished;
}

void SnapshotRemoveRequest::send_load_map() {
  CephContext *cct = m_image_ctx.cct;
  std::string snap_oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << this << " " << __func__ << ": snap_oid=" << snap_oid
                << dendl;
  m_state = STATE_LOAD_MAP;

  librados::ObjectReadOperation op;
  cls_client::object_map_load_start(&op);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(snap_oid, rados_completion, &op,
                                         &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::send_remove_snapshot() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_next_snap_id));
  ldout(cct, 5) << this << " " << __func__ << ": oid=" << oid << dendl;
  m_state = STATE_REMOVE_SNAPSHOT;

  librados::ObjectWriteOperation op;
  if (m_next_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_snap_remove(&op, m_snap_object_map);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::send_invalidate_next_map() {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_wlocked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_INVALIDATE_NEXT_MAP;

  InvalidateRequest<> *req = new InvalidateRequest<>(m_image_ctx,
                                                     m_next_snap_id, true,
                                                     create_callback_context());
  req->send();
}

void SnapshotRemoveRequest::send_remove_map() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << this << " " << __func__ << ": oid=" << oid << dendl;
  m_state = STATE_REMOVE_MAP;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::compute_next_snap_id() {
  assert(m_image_ctx.snap_lock.is_locked());

  m_next_snap_id = CEPH_NOSNAP;
  std::map<librados::snap_t, SnapInfo>::const_iterator it =
    m_image_ctx.snap_info.find(m_snap_id);
  assert(it != m_image_ctx.snap_info.end());

  ++it;
  if (it != m_image_ctx.snap_info.end()) {
    m_next_snap_id = it->first;
  }
}

void SnapshotRemoveRequest::update_object_map() {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  RWLock::WLocker object_map_locker(m_image_ctx.object_map_lock);
  if (m_next_snap_id == m_image_ctx.snap_id && m_next_snap_id == CEPH_NOSNAP) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << this << " " << __func__ << dendl;

    for (uint64_t i = 0; i < m_object_map.size(); ++i) {
      if (m_object_map[i] == OBJECT_EXISTS_CLEAN &&
          (i >= m_snap_object_map.size() ||
           m_snap_object_map[i] == OBJECT_EXISTS)) {
        m_object_map[i] = OBJECT_EXISTS;
      }
    }
  }
}

} // namespace object_map
} // namespace librbd
