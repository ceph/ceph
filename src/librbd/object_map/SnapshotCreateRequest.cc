// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/SnapshotCreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "cls/lock/cls_lock_client.h"
#include <iostream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::SnapshotCreateRequest: "

namespace librbd {
namespace object_map {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotCreateRequest::State& state) {
  switch(state) {
  case SnapshotCreateRequest::STATE_READ_MAP:
    os << "READ_MAP";
    break;
  case SnapshotCreateRequest::STATE_WRITE_MAP:
    os << "WRITE_MAP";
    break;
  case SnapshotCreateRequest::STATE_ADD_SNAPSHOT:
    os << "ADD_SNAPSHOT";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

void SnapshotCreateRequest::send() {
  send_read_map();
}

bool SnapshotCreateRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0 && m_ret_val == 0) {
    m_ret_val = r;
  }
  if (m_ret_val < 0) {
    // pass errors down to base class to invalidate the object map
    return Request::should_complete(r);
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_READ_MAP:
    send_write_map();
    break;
  case STATE_WRITE_MAP:
    finished = send_add_snapshot();
    break;
  case STATE_ADD_SNAPSHOT:
    update_object_map();
    finished = true;
    break;
  default:
    assert(false);
    break;
  }
  return finished;
}

void SnapshotCreateRequest::send_read_map() {
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.get_snap_info(m_snap_id) != NULL);

  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 5) << this << " " << __func__ << ": oid=" << oid << dendl;
  m_state = STATE_READ_MAP;

  // IO is blocked due to the snapshot creation -- consistent to read from disk
  librados::ObjectReadOperation op;
  op.read(0, 0, NULL, NULL);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op,
                                         &m_read_bl);
  assert(r == 0);
  rados_completion->release();
}

void SnapshotCreateRequest::send_write_map() {
  CephContext *cct = m_image_ctx.cct;
  std::string snap_oid(ObjectMap::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << this << " " << __func__ << ": snap_oid=" << snap_oid
                << dendl;
  m_state = STATE_WRITE_MAP;

  librados::ObjectWriteOperation op;
  op.write_full(m_read_bl);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(snap_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

bool SnapshotCreateRequest::send_add_snapshot() {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) == 0) {
    return true;
  }

  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 5) << this << " " << __func__ << ": oid=" << oid << dendl;
  m_state = STATE_ADD_SNAPSHOT;

  librados::ObjectWriteOperation op;
  rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  cls_client::object_map_snap_add(&op);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

void SnapshotCreateRequest::update_object_map() {
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  RWLock::WLocker object_map_locker(m_image_ctx.object_map_lock);

  for (uint64_t i = 0; i < m_object_map.size(); ++i) {
    if (m_object_map[i] == OBJECT_EXISTS) {
      m_object_map[i] = OBJECT_EXISTS_CLEAN;
    }
  }
}

} // namespace object_map
} // namespace librbd
