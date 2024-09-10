// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/SnapshotRollbackRequest.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "cls/lock/cls_lock_client.h"
#include <iostream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::SnapshotRollbackRequest: "

namespace librbd {
namespace object_map {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotRollbackRequest::State& state) {
  switch(state) {
  case SnapshotRollbackRequest::STATE_READ_MAP:
    os << "READ_MAP";
    break;
  case SnapshotRollbackRequest::STATE_INVALIDATE_MAP:
    os << "INVALIDATE_MAP";
    break;
  case SnapshotRollbackRequest::STATE_WRITE_MAP:
    os << "WRITE_MAP";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

void SnapshotRollbackRequest::send() {
  send_read_map();
}

bool SnapshotRollbackRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0 && m_ret_val == 0) {
    m_ret_val = r;
  }

  bool finished = false;
  switch (m_state) {
  case STATE_READ_MAP:
    if (r < 0) {
      // invalidate the snapshot object map
      send_invalidate_map();
    } else {
      send_write_map();
    }
    break;
  case STATE_INVALIDATE_MAP:
    // invalidate the HEAD object map as well
    finished = Request::should_complete(m_ret_val);
    break;
  case STATE_WRITE_MAP:
    finished = Request::should_complete(r);
    break;
  default:
    ceph_abort();
    break;
  }
  return finished;
}

void SnapshotRollbackRequest::send_read_map() {
  std::string snap_oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": snap_oid=" << snap_oid
                << dendl;
  m_state = STATE_READ_MAP;

  librados::ObjectReadOperation op;
  op.read(0, 0, NULL, NULL);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(snap_oid, rados_completion, &op,
                                         &m_read_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

void SnapshotRollbackRequest::send_write_map() {
  std::shared_lock owner_locker{m_image_ctx.owner_lock};

  CephContext *cct = m_image_ctx.cct;
  std::string snap_oid(ObjectMap<>::object_map_name(m_image_ctx.id,
                                                    CEPH_NOSNAP));
  ldout(cct, 5) << this << " " << __func__ << ": snap_oid=" << snap_oid
                << dendl;
  m_state = STATE_WRITE_MAP;

  librados::ObjectWriteOperation op;
  rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, ClsLockType::EXCLUSIVE, "", "");
  op.write_full(m_read_bl);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(snap_oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

void SnapshotRollbackRequest::send_invalidate_map() {
  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  std::unique_lock image_locker{m_image_ctx.image_lock};

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_INVALIDATE_MAP;

  InvalidateRequest<> *req = new InvalidateRequest<>(m_image_ctx, m_snap_id,
                                                     false,
                                                     create_callback_context());
  req->send();
}

} // namespace object_map
} // namespace librbd
