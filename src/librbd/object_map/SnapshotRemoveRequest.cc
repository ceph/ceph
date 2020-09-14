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
#define dout_prefix *_dout << "librbd::object_map::SnapshotRemoveRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace object_map {

void SnapshotRemoveRequest::send() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_wlocked(m_image_ctx.image_lock));

  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    int r = m_image_ctx.get_flags(m_snap_id, &m_flags);
    ceph_assert(r == 0);

    compute_next_snap_id();
    load_map();
  } else {
    remove_map();
  }
}

void SnapshotRemoveRequest::load_map() {
  CephContext *cct = m_image_ctx.cct;
  std::string snap_oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << "snap_oid=" << snap_oid << dendl;

  librados::ObjectReadOperation op;
  cls_client::object_map_load_start(&op);

  auto rados_completion = librbd::util::create_rados_callback<
    SnapshotRemoveRequest, &SnapshotRemoveRequest::handle_load_map>(this);
  int r = m_image_ctx.md_ctx.aio_operate(snap_oid, rados_completion, &op,
                                         &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::handle_load_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::object_map_load_finish(&it, &m_snap_object_map);
  }
  if (r == -ENOENT) {
    // implies we have already deleted this snapshot and handled the
    // necessary fast-diff cleanup
    complete(0);
    return;
  } else if (r < 0) {
    std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
    lderr(cct) << "failed to load object map " << oid << ": "
               << cpp_strerror(r) << dendl;

    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    std::unique_lock image_locker{m_image_ctx.image_lock};
    invalidate_next_map();
    return;
  }

  remove_snapshot();
}

void SnapshotRemoveRequest::remove_snapshot() {
  if ((m_flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
    // snapshot object map exists on disk but is invalid. cannot clean fast-diff
    // on next snapshot if current snapshot was invalid.
    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    std::unique_lock image_locker{m_image_ctx.image_lock};
    invalidate_next_map();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_next_snap_id));
  ldout(cct, 5) << "oid=" << oid << dendl;

  librados::ObjectWriteOperation op;
  if (m_next_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, ClsLockType::EXCLUSIVE, "", "");
  }
  cls_client::object_map_snap_remove(&op, m_snap_object_map);

  auto rados_completion = librbd::util::create_rados_callback<
    SnapshotRemoveRequest,
    &SnapshotRemoveRequest::handle_remove_snapshot>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::handle_remove_snapshot(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id,
                                                 m_next_snap_id));
    lderr(cct) << "failed to remove object map snapshot " << oid << ": "
               << cpp_strerror(r) << dendl;

    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    std::unique_lock image_locker{m_image_ctx.image_lock};
    invalidate_next_map();
    return;
  }

  std::shared_lock image_locker{m_image_ctx.image_lock};
  update_object_map();
  remove_map();
}

void SnapshotRemoveRequest::invalidate_next_map() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_wlocked(m_image_ctx.image_lock));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  auto ctx = librbd::util::create_context_callback<
    SnapshotRemoveRequest,
    &SnapshotRemoveRequest::handle_invalidate_next_map>(this);
  InvalidateRequest<> *req = new InvalidateRequest<>(m_image_ctx,
                                                     m_next_snap_id, true, ctx);
  req->send();
}

void SnapshotRemoveRequest::handle_invalidate_next_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id,
                                                 m_next_snap_id));
    lderr(cct) << "failed to invalidate object map " << oid << ": "
               << cpp_strerror(r) << dendl;
    complete(r);
    return;
  }

  remove_map();
}

void SnapshotRemoveRequest::remove_map() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 5) << "oid=" << oid << dendl;

  librados::ObjectWriteOperation op;
  op.remove();

  auto rados_completion = librbd::util::create_rados_callback<
    SnapshotRemoveRequest, &SnapshotRemoveRequest::handle_remove_map>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

void SnapshotRemoveRequest::handle_remove_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
    lderr(cct) << "failed to remove object map " << oid << ": "
               << cpp_strerror(r) << dendl;
    complete(r);
    return;
  }

  complete(0);
}

void SnapshotRemoveRequest::compute_next_snap_id() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));

  m_next_snap_id = CEPH_NOSNAP;
  std::map<librados::snap_t, SnapInfo>::const_iterator it =
    m_image_ctx.snap_info.find(m_snap_id);
  ceph_assert(it != m_image_ctx.snap_info.end());

  ++it;
  if (it != m_image_ctx.snap_info.end()) {
    m_next_snap_id = it->first;
  }
}

void SnapshotRemoveRequest::update_object_map() {
  assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  std::unique_lock object_map_locker{*m_object_map_lock};
  if (m_next_snap_id == m_image_ctx.snap_id && m_next_snap_id == CEPH_NOSNAP) {
    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << dendl;

    auto it = m_object_map.begin();
    auto end_it = m_object_map.end();
    auto snap_it = m_snap_object_map.begin();
    uint64_t i = 0;
    for (; it != end_it; ++it) {
      if (*it == OBJECT_EXISTS_CLEAN &&
          (i >= m_snap_object_map.size() ||
           *snap_it == OBJECT_EXISTS)) {
        *it = OBJECT_EXISTS;
      }
      if (i < m_snap_object_map.size()) {
        ++snap_it;
      }
      ++i;
    }
  }
}

} // namespace object_map
} // namespace librbd
