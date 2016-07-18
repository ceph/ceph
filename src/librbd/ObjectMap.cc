// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ObjectMap.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "librbd/object_map/RefreshRequest.h"
#include "librbd/object_map/ResizeRequest.h"
#include "librbd/object_map/SnapshotCreateRequest.h"
#include "librbd/object_map/SnapshotRemoveRequest.h"
#include "librbd/object_map/SnapshotRollbackRequest.h"
#include "librbd/object_map/UnlockRequest.h"
#include "librbd/object_map/UpdateRequest.h"
#include "librbd/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/stringify.h"
#include "osdc/Striper.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMap: "

namespace librbd {

ObjectMap::ObjectMap(ImageCtx &image_ctx, uint64_t snap_id)
  : m_image_ctx(image_ctx), m_snap_id(snap_id)
{
}

int ObjectMap::remove(librados::IoCtx &io_ctx, const std::string &image_id) {
  return io_ctx.remove(object_map_name(image_id, CEPH_NOSNAP));
}

std::string ObjectMap::object_map_name(const std::string &image_id,
				       uint64_t snap_id) {
  std::string oid(RBD_OBJECT_MAP_PREFIX + image_id);
  if (snap_id != CEPH_NOSNAP) {
    std::stringstream snap_suffix;
    snap_suffix << "." << std::setfill('0') << std::setw(16) << std::hex
		<< snap_id;
    oid += snap_suffix.str();
  }
  return oid;
}

bool ObjectMap::is_compatible(const file_layout_t& layout, uint64_t size) {
  uint64_t object_count = Striper::get_num_objects(layout, size);
  return (object_count <= cls::rbd::MAX_OBJECT_MAP_OBJECT_COUNT);
}

ceph::BitVector<2u>::Reference ObjectMap::operator[](uint64_t object_no)
{
  assert(m_image_ctx.object_map_lock.is_wlocked());
  assert(object_no < m_object_map.size());
  return m_object_map[object_no];
}

uint8_t ObjectMap::operator[](uint64_t object_no) const
{
  assert(m_image_ctx.object_map_lock.is_locked());
  assert(object_no < m_object_map.size());
  return m_object_map[object_no];
}

bool ObjectMap::object_may_exist(uint64_t object_no) const
{
  assert(m_image_ctx.snap_lock.is_locked());

  // Fall back to default logic if object map is disabled or invalid
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                 m_image_ctx.snap_lock) ||
      m_image_ctx.test_flags(RBD_FLAG_OBJECT_MAP_INVALID,
                             m_image_ctx.snap_lock)) {
    return true;
  }

  RWLock::RLocker l(m_image_ctx.object_map_lock);
  uint8_t state = (*this)[object_no];
  bool exists = (state != OBJECT_NONEXISTENT);
  ldout(m_image_ctx.cct, 20) << &m_image_ctx << " object_may_exist: "
			     << "object_no=" << object_no << " r=" << exists
			     << dendl;
  return exists;
}

bool ObjectMap::update_required(uint64_t object_no, uint8_t new_state) {
  assert(m_image_ctx.object_map_lock.is_wlocked());
  uint8_t state = (*this)[object_no];

  if ((state == new_state) ||
      (new_state == OBJECT_PENDING && state == OBJECT_NONEXISTENT) ||
      (new_state == OBJECT_NONEXISTENT && state != OBJECT_PENDING)) {
    return false;
  }
  return true;
}

void ObjectMap::open(Context *on_finish) {
  object_map::RefreshRequest<> *req = new object_map::RefreshRequest<>(
    m_image_ctx, &m_object_map, m_snap_id, on_finish);
  req->send();
}

void ObjectMap::close(Context *on_finish) {
  if (m_snap_id != CEPH_NOSNAP) {
    m_image_ctx.op_work_queue->queue(on_finish, 0);
    return;
  }

  object_map::UnlockRequest<> *req = new object_map::UnlockRequest<>(
    m_image_ctx, on_finish);
  req->send();
}

void ObjectMap::rollback(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.object_map_lock.is_wlocked());

  object_map::SnapshotRollbackRequest *req =
    new object_map::SnapshotRollbackRequest(m_image_ctx, snap_id, on_finish);
  req->send();
}

void ObjectMap::snapshot_add(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(snap_id != CEPH_NOSNAP);

  object_map::SnapshotCreateRequest *req =
    new object_map::SnapshotCreateRequest(m_image_ctx, &m_object_map, snap_id,
                                          on_finish);
  req->send();
}

void ObjectMap::snapshot_remove(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(snap_id != CEPH_NOSNAP);

  object_map::SnapshotRemoveRequest *req =
    new object_map::SnapshotRemoveRequest(m_image_ctx, &m_object_map, snap_id,
                                          on_finish);
  req->send();
}

void ObjectMap::aio_save(Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                   m_image_ctx.snap_lock));
  RWLock::RLocker object_map_locker(m_image_ctx.object_map_lock);

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_save(&op, m_object_map);

  std::string oid(object_map_name(m_image_ctx.id, m_snap_id));
  librados::AioCompletion *comp = util::create_rados_safe_callback(on_finish);

  int r = m_image_ctx.md_ctx.aio_operate(oid, comp, &op);
  assert(r == 0);
  comp->release();
}

void ObjectMap::aio_resize(uint64_t new_size, uint8_t default_object_state,
			   Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                   m_image_ctx.snap_lock));
  assert(m_image_ctx.image_watcher != NULL);
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());

  object_map::ResizeRequest *req = new object_map::ResizeRequest(
    m_image_ctx, &m_object_map, m_snap_id, new_size, default_object_state,
    on_finish);
  req->send();
}

bool ObjectMap::aio_update(uint64_t object_no, uint8_t new_state,
			   const boost::optional<uint8_t> &current_state,
			   Context *on_finish)
{
  return aio_update(object_no, object_no + 1, new_state, current_state,
		    on_finish);
}

bool ObjectMap::aio_update(uint64_t start_object_no, uint64_t end_object_no,
			   uint8_t new_state,
                           const boost::optional<uint8_t> &current_state,
                           Context *on_finish)
{
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.image_watcher != NULL);
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());
  assert(m_image_ctx.object_map_lock.is_wlocked());
  assert(start_object_no < end_object_no);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << &m_image_ctx << " aio_update: start=" << start_object_no
		 << ", end=" << end_object_no << ", "
                 << (current_state ?
                       stringify(static_cast<uint32_t>(*current_state)) : "")
		 << "->" << static_cast<uint32_t>(new_state) << dendl;
  if (end_object_no > m_object_map.size()) {
    ldout(cct, 20) << "skipping update of invalid object map" << dendl;
    return false;
  }

  for (uint64_t object_no = start_object_no; object_no < end_object_no;
       ++object_no) {
    uint8_t state = m_object_map[object_no];
    if ((!current_state || state == *current_state ||
          (*current_state == OBJECT_EXISTS && state == OBJECT_EXISTS_CLEAN)) &&
        state != new_state) {
      aio_update(m_snap_id, start_object_no, end_object_no, new_state,
                 current_state, on_finish);
      return true;
    }
  }
  return false;
}

void ObjectMap::aio_update(uint64_t snap_id, uint64_t start_object_no,
                           uint64_t end_object_no, uint8_t new_state,
                           const boost::optional<uint8_t> &current_state,
                           Context *on_finish) {
  object_map::UpdateRequest *req = new object_map::UpdateRequest(
    m_image_ctx, &m_object_map, snap_id, start_object_no, end_object_no,
    new_state, current_state, on_finish);
  req->send();
}

} // namespace librbd
