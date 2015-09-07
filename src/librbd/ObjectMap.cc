// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ObjectMap.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "librbd/object_map/ResizeRequest.h"
#include "librbd/object_map/SnapshotCreateRequest.h"
#include "librbd/object_map/SnapshotRemoveRequest.h"
#include "librbd/object_map/SnapshotRollbackRequest.h"
#include "librbd/object_map/UpdateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/lock/cls_lock_client.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMap: "

namespace librbd {

ObjectMap::ObjectMap(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), m_snap_id(CEPH_NOSNAP), m_enabled(false)
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

bool ObjectMap::enabled() const
{
  RWLock::RLocker l(m_image_ctx.object_map_lock);
  return m_enabled;
}

bool ObjectMap::enabled(const RWLock &object_map_lock) const {
  assert(m_image_ctx.object_map_lock.is_locked());
  return m_enabled;
}

int ObjectMap::lock()
{
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    return 0;
  }

  {
    RWLock::RLocker l(m_image_ctx.object_map_lock);
    if (!m_enabled) {
      return 0;
    }
  }

  bool broke_lock = false;
  CephContext *cct = m_image_ctx.cct;
  std::string oid(object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  while (true) {
    int r;
    ldout(cct, 10) << &m_image_ctx << " locking object map" << dendl;
    r = rados::cls::lock::lock(&m_image_ctx.md_ctx, oid,
			       RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "", "",
			       utime_t(), 0);
    if (r == 0) {
      break;
    } else if (broke_lock || r != -EBUSY) {
      lderr(cct) << "failed to lock object map: " << cpp_strerror(r) << dendl;
      return r;
    }

    typedef std::map<rados::cls::lock::locker_id_t,
                     rados::cls::lock::locker_info_t> lockers_t;
    lockers_t lockers;
    ClsLockType lock_type;
    std::string lock_tag;
    r = rados::cls::lock::get_lock_info(&m_image_ctx.md_ctx, oid,
                                        RBD_LOCK_NAME, &lockers,
                                        &lock_type, &lock_tag);
    if (r == -ENOENT) {
      continue;
    } else if (r < 0) {
      lderr(cct) << "failed to list object map locks: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 10) << "breaking current object map lock" << dendl;
    for (lockers_t::iterator it = lockers.begin();
         it != lockers.end(); ++it) {
      const rados::cls::lock::locker_id_t &locker = it->first;
      r = rados::cls::lock::break_lock(&m_image_ctx.md_ctx, oid,
                                       RBD_LOCK_NAME, locker.cookie,
                                       locker.locker);
      if (r < 0 && r != -ENOENT) {
        lderr(cct) << "failed to break object map lock: " << cpp_strerror(r)
                   << dendl;
        return r;
      }
    }



    broke_lock = true;
  }
  return 0;
}

int ObjectMap::unlock()
{
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    return 0;
  }

  ldout(m_image_ctx.cct, 10) << &m_image_ctx << " unlocking object map"
			     << dendl;
  std::string oid = object_map_name(m_image_ctx.id, CEPH_NOSNAP);
  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx, oid,
                                   RBD_LOCK_NAME, "");
  if (r < 0 && r != -ENOENT) {
    lderr(m_image_ctx.cct) << "failed to release object map lock: "
			   << cpp_strerror(r) << dendl;
  }
  return r;
}

bool ObjectMap::object_may_exist(uint64_t object_no) const
{
  // Fall back to default logic if object map is disabled or invalid
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP) ||
      m_image_ctx.test_flags(RBD_FLAG_OBJECT_MAP_INVALID)) {
    return true;
  }

  RWLock::RLocker l(m_image_ctx.object_map_lock);
  if (!m_enabled) {
    return true;
  }
  uint8_t state = (*this)[object_no];
  bool exists = (state != OBJECT_NONEXISTENT);
  ldout(m_image_ctx.cct, 20) << &m_image_ctx << " object_may_exist: "
			     << "object_no=" << object_no << " r=" << exists
			     << dendl;
  return exists;
}

void ObjectMap::refresh(uint64_t snap_id)
{
  assert(m_image_ctx.snap_lock.is_wlocked());
  RWLock::WLocker l(m_image_ctx.object_map_lock);
  m_snap_id = snap_id;

  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0 ||
      (m_image_ctx.snap_id == snap_id && !m_image_ctx.snap_exists)) {
    m_object_map.clear();
    m_enabled = false;
    return;
  }
  m_enabled = true;

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << &m_image_ctx << " refreshing object map" << dendl;

  uint64_t num_objs = Striper::get_num_objects(
    m_image_ctx.layout, m_image_ctx.get_image_size(snap_id));

  std::string oid(object_map_name(m_image_ctx.id, snap_id));
  int r = cls_client::object_map_load(&m_image_ctx.md_ctx, oid,
                                      &m_object_map);
  if (r == -EINVAL) {
    // object map is corrupt on-disk -- clear it and properly size it
    // so future IO can keep the object map in sync
    invalidate(snap_id);

    librados::ObjectWriteOperation op;
    if (snap_id == CEPH_NOSNAP) {
      rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "",
                                      "");
    }
    op.truncate(0);
    cls_client::object_map_resize(&op, num_objs, OBJECT_NONEXISTENT);

    r = m_image_ctx.md_ctx.operate(oid, &op);
    if (r == 0) {
      m_object_map.clear();
      object_map::ResizeRequest::resize(&m_object_map, num_objs,
                                        OBJECT_NONEXISTENT);
    }
  }
  if (r < 0) {
    lderr(cct) << "error refreshing object map: " << cpp_strerror(r)
               << dendl;
    invalidate(snap_id);
    m_object_map.clear();
    return;
  }

  ldout(cct, 20) << "refreshed object map: " << m_object_map.size()
                 << dendl;

  if (m_object_map.size() < num_objs) {
    lderr(cct) << "object map smaller than current object count: "
               << m_object_map.size() << " != " << num_objs << dendl;
    invalidate(snap_id);

    // correct the size issue so future IO can keep the object map in sync
    librados::ObjectWriteOperation op;
    if (snap_id == CEPH_NOSNAP) {
      rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "",
                                      "");
    }
    cls_client::object_map_resize(&op, num_objs, OBJECT_NONEXISTENT);

    r = m_image_ctx.md_ctx.operate(oid, &op);
    if (r == 0) {
      object_map::ResizeRequest::resize(&m_object_map, num_objs,
                                        OBJECT_NONEXISTENT);
    }
  } else if (m_object_map.size() > num_objs) {
    // resize op might have been interrupted
    ldout(cct, 1) << "object map larger than current object count: "
                  << m_object_map.size() << " != " << num_objs << dendl;
  }
}

void ObjectMap::rollback(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert(m_image_ctx.object_map_lock.is_wlocked());
  assert(m_enabled);

  object_map::SnapshotRollbackRequest *req =
    new object_map::SnapshotRollbackRequest(m_image_ctx, snap_id, on_finish);
  req->send();
}

void ObjectMap::snapshot_add(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);

  object_map::SnapshotCreateRequest *req =
    new object_map::SnapshotCreateRequest(m_image_ctx, &m_object_map, snap_id,
                                          on_finish);
  req->send();
}

void ObjectMap::snapshot_remove(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(snap_id != CEPH_NOSNAP);

  object_map::SnapshotRemoveRequest *req =
    new object_map::SnapshotRemoveRequest(m_image_ctx, &m_object_map, snap_id,
                                          on_finish);
  req->send();
}

void ObjectMap::aio_save(Context *on_finish)
{
  assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP));
  assert(m_image_ctx.owner_lock.is_locked());
  RWLock::RLocker object_map_locker(m_image_ctx.object_map_lock);

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_save(&op, m_object_map);

  std::string oid(object_map_name(m_image_ctx.id, m_snap_id));
  librados::AioCompletion *comp = librados::Rados::aio_create_completion(
    on_finish, NULL, rados_ctx_cb);

  int r = m_image_ctx.md_ctx.aio_operate(oid, comp, &op);
  assert(r == 0);
  comp->release();
}

void ObjectMap::aio_resize(uint64_t new_size, uint8_t default_object_state,
			   Context *on_finish) {
  assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP));
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.image_watcher != NULL);
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
         m_image_ctx.image_watcher->is_lock_owner());

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
  assert(!m_image_ctx.image_watcher->is_lock_supported(m_image_ctx.snap_lock) ||
         m_image_ctx.image_watcher->is_lock_owner());
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

void ObjectMap::invalidate(uint64_t snap_id) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  assert(m_image_ctx.object_map_lock.is_wlocked());
  uint64_t flags;
  m_image_ctx.get_flags(snap_id, &flags);
  if ((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
    return;
  }

  // TODO remove once all methods are async
  C_SaferCond cond_ctx;
  object_map::InvalidateRequest *req = new object_map::InvalidateRequest(
    m_image_ctx, m_snap_id, &cond_ctx);
  req->send();

  cond_ctx.wait();
}

} // namespace librbd
