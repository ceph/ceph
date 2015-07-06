// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ObjectMap.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
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
      resize(num_objs, OBJECT_NONEXISTENT);
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
      resize(num_objs, OBJECT_NONEXISTENT);
    }
  } else if (m_object_map.size() > num_objs) {
    // resize op might have been interrupted
    ldout(cct, 1) << "object map larger than current object count: "
                  << m_object_map.size() << " != " << num_objs << dendl;
  }
}

void ObjectMap::rollback(uint64_t snap_id) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  int r;
  std::string oid(object_map_name(m_image_ctx.id, CEPH_NOSNAP));

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << &m_image_ctx << " rollback object map" << dendl;

  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    r = m_image_ctx.md_ctx.remove(oid);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << "unable to remove object map: " << cpp_strerror(r)
		 << dendl;
    }
    return;
  }

  RWLock::WLocker l(m_image_ctx.object_map_lock);
  if (!m_enabled) {
    return;
  }

  std::string snap_oid(object_map_name(m_image_ctx.id, snap_id));
  bufferlist bl;
  r = m_image_ctx.md_ctx.read(snap_oid, bl, 0, 0);
  if (r < 0) {
    lderr(cct) << "unable to load snapshot object map '" << snap_oid << "': "
	       << cpp_strerror(r) << dendl;
    invalidate(snap_id);
    return;
  }

  librados::ObjectWriteOperation op;
  rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  op.write_full(bl);

  r = m_image_ctx.md_ctx.operate(oid, &op);
  if (r < 0) {
    lderr(cct) << "unable to rollback object map: " << cpp_strerror(r)
	       << dendl;
    invalidate(CEPH_NOSNAP);
  }
}

void ObjectMap::snapshot_add(uint64_t snap_id) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << &m_image_ctx << " snapshot object map" << dendl;

  int r;
  bufferlist bl;
  RWLock::WLocker l(m_image_ctx.object_map_lock);
  if (!m_enabled) {
    return;
  }
  std::string oid(object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  r = m_image_ctx.md_ctx.read(oid, bl, 0, 0);
  if (r < 0) {
    lderr(cct) << "unable to load object map: " << cpp_strerror(r)
	       << dendl;
    invalidate(CEPH_NOSNAP);
    return;
  }

  std::string snap_oid(object_map_name(m_image_ctx.id, snap_id));
  r = m_image_ctx.md_ctx.write_full(snap_oid, bl);
  if (r < 0) {
    lderr(cct) << "unable to snapshot object map '" << snap_oid << "': "
	       << cpp_strerror(r) << dendl;
    invalidate(snap_id);
    return;
  }

  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    librados::ObjectWriteOperation op;
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
    cls_client::object_map_snap_add(&op);
    r = m_image_ctx.md_ctx.operate(oid, &op);
    if (r < 0) {
      lderr(cct) << "unable to snapshot object map: " << cpp_strerror(r)
                 << dendl;
      invalidate(CEPH_NOSNAP);
      return;
    }

    for (uint64_t i = 0; i < m_object_map.size(); ++i) {
      if (m_object_map[i] == OBJECT_EXISTS) {
        m_object_map[i] = OBJECT_EXISTS_CLEAN;
      }
    }
  }
}

int ObjectMap::snapshot_remove(uint64_t snap_id) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  assert(snap_id != CEPH_NOSNAP);
  CephContext *cct = m_image_ctx.cct;

  int r;
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    RWLock::WLocker l(m_image_ctx.object_map_lock);

    uint64_t next_snap_id = CEPH_NOSNAP;
    std::map<librados::snap_t, SnapInfo>::const_iterator it =
      m_image_ctx.snap_info.find(snap_id);
    assert(it != m_image_ctx.snap_info.end());

    ++it;
    if (it != m_image_ctx.snap_info.end()) {
      next_snap_id = it->first;
    }

    ceph::BitVector<2> snap_object_map;
    std::string snap_oid(object_map_name(m_image_ctx.id, snap_id));
    r = cls_client::object_map_load(&m_image_ctx.md_ctx, snap_oid,
                                    &snap_object_map);
    if (r < 0) {
      lderr(cct) << "error loading snapshot object map: " << cpp_strerror(r)
                 << dendl;
    }

    if (r == 0) {
      uint64_t flags;
      m_image_ctx.get_flags(snap_id, &flags);
      if ((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
        invalidate(next_snap_id);
        r = -EINVAL;
      }
    }

    if (r == 0) {
      std::string oid(object_map_name(m_image_ctx.id, next_snap_id));
      librados::ObjectWriteOperation op;
      if (next_snap_id == CEPH_NOSNAP) {
        rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "",
                                        "");
      }
      cls_client::object_map_snap_remove(&op, snap_object_map);

      r = m_image_ctx.md_ctx.operate(oid, &op);
      if (r < 0) {
        lderr(cct) << "unable to remove object map snapshot: "
                   << cpp_strerror(r) << dendl;
        invalidate(next_snap_id);
      }
    }

    if (r == 0 && next_snap_id == CEPH_NOSNAP) {
      for (uint64_t i = 0; i < m_object_map.size(); ++i) {
        if (m_object_map[i] == OBJECT_EXISTS_CLEAN &&
            (i >= snap_object_map.size() ||
             snap_object_map[i] == OBJECT_EXISTS)) {
          m_object_map[i] = OBJECT_EXISTS;
        }
      }
    }
  }

  std::string oid(object_map_name(m_image_ctx.id, snap_id));
  r = m_image_ctx.md_ctx.remove(oid);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
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

  ResizeRequest *req = new ResizeRequest(
    m_image_ctx, m_snap_id, new_size, default_object_state, on_finish);
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
  UpdateRequest *req = new UpdateRequest(m_image_ctx, snap_id,
                                         start_object_no, end_object_no,
                                         new_state, current_state,
                                         on_finish);
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

  flags = RBD_FLAG_OBJECT_MAP_INVALID;
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    flags |= RBD_FLAG_FAST_DIFF_INVALID;
  }

  CephContext *cct = m_image_ctx.cct;
  lderr(cct) << &m_image_ctx << " invalidating object map" << dendl;
  int r = m_image_ctx.update_flags(snap_id, flags, true);
  if (r < 0) {
    lderr(cct) << "failed to invalidate in-memory object map: "
               << cpp_strerror(r) << dendl;
    return;
  }

  // do not update on-disk flags if not image owner
  if (m_image_ctx.image_watcher == NULL ||
      (m_image_ctx.image_watcher->is_lock_supported(m_image_ctx.snap_lock) &&
       !m_image_ctx.image_watcher->is_lock_owner())) {
    return;
  }

  librados::ObjectWriteOperation op;
  if (snap_id == CEPH_NOSNAP) {
    m_image_ctx.image_watcher->assert_header_locked(&op);
  }
  cls_client::set_flags(&op, snap_id, flags, flags);

  r = m_image_ctx.md_ctx.operate(m_image_ctx.header_oid, &op);
  if (r == -EBUSY) {
    ldout(cct, 5) << "skipping on-disk object map invalidation: "
                  << "image not locked by client" << dendl;
  } else if (r < 0) {
    lderr(cct) << "failed to invalidate on-disk object map: " << cpp_strerror(r)
	       << dendl;
  }
}

void ObjectMap::resize(uint64_t num_objs, uint8_t defualt_state) {
  size_t orig_object_map_size = m_object_map.size();
  m_object_map.resize(num_objs);
  for (uint64_t i = orig_object_map_size;
       i < m_object_map.size(); ++i) {
    m_object_map[i] = defualt_state;
  }
}

bool ObjectMap::Request::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << &m_image_ctx << " should_complete: r=" << r << dendl;

  switch (m_state)
  {
  case STATE_REQUEST:
    if (r == -EBUSY) {
      lderr(cct) << "object map lock not owned by client" << dendl;
      return true;
    } else if (r < 0) {
      lderr(cct) << "failed to update object map: " << cpp_strerror(r)
		 << dendl;
      return invalidate();
    }

    {
      RWLock::WLocker l2(m_image_ctx.object_map_lock);
      finish(&m_image_ctx.object_map);
    }
    return true;

  case STATE_INVALIDATE:
    ldout(cct, 20) << "INVALIDATE" << dendl;
    if (r < 0) {
      lderr(cct) << "failed to invalidate object map: " << cpp_strerror(r)
		 << dendl;
    }
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return false;
}

bool ObjectMap::Request::invalidate() {
  if (m_image_ctx.test_flags(RBD_FLAG_OBJECT_MAP_INVALID)) {
    return true;
  }

  CephContext *cct = m_image_ctx.cct;
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);

  // requests shouldn't be running while using snapshots
  assert(m_image_ctx.snap_id == CEPH_NOSNAP);

  uint64_t flags = RBD_FLAG_OBJECT_MAP_INVALID;
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    flags |= RBD_FLAG_FAST_DIFF_INVALID;
  }

  lderr(cct) << &m_image_ctx << " invalidating object map" << dendl;
  m_state = STATE_INVALIDATE;
  m_image_ctx.flags |= flags;

  librados::ObjectWriteOperation op;
  m_image_ctx.image_watcher->assert_header_locked(&op);
  cls_client::set_flags(&op, CEPH_NOSNAP, flags, flags);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
					 rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

void ObjectMap::ResizeRequest::send() {
  CephContext *cct = m_image_ctx.cct;

  RWLock::WLocker l(m_image_ctx.object_map_lock);
  m_num_objs = Striper::get_num_objects(m_image_ctx.layout, m_new_size);

  ldout(cct, 5) << &m_image_ctx << " resizing on-disk object map: "
		<< m_num_objs << dendl;

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_resize(&op, m_num_objs, m_default_object_state);

  librados::AioCompletion *rados_completion = create_callback_completion();
  std::string oid(object_map_name(m_image_ctx.id, m_snap_id));
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void ObjectMap::ResizeRequest::finish(ObjectMap *object_map) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << &m_image_ctx << " resizing in-memory object map: "
		<< m_num_objs << dendl;
  object_map->resize(m_num_objs, m_default_object_state);
}

void ObjectMap::UpdateRequest::send() {
  assert(m_image_ctx.object_map_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  // safe to update in-memory state first without handling rollback since any
  // failures will invalidate the object map
  ldout(cct, 20) << &m_image_ctx << " updating object map"
                 << (m_snap_id != CEPH_NOSNAP ?
                       " snap " + stringify(m_snap_id) : std::string())
                 << ": ["
		 << m_start_object_no << "," << m_end_object_no << ") = "
		 << (m_current_state ?
		       stringify(static_cast<uint32_t>(*m_current_state)) : "")
		 << "->" << static_cast<uint32_t>(m_new_state)
		 << dendl;

  ObjectMap& object_map = m_image_ctx.object_map;
  if (m_snap_id == object_map.m_snap_id) {
    assert(m_image_ctx.object_map_lock.is_wlocked());
    for (uint64_t object_no = m_start_object_no;
         object_no < MIN(m_end_object_no, object_map.m_object_map.size());
         ++object_no) {
      uint8_t state = object_map.m_object_map[object_no];
      if (!m_current_state || state == *m_current_state ||
          (*m_current_state == OBJECT_EXISTS && state == OBJECT_EXISTS_CLEAN)) {
        object_map.m_object_map[object_no] = m_new_state;
      }
    }
  }

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_update(&op, m_start_object_no, m_end_object_no,
				m_new_state, m_current_state);

  librados::AioCompletion *rados_completion = create_callback_completion();
  std::string oid(object_map_name(m_image_ctx.id, m_snap_id));
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

void ObjectMap::UpdateRequest::finish(ObjectMap *object_map) {
  ldout(m_image_ctx.cct, 20) << &m_image_ctx << " on-disk object map updated"
                             << dendl;
}

} // namespace librbd
