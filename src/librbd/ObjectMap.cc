// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ObjectMap.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMap: "

namespace librbd {

ObjectMap::ObjectMap(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx)
{
}

int ObjectMap::lock()
{
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return 0;
  }

  int r;
  bool broke_lock = false;
  CephContext *cct = m_image_ctx.cct;
  while (true) {
    ldout(cct, 10) << "locking object map" << dendl;
    r = rados::cls::lock::lock(&m_image_ctx.md_ctx,
			       object_map_name(m_image_ctx.id),
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
    int r = rados::cls::lock::get_lock_info(&m_image_ctx.md_ctx,
					    object_map_name(m_image_ctx.id),
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
      r = rados::cls::lock::break_lock(&m_image_ctx.md_ctx,
				       object_map_name(m_image_ctx.id),
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
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return 0;
  }

  int r = rados::cls::lock::unlock(&m_image_ctx.md_ctx,
				   object_map_name(m_image_ctx.id),
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
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0 ||
      ((m_image_ctx.flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0)) {
    return true;
  }

  RWLock::RLocker l(m_image_ctx.object_map_lock);
  assert(object_no < object_map.size());
  return (object_map[object_no] == OBJECT_EXISTS ||
          object_map[object_no] == OBJECT_PENDING);
}

int ObjectMap::refresh()
{ 
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return 0;
  }
  
  RWLock::WLocker l(m_image_ctx.object_map_lock);
  CephContext *cct = m_image_ctx.cct;
  int r = cls_client::object_map_load(&m_image_ctx.data_ctx,
				      object_map_name(m_image_ctx.id),
                                      &object_map);
  if (r < 0) { 
    lderr(cct) << "error refreshing object map: " << cpp_strerror(r)
               << dendl;
    invalidate();
    object_map.clear();
    return r;
  }
  
  ldout(cct, 20) << "refreshed object map: " << object_map.size()
                 << dendl;
  
  uint64_t num_objs = Striper::get_num_objects(m_image_ctx.layout,
					       m_image_ctx.get_current_size());
  if (object_map.size() != num_objs) {
    // resize op might have been interrupted
    lderr(cct) << "incorrect object map size: " << object_map.size()
               << " != " << num_objs << dendl;
    invalidate();
    return -EINVAL;
  }
  return 0;
}
int ObjectMap::resize(uint8_t default_object_state)
{
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return 0;
  }

  RWLock::WLocker l(m_image_ctx.object_map_lock);
  CephContext *cct = m_image_ctx.cct;
  uint64_t num_objs = Striper::get_num_objects(m_image_ctx.layout,
                                               m_image_ctx.get_current_size());
  ldout(cct, 20) << "resizing object map: " << num_objs << dendl;
  librados::ObjectWriteOperation op;
  rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  cls_client::object_map_resize(&op, num_objs, default_object_state);
  int r = m_image_ctx.data_ctx.operate(object_map_name(m_image_ctx.id), &op);
  if (r == -EBUSY) {
    lderr(cct) << "object map lock not owned by client" << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "error resizing object map: size=" << num_objs << ", "
               << "state=" << default_object_state << ", "
               << "error=" << cpp_strerror(r) << dendl;
    invalidate();
    return 0;
  }

  size_t orig_object_map_size = object_map.size();
  object_map.resize(num_objs);
  for (uint64_t i = orig_object_map_size; i < object_map.size(); ++i) {
    object_map[i] = default_object_state;
  }
  return 0;
}

int ObjectMap::update(uint64_t object_no, uint8_t new_state,
		      const boost::optional<uint8_t> &current_state)
{
  return update(object_no, object_no + 1, new_state, current_state);
}

int ObjectMap::update(uint64_t start_object_no,
                      uint64_t end_object_no, uint8_t new_state,
                      const boost::optional<uint8_t> &current_state)
{
  if ((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) == 0) {
    return 0;
  }

  RWLock::WLocker l(m_image_ctx.object_map_lock);
  CephContext *cct = m_image_ctx.cct;
  assert(start_object_no <= end_object_no);
  assert(end_object_no <= object_map.size() ||
         (m_image_ctx.flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0);
  if (end_object_no > object_map.size()) {
    ldout(cct, 20) << "skipping update of invalid object map" << dendl;
    return 0;
  }

  bool update_required = false;
  for (uint64_t object_no = start_object_no; object_no < end_object_no;
       ++object_no) {
    if ((!current_state || object_map[object_no] == *current_state) &&
        object_map[object_no] != new_state) {
      update_required = true;
      break;
    }
  }

  if (!update_required) {
    return 0;
  }

  ldout(cct, 20) << "updating object map: [" << start_object_no << ","
                 << end_object_no << ") = "
                 << static_cast<uint32_t>(new_state) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  cls_client::object_map_update(&op, start_object_no, end_object_no,
                                new_state, current_state);
  int r = m_image_ctx.data_ctx.operate(object_map_name(m_image_ctx.id), &op);
  if (r == -EBUSY) {
    lderr(cct) << "object map lock not owned by client" << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "object map update failed: " << cpp_strerror(r) << dendl;
    invalidate();
  } else {
    for (uint64_t object_no = start_object_no; object_no < end_object_no;
         ++object_no) {
      if (!current_state || object_map[object_no] == *current_state) {
        object_map[object_no] = new_state;
      }
    }
  }
  return 0;
}

void ObjectMap::invalidate()
{
  // TODO: md_lock
  m_image_ctx.flags |= RBD_FLAG_OBJECT_MAP_INVALID;
  int r = cls_client::set_flags(&m_image_ctx.md_ctx,
				m_image_ctx.header_oid,
				m_image_ctx.flags,
                                RBD_FLAG_OBJECT_MAP_INVALID);
  if (r < 0) {
    lderr(m_image_ctx.cct) << "Failed to invalidate object map: "
			   << cpp_strerror(r) << dendl;
  }
}

} // namespace librbd
