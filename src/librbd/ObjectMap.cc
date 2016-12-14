// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/ObjectMap.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/object_map/RefreshRequest.h"
#include "librbd/object_map/ResizeRequest.h"
#include "librbd/object_map/SnapshotCreateRequest.h"
#include "librbd/object_map/SnapshotRemoveRequest.h"
#include "librbd/object_map/SnapshotRollbackRequest.h"
#include "librbd/object_map/UnlockRequest.h"
#include "librbd/object_map/UpdateRequest.h"
#include "librbd/object_map/BatchUpdateRequest.h"
#include "librbd/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"

#include "include/rados/librados.hpp"

#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/stringify.h"
#include "osdc/Striper.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMap: "

namespace librbd {

namespace {

struct C_ObjectMapViewSetSize : public Context {
  ObjectMapView *object_map_view;
  uint8_t state;
  Context *on_finish;

  C_ObjectMapViewSetSize(ObjectMapView *_object_map_view,
                         uint8_t _state, Context *_on_finish)
    : object_map_view(_object_map_view), state(_state), on_finish(_on_finish) {}

  virtual void finish(int r) {
    if (r < 0) {
      on_finish->complete(r);
      return;
    }

    object_map_view->resize(object_map_view->size(), state);
    on_finish->complete(0);
  }
};


} // anonymous namespace

ceph::BitVector<2u>::Reference ObjectMapView::operator[](uint64_t object_no) {
  assert(object_no < m_level0_view.size());

  for (int i = OBJECT_MAP_VIEW_LEVELS-1; i >= 0; --i) {
    View *level = &m_level_view[i];
    if (level->m_tracked && (level->m_lookup[object_no] == 1)) {
      return level->m_map_track[object_no];
    }
  }

  return m_level0_view[object_no];
}

uint8_t ObjectMapView::operator[](uint64_t object_no) const {
  assert(object_no < m_level0_view.size());

  for (int i = OBJECT_MAP_VIEW_LEVELS-1; i >= 0; --i) {
    const View *level = &m_level_view[i];
    if (level->m_tracked && (level->m_lookup[object_no] == 1)) {
      return level->m_map_track[object_no];
    }
  }

  return m_level0_view[object_no];
}

void ObjectMapView::sync_view(ImageCtx &image_ctx, uint64_t snap_id,
                              uint8_t view_idx, Context *on_finish) {
  assert(view_idx < OBJECT_MAP_VIEW_LEVELS);

  View *level = &m_level_view[view_idx];
  if (!level->m_tracked) {
    on_finish->complete(0);
    return;
  }

  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  RWLock::WLocker object_map_locker(image_ctx.object_map_lock);
  object_map::UpdateRequest *req = new object_map::UpdateRequest(
    image_ctx, &m_level0_view, snap_id, *(level->m_object_start), level->m_object_end+1,
    level->m_new_state, level->m_current_state, on_finish);
  req->send();
}

void ObjectMapView::reset_view(uint8_t view_idx) {
  assert(view_idx < OBJECT_MAP_VIEW_LEVELS);

  View *level = &m_level_view[view_idx];

  level->m_tracked = 0;
  level->m_object_start = boost::none;
  level->m_object_end = 0;
  level->m_current_state = boost::none;
  level->m_new_state = 0;
}

void ObjectMapView::update_view(uint64_t object_no,
                                const boost::optional<uint8_t> &current_state,
                                uint8_t new_state, uint8_t view_idx) {
  assert(view_idx < OBJECT_MAP_VIEW_LEVELS);
  assert(object_no < m_level0_view.size());

  View *level = &m_level_view[view_idx];

  ++level->m_tracked;
  level->m_lookup[object_no] = 1;
  level->m_map_track[object_no] = new_state;
  if (!level->m_object_start) {
    level->m_object_start = level->m_object_end = object_no;
    level->m_current_state = current_state;
    level->m_new_state = new_state;
  } else {
    assert(new_state == level->m_new_state);

    if (object_no < level->m_object_start) {
      level->m_object_start = object_no;
    } else if (object_no > level->m_object_end) {
      level->m_object_end = object_no;
    }
  }
}

void ObjectMapView::apply_view() {
  for (int i = 0; i < OBJECT_MAP_VIEW_LEVELS; ++i) {
    View *level = &m_level_view[i];
    if (!level->m_tracked) {
      continue;
    }

    for (uint64_t object_no = *(level->m_object_start); object_no <= level->m_object_end;
         ++object_no) {
      if (level->m_lookup[object_no] == 0) {
        continue;
      }

      --level->m_tracked;
      level->m_lookup[object_no] = 0;
      level->m_map_track[object_no] = OBJECT_NONEXISTENT;
      m_level0_view[object_no] = level->m_new_state;
    }

    assert(level->m_tracked == 0);
    reset_view(i);
  }
}

bool ObjectMapView::in_batch_mode(uint64_t object_no) {
  for (int i = 0; i < OBJECT_MAP_VIEW_LEVELS; ++i) {
    View *level = &m_level_view[i];
    if (level->m_tracked && ((object_no > level->m_object_start) &&
                             (object_no < level->m_object_end))) {
      return true;
    }
  }

  return false;
}

void ObjectMapView::resize(uint64_t new_size, uint8_t object_state) {
  for (int i = 0; i < OBJECT_MAP_VIEW_LEVELS; ++i) {
    View *level = &m_level_view[i];

    reset_view(i);

    // resize map and object lookup for this level
    level->m_lookup.clear();
    level->m_lookup.resize(new_size);
    object_map::ResizeRequest::resize(&level->m_map_track, new_size, object_state);
  }
}

uint32_t ObjectMapView::batch_size() {
  uint32_t batched = 0;
  for (int i = 0; i < OBJECT_MAP_VIEW_LEVELS; ++i) {
    View *level = &m_level_view[i];
    batched += level->m_tracked;
  }

  return batched;
}

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
  return m_object_map[object_no];
}

uint8_t ObjectMap::operator[](uint64_t object_no) const
{
  assert(m_image_ctx.object_map_lock.is_locked());
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
  ceph::BitVector<2> *object_map = level0_map();
  C_ObjectMapViewSetSize *ctx = new C_ObjectMapViewSetSize(
    &m_object_map, OBJECT_NONEXISTENT, on_finish);

  object_map::RefreshRequest<> *req = new object_map::RefreshRequest<>(
    m_image_ctx, object_map, m_snap_id, ctx);
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

  ceph::BitVector<2> *object_map = level0_map();
  object_map::SnapshotCreateRequest *req =
    new object_map::SnapshotCreateRequest(m_image_ctx, object_map, snap_id,
                                          on_finish);
  req->send();
}

void ObjectMap::snapshot_remove(uint64_t snap_id, Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_wlocked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(snap_id != CEPH_NOSNAP);

  ceph::BitVector<2> *object_map = level0_map();
  object_map::SnapshotRemoveRequest *req =
    new object_map::SnapshotRemoveRequest(m_image_ctx, object_map, snap_id,
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

  ceph::BitVector<2> *object_map = level0_map();
  cls_client::object_map_save(&op, *object_map);

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

  ceph::BitVector<2> *object_map = level0_map();
  C_ObjectMapViewSetSize  *ctx = new C_ObjectMapViewSetSize(
    &m_object_map, default_object_state, on_finish);
  object_map::ResizeRequest *req = new object_map::ResizeRequest(
    m_image_ctx, object_map, m_snap_id, new_size, default_object_state, ctx);
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
  assert(m_image_ctx.image_watcher != NULL);
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());
  assert(m_image_ctx.object_map_lock.is_wlocked());
  assert(start_object_no < end_object_no);
  assert(!m_object_map.in_batch_mode(start_object_no));
  assert(!m_object_map.in_batch_mode(end_object_no));

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

  ceph::BitVector<2> *object_map = level0_map();
  for (uint64_t object_no = start_object_no; object_no < end_object_no;
       ++object_no) {
    uint8_t state = (*object_map)[object_no];
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
  ceph::BitVector<2> *object_map = level0_map();
  object_map::UpdateRequest *req = new object_map::UpdateRequest(
    m_image_ctx, object_map, snap_id, start_object_no, end_object_no,
    new_state, current_state, on_finish);
  req->send();
}

bool ObjectMap::aio_batch(uint64_t object_no, uint8_t new_state,
                          const boost::optional<uint8_t> &current_state, uint8_t view_idx) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(m_image_ctx.image_watcher != NULL);
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());
  assert(m_image_ctx.object_map_lock.is_wlocked());
  assert(view_idx < OBJECT_MAP_VIEW_LEVELS);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << &m_image_ctx << " aio_batch: start=" << object_no
                 << ", " << (current_state ?
                             stringify(static_cast<uint32_t>(*current_state)) : "")
                 << "->" << static_cast<uint32_t>(new_state) << ", view idx="
                 << static_cast<uint32_t>(view_idx) << dendl;

  uint8_t state = m_object_map[object_no];
  if ((!current_state || state == *current_state ||
       (*current_state == OBJECT_EXISTS && state == OBJECT_EXISTS_CLEAN)) &&
      state != new_state) {
    m_object_map.update_view(object_no, current_state, new_state, view_idx);
    return true;
  }

  return false;
}

void ObjectMap::aio_update_batch(Context *on_finish) {
  assert(m_image_ctx.snap_lock.is_locked());
  assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  assert(m_image_ctx.image_watcher != NULL);
  assert(m_image_ctx.exclusive_lock == nullptr ||
         m_image_ctx.exclusive_lock->is_lock_owner());
  assert(m_image_ctx.object_map_lock.is_wlocked());

  object_map::BatchUpdateRequest<> *req = new object_map::BatchUpdateRequest<>(
    m_image_ctx, &m_object_map, m_snap_id, on_finish);
  req->send();
}

uint32_t ObjectMap::batch_size() {
  return m_object_map.batch_size();
}

} // namespace librbd
