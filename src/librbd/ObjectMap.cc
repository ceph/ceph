// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ObjectMap.h"
#include "librbd/BlockGuard.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"
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

#include "include/rados/librados.hpp"

#include "cls/lock/cls_lock_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/stringify.h"
#include "osdc/Striper.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ObjectMap: " << this << " " << __func__ \
                           << ": "

namespace librbd {

using librbd::util::create_context_callback;

template <typename I>
ObjectMap<I>::ObjectMap(I &image_ctx, uint64_t snap_id)
  : RefCountedObject(image_ctx.cct),
    m_image_ctx(image_ctx), m_snap_id(snap_id),
    m_lock(ceph::make_shared_mutex(util::unique_lock_name("librbd::ObjectMap::lock", this))),
    m_update_guard(new UpdateGuard(m_image_ctx.cct)) {
}

template <typename I>
ObjectMap<I>::~ObjectMap() {
  delete m_update_guard;
}

template <typename I>
int ObjectMap<I>::aio_remove(librados::IoCtx &io_ctx, const std::string &image_id,
			     librados::AioCompletion *c) {
  return io_ctx.aio_remove(object_map_name(image_id, CEPH_NOSNAP), c);
}

template <typename I>
std::string ObjectMap<I>::object_map_name(const std::string &image_id,
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

template <typename I>
bool ObjectMap<I>::is_compatible(const file_layout_t& layout, uint64_t size) {
  uint64_t object_count = Striper::get_num_objects(layout, size);
  return (object_count <= cls::rbd::MAX_OBJECT_MAP_OBJECT_COUNT);
}

template <typename I>
uint8_t ObjectMap<I>::operator[](uint64_t object_no) const
{
  std::shared_lock locker{m_lock};
  ceph_assert(object_no < m_object_map.size());
  return m_object_map[object_no];
}

template <typename I>
bool ObjectMap<I>::object_may_exist(uint64_t object_no) const
{
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));

  // Fall back to default logic if object map is disabled or invalid
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                 m_image_ctx.image_lock)) {
    return true;
  }

  bool flags_set;
  int r = m_image_ctx.test_flags(m_image_ctx.snap_id,
                                 RBD_FLAG_OBJECT_MAP_INVALID,
                                 m_image_ctx.image_lock, &flags_set);
  if (r < 0 || flags_set) {
    return true;
  }

  uint8_t state = (*this)[object_no];
  bool exists = (state != OBJECT_NONEXISTENT);
  ldout(m_image_ctx.cct, 20) << "object_no=" << object_no << " r=" << exists
			     << dendl;
  return exists;
}

template <typename I>
bool ObjectMap<I>::object_may_not_exist(uint64_t object_no) const
{
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));

  // Fall back to default logic if object map is disabled or invalid
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                 m_image_ctx.image_lock)) {
    return true;
  }

  bool flags_set;
  int r = m_image_ctx.test_flags(m_image_ctx.snap_id,
                                 RBD_FLAG_OBJECT_MAP_INVALID,
                                 m_image_ctx.image_lock, &flags_set);
  if (r < 0 || flags_set) {
    return true;
  }

  uint8_t state = (*this)[object_no];
  bool nonexistent = (state != OBJECT_EXISTS && state != OBJECT_EXISTS_CLEAN);
  ldout(m_image_ctx.cct, 20) << "object_no=" << object_no << " r="
                             << nonexistent << dendl;
  return nonexistent;
}

template <typename I>
bool ObjectMap<I>::update_required(const ceph::BitVector<2>::Iterator& it,
                                   uint8_t new_state) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  uint8_t state = *it;
  if ((state == new_state) ||
      (new_state == OBJECT_PENDING && state == OBJECT_NONEXISTENT) ||
      (new_state == OBJECT_NONEXISTENT && state != OBJECT_PENDING)) {
    return false;
  }
  return true;
}

template <typename I>
void ObjectMap<I>::open(Context *on_finish) {
  Context *ctx = create_context_callback<Context>(on_finish, this);

  auto req = object_map::RefreshRequest<I>::create(
    m_image_ctx, &m_lock, &m_object_map, m_snap_id, ctx);
  req->send();
}

template <typename I>
void ObjectMap<I>::close(Context *on_finish) {
  Context *ctx = create_context_callback<Context>(on_finish, this);

  if (m_snap_id != CEPH_NOSNAP) {
    m_image_ctx.op_work_queue->queue(ctx, 0);
    return;
  }

  ctx = new LambdaContext([this, ctx](int r) {
      auto req = object_map::UnlockRequest<I>::create(m_image_ctx, ctx);
      req->send();
    });

  // ensure the block guard for aio updates is empty before unlocking
  // the object map
  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
bool ObjectMap<I>::set_object_map(ceph::BitVector<2> &target_object_map) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_image_ctx.image_lock));
  std::unique_lock locker{m_lock};
  m_object_map = target_object_map;
  return true;
}

template <typename I>
void ObjectMap<I>::rollback(uint64_t snap_id, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));

  std::unique_lock locker{m_lock};
  Context *ctx = create_context_callback<Context>(on_finish, this);

  object_map::SnapshotRollbackRequest *req =
    new object_map::SnapshotRollbackRequest(m_image_ctx, snap_id, ctx);
  req->send();
}

template <typename I>
void ObjectMap<I>::snapshot_add(uint64_t snap_id, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  ceph_assert(snap_id != CEPH_NOSNAP);

  Context *ctx = create_context_callback<Context>(on_finish, this);

  object_map::SnapshotCreateRequest *req =
    new object_map::SnapshotCreateRequest(m_image_ctx, &m_lock, &m_object_map,
                                          snap_id, ctx);
  req->send();
}

template <typename I>
void ObjectMap<I>::snapshot_remove(uint64_t snap_id, Context *on_finish) {
  ceph_assert(ceph_mutex_is_wlocked(m_image_ctx.image_lock));
  ceph_assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  ceph_assert(snap_id != CEPH_NOSNAP);

  Context *ctx = create_context_callback<Context>(on_finish, this);

  object_map::SnapshotRemoveRequest *req =
    new object_map::SnapshotRemoveRequest(m_image_ctx, &m_lock, &m_object_map,
                                          snap_id, ctx);
  req->send();
}

template <typename I>
void ObjectMap<I>::aio_save(Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_image_ctx.image_lock));
  std::shared_lock locker{m_lock};

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  cls_client::object_map_save(&op, m_object_map);

  Context *ctx = create_context_callback<Context>(on_finish, this);

  std::string oid(object_map_name(m_image_ctx.id, m_snap_id));
  librados::AioCompletion *comp = util::create_rados_callback(ctx);

  int r = m_image_ctx.md_ctx.aio_operate(oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectMap<I>::aio_resize(uint64_t new_size, uint8_t default_object_state,
			      Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_image_ctx.image_lock));
  ceph_assert(m_image_ctx.image_watcher != NULL);
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  Context *ctx = create_context_callback<Context>(on_finish, this);

  object_map::ResizeRequest *req = new object_map::ResizeRequest(
    m_image_ctx, &m_lock, &m_object_map, m_snap_id, new_size,
    default_object_state, ctx);
  req->send();
}

template <typename I>
void ObjectMap<I>::detained_aio_update(UpdateOperation &&op) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert(ceph_mutex_is_wlocked(m_lock));

  BlockGuardCell *cell;
  int r = m_update_guard->detain({op.start_object_no, op.end_object_no},
                                 &op, &cell);
  if (r < 0) {
    lderr(cct) << "failed to detain object map update: " << cpp_strerror(r)
               << dendl;
    m_image_ctx.op_work_queue->queue(op.on_finish, r);
    m_async_op_tracker.finish_op();
    return;
  } else if (r > 0) {
    ldout(cct, 20) << "detaining object map update due to in-flight update: "
                   << "start=" << op.start_object_no << ", "
		   << "end=" << op.end_object_no << ", "
                   << (op.current_state ?
                         stringify(static_cast<uint32_t>(*op.current_state)) :
                         "")
		   << "->" << static_cast<uint32_t>(op.new_state) << dendl;
    return;
  }

  ldout(cct, 20) << "in-flight update cell: " << cell << dendl;
  Context *on_finish = op.on_finish;
  Context *ctx = new LambdaContext([this, cell, on_finish](int r) {
      handle_detained_aio_update(cell, r, on_finish);
    });
  aio_update(CEPH_NOSNAP, op.start_object_no, op.end_object_no, op.new_state,
             op.current_state, op.parent_trace, op.ignore_enoent, ctx);
}

template <typename I>
void ObjectMap<I>::handle_detained_aio_update(BlockGuardCell *cell, int r,
                                              Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "cell=" << cell << ", r=" << r << dendl;

  typename UpdateGuard::BlockOperations block_ops;
  m_update_guard->release(cell, &block_ops);

  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    std::unique_lock locker{m_lock};
    for (auto &op : block_ops) {
      detained_aio_update(std::move(op));
    }
  }

  on_finish->complete(r);
  m_async_op_tracker.finish_op();
}

template <typename I>
void ObjectMap<I>::aio_update(uint64_t snap_id, uint64_t start_object_no,
                              uint64_t end_object_no, uint8_t new_state,
                              const boost::optional<uint8_t> &current_state,
                              const ZTracer::Trace &parent_trace,
                              bool ignore_enoent, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  ceph_assert((m_image_ctx.features & RBD_FEATURE_OBJECT_MAP) != 0);
  ceph_assert(m_image_ctx.image_watcher != nullptr);
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());
  ceph_assert(start_object_no < end_object_no);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "start=" << start_object_no << ", "
		 << "end=" << end_object_no << ", "
                 << (current_state ?
                       stringify(static_cast<uint32_t>(*current_state)) : "")
		 << "->" << static_cast<uint32_t>(new_state) << dendl;
  if (snap_id == CEPH_NOSNAP) {
    ceph_assert(ceph_mutex_is_wlocked(m_lock));
    end_object_no = std::min(end_object_no, m_object_map.size());
    if (start_object_no >= end_object_no) {
      ldout(cct, 20) << "skipping update of invalid object map" << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, 0);
      return;
    }

    auto it = m_object_map.begin() + start_object_no;
    auto end_it = m_object_map.begin() + end_object_no;
    for (; it != end_it; ++it) {
      if (update_required(it, new_state)) {
        break;
      }
    }
    if (it == end_it) {
      ldout(cct, 20) << "object map update not required" << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, 0);
      return;
    }
  }

  auto req = object_map::UpdateRequest<I>::create(
    m_image_ctx, &m_lock, &m_object_map, snap_id, start_object_no,
    end_object_no, new_state, current_state, parent_trace, ignore_enoent,
    on_finish);
  req->send();
}

} // namespace librbd

template class librbd::ObjectMap<librbd::ImageCtx>;

