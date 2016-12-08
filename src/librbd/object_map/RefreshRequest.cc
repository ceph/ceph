// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/RefreshRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/InvalidateRequest.h"
#include "librbd/object_map/LockRequest.h"
#include "librbd/object_map/ResizeRequest.h"
#include "librbd/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::RefreshRequest: "

namespace librbd {

using util::create_context_callback;
using util::create_rados_ack_callback;
using util::create_rados_safe_callback;

namespace object_map {

template <typename I>
RefreshRequest<I>::RefreshRequest(I &image_ctx, ceph::BitVector<2> *object_map,
                                  uint64_t snap_id, Context *on_finish)
  : m_image_ctx(image_ctx), m_object_map(object_map), m_snap_id(snap_id),
    m_on_finish(on_finish), m_object_count(0),
    m_truncate_on_disk_object_map(false) {
}

template <typename I>
void RefreshRequest<I>::send() {
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    m_object_count = Striper::get_num_objects(
      m_image_ctx.layout, m_image_ctx.get_image_size(m_snap_id));
  }


  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "object_count=" << m_object_count << dendl;
  send_lock();
}

template <typename I>
void RefreshRequest<I>::apply() {
  uint64_t num_objs;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    num_objs = Striper::get_num_objects(
      m_image_ctx.layout, m_image_ctx.get_image_size(m_snap_id));
  }
  assert(m_on_disk_object_map.size() >= num_objs);

  *m_object_map = m_on_disk_object_map;
}

template <typename I>
void RefreshRequest<I>::send_lock() {
  CephContext *cct = m_image_ctx.cct;
  if (m_object_count > cls::rbd::MAX_OBJECT_MAP_OBJECT_COUNT) {
    send_invalidate_and_close();
    return;
  } else if (m_snap_id != CEPH_NOSNAP) {
    send_load();
    return;
  }

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_lock>(this);

  LockRequest<I> *req = LockRequest<I>::create(m_image_ctx, ctx);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_lock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  assert(*ret_val == 0);
  send_load();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_load() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  librados::ObjectReadOperation op;
  cls_client::object_map_load_start(&op);

  using klass = RefreshRequest<I>;
  m_out_bl.clear();
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_load>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_load(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val == 0) {
    bufferlist::iterator bl_it = m_out_bl.begin();
    *ret_val = cls_client::object_map_load_finish(&bl_it,
                                                  &m_on_disk_object_map);
  }

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  if (*ret_val == -EINVAL) {
     // object map is corrupt on-disk -- clear it and properly size it
     // so future IO can keep the object map in sync
    lderr(cct) << "object map corrupt on-disk: " << oid << dendl;
    m_truncate_on_disk_object_map = true;
    send_resize_invalidate();
    return nullptr;
  } else if (*ret_val < 0) {
    lderr(cct) << "failed to load object map: " << oid << dendl;
    send_invalidate();
    return nullptr;
  }

  if (m_on_disk_object_map.size() < m_object_count) {
    lderr(cct) << "object map smaller than current object count: "
               << m_on_disk_object_map.size() << " != "
               << m_object_count << dendl;
    send_resize_invalidate();
    return nullptr;
  }

  ldout(cct, 20) << "refreshed object map: num_objs="
                 << m_on_disk_object_map.size() << dendl;
  if (m_on_disk_object_map.size() > m_object_count) {
    // resize op might have been interrupted
    ldout(cct, 1) << "object map larger than current object count: "
                  << m_on_disk_object_map.size() << " != "
                  << m_object_count << dendl;
  }

  apply();
  return m_on_finish;
}

template <typename I>
void RefreshRequest<I>::send_invalidate() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_on_disk_object_map.clear();
  object_map::ResizeRequest::resize(&m_on_disk_object_map, m_object_count,
                                    OBJECT_EXISTS);

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_invalidate>(this);
  InvalidateRequest<I> *req = InvalidateRequest<I>::create(
    m_image_ctx, m_snap_id, false, ctx);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_invalidate(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  assert(*ret_val == 0);
  apply();
  return m_on_finish;
}

template <typename I>
void RefreshRequest<I>::send_resize_invalidate() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_on_disk_object_map.clear();
  object_map::ResizeRequest::resize(&m_on_disk_object_map, m_object_count,
                                    OBJECT_EXISTS);

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_resize_invalidate>(this);
  InvalidateRequest<I> *req = InvalidateRequest<I>::create(
    m_image_ctx, m_snap_id, false, ctx);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_resize_invalidate(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  assert(*ret_val == 0);
  send_resize();
  return nullptr;
}

template <typename I>
void RefreshRequest<I>::send_resize() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap<>::object_map_name(m_image_ctx.id, m_snap_id));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP) {
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "");
  }
  if (m_truncate_on_disk_object_map) {
    op.truncate(0);
  }
  cls_client::object_map_resize(&op, m_object_count, OBJECT_NONEXISTENT);

  using klass = RefreshRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_resize>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *RefreshRequest<I>::handle_resize(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to adjust object map size: " << cpp_strerror(*ret_val)
               << dendl;
    *ret_val = 0;
  }
  apply();
  return m_on_finish;
}

template <typename I>
void RefreshRequest<I>::send_invalidate_and_close() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_invalidate_and_close>(this);
  InvalidateRequest<I> *req = InvalidateRequest<I>::create(
    m_image_ctx, m_snap_id, false, ctx);

  lderr(cct) << "object map too large: " << m_object_count << dendl;
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  req->send();
}

template <typename I>
Context *RefreshRequest<I>::handle_invalidate_and_close(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  assert(*ret_val == 0);

  *ret_val = -EFBIG;
  m_object_map->clear();
  return m_on_finish;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::RefreshRequest<librbd::ImageCtx>;
