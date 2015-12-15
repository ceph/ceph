// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/LockRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::LockRequest: "

namespace librbd {
namespace object_map {

using util::create_rados_ack_callback;
using util::create_rados_safe_callback;

template <typename I>
LockRequest<I>::LockRequest(I &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_broke_lock(false) {
}

template <typename I>
void LockRequest<I>::send() {
  send_lock();
}

template <typename I>
void LockRequest<I>::send_lock() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::lock(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, "", "", "",
                           utime_t(), 0);

  using klass = LockRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_lock>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *LockRequest<I>::handle_lock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val == 0) {
    return m_on_finish;
  } else if (m_broke_lock || *ret_val != -EBUSY) {
    lderr(cct) << "failed to lock object map: " << cpp_strerror(*ret_val)
               << dendl;
    *ret_val = 0;
    return m_on_finish;
  }

  send_get_lock_info();
  return nullptr;
}

template <typename I>
void LockRequest<I>::send_get_lock_info() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = LockRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_lock_info>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *LockRequest<I>::handle_get_lock_info(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val == -ENOENT) {
    send_lock();
    return nullptr;
  }

  ClsLockType lock_type;
  std::string lock_tag;
  if (*ret_val == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    *ret_val = rados::cls::lock::get_lock_info_finish(&it, &m_lockers,
                                                      &lock_type, &lock_tag);
  }
  if (*ret_val < 0) {
    lderr(cct) << "failed to list object map locks: " << cpp_strerror(*ret_val)
               << dendl;
    *ret_val = 0;
    return m_on_finish;
  }

  send_break_locks();
  return nullptr;
}

template <typename I>
void LockRequest<I>::send_break_locks() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << ", "
                 << "num_lockers=" << m_lockers.size() << dendl;

  librados::ObjectWriteOperation op;
  for (auto &locker : m_lockers) {
    rados::cls::lock::break_lock(&op, RBD_LOCK_NAME, locker.first.cookie,
                                 locker.first.locker);
  }

  using klass = LockRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_break_locks>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *LockRequest<I>::handle_break_locks(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  m_broke_lock = true;
  if (*ret_val == 0 || *ret_val == -ENOENT) {
    send_lock();
    return nullptr;
  }

  lderr(cct) << "failed to break object map lock: " << cpp_strerror(*ret_val)
             << dendl;
  *ret_val = 0;
  return m_on_finish;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::LockRequest<librbd::ImageCtx>;
