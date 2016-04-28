// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRollbackRequest.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/operation/ResizeRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRollbackRequest: "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_safe_callback;

namespace {

template <typename I>
class C_RollbackObject : public C_AsyncObjectThrottle<I> {
public:
  C_RollbackObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                   uint64_t snap_id, uint64_t object_num)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_snap_id(snap_id),
      m_object_num(object_num) {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_RollbackObject: " << __func__ << ": object_num="
                   << m_object_num << dendl;

    std::string oid = image_ctx.get_object_name(m_object_num);

    librados::ObjectWriteOperation op;
    op.selfmanaged_snap_rollback(m_snap_id);

    librados::AioCompletion *rados_completion =
      util::create_rados_safe_callback(this);
    image_ctx.data_ctx.aio_operate(oid, rados_completion, &op);
    rados_completion->release();
    return 0;
  }

private:
  uint64_t m_snap_id;
  uint64_t m_object_num;
};

} // anonymous namespace

template <typename I>
SnapshotRollbackRequest<I>::SnapshotRollbackRequest(I &image_ctx,
                                                    Context *on_finish,
                                                    const std::string &snap_name,
                                                    uint64_t snap_id,
                                                    uint64_t snap_size,
                                                    ProgressContext &prog_ctx)
  : Request<I>(image_ctx, on_finish), m_snap_name(snap_name),
    m_snap_id(snap_id), m_snap_size(snap_size), m_prog_ctx(prog_ctx),
    m_object_map(nullptr) {
}

template <typename I>
SnapshotRollbackRequest<I>::~SnapshotRollbackRequest() {
  I &image_ctx = this->m_image_ctx;
  if (m_blocking_writes) {
    image_ctx.aio_work_queue->unblock_writes();
  }
  delete m_object_map;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_op() {
  send_block_writes();
}

template <typename I>
void SnapshotRollbackRequest<I>::send_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_blocking_writes = true;
  image_ctx.aio_work_queue->block_writes(create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_block_writes>(this));
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    return this->create_context_finisher();
  }

  send_resize_image();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_resize_image() {
  I &image_ctx = this->m_image_ctx;

  uint64_t current_size;
  {
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    current_size = image_ctx.get_image_size(CEPH_NOSNAP);
  }

  if (current_size == m_snap_size) {
    send_rollback_object_map();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_resize_image>(this);
  ResizeRequest<I> *req = ResizeRequest<I>::create(image_ctx, ctx, m_snap_size,
                                                   m_no_op_prog_ctx, 0, true);
  req->send();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_resize_image(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to resize image for rollback: "
               << cpp_strerror(*result) << dendl;
    return this->create_context_finisher();
  }

  send_rollback_object_map();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_object_map() {
  I &image_ctx = this->m_image_ctx;

  {
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::WLocker object_map_lock(image_ctx.object_map_lock);
    if (image_ctx.object_map != nullptr) {
      CephContext *cct = image_ctx.cct;
      ldout(cct, 5) << this << " " << __func__ << dendl;

      Context *ctx = create_context_callback<
        SnapshotRollbackRequest<I>,
        &SnapshotRollbackRequest<I>::handle_rollback_object_map>(this);
      image_ctx.object_map->rollback(m_snap_id, ctx);
      return;
    }
  }

  send_rollback_objects();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_rollback_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(*result == 0);
  send_rollback_objects();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_objects() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  uint64_t num_objects;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    num_objects = Striper::get_num_objects(image_ctx.layout,
                                           image_ctx.get_current_size());
  }

  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_rollback_objects>(this);
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_RollbackObject<I> >(),
      boost::lambda::_1, &image_ctx, m_snap_id, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, num_objects);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_rollback_objects(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ERESTART) {
    ldout(cct, 5) << "snapshot rollback operation interrupted" << dendl;
    return this->create_context_finisher();
  } else if (*result < 0) {
    lderr(cct) << "failed to rollback objects: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher();
  }

  return send_refresh_object_map();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::send_refresh_object_map() {
  I &image_ctx = this->m_image_ctx;

  bool object_map_enabled;
  {
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    object_map_enabled = (image_ctx.object_map != nullptr);
  }
  if (!object_map_enabled) {
    return send_invalidate_cache();
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_object_map = image_ctx.create_object_map(CEPH_NOSNAP);

  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_refresh_object_map>(this);
  m_object_map->open(ctx);
  return nullptr;
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_refresh_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  assert(*result == 0);
  return send_invalidate_cache();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::send_invalidate_cache() {
  I &image_ctx = this->m_image_ctx;

  apply();
  if (image_ctx.object_cacher == NULL) {
    return this->create_context_finisher();
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_invalidate_cache>(this);
  image_ctx.invalidate_cache(ctx);
  return nullptr;
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_invalidate_cache(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to invalidate cache: " << cpp_strerror(*result)
               << dendl;
  }
  return this->create_context_finisher();
}

template <typename I>
void SnapshotRollbackRequest<I>::apply() {
  I &image_ctx = this->m_image_ctx;

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  if (image_ctx.object_map != nullptr) {
    std::swap(m_object_map, image_ctx.object_map);
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRollbackRequest<librbd::ImageCtx>;
