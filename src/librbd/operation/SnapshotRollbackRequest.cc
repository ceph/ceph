// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRollbackRequest.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/operation/ResizeRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRollbackRequest: "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_callback;

namespace {

template <typename I>
class C_RollbackObject : public C_AsyncObjectThrottle<I> {
public:
  C_RollbackObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                   uint64_t snap_id, uint64_t object_num,
                   uint64_t head_num_objects,
                   decltype(I::object_map) snap_object_map)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_snap_id(snap_id),
      m_object_num(object_num), m_head_num_objects(head_num_objects),
      m_snap_object_map(snap_object_map) {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_RollbackObject: " << __func__ << ": object_num="
                   << m_object_num << dendl;

    {
      std::shared_lock image_locker{image_ctx.image_lock};
      if (m_object_num < m_head_num_objects &&
          m_snap_object_map != nullptr &&
          !image_ctx.object_map->object_may_exist(m_object_num) &&
          !m_snap_object_map->object_may_exist(m_object_num)) {
        return 1;
      }
    }

    std::string oid = image_ctx.get_object_name(m_object_num);

    librados::ObjectWriteOperation op;
    op.selfmanaged_snap_rollback(m_snap_id);

    librados::AioCompletion *rados_completion =
      util::create_rados_callback(this);
    image_ctx.data_ctx.aio_operate(oid, rados_completion, &op);
    rados_completion->release();
    return 0;
  }

private:
  uint64_t m_snap_id;
  uint64_t m_object_num;
  uint64_t m_head_num_objects;
  decltype(I::object_map) m_snap_object_map;
};

} // anonymous namespace

template <typename I>
SnapshotRollbackRequest<I>::SnapshotRollbackRequest(I &image_ctx,
                                                    Context *on_finish,
						    const cls::rbd::SnapshotNamespace &snap_namespace,
                                                    const std::string &snap_name,
                                                    uint64_t snap_id,
                                                    uint64_t snap_size,
                                                    ProgressContext &prog_ctx)
  : Request<I>(image_ctx, on_finish), m_snap_namespace(snap_namespace),
    m_snap_name(snap_name), m_snap_id(snap_id),
    m_snap_size(snap_size), m_prog_ctx(prog_ctx),
    m_object_map(nullptr), m_snap_object_map(nullptr) {
}

template <typename I>
SnapshotRollbackRequest<I>::~SnapshotRollbackRequest() {
  I &image_ctx = this->m_image_ctx;
  if (m_blocking_writes) {
    image_ctx.io_image_dispatcher->unblock_writes();
  }
  if (m_object_map) {
    m_object_map->put();
    m_object_map = nullptr;
  }
  if (m_snap_object_map) {
    m_snap_object_map->put();
    m_snap_object_map = nullptr;
  }
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
  image_ctx.io_image_dispatcher->block_writes(create_context_callback<
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
    return this->create_context_finisher(*result);
  }

  send_resize_image();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_resize_image() {
  I &image_ctx = this->m_image_ctx;

  uint64_t current_size;
  {
    std::shared_lock owner_locker{image_ctx.owner_lock};
    std::shared_lock image_locker{image_ctx.image_lock};
    current_size = image_ctx.get_image_size(CEPH_NOSNAP);
  }

  m_head_num_objects = Striper::get_num_objects(image_ctx.layout, current_size);

  if (current_size == m_snap_size) {
    send_get_snap_object_map();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_resize_image>(this);
  ResizeRequest<I> *req = ResizeRequest<I>::create(image_ctx, ctx, m_snap_size,
                                                   true, m_no_op_prog_ctx, 0, true);
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
    return this->create_context_finisher(*result);
  }

  send_get_snap_object_map();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_get_snap_object_map() {
  I &image_ctx = this->m_image_ctx;

  uint64_t flags = 0;
  bool object_map_enabled;
  CephContext *cct = image_ctx.cct;
  {
    std::shared_lock owner_locker{image_ctx.owner_lock};
    std::shared_lock image_locker{image_ctx.image_lock};
    object_map_enabled = (image_ctx.object_map != nullptr);
    int r = image_ctx.get_flags(m_snap_id, &flags);
    if (r < 0) {
      object_map_enabled = false;
    }
  }
  if (object_map_enabled &&
      (flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
    lderr(cct) << "warning: object-map is invalid for snapshot" << dendl;
    object_map_enabled = false;
  }
  if (!object_map_enabled) {
    send_rollback_object_map();
    return;
  }

  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_snap_object_map = image_ctx.create_object_map(m_snap_id);

  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_get_snap_object_map>(this);
  m_snap_object_map->open(ctx);
  return;
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_get_snap_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to open object map: "
               << cpp_strerror(*result) << dendl;
    m_snap_object_map->put();
    m_snap_object_map = nullptr;
  }

  send_rollback_object_map();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_object_map() {
  I &image_ctx = this->m_image_ctx;

  {
    std::shared_lock owner_locker{image_ctx.owner_lock};
    std::shared_lock image_locker{image_ctx.image_lock};
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

  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to roll back object "
               << "map: " << cpp_strerror(*result) << dendl;

    ceph_assert(m_object_map == nullptr);
    apply();
    return this->create_context_finisher(*result);
  }

  send_rollback_objects();
  return nullptr;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_objects() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  uint64_t num_objects;
  {
    std::shared_lock image_locker{image_ctx.image_lock};
    num_objects = Striper::get_num_objects(image_ctx.layout,
                                           image_ctx.get_current_size());
  }

  Context *ctx = create_context_callback<
    SnapshotRollbackRequest<I>,
    &SnapshotRollbackRequest<I>::handle_rollback_objects>(this);
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_RollbackObject<I> >(),
      boost::lambda::_1, &image_ctx, m_snap_id, boost::lambda::_2,
      m_head_num_objects, m_snap_object_map));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, num_objects);
  throttle->start_ops(
    image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
Context *SnapshotRollbackRequest<I>::handle_rollback_objects(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ERESTART) {
    ldout(cct, 5) << "snapshot rollback operation interrupted" << dendl;
    return this->create_context_finisher(*result);
  } else if (*result < 0) {
    lderr(cct) << "failed to rollback objects: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher(*result);
  }

  return send_refresh_object_map();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::send_refresh_object_map() {
  I &image_ctx = this->m_image_ctx;

  bool object_map_enabled;
  {
    std::shared_lock owner_locker{image_ctx.owner_lock};
    std::shared_lock image_locker{image_ctx.image_lock};
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

  if (*result < 0) {
    lderr(cct) << this << " " << __func__ << ": failed to open object map: "
               << cpp_strerror(*result) << dendl;
    m_object_map->put();
    m_object_map = nullptr;
    apply();

    return this->create_context_finisher(*result);
  }

  return send_invalidate_cache();
}

template <typename I>
Context *SnapshotRollbackRequest<I>::send_invalidate_cache() {
  I &image_ctx = this->m_image_ctx;

  apply();

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  if(m_object_map != nullptr) {
    Context *ctx = create_context_callback<
      SnapshotRollbackRequest<I>,
      &SnapshotRollbackRequest<I>::handle_invalidate_cache>(this, m_object_map);
    image_ctx.io_image_dispatcher->invalidate_cache(ctx);
  }
  else {
    Context *ctx = create_context_callback<
      SnapshotRollbackRequest<I>,
      &SnapshotRollbackRequest<I>::handle_invalidate_cache>(this);
    image_ctx.io_image_dispatcher->invalidate_cache(ctx);
  }
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
  return this->create_context_finisher(*result);
}

template <typename I>
void SnapshotRollbackRequest<I>::apply() {
  I &image_ctx = this->m_image_ctx;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  std::unique_lock image_locker{image_ctx.image_lock};
  if (image_ctx.object_map != nullptr) {
    std::swap(m_object_map, image_ctx.object_map);
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRollbackRequest<librbd::ImageCtx>;
