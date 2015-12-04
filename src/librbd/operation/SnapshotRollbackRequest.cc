// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRollbackRequest.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/ResizeRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRollbackRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotRollbackRequest<I>::State& state) {
  switch(state) {
  case SnapshotRollbackRequest<I>::STATE_RESIZE_IMAGE:
    os << "RESIZE_IMAGE";
    break;
  case SnapshotRollbackRequest<I>::STATE_ROLLBACK_OBJECT_MAP:
    os << "ROLLBACK_OBJECT_MAP";
    break;
  case SnapshotRollbackRequest<I>::STATE_ROLLBACK_OBJECTS:
    os << "ROLLBACK_OBJECTS";
    break;
  case SnapshotRollbackRequest<I>::STATE_INVALIDATE_CACHE:
    os << "INVALIDATE_CACHE";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

template <typename I>
class C_RollbackObject : public C_AsyncObjectThrottle<> {
public:
  C_RollbackObject(AsyncObjectThrottle<> &throttle, I *image_ctx,
                   uint64_t snap_id, uint64_t object_num)
    : C_AsyncObjectThrottle(throttle, *image_ctx), m_snap_id(snap_id),
      m_object_num(object_num) {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    ldout(cct, 20) << "C_RollbackObject: " << __func__ << ": object_num="
                   << m_object_num << dendl;

    std::string oid = image_ctx.get_object_name(m_object_num);

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_ctx_cb);
    librados::ObjectWriteOperation op;
    op.selfmanaged_snap_rollback(m_snap_id);
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
    m_snap_id(snap_id), m_snap_size(snap_size), m_prog_ctx(prog_ctx) {
}

template <typename I>
void SnapshotRollbackRequest<I>::send_op() {
  send_resize_image();
}

template <typename I>
bool SnapshotRollbackRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    return true;
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_RESIZE_IMAGE:
    send_rollback_object_map();
    break;
  case STATE_ROLLBACK_OBJECT_MAP:
    send_rollback_objects();
    break;
  case STATE_ROLLBACK_OBJECTS:
    finished = send_invalidate_cache();
    break;
  case STATE_INVALIDATE_CACHE:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
void SnapshotRollbackRequest<I>::send_resize_image() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  uint64_t current_size;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    current_size = image_ctx.get_image_size(CEPH_NOSNAP);
  }

  if (current_size == m_snap_size) {
    send_rollback_object_map();
    return;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_RESIZE_IMAGE;

  ResizeRequest<I> *req = new ResizeRequest<I>(image_ctx,
                                               this->create_callback_context(),
                                               m_snap_size, m_no_op_prog_ctx);
  req->send();
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_object_map() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::WLocker object_map_lock(image_ctx.object_map_lock);
    if (image_ctx.object_map.enabled(image_ctx.object_map_lock)) {
      CephContext *cct = image_ctx.cct;
      ldout(cct, 5) << this << " " << __func__ << dendl;
      m_state = STATE_ROLLBACK_OBJECT_MAP;

      image_ctx.object_map.rollback(m_snap_id, this->create_callback_context());
      return;
    }
  }

  send_rollback_objects();
}

template <typename I>
void SnapshotRollbackRequest<I>::send_rollback_objects() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_ROLLBACK_OBJECTS;

  uint64_t num_objects;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    num_objects = Striper::get_num_objects(image_ctx.layout,
                                           image_ctx.get_current_size());
  }

  Context *ctx = this->create_callback_context();
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_RollbackObject<I> >(),
      boost::lambda::_1, &image_ctx, m_snap_id, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, num_objects);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
bool SnapshotRollbackRequest<I>::send_invalidate_cache() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  if (image_ctx.object_cacher == NULL) {
    return true;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_INVALIDATE_CACHE;

  image_ctx.invalidate_cache(this->create_callback_context());
  return false;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRollbackRequest<librbd::ImageCtx>;
