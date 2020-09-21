// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/FlattenRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/image/DetachParentRequest.h"
#include "librbd/Types.h"
#include "librbd/io/ObjectRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::operation::FlattenRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
class C_FlattenObject : public C_AsyncObjectThrottle<I> {
public:
  C_FlattenObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                  ::SnapContext snapc, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_snapc(snapc),
      m_object_no(object_no) {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    CephContext *cct = image_ctx.cct;

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during flatten" << dendl;
      return -ERESTART;
    }

    {
      std::shared_lock image_lock{image_ctx.image_lock};
      if (image_ctx.object_map != nullptr &&
          !image_ctx.object_map->object_may_not_exist(m_object_no)) {
        // can skip because the object already exists
        return 1;
      }
    }

    bufferlist bl;
    auto req = new io::ObjectWriteRequest<I>(&image_ctx, m_object_no, 0,
                                             std::move(bl), m_snapc, 0, 0,
                                             std::nullopt, {}, this);
    if (!req->has_parent()) {
      // stop early if the parent went away - it just means
      // another flatten finished first or the image was resized
      delete req;
      return 1;
    }

    req->send();
    return 0;
  }

private:
  ::SnapContext m_snapc;
  uint64_t m_object_no;
};

template <typename I>
bool FlattenRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void FlattenRequest<I>::send_op() {
  flatten_objects();
}

template <typename I>
void FlattenRequest<I>::flatten_objects() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  auto ctx = create_context_callback<
    FlattenRequest<I>,
    &FlattenRequest<I>::handle_flatten_objects>(this);
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_FlattenObject<I> >(),
      boost::lambda::_1, &image_ctx, m_snapc, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, m_overlap_objects);
  throttle->start_ops(
    image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void FlattenRequest<I>::handle_flatten_objects(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -ERESTART) {
    ldout(cct, 5) << "flatten operation interrupted" << dendl;
    this->complete(r);
    return;
  } else if (r < 0) {
    lderr(cct) << "flatten encountered an error: " << cpp_strerror(r) << dendl;
    this->complete(r);
    return;
  }

  detach_child();
}

template <typename I>
void FlattenRequest<I>::detach_child() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  // should have been canceled prior to releasing lock
  image_ctx.owner_lock.lock_shared();
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // if there are no snaps, remove from the children object as well
  // (if snapshots remain, they have their own parent info, and the child
  // will be removed when the last snap goes away)
  image_ctx.image_lock.lock_shared();
  if ((image_ctx.features & RBD_FEATURE_DEEP_FLATTEN) == 0 &&
      !image_ctx.snaps.empty()) {
    image_ctx.image_lock.unlock_shared();
    image_ctx.owner_lock.unlock_shared();
    detach_parent();
    return;
  }
  image_ctx.image_lock.unlock_shared();

  ldout(cct, 5) << dendl;
  auto ctx = create_context_callback<
    FlattenRequest<I>,
    &FlattenRequest<I>::handle_detach_child>(this);
  auto req = image::DetachChildRequest<I>::create(image_ctx, ctx);
  req->send();
  image_ctx.owner_lock.unlock_shared();
}

template <typename I>
void FlattenRequest<I>::handle_detach_child(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "detach encountered an error: " << cpp_strerror(r) << dendl;
    this->complete(r);
    return;
  }

  detach_parent();
}

template <typename I>
void FlattenRequest<I>::detach_parent() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  // should have been canceled prior to releasing lock
  image_ctx.owner_lock.lock_shared();
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  // stop early if the parent went away - it just means
  // another flatten finished first, so this one is useless.
  image_ctx.image_lock.lock_shared();
  if (!image_ctx.parent) {
    ldout(cct, 5) << "image already flattened" << dendl;
    image_ctx.image_lock.unlock_shared();
    image_ctx.owner_lock.unlock_shared();
    this->complete(0);
    return;
  }
  image_ctx.image_lock.unlock_shared();

  // remove parent from this (base) image
  auto ctx = create_context_callback<
    FlattenRequest<I>,
    &FlattenRequest<I>::handle_detach_parent>(this);
  auto req = image::DetachParentRequest<I>::create(image_ctx, ctx);
  req->send();
  image_ctx.owner_lock.unlock_shared();
}

template <typename I>
void FlattenRequest<I>::handle_detach_parent(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "remove parent encountered an error: " << cpp_strerror(r)
               << dendl;
  }

  this->complete(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::FlattenRequest<librbd::ImageCtx>;
