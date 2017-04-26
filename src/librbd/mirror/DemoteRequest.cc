// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/DemoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GetInfoRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::DemoteRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;

template <typename I>
void DemoteRequest<I>::send() {
  get_info();
}

template <typename I>
void DemoteRequest<I>::get_info() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    DemoteRequest<I>, &DemoteRequest<I>::handle_get_info>(this);
  auto req = GetInfoRequest<I>::create(m_image_ctx, &m_mirror_image,
                                       &m_promotion_state, ctx);
  req->send();
}

template <typename I>
void DemoteRequest<I>::handle_get_info(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  } else if (m_mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    lderr(cct) << "mirroring is not currently enabled" << dendl;
    finish(-EINVAL);
    return;
  } else if (m_promotion_state != PROMOTION_STATE_PRIMARY) {
    lderr(cct) << "image is not primary" << dendl;
    finish(-EINVAL);
    return;
  }

  acquire_lock();
}

template <typename I>
void DemoteRequest<I>::acquire_lock() {
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.owner_lock.get_read();
  if (m_image_ctx.exclusive_lock == nullptr) {
    m_image_ctx.owner_lock.put_read();
    lderr(cct) << "exclusive lock is not active" << dendl;
    finish(-EINVAL);
    return;
  }

  // avoid accepting new requests from peers while we demote
  // the image
  m_image_ctx.exclusive_lock->block_requests(0);
  m_blocked_requests = true;

  if (m_image_ctx.exclusive_lock->is_lock_owner()) {
    m_image_ctx.owner_lock.put_read();
    demote();
    return;
  }

  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    DemoteRequest<I>, &DemoteRequest<I>::handle_acquire_lock>(this);
  m_image_ctx.exclusive_lock->acquire_lock(ctx);
  m_image_ctx.owner_lock.put_read();
}

template <typename I>
void DemoteRequest<I>::handle_acquire_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to lock image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_image_ctx.owner_lock.get_read();
  if (m_image_ctx.exclusive_lock == nullptr ||
      !m_image_ctx.exclusive_lock->is_lock_owner()) {
    m_image_ctx.owner_lock.put_read();
    lderr(cct) << "failed to acquire exclusive lock" << dendl;
    finish(-EROFS);
    return;
  }
  m_image_ctx.owner_lock.put_read();

  demote();
}

template <typename I>
void DemoteRequest<I>::demote() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    DemoteRequest<I>, &DemoteRequest<I>::handle_demote>(this);
  Journal<I>::demote(&m_image_ctx, ctx);
}

template <typename I>
void DemoteRequest<I>::handle_demote(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(cct) << "failed to demote image: " << cpp_strerror(r) << dendl;
  }

  release_lock();
}

template <typename I>
void DemoteRequest<I>::release_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_image_ctx.owner_lock.get_read();
  if (m_image_ctx.exclusive_lock == nullptr) {
    m_image_ctx.owner_lock.put_read();
    finish(0);
    return;
  }

  auto ctx = create_context_callback<
    DemoteRequest<I>, &DemoteRequest<I>::handle_release_lock>(this);
  m_image_ctx.exclusive_lock->release_lock(ctx);
  m_image_ctx.owner_lock.put_read();
}

template <typename I>
void DemoteRequest<I>::handle_release_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to release exclusive lock: " << cpp_strerror(r)
               << dendl;
  }

  finish(r);
}

template <typename I>
void DemoteRequest<I>::finish(int r) {
  if (m_ret_val < 0) {
    r = m_ret_val;
  }

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    if (m_blocked_requests && m_image_ctx.exclusive_lock != nullptr) {
      m_image_ctx.exclusive_lock->unblock_requests();
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::DemoteRequest<librbd::ImageCtx>;
