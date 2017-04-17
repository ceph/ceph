// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image_watcher/RewatchRequest.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image_watcher::RewatchRequest: " \
                           << this << " " << __func__ << " "

namespace librbd {
namespace image_watcher {

using librbd::util::create_context_callback;
using librbd::util::create_rados_safe_callback;

template <typename I>
RewatchRequest<I>::RewatchRequest(I &image_ctx, RWLock &watch_lock,
                                  librados::WatchCtx2 *watch_ctx,
                                  uint64_t *watch_handle, Context *on_finish)
  : m_image_ctx(image_ctx), m_watch_lock(watch_lock), m_watch_ctx(watch_ctx),
    m_watch_handle(watch_handle), m_on_finish(on_finish) {
}

template <typename I>
void RewatchRequest<I>::send() {
  unwatch();
}

template <typename I>
void RewatchRequest<I>::unwatch() {
  assert(m_watch_lock.is_wlocked());
  assert(*m_watch_handle != 0);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::AioCompletion *aio_comp = create_rados_safe_callback<
    RewatchRequest<I>, &RewatchRequest<I>::handle_unwatch>(this);
  int r = m_image_ctx.md_ctx.aio_unwatch(*m_watch_handle, aio_comp);
  assert(r == 0);
  aio_comp->release();

  *m_watch_handle = 0;
}

template <typename I>
void RewatchRequest<I>::handle_unwatch(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    lderr(cct) << "client blacklisted" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to unwatch: " << cpp_strerror(r) << dendl;
  }
  rewatch();
}

template <typename I>
void RewatchRequest<I>::rewatch() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::AioCompletion *aio_comp = create_rados_safe_callback<
    RewatchRequest<I>, &RewatchRequest<I>::handle_rewatch>(this);
  int r = m_image_ctx.md_ctx.aio_watch(m_image_ctx.header_oid, aio_comp,
                                       &m_rewatch_handle, m_watch_ctx);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RewatchRequest<I>::handle_rewatch(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    lderr(cct) << "client blacklisted" << dendl;
    finish(r);
    return;
  } else if (r == -ENOENT) {
    ldout(cct, 5) << "image header deleted" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to watch image header: " << cpp_strerror(r)
               << dendl;
    rewatch();
    return;
  }

  {
    RWLock::WLocker watch_locker(m_watch_lock);
    *m_watch_handle = m_rewatch_handle;
  }

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    if (m_image_ctx.exclusive_lock != nullptr) {
      // update the lock cookie with the new watch handle
      m_image_ctx.exclusive_lock->reacquire_lock();
    }
  }
  finish(0);
}

template <typename I>
void RewatchRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_watcher
} // namespace librbd

template class librbd::image_watcher::RewatchRequest<librbd::ImageCtx>;
