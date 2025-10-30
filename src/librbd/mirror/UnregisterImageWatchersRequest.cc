// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/UnregisterImageWatchersRequest.h"
#include "librbd/ImageState.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/Utils.h"
#include "common/Timer.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/image/ListWatchersRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::UnregisterImageWatchersRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;

template <typename I>
void UnregisterImageWatchersRequest<I>::send() {
  list_watchers();
}

template <typename I>
void UnregisterImageWatchersRequest<I>::list_watchers() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    UnregisterImageWatchersRequest<I>,
    &UnregisterImageWatchersRequest<I>::handle_list_watchers>(this);

  m_watchers.clear();
  auto flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
               librbd::image::LIST_WATCHERS_MIRROR_INSTANCES_ONLY;
  auto req = librbd::image::ListWatchersRequest<I>::create(
    *m_image_ctx, flags, &m_watchers, ctx);
  req->send();
}

template <typename I>
void UnregisterImageWatchersRequest<I>::handle_list_watchers(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to list watchers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_watchers.empty()) {
    finish(0);
    return;
  }

  wait_update_notify();
}

template <typename I>
void UnregisterImageWatchersRequest<I>::wait_update_notify() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  ImageCtx::get_timer_instance(cct, &m_timer, &m_timer_lock);

  std::lock_guard timer_lock{*m_timer_lock};

  m_scheduler_ticks = 5;

  int r = m_image_ctx->state->register_update_watcher(&m_update_watch_ctx,
                                                      &m_update_watcher_handle);
  if (r < 0) {
    lderr(cct) << "failed to register update watcher: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  scheduler_unregister_update_watcher();
}

template <typename I>
void UnregisterImageWatchersRequest<I>::handle_update_notify() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  std::lock_guard timer_lock{*m_timer_lock};
  m_scheduler_ticks = 0;
}

template <typename I>
void UnregisterImageWatchersRequest<I>::scheduler_unregister_update_watcher() {
  ceph_assert(ceph_mutex_is_locked(*m_timer_lock));

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "scheduler_ticks=" << m_scheduler_ticks << dendl;

  if (m_scheduler_ticks > 0) {
    m_scheduler_ticks--;
    m_timer->add_event_after(1, new LambdaContext([this](int) {
        scheduler_unregister_update_watcher();
      }));
    return;
  }

  m_image_ctx->op_work_queue->queue(new LambdaContext([this](int) {
      unregister_update_watcher();
    }), 0);
}

template <typename I>
void UnregisterImageWatchersRequest<I>::unregister_update_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    UnregisterImageWatchersRequest<I>,
    &UnregisterImageWatchersRequest<I>::handle_unregister_update_watcher>(this);

  m_image_ctx->state->unregister_update_watcher(m_update_watcher_handle, ctx);
}

template <typename I>
void UnregisterImageWatchersRequest<I>::handle_unregister_update_watcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to unregister update watcher: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  list_watchers();
}

template <typename I>
void UnregisterImageWatchersRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::UnregisterImageWatchersRequest<librbd::ImageCtx>;
