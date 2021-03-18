// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/PromoteRequest.h"
#include "common/Timer.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::PromoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void PromoteRequest<I>::send() {
  CephContext *cct = m_image_ctx->cct;
  bool requires_orphan = false;
  if (!util::can_create_primary_snapshot(m_image_ctx, false, true,
                                         &requires_orphan,
                                         &m_rollback_snap_id)) {
    lderr(cct) << "cannot promote" << dendl;
    finish(-EINVAL);
    return;
  } else if (m_rollback_snap_id == CEPH_NOSNAP && !requires_orphan) {
    create_promote_snapshot();
    return;
  }

  ldout(cct, 15) << "requires_orphan=" << requires_orphan << ", "
                 << "rollback_snap_id=" << m_rollback_snap_id << dendl;
  create_orphan_snapshot();
}

template <typename I>
void PromoteRequest<I>::create_orphan_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>,
    &PromoteRequest<I>::handle_create_orphan_snapshot>(this);

  auto req = CreateNonPrimaryRequest<I>::create(
    m_image_ctx, false, "", CEPH_NOSNAP, {}, {}, nullptr, ctx);
  req->send();
}

template <typename I>
void PromoteRequest<I>::handle_create_orphan_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create orphan snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  list_watchers();
}

template <typename I>
void PromoteRequest<I>::list_watchers() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>,
    &PromoteRequest<I>::handle_list_watchers>(this);

  m_watchers.clear();
  auto flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
               librbd::image::LIST_WATCHERS_MIRROR_INSTANCES_ONLY;
  auto req = librbd::image::ListWatchersRequest<I>::create(
    *m_image_ctx, flags, &m_watchers, ctx);
  req->send();
}

template <typename I>
void PromoteRequest<I>::handle_list_watchers(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to list watchers: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_watchers.empty()) {
    acquire_exclusive_lock();
    return;
  }

  wait_update_notify();
}

template <typename I>
void PromoteRequest<I>::wait_update_notify() {
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
void PromoteRequest<I>::handle_update_notify() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  std::lock_guard timer_lock{*m_timer_lock};
  m_scheduler_ticks = 0;
}

template <typename I>
void PromoteRequest<I>::scheduler_unregister_update_watcher() {
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
void PromoteRequest<I>::unregister_update_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>,
    &PromoteRequest<I>::handle_unregister_update_watcher>(this);

  m_image_ctx->state->unregister_update_watcher(m_update_watcher_handle, ctx);
}

template <typename I>
void PromoteRequest<I>::handle_unregister_update_watcher(int r) {
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
void PromoteRequest<I>::acquire_exclusive_lock() {
  {
    std::unique_lock locker{m_image_ctx->owner_lock};
    if (m_image_ctx->exclusive_lock != nullptr &&
        !m_image_ctx->exclusive_lock->is_lock_owner()) {
      CephContext *cct = m_image_ctx->cct;
      ldout(cct, 15) << dendl;

      m_lock_acquired = true;
      m_image_ctx->exclusive_lock->block_requests(0);

      auto ctx = create_context_callback<
        PromoteRequest<I>,
        &PromoteRequest<I>::handle_acquire_exclusive_lock>(this);

      m_image_ctx->exclusive_lock->acquire_lock(ctx);
      return;
    }
  }

  rollback();
}

template <typename I>
void PromoteRequest<I>::handle_acquire_exclusive_lock(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to acquire exclusive lock: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  } else {
    std::unique_lock locker{m_image_ctx->owner_lock};
    if (m_image_ctx->exclusive_lock != nullptr &&
        !m_image_ctx->exclusive_lock->is_lock_owner()) {
      lderr(cct) << "failed to acquire exclusive lock" << dendl;
      r = m_image_ctx->exclusive_lock->get_unlocked_op_error();
      locker.unlock();
      finish(r);
      return;
    }
  }

  rollback();
}

template <typename I>
void PromoteRequest<I>::rollback() {
  if (m_rollback_snap_id == CEPH_NOSNAP) {
    create_promote_snapshot();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  std::shared_lock image_locker{m_image_ctx->image_lock};

  auto info = m_image_ctx->get_snap_info(m_rollback_snap_id);
  ceph_assert(info != nullptr);
  auto snap_namespace = info->snap_namespace;
  auto snap_name = info->name;

  image_locker.unlock();

  auto ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      PromoteRequest<I>, &PromoteRequest<I>::handle_rollback>(this));

  m_image_ctx->operations->execute_snap_rollback(snap_namespace, snap_name,
                                                 m_progress_ctx, ctx);
}

template <typename I>
void PromoteRequest<I>::handle_rollback(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to rollback: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_promote_snapshot();
}

template <typename I>
void PromoteRequest<I>::create_promote_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>,
    &PromoteRequest<I>::handle_create_promote_snapshot>(this);

  auto req = CreatePrimaryRequest<I>::create(
    m_image_ctx, m_global_image_id, CEPH_NOSNAP,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
     snapshot::CREATE_PRIMARY_FLAG_FORCE), nullptr, ctx);
  req->send();
}

template <typename I>
void PromoteRequest<I>::handle_create_promote_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create promote snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  disable_non_primary_feature();
}

template <typename I>
void PromoteRequest<I>::disable_non_primary_feature() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // remove the non-primary feature flag so that the image can be
  // R/W by standard RBD clients
  librados::ObjectWriteOperation op;
  cls_client::set_features(&op, 0U, RBD_FEATURE_NON_PRIMARY);

  auto aio_comp = create_rados_callback<
    PromoteRequest<I>,
    &PromoteRequest<I>::handle_disable_non_primary_feature>(this);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PromoteRequest<I>::handle_disable_non_primary_feature(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to disable non-primary feature: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  release_exclusive_lock();
}

template <typename I>
void PromoteRequest<I>::release_exclusive_lock() {
  if (m_lock_acquired) {
    std::unique_lock locker{m_image_ctx->owner_lock};
    if (m_image_ctx->exclusive_lock != nullptr) {
      CephContext *cct = m_image_ctx->cct;
      ldout(cct, 15) << dendl;

      m_image_ctx->exclusive_lock->unblock_requests();

      auto ctx = create_context_callback<
        PromoteRequest<I>,
        &PromoteRequest<I>::handle_release_exclusive_lock>(this);

      m_image_ctx->exclusive_lock->release_lock(ctx);
      return;
    }
  }

  finish(0);
}

template <typename I>
void PromoteRequest<I>::handle_release_exclusive_lock(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to release exclusive lock: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void PromoteRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::PromoteRequest<librbd::ImageCtx>;
