// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "tools/rbd_mirror/MirrorStatusWatcher.h"
#include "tools/rbd_mirror/Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::MirrorStatusUpdater " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace mirror {

static const double UPDATE_INTERVAL_SECONDS = 30;
static const uint32_t MAX_UPDATES_PER_OP = 100;

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
MirrorStatusUpdater<I>::MirrorStatusUpdater(
    librados::IoCtx& io_ctx, Threads<I> *threads,
    const std::string& local_mirror_uuid)
  : m_io_ctx(io_ctx), m_threads(threads),
    m_local_mirror_uuid(local_mirror_uuid),
    m_lock(ceph::make_mutex("rbd::mirror::MirrorStatusUpdater " +
                              stringify(m_io_ctx.get_id()))) {
  dout(10) << "local_mirror_uuid=" << local_mirror_uuid << ", "
           << "pool_id=" << m_io_ctx.get_id() << dendl;
}

template <typename I>
MirrorStatusUpdater<I>::~MirrorStatusUpdater() {
  ceph_assert(!m_initialized);
  delete m_mirror_status_watcher;
}

template <typename I>
void MirrorStatusUpdater<I>::init(Context* on_finish) {
  dout(10) << dendl;

  ceph_assert(!m_initialized);
  m_initialized = true;

  {
    std::lock_guard timer_locker{m_threads->timer_lock};
    schedule_timer_task();
  }

  init_mirror_status_watcher(on_finish);
}

template <typename I>
void MirrorStatusUpdater<I>::init_mirror_status_watcher(Context* on_finish) {
  dout(10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_init_mirror_status_watcher(r, on_finish);
    });
  m_mirror_status_watcher = MirrorStatusWatcher<I>::create(
    m_io_ctx, m_threads->work_queue);
  m_mirror_status_watcher->init(ctx);
}

template <typename I>
void MirrorStatusUpdater<I>::handle_init_mirror_status_watcher(
    int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to init mirror status watcher: " << cpp_strerror(r)
         << dendl;

    delete m_mirror_status_watcher;
    m_mirror_status_watcher = nullptr;

    on_finish = new LambdaContext([r, on_finish](int) {
        on_finish->complete(r);
      });
    shut_down(on_finish);
    return;
  }

  m_threads->work_queue->queue(on_finish, 0);
}

template <typename I>
void MirrorStatusUpdater<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  {
    std::lock_guard timer_locker{m_threads->timer_lock};
    ceph_assert(m_timer_task != nullptr);
    m_threads->timer->cancel_event(m_timer_task);
  }

  {
    std::unique_lock locker(m_lock);
    ceph_assert(m_initialized);
    m_initialized = false;
  }

  shut_down_mirror_status_watcher(on_finish);
}

template <typename I>
void MirrorStatusUpdater<I>::shut_down_mirror_status_watcher(
    Context* on_finish) {
  if (m_mirror_status_watcher == nullptr) {
    finalize_shutdown(0, on_finish);
    return;
  }

  dout(10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_shut_down_mirror_status_watcher(r, on_finish);
    });
  m_mirror_status_watcher->shut_down(ctx);
}

template <typename I>
void MirrorStatusUpdater<I>::handle_shut_down_mirror_status_watcher(
    int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "failed to shut down mirror status watcher: " << cpp_strerror(r)
         << dendl;
  }

  finalize_shutdown(r, on_finish);
}

template <typename I>
void MirrorStatusUpdater<I>::finalize_shutdown(int r, Context* on_finish) {
  dout(10) << dendl;

  {
    std::unique_lock locker(m_lock);
    if (m_update_in_progress) {
      if (r < 0) {
        on_finish = new LambdaContext([r, on_finish](int) {
            on_finish->complete(r);
          });
      }

      m_update_on_finish_ctxs.push_back(on_finish);
      return;
    }
  }

  m_threads->work_queue->queue(on_finish, r);
}

template <typename I>
bool MirrorStatusUpdater<I>::exists(const std::string& global_image_id) {
  dout(15) << "global_image_id=" << global_image_id << dendl;

  std::unique_lock locker(m_lock);
  return (m_global_image_status.count(global_image_id) > 0);
}

template <typename I>
void MirrorStatusUpdater<I>::set_mirror_image_status(
    const std::string& global_image_id,
    const cls::rbd::MirrorImageSiteStatus& mirror_image_site_status,
    bool immediate_update) {
  dout(15) << "global_image_id=" << global_image_id << ", "
           << "mirror_image_site_status=" << mirror_image_site_status << dendl;

  std::unique_lock locker(m_lock);

  m_global_image_status[global_image_id] = mirror_image_site_status;
  if (immediate_update) {
    m_update_global_image_ids.insert(global_image_id);
    queue_update_task(std::move(locker));
  }
}

template <typename I>
void MirrorStatusUpdater<I>::remove_refresh_mirror_image_status(
    const std::string& global_image_id,
    Context* on_finish) {
  if (try_remove_mirror_image_status(global_image_id, false, false,
                                     on_finish)) {
    m_threads->work_queue->queue(on_finish, 0);
  }
}

template <typename I>
void MirrorStatusUpdater<I>::remove_mirror_image_status(
    const std::string& global_image_id, bool immediate_update,
    Context* on_finish) {
  if (try_remove_mirror_image_status(global_image_id, true, immediate_update,
                                     on_finish)) {
    m_threads->work_queue->queue(on_finish, 0);
  }
}

template <typename I>
bool MirrorStatusUpdater<I>::try_remove_mirror_image_status(
    const std::string& global_image_id, bool queue_update,
    bool immediate_update, Context* on_finish) {
  dout(15) << "global_image_id=" << global_image_id << ", "
           << "queue_update=" << queue_update << ", "
           << "immediate_update=" << immediate_update << dendl;

  std::unique_lock locker(m_lock);
  if ((m_update_in_flight &&
       m_updating_global_image_ids.count(global_image_id) > 0) ||
      ((m_update_in_progress || m_update_requested) &&
       m_update_global_image_ids.count(global_image_id) > 0)) {
    // if update is scheduled/in-progress, wait for it to complete
    on_finish = new LambdaContext(
      [this, global_image_id, queue_update, immediate_update,
             on_finish](int r) {
        if (try_remove_mirror_image_status(global_image_id, queue_update,
                                           immediate_update, on_finish)) {
          on_finish->complete(0);
        }
      });
    m_update_on_finish_ctxs.push_back(on_finish);
    return false;
  }

  m_global_image_status.erase(global_image_id);
  if (queue_update) {
    m_update_global_image_ids.insert(global_image_id);
    if (immediate_update) {
      queue_update_task(std::move(locker));
    }
  }

  return true;
}

template <typename I>
void MirrorStatusUpdater<I>::schedule_timer_task() {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_timer_task == nullptr);
  m_timer_task = create_context_callback<
    MirrorStatusUpdater<I>,
    &MirrorStatusUpdater<I>::handle_timer_task>(this);
  m_threads->timer->add_event_after(UPDATE_INTERVAL_SECONDS, m_timer_task);
}

template <typename I>
void MirrorStatusUpdater<I>::handle_timer_task(int r) {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_timer_task != nullptr);
  m_timer_task = nullptr;
  schedule_timer_task();

  std::unique_lock locker(m_lock);
  for (auto& pair : m_global_image_status) {
    m_update_global_image_ids.insert(pair.first);
  }

  queue_update_task(std::move(locker));
}

template <typename I>
void MirrorStatusUpdater<I>::queue_update_task(
  std::unique_lock<ceph::mutex>&& locker) {
  if (!m_initialized) {
    return;
  }

  if (m_update_in_progress) {
    if (m_update_in_flight) {
      dout(10) << "deferring update due to in-flight ops" << dendl;
      m_update_requested = true;
    }
    return;
  }

  m_update_in_progress = true;
  ceph_assert(!m_update_in_flight);
  ceph_assert(!m_update_requested);
  locker.unlock();

  dout(10) << dendl;
  auto ctx = create_context_callback<
    MirrorStatusUpdater<I>,
    &MirrorStatusUpdater<I>::update_task>(this);
  m_threads->work_queue->queue(ctx);
}

template <typename I>
void MirrorStatusUpdater<I>::update_task(int r) {
  dout(10) << dendl;

  std::unique_lock locker(m_lock);
  ceph_assert(m_update_in_progress);
  ceph_assert(!m_update_in_flight);
  m_update_in_flight = true;

  std::swap(m_updating_global_image_ids, m_update_global_image_ids);
  auto updating_global_image_ids = m_updating_global_image_ids;
  auto global_image_status = m_global_image_status;
  locker.unlock();

  Context* ctx = create_context_callback<
    MirrorStatusUpdater<I>,
    &MirrorStatusUpdater<I>::handle_update_task>(this);
  if (updating_global_image_ids.empty()) {
    ctx->complete(0);
    return;
  }

  auto gather = new C_Gather(g_ceph_context, ctx);

  auto it = updating_global_image_ids.begin();
  while (it != updating_global_image_ids.end()) {
    librados::ObjectWriteOperation op;
    uint32_t op_count = 0;

    while (it != updating_global_image_ids.end() &&
           op_count < MAX_UPDATES_PER_OP) {
      auto& global_image_id = *it;
      ++it;

      auto status_it = global_image_status.find(global_image_id);
      if (status_it == global_image_status.end()) {
        librbd::cls_client::mirror_image_status_remove(&op, global_image_id);
        ++op_count;
        continue;
      }

      status_it->second.mirror_uuid = m_local_mirror_uuid;
      librbd::cls_client::mirror_image_status_set(&op, global_image_id,
                                                  status_it->second);
      ++op_count;
    }

    auto aio_comp = create_rados_callback(gather->new_sub());
    int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
    ceph_assert(r == 0);
    aio_comp->release();
  }

  gather->activate();
}

template <typename I>
void MirrorStatusUpdater<I>::handle_update_task(int r) {
  dout(10) << dendl;
  if (r < 0) {
    derr << "failed to update mirror image statuses: " << cpp_strerror(r)
         << dendl;
  }

  std::unique_lock locker(m_lock);

  Contexts on_finish_ctxs;
  std::swap(on_finish_ctxs, m_update_on_finish_ctxs);

  ceph_assert(m_update_in_progress);
  m_update_in_progress = false;

  ceph_assert(m_update_in_flight);
  m_update_in_flight = false;

  m_updating_global_image_ids.clear();

  if (m_update_requested) {
    m_update_requested = false;
    queue_update_task(std::move(locker));
  } else {
    locker.unlock();
  }

  for (auto on_finish : on_finish_ctxs) {
    on_finish->complete(0);
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::MirrorStatusUpdater<librbd::ImageCtx>;
