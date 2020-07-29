// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/PoolWatcher.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
#include "librbd/api/Mirror.h"
#include "librbd/asio/ContextWQ.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/pool_watcher/RefreshImagesRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::PoolWatcher: " << this << " " \
                           << __func__ << ": "

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace rbd {
namespace mirror {

template <typename I>
class PoolWatcher<I>::MirroringWatcher : public librbd::MirroringWatcher<I> {
public:
  using ContextWQ = typename std::decay<
    typename std::remove_pointer<
      decltype(Threads<I>::work_queue)>::type>::type;

  MirroringWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                   PoolWatcher *pool_watcher)
    : librbd::MirroringWatcher<I>(io_ctx, work_queue),
      m_pool_watcher(pool_watcher) {
  }

  void handle_rewatch_complete(int r) override {
    m_pool_watcher->handle_rewatch_complete(r);
  }

  void handle_mode_updated(cls::rbd::MirrorMode mirror_mode) override {
    // invalidate all image state and refresh the pool contents
    m_pool_watcher->schedule_refresh_images(5);
  }

  void handle_image_updated(cls::rbd::MirrorImageState state,
                            const std::string &image_id,
                            const std::string &global_image_id) override {
    bool enabled = (state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
    m_pool_watcher->handle_image_updated(image_id, global_image_id,
                                         enabled);
  }

private:
  PoolWatcher *m_pool_watcher;
};

template <typename I>
PoolWatcher<I>::PoolWatcher(Threads<I> *threads,
                            librados::IoCtx &io_ctx,
                            const std::string& mirror_uuid,
                            pool_watcher::Listener &listener)
  : m_threads(threads),
    m_io_ctx(io_ctx),
    m_mirror_uuid(mirror_uuid),
    m_listener(listener),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
                              "rbd::mirror::PoolWatcher", this))) {
  m_mirroring_watcher = new MirroringWatcher(m_io_ctx,
                                             m_threads->work_queue, this);
}

template <typename I>
PoolWatcher<I>::~PoolWatcher() {
  delete m_mirroring_watcher;
}

template <typename I>
bool PoolWatcher<I>::is_blacklisted() const {
  std::lock_guard locker{m_lock};
  return m_blacklisted;
}

template <typename I>
void PoolWatcher<I>::init(Context *on_finish) {
  dout(5) << dendl;

  {
    std::lock_guard locker{m_lock};
    m_on_init_finish = on_finish;

    ceph_assert(!m_refresh_in_progress);
    m_refresh_in_progress = true;
  }

  // start async updates for mirror image directory
  register_watcher();
}

template <typename I>
void PoolWatcher<I>::shut_down(Context *on_finish) {
  dout(5) << dendl;

  {
    std::scoped_lock locker{m_threads->timer_lock, m_lock};

    ceph_assert(!m_shutting_down);
    m_shutting_down = true;
    if (m_timer_ctx != nullptr) {
      m_threads->timer->cancel_event(m_timer_ctx);
      m_timer_ctx = nullptr;
    }
  }

  // in-progress unregister tracked as async op
  unregister_watcher();

  m_async_op_tracker.wait_for_ops(on_finish);
}

template <typename I>
void PoolWatcher<I>::register_watcher() {
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_image_ids_invalid);
    ceph_assert(m_refresh_in_progress);
  }

  // if the watch registration is in-flight, let the watcher
  // handle the transition -- only (re-)register if it's not registered
  if (!m_mirroring_watcher->is_unregistered()) {
    refresh_images();
    return;
  }

  // first time registering or the watch failed
  dout(5) << dendl;
  m_async_op_tracker.start_op();

  Context *ctx = create_context_callback<
    PoolWatcher, &PoolWatcher<I>::handle_register_watcher>(this);
  m_mirroring_watcher->register_watch(ctx);
}

template <typename I>
void PoolWatcher<I>::handle_register_watcher(int r) {
  dout(5) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_image_ids_invalid);
    ceph_assert(m_refresh_in_progress);
    if (r < 0) {
      m_refresh_in_progress = false;
    }
  }

  Context *on_init_finish = nullptr;
  if (r >= 0) {
    refresh_images();
  } else if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;

    std::lock_guard locker{m_lock};
    m_blacklisted = true;
    std::swap(on_init_finish, m_on_init_finish);
  } else if (r == -ENOENT) {
    dout(5) << "mirroring directory does not exist" << dendl;
    schedule_refresh_images(30);

    std::lock_guard locker{m_lock};
    std::swap(on_init_finish, m_on_init_finish);
  } else {
    derr << "unexpected error registering mirroring directory watch: "
         << cpp_strerror(r) << dendl;
    schedule_refresh_images(10);
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void PoolWatcher<I>::unregister_watcher() {
  dout(5) << dendl;

  m_async_op_tracker.start_op();
  Context *ctx = new LambdaContext([this](int r) {
      dout(5) << "unregister_watcher: r=" << r << dendl;
      if (r < 0) {
        derr << "error unregistering watcher for "
             << m_mirroring_watcher->get_oid() << " object: " << cpp_strerror(r)
             << dendl;
      }
      m_async_op_tracker.finish_op();
    });

  m_mirroring_watcher->unregister_watch(ctx);
}

template <typename I>
void PoolWatcher<I>::refresh_images() {
  dout(5) << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_image_ids_invalid);
    ceph_assert(m_refresh_in_progress);

    // clear all pending notification events since we need to perform
    // a full image list refresh
    m_pending_added_image_ids.clear();
    m_pending_removed_image_ids.clear();
  }

  m_async_op_tracker.start_op();
  m_refresh_image_ids.clear();
  Context *ctx = create_context_callback<
    PoolWatcher, &PoolWatcher<I>::handle_refresh_images>(this);
  auto req = pool_watcher::RefreshImagesRequest<I>::create(m_io_ctx,
                                                           &m_refresh_image_ids,
                                                           ctx);
  req->send();
}

template <typename I>
void PoolWatcher<I>::handle_refresh_images(int r) {
  dout(5) << "r=" << r << dendl;

  bool deferred_refresh = false;
  bool retry_refresh = false;
  Context *on_init_finish = nullptr;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_image_ids_invalid);
    ceph_assert(m_refresh_in_progress);
    m_refresh_in_progress = false;

    if (r == -ENOENT) {
      dout(5) << "mirroring directory not found" << dendl;
      r = 0;
      m_refresh_image_ids.clear();
    }

    if (m_deferred_refresh) {
      // need to refresh -- skip the notification
      deferred_refresh = true;
    } else if (r >= 0) {
      m_pending_image_ids = std::move(m_refresh_image_ids);
      m_image_ids_invalid = false;
      std::swap(on_init_finish, m_on_init_finish);

      schedule_listener();
    } else if (r == -EBLACKLISTED) {
      dout(0) << "detected client is blacklisted during image refresh" << dendl;

      m_blacklisted = true;
      std::swap(on_init_finish, m_on_init_finish);
    } else {
      retry_refresh = true;
    }
  }

  if (deferred_refresh) {
    dout(5) << "scheduling deferred refresh" << dendl;
    schedule_refresh_images(0);
  } else if (retry_refresh) {
    derr << "failed to retrieve mirroring directory: " << cpp_strerror(r)
         << dendl;
    schedule_refresh_images(10);
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void PoolWatcher<I>::schedule_refresh_images(double interval) {
  std::scoped_lock locker{m_threads->timer_lock, m_lock};
  if (m_shutting_down || m_refresh_in_progress || m_timer_ctx != nullptr) {
    if (m_refresh_in_progress && !m_deferred_refresh) {
      dout(5) << "deferring refresh until in-flight refresh completes" << dendl;
      m_deferred_refresh = true;
    }
    return;
  }

  m_image_ids_invalid = true;
  m_timer_ctx = m_threads->timer->add_event_after(
    interval,
    new LambdaContext([this](int r) {
	process_refresh_images();
      }));
}

template <typename I>
void PoolWatcher<I>::handle_rewatch_complete(int r) {
  dout(5) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;

    std::lock_guard locker{m_lock};
    m_blacklisted = true;
    return;
  } else if (r == -ENOENT) {
    dout(5) << "mirroring directory deleted" << dendl;
  } else if (r < 0) {
    derr << "unexpected error re-registering mirroring directory watch: "
         << cpp_strerror(r) << dendl;
  }

  schedule_refresh_images(5);
}

template <typename I>
void PoolWatcher<I>::handle_image_updated(const std::string &id,
                                          const std::string &global_image_id,
                                          bool enabled) {
  dout(10) << "image_id=" << id << ", "
           << "global_image_id=" << global_image_id << ", "
           << "enabled=" << enabled << dendl;

  std::lock_guard locker{m_lock};
  ImageId image_id(global_image_id, id);
  m_pending_added_image_ids.erase(image_id);
  m_pending_removed_image_ids.erase(image_id);

  if (enabled) {
    m_pending_added_image_ids.insert(image_id);
    schedule_listener();
  } else {
    m_pending_removed_image_ids.insert(image_id);
    schedule_listener();
  }
}

template <typename I>
void PoolWatcher<I>::process_refresh_images() {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_timer_ctx != nullptr);
  m_timer_ctx = nullptr;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(!m_refresh_in_progress);
    m_refresh_in_progress = true;
    m_deferred_refresh = false;
  }

  // execute outside of the timer's lock
  m_async_op_tracker.start_op();
  Context *ctx = new LambdaContext([this](int r) {
      register_watcher();
      m_async_op_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void PoolWatcher<I>::schedule_listener() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  m_pending_updates = true;
  if (m_shutting_down || m_image_ids_invalid || m_notify_listener_in_progress) {
    return;
  }

  dout(20) << dendl;

  m_async_op_tracker.start_op();
  Context *ctx = new LambdaContext([this](int r) {
      notify_listener();
      m_async_op_tracker.finish_op();
    });

  m_notify_listener_in_progress = true;
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void PoolWatcher<I>::notify_listener() {
  dout(10) << dendl;

  std::string mirror_uuid;
  ImageIds added_image_ids;
  ImageIds removed_image_ids;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_notify_listener_in_progress);
  }

  if (!removed_image_ids.empty()) {
    m_listener.handle_update(mirror_uuid, {}, std::move(removed_image_ids));
    removed_image_ids.clear();
  }

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_notify_listener_in_progress);

    // if the watch failed while we didn't own the lock, we are going
    // to need to perform a full refresh
    if (m_image_ids_invalid) {
      m_notify_listener_in_progress = false;
      return;
    }

    // merge add/remove notifications into pending set (a given image
    // can only be in one set or another)
    for (auto &image_id : m_pending_removed_image_ids) {
      dout(20) << "image_id=" << image_id << dendl;
      m_pending_image_ids.erase(image_id);
    }

    for (auto &image_id : m_pending_added_image_ids) {
      dout(20) << "image_id=" << image_id << dendl;
      m_pending_image_ids.erase(image_id);
      m_pending_image_ids.insert(image_id);
    }
    m_pending_added_image_ids.clear();

    // compute added/removed images
    for (auto &image_id : m_image_ids) {
      auto it = m_pending_image_ids.find(image_id);
      if (it == m_pending_image_ids.end() || it->id != image_id.id) {
        removed_image_ids.insert(image_id);
      }
    }
    for (auto &image_id : m_pending_image_ids) {
      auto it = m_image_ids.find(image_id);
      if (it == m_image_ids.end() || it->id != image_id.id) {
        added_image_ids.insert(image_id);
      }
    }

    m_pending_updates = false;
    m_image_ids = m_pending_image_ids;
  }

  m_listener.handle_update(m_mirror_uuid, std::move(added_image_ids),
                           std::move(removed_image_ids));

  {
    std::lock_guard locker{m_lock};
    m_notify_listener_in_progress = false;
    if (m_pending_updates) {
      schedule_listener();
    }
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::PoolWatcher<librbd::ImageCtx>;
