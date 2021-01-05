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
#include "tools/rbd_mirror/pool_watcher/RefreshEntitiesRequest.h"

#include <numeric>

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
    m_pool_watcher->schedule_refresh_entities(5);
  }

  void handle_image_updated(cls::rbd::MirrorImageState state,
                            const std::string &image_id,
                            const std::string &global_image_id) override {
    bool enabled = (state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
    m_pool_watcher->handle_image_updated(image_id, global_image_id,
                                         enabled);
  }

  void handle_group_updated(cls::rbd::MirrorGroupState state,
                            const std::string &group_id,
                            const std::string &global_group_id,
                            size_t image_count) override {
    bool enabled = (state == cls::rbd::MIRROR_GROUP_STATE_ENABLED);
    m_pool_watcher->handle_group_updated(group_id, global_group_id, enabled,
                                         image_count);
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
bool PoolWatcher<I>::is_blocklisted() const {
  std::lock_guard locker{m_lock};
  return m_blocklisted;
}

template <typename I>
size_t PoolWatcher<I>::get_image_count() const {
  std::lock_guard locker{m_lock};
  return std::accumulate(m_entities.begin(), m_entities.end(), 0,
                         [](size_t count,
                            const std::pair<MirrorEntity, std::string> &p) {
                           return count + p.first.count;
                         });
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
    ceph_assert(m_entities_invalid);
    ceph_assert(m_refresh_in_progress);
  }

  // if the watch registration is in-flight, let the watcher
  // handle the transition -- only (re-)register if it's not registered
  if (!m_mirroring_watcher->is_unregistered()) {
    refresh_entities();
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
    ceph_assert(m_entities_invalid);
    ceph_assert(m_refresh_in_progress);
    if (r < 0) {
      m_refresh_in_progress = false;
    }
  }

  Context *on_init_finish = nullptr;
  if (r >= 0) {
    refresh_entities();
  } else if (r == -EBLOCKLISTED) {
    dout(0) << "detected client is blocklisted" << dendl;

    std::lock_guard locker{m_lock};
    m_blocklisted = true;
    std::swap(on_init_finish, m_on_init_finish);
  } else if (r == -ENOENT) {
    dout(5) << "mirroring directory does not exist" << dendl;
    {
      std::lock_guard locker{m_lock};
      std::swap(on_init_finish, m_on_init_finish);
    }

    schedule_refresh_entities(30);
  } else {
    derr << "unexpected error registering mirroring directory watch: "
         << cpp_strerror(r) << dendl;
    schedule_refresh_entities(10);
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
void PoolWatcher<I>::refresh_entities() {
  dout(5) << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_entities_invalid);
    ceph_assert(m_refresh_in_progress);

    // clear all pending notification events since we need to perform
    // a full entity list refresh
    m_pending_added_entities.clear();
    m_pending_removed_entities.clear();
  }

  m_async_op_tracker.start_op();
  m_refresh_entities.clear();
  Context *ctx = create_context_callback<
    PoolWatcher, &PoolWatcher<I>::handle_refresh_entities>(this);
  auto req = pool_watcher::RefreshEntitiesRequest<I>::create(
    m_io_ctx, &m_refresh_entities, ctx);
  req->send();
}

template <typename I>
void PoolWatcher<I>::handle_refresh_entities(int r) {
  dout(5) << "r=" << r << dendl;

  bool deferred_refresh = false;
  bool retry_refresh = false;
  Context *on_init_finish = nullptr;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_entities_invalid);
    ceph_assert(m_refresh_in_progress);
    m_refresh_in_progress = false;

    if (r == -ENOENT) {
      dout(5) << "mirroring directory not found" << dendl;
      r = 0;
      m_refresh_entities.clear();
    }

    if (m_deferred_refresh) {
      // need to refresh -- skip the notification
      deferred_refresh = true;
    } else if (r >= 0) {
      m_pending_entities = std::move(m_refresh_entities);
      m_entities_invalid = false;
      std::swap(on_init_finish, m_on_init_finish);

      schedule_listener();
    } else if (r == -EBLOCKLISTED) {
      dout(0) << "detected client is blocklisted during entity refresh" << dendl;

      m_blocklisted = true;
      std::swap(on_init_finish, m_on_init_finish);
    } else {
      retry_refresh = true;
    }
  }

  if (deferred_refresh) {
    dout(5) << "scheduling deferred refresh" << dendl;
    schedule_refresh_entities(0);
  } else if (retry_refresh) {
    derr << "failed to retrieve mirroring directory: " << cpp_strerror(r)
         << dendl;
    schedule_refresh_entities(10);
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void PoolWatcher<I>::schedule_refresh_entities(double interval) {
  std::scoped_lock locker{m_threads->timer_lock, m_lock};
  if (m_shutting_down || m_refresh_in_progress || m_timer_ctx != nullptr) {
    if (m_refresh_in_progress && !m_deferred_refresh) {
      dout(5) << "deferring refresh until in-flight refresh completes" << dendl;
      m_deferred_refresh = true;
    }
    return;
  }

  m_entities_invalid = true;
  m_timer_ctx = m_threads->timer->add_event_after(
    interval,
    new LambdaContext([this](int r) {
	process_refresh_entities();
      }));
}

template <typename I>
void PoolWatcher<I>::handle_rewatch_complete(int r) {
  dout(5) << "r=" << r << dendl;

  if (r == -EBLOCKLISTED) {
    dout(0) << "detected client is blocklisted" << dendl;

    std::lock_guard locker{m_lock};
    m_blocklisted = true;
    return;
  } else if (r == -ENOENT) {
    dout(5) << "mirroring directory deleted" << dendl;
  } else if (r < 0) {
    derr << "unexpected error re-registering mirroring directory watch: "
         << cpp_strerror(r) << dendl;
  }

  schedule_refresh_entities(5);
}

template <typename I>
void PoolWatcher<I>::handle_image_updated(const std::string &id,
                                          const std::string &global_image_id,
                                          bool enabled) {
  dout(10) << "image_id=" << id << ", "
           << "global_image_id=" << global_image_id << ", "
           << "enabled=" << enabled << dendl;

  std::lock_guard locker{m_lock};
  MirrorEntity entity(MIRROR_ENTITY_TYPE_IMAGE, global_image_id, 1);
  m_pending_added_entities.erase(entity);
  m_pending_removed_entities.erase(entity);

  if (enabled) {
    m_pending_added_entities.insert({entity, id});
    schedule_listener();
  } else {
    m_pending_removed_entities.insert({entity, id});
    schedule_listener();
  }
}

template <typename I>
void PoolWatcher<I>::handle_group_updated(const std::string &id,
                                          const std::string &global_group_id,
                                          bool enabled, size_t image_count) {
  dout(10) << "group_id=" << id << ", "
           << "global_group_id=" << global_group_id << ", "
           << "image_count=" << image_count << ", "
           << "enabled=" << enabled << dendl;

  std::lock_guard locker{m_lock};
  MirrorEntity entity(MIRROR_ENTITY_TYPE_GROUP, global_group_id, image_count);
  m_pending_added_entities.erase(entity);
  m_pending_removed_entities.erase(entity);

  if (enabled) {
    m_pending_added_entities.insert({entity, id});
    schedule_listener();
  } else {
    m_pending_removed_entities.insert({entity, id});
    schedule_listener();
  }
}

template <typename I>
void PoolWatcher<I>::process_refresh_entities() {
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
  if (m_shutting_down || m_entities_invalid || m_notify_listener_in_progress) {
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
  MirrorEntities added_entities;
  MirrorEntities removed_entities;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_notify_listener_in_progress);

    // if the watch failed while we didn't own the lock, we are going
    // to need to perform a full refresh
    if (m_entities_invalid) {
      m_notify_listener_in_progress = false;
      return;
    }

    // merge add/remove notifications into pending set (a given entity
    // can only be in one set or another)
    for (auto &[entity, id] : m_pending_removed_entities) {
      dout(20) << "removed pending entity={" << entity << "}" << dendl;
      m_pending_entities.erase(entity);
      auto it = m_entities.find(entity);
      if (it != m_entities.end()) {
        m_entities.erase(entity);
        m_entities.insert({entity, id});
      }
    }
    m_pending_removed_entities.clear();

    for (auto &[entity, id] : m_pending_added_entities) {
      dout(20) << "added pending entity={" << entity << "}" << dendl;
      m_pending_entities.erase(entity);
      m_pending_entities.insert({entity, id});
    }
    m_pending_added_entities.clear();

    // compute added/removed images
    for (auto &[entity, id] : m_entities) {
      auto it = m_pending_entities.find(entity);
      // If previous entity is not there in current set of entities or if
      // their id's don't match then consider its removed
      if (it == m_pending_entities.end() || it->second != id) { /* PK? */
        removed_entities.insert(entity);
      }
    }
    for (auto &[entity, id] : m_pending_entities) {
      auto it = m_entities.find(entity);
      // If current entity is not there in previous set of entities or if
      // their id's don't match then consider its added
      if (it == m_entities.end() || it->second != id ||
          it->first.count < entity.count) {
        added_entities.insert(entity);
      }
    }

    m_pending_updates = false;
    m_entities = m_pending_entities;
  }

  m_listener.handle_update(m_mirror_uuid, std::move(added_entities),
                           std::move(removed_entities));
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
