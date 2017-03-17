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
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/pool_watcher/RefreshImagesRequest.h"
#include <boost/bind.hpp>

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
    // do nothing
  }

  void handle_image_updated(cls::rbd::MirrorImageState state,
                            const std::string &remote_image_id,
                            const std::string &global_image_id) override {
    bool enabled = (state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
    m_pool_watcher->handle_image_updated(remote_image_id, global_image_id,
                                         enabled);
  }

private:
  PoolWatcher *m_pool_watcher;
};

template <typename I>
PoolWatcher<I>::PoolWatcher(Threads<I> *threads, librados::IoCtx &remote_io_ctx,
                            Listener &listener)
  : m_threads(threads), m_remote_io_ctx(remote_io_ctx), m_listener(listener),
    m_lock(librbd::util::unique_lock_name("rbd::mirror::PoolWatcher", this)) {
  m_mirroring_watcher = new MirroringWatcher(m_remote_io_ctx,
                                             m_threads->work_queue, this);
}

template <typename I>
PoolWatcher<I>::~PoolWatcher() {
  delete m_mirroring_watcher;
}

template <typename I>
bool PoolWatcher<I>::is_blacklisted() const {
  Mutex::Locker locker(m_lock);
  return m_blacklisted;
}

template <typename I>
void PoolWatcher<I>::init(Context *on_finish) {
  dout(5) << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_on_init_finish = on_finish;
  }

  // start async updates for mirror image directory
  register_watcher();
}

template <typename I>
void PoolWatcher<I>::shut_down(Context *on_finish) {
  dout(5) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);

    assert(!m_shutting_down);
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
    Mutex::Locker locker(m_lock);
    assert(m_image_ids_invalid);
    assert(!m_refresh_in_progress);
    m_refresh_in_progress = true;
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
    Mutex::Locker locker(m_lock);
    assert(m_image_ids_invalid);
    assert(m_refresh_in_progress);
    if (r < 0) {
      m_refresh_in_progress = false;
    }
  }

  Context *on_init_finish = nullptr;
  if (r >= 0) {
    refresh_images();
  } else if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;

    Mutex::Locker locker(m_lock);
    m_blacklisted = true;
    std::swap(on_init_finish, m_on_init_finish);
  } else if (r == -ENOENT) {
    dout(5) << "mirroring directory does not exist" << dendl;
    schedule_refresh_images(30);
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
  Context *ctx = new FunctionContext([this](int r) {
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
    Mutex::Locker locker(m_lock);
    assert(m_image_ids_invalid);
    assert(m_refresh_in_progress);

    // clear all pending notification events since we need to perform
    // a full image list refresh
    m_pending_added_image_ids.clear();
    m_pending_removed_image_ids.clear();
    if (!m_updated_images.empty()) {
      auto it = m_updated_images.begin();
      it->invalid = true;

      // only have a single in-flight request -- remove the rest
      ++it;
      while (it != m_updated_images.end()) {
        m_id_to_updated_images.erase({it->global_image_id,
                                      it->remote_image_id});
        it = m_updated_images.erase(it);
      }
    }
  }

  m_async_op_tracker.start_op();
  m_refresh_image_ids.clear();
  Context *ctx = create_context_callback<
    PoolWatcher, &PoolWatcher<I>::handle_refresh_images>(this);
  auto req = pool_watcher::RefreshImagesRequest<I>::create(m_remote_io_ctx,
                                                           &m_refresh_image_ids,
                                                           ctx);
  req->send();
}

template <typename I>
void PoolWatcher<I>::handle_refresh_images(int r) {
  dout(5) << "r=" << r << dendl;

  bool retry_refresh = false;
  Context *on_init_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ids_invalid);
    assert(m_refresh_in_progress);
    m_refresh_in_progress = false;

    if (r >= 0) {
      m_image_ids_invalid = false;
      m_pending_image_ids = m_refresh_image_ids;
      std::swap(on_init_finish, m_on_init_finish);
      schedule_listener();
    } else if (r == -EBLACKLISTED) {
      dout(0) << "detected client is blacklisted during image refresh" << dendl;

      m_blacklisted = true;
      std::swap(on_init_finish, m_on_init_finish);
    } else if (r == -ENOENT) {
      dout(5) << "mirroring directory not found" << dendl;
      m_image_ids_invalid = false;
      m_pending_image_ids.clear();
      std::swap(on_init_finish, m_on_init_finish);
      r = 0;
      schedule_listener();
    } else {
      retry_refresh = true;
    }
  }

  if (retry_refresh) {
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
  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  if (m_shutting_down || m_refresh_in_progress || m_timer_ctx != nullptr) {
    return;
  }

  m_image_ids_invalid = true;
  m_timer_ctx = new FunctionContext([this](int r) {
      processs_refresh_images();
    });
  m_threads->timer->add_event_after(interval, m_timer_ctx);
}

template <typename I>
void PoolWatcher<I>::handle_rewatch_complete(int r) {
  dout(5) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;

    Mutex::Locker locker(m_lock);
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
void PoolWatcher<I>::handle_image_updated(const std::string &remote_image_id,
                                       const std::string &global_image_id,
                                       bool enabled) {
  dout(10) << "remote_image_id=" << remote_image_id << ", "
           << "global_image_id=" << global_image_id << ", "
           << "enabled=" << enabled << dendl;

  Mutex::Locker locker(m_lock);
  ImageId image_id(global_image_id, remote_image_id);
  m_pending_added_image_ids.erase(image_id);
  m_pending_removed_image_ids.erase(image_id);

  auto id = std::make_pair(global_image_id, remote_image_id);
  auto id_it = m_id_to_updated_images.find(id);
  if (id_it != m_id_to_updated_images.end()) {
    id_it->second->enabled = enabled;
    id_it->second->invalid = false;
  } else if (enabled) {
    // need to resolve the image name before notifying listener
    auto it = m_updated_images.emplace(m_updated_images.end(),
                                       global_image_id, remote_image_id);
    m_id_to_updated_images[id] = it;
    schedule_get_image_name();
  } else {
    // delete image w/ if no resolve name in-flight
    m_pending_removed_image_ids.insert(image_id);
    schedule_listener();
  }
}

template <typename I>
void PoolWatcher<I>::schedule_get_image_name() {
  assert(m_lock.is_locked());
  if (m_shutting_down || m_blacklisted || m_updated_images.empty() ||
      m_get_name_in_progress) {
    return;
  }
  m_get_name_in_progress = true;

  auto &updated_image = m_updated_images.front();
  dout(10) << "global_image_id=" << updated_image.global_image_id << ", "
           << "remote_image_id=" << updated_image.remote_image_id << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, updated_image.remote_image_id);

  m_async_op_tracker.start_op();

  m_out_bl.clear();
  librados::AioCompletion *aio_comp = create_rados_callback<
    PoolWatcher, &PoolWatcher<I>::handle_get_image_name>(this);
  int r = m_remote_io_ctx.aio_operate(RBD_DIRECTORY, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void PoolWatcher<I>::handle_get_image_name(int r) {
  dout(10) << "r=" << r << dendl;

  std::string name;
  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::dir_get_name_finish(&it, &name);
  }

  bool image_ids_invalid = false;
  {
    Mutex::Locker locker(m_lock);
    assert(!m_updated_images.empty());
    m_get_name_in_progress = false;

    auto updated_image = m_updated_images.front();
    m_updated_images.pop_front();
    m_id_to_updated_images.erase(std::make_pair(updated_image.global_image_id,
                                                updated_image.remote_image_id));

    if (r == 0) {
      // since names are resolved in event order -- the current update is
      // the latest state
      ImageId image_id(updated_image.global_image_id,
                       updated_image.remote_image_id, name);
      m_pending_added_image_ids.erase(image_id);
      m_pending_removed_image_ids.erase(image_id);
      if (!updated_image.invalid) {
        if (updated_image.enabled) {
          m_pending_added_image_ids.insert(image_id);
        } else {
          m_pending_removed_image_ids.insert(image_id);
        }
        schedule_listener();
      }
    } else if (r == -EBLACKLISTED) {
      dout(0) << "detected client is blacklisted" << dendl;

      m_blacklisted = true;
    } else if (r == -ENOENT) {
      dout(10) << "image removed after add notification" << dendl;
    } else {
      derr << "error resolving image name " << updated_image.remote_image_id
           << " (" << updated_image.global_image_id << "): " << cpp_strerror(r)
           << dendl;
      image_ids_invalid = true;
    }

    if (!image_ids_invalid) {
      schedule_get_image_name();
    }
  }

  if (image_ids_invalid) {
    schedule_refresh_images(5);
  }
  m_async_op_tracker.finish_op();
}

template <typename I>
void PoolWatcher<I>::processs_refresh_images() {
  assert(m_threads->timer_lock.is_locked());
  assert(m_timer_ctx != nullptr);
  m_timer_ctx = nullptr;

  // execute outside of the timer's lock
  m_async_op_tracker.start_op();
  Context *ctx = new FunctionContext([this](int r) {
      register_watcher();
      m_async_op_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void PoolWatcher<I>::schedule_listener() {
  assert(m_lock.is_locked());
  m_pending_updates = true;
  if (m_shutting_down || m_image_ids_invalid || m_notify_listener_in_progress) {
    return;
  }

  dout(20) << dendl;

  m_async_op_tracker.start_op();
  Context *ctx = new FunctionContext([this](int r) {
      notify_listener();
      m_async_op_tracker.finish_op();
    });

  m_notify_listener_in_progress = true;
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void PoolWatcher<I>::notify_listener() {
  dout(10) << dendl;

  ImageIds added_image_ids;
  ImageIds removed_image_ids;
  {
    Mutex::Locker locker(m_lock);
    assert(m_notify_listener_in_progress);

    // if the watch failed while we didn't own the lock, we are going
    // to need to perform a full refresh
    if (m_image_ids_invalid) {
      m_notify_listener_in_progress = false;
      return;
    }

    // merge add/remove notifications into pending set (a given image
    // can only be in one set or another)
    for (auto it = m_pending_removed_image_ids.begin();
         it != m_pending_removed_image_ids.end(); ) {
      if (std::find_if(m_updated_images.begin(), m_updated_images.end(),
                       [&it](const UpdatedImage &updated_image) {
              return (it->id == updated_image.remote_image_id);
            }) != m_updated_images.end()) {
        // still resolving the name -- so keep it in the pending set
        auto image_id_it = m_image_ids.find(*it);
        if (image_id_it != m_image_ids.end()) {
          m_pending_image_ids.insert(*image_id_it);
        }
        ++it;
        continue;
      }

      // merge the remove event into the pending set
      m_pending_image_ids.erase(*it);
      it = m_pending_removed_image_ids.erase(it);
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

  m_listener.handle_update(added_image_ids, removed_image_ids);

  {
    Mutex::Locker locker(m_lock);
    m_notify_listener_in_progress = false;
    if (m_pending_updates) {
      schedule_listener();
    }
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::PoolWatcher<librbd::ImageCtx>;
