// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/TrashWatcher.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::TrashWatcher: " \
                           << this << " " << __func__ << ": "

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace rbd {
namespace mirror {
namespace image_deleter {

namespace {

const size_t MAX_RETURN = 1024;

} // anonymous namespace

template <typename I>
TrashWatcher<I>::TrashWatcher(librados::IoCtx &io_ctx, Threads<I> *threads,
                              TrashListener& trash_listener)
  : librbd::TrashWatcher<I>(io_ctx, threads->work_queue),
    m_io_ctx(io_ctx), m_threads(threads), m_trash_listener(trash_listener),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::image_deleter::TrashWatcher", this))) {
}

template <typename I>
void TrashWatcher<I>::init(Context *on_finish) {
  dout(5) << dendl;

  {
    std::lock_guard locker{m_lock};
    m_on_init_finish = on_finish;

    ceph_assert(!m_trash_list_in_progress);
    m_trash_list_in_progress = true;
  }

  create_trash();
}

template <typename I>
void TrashWatcher<I>::shut_down(Context *on_finish) {
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

  auto ctx = new LambdaContext([this, on_finish](int r) {
      unregister_watcher(on_finish);
    });
  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void TrashWatcher<I>::handle_image_added(const std::string &image_id,
                                         const cls::rbd::TrashImageSpec& spec) {
  dout(10) << "image_id=" << image_id << dendl;

  std::lock_guard locker{m_lock};
  add_image(image_id, spec);
}

template <typename I>
void TrashWatcher<I>::handle_image_removed(const std::string &image_id) {
  // ignore removals -- the image deleter will ignore -ENOENTs
}

template <typename I>
void TrashWatcher<I>::handle_rewatch_complete(int r) {
  dout(5) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;
    return;
  } else if (r == -ENOENT) {
    dout(5) << "trash directory deleted" << dendl;
  } else if (r < 0) {
    derr << "unexpected error re-registering trash directory watch: "
         << cpp_strerror(r) << dendl;
  }
  schedule_trash_list(30);
}

template <typename I>
void TrashWatcher<I>::create_trash() {
  dout(20) << dendl;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
  }

  librados::ObjectWriteOperation op;
  op.create(false);

  m_async_op_tracker.start_op();
  auto aio_comp = create_rados_callback<
    TrashWatcher<I>, &TrashWatcher<I>::handle_create_trash>(this);
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashWatcher<I>::handle_create_trash(int r) {
  dout(20) << "r=" << r << dendl;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
  }

  Context* on_init_finish = nullptr;
  if (r == -EBLACKLISTED || r == -ENOENT) {
    if (r == -EBLACKLISTED) {
      dout(0) << "detected client is blacklisted" << dendl;
    } else {
      dout(0) << "detected pool no longer exists" << dendl;
    }

    std::lock_guard locker{m_lock};
    std::swap(on_init_finish, m_on_init_finish);
    m_trash_list_in_progress = false;
  } else if (r < 0 && r != -EEXIST) {
    derr << "failed to create trash object: " << cpp_strerror(r) << dendl;
    {
      std::lock_guard locker{m_lock};
      m_trash_list_in_progress = false;
    }

    schedule_trash_list(30);
  } else {
    register_watcher();
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void TrashWatcher<I>::register_watcher() {
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
  }

  // if the watch registration is in-flight, let the watcher
  // handle the transition -- only (re-)register if it's not registered
  if (!this->is_unregistered()) {
    trash_list(true);
    return;
  }

  // first time registering or the watch failed
  dout(5) << dendl;
  m_async_op_tracker.start_op();

  Context *ctx = create_context_callback<
    TrashWatcher, &TrashWatcher<I>::handle_register_watcher>(this);
  this->register_watch(ctx);
}

template <typename I>
void TrashWatcher<I>::handle_register_watcher(int r) {
  dout(5) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
    if (r < 0) {
      m_trash_list_in_progress = false;
    }
  }

  Context *on_init_finish = nullptr;
  if (r >= 0) {
    trash_list(true);
  } else if (r == -EBLACKLISTED) {
    dout(0) << "detected client is blacklisted" << dendl;

    std::lock_guard locker{m_lock};
    std::swap(on_init_finish, m_on_init_finish);
  } else {
    derr << "unexpected error registering trash directory watch: "
         << cpp_strerror(r) << dendl;
    schedule_trash_list(10);
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void TrashWatcher<I>::unregister_watcher(Context* on_finish) {
  dout(5) << dendl;

  m_async_op_tracker.start_op();
  Context *ctx = new LambdaContext([this, on_finish](int r) {
      handle_unregister_watcher(r, on_finish);
    });
  this->unregister_watch(ctx);
}

template <typename I>
void TrashWatcher<I>::handle_unregister_watcher(int r, Context* on_finish) {
  dout(5) << "unregister_watcher: r=" << r << dendl;
  if (r < 0) {
    derr << "error unregistering watcher for trash directory: "
         << cpp_strerror(r) << dendl;
  }
  m_async_op_tracker.finish_op();
  on_finish->complete(0);
}

template <typename I>
void TrashWatcher<I>::trash_list(bool initial_request) {
  if (initial_request) {
    m_async_op_tracker.start_op();
    m_last_image_id = "";
  }

  dout(5) << "last_image_id=" << m_last_image_id << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
  }

  librados::ObjectReadOperation op;
  librbd::cls_client::trash_list_start(&op, m_last_image_id, MAX_RETURN);

  librados::AioCompletion *aio_comp = create_rados_callback<
    TrashWatcher<I>, &TrashWatcher<I>::handle_trash_list>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void TrashWatcher<I>::handle_trash_list(int r) {
  dout(5) << "r=" << r << dendl;

  std::map<std::string, cls::rbd::TrashImageSpec> images;
  if (r >= 0) {
    auto bl_it = m_out_bl.cbegin();
    r = librbd::cls_client::trash_list_finish(&bl_it, &images);
  }

  Context *on_init_finish = nullptr;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_trash_list_in_progress);
    if (r >= 0) {
      for (auto& image : images) {
        add_image(image.first, image.second);
      }
    } else if (r == -ENOENT) {
      r = 0;
    }

    if (r == -EBLACKLISTED) {
      dout(0) << "detected client is blacklisted during trash refresh" << dendl;
      m_trash_list_in_progress = false;
      std::swap(on_init_finish, m_on_init_finish);
    } else if (r >= 0 && images.size() < MAX_RETURN) {
      m_trash_list_in_progress = false;
      std::swap(on_init_finish, m_on_init_finish);
    } else if (r < 0) {
      m_trash_list_in_progress = false;
    }
  }

  if (r >= 0 && images.size() == MAX_RETURN) {
    m_last_image_id = images.rbegin()->first;
    trash_list(false);
    return;
  } else if (r < 0 && r != -EBLACKLISTED) {
    derr << "failed to retrieve trash directory: " << cpp_strerror(r) << dendl;
    schedule_trash_list(10);
  }

  m_async_op_tracker.finish_op();
  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
  }
}

template <typename I>
void TrashWatcher<I>::schedule_trash_list(double interval) {
  std::scoped_lock locker{m_threads->timer_lock, m_lock};
  if (m_shutting_down || m_trash_list_in_progress || m_timer_ctx != nullptr) {
    if (m_trash_list_in_progress && !m_deferred_trash_list) {
      dout(5) << "deferring refresh until in-flight refresh completes" << dendl;
      m_deferred_trash_list = true;
    }
    return;
  }

  dout(5) << dendl;
  m_timer_ctx = m_threads->timer->add_event_after(
    interval,
    new LambdaContext([this](int r) {
	process_trash_list();
      }));
}

template <typename I>
void TrashWatcher<I>::process_trash_list() {
  dout(5) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_timer_ctx != nullptr);
  m_timer_ctx = nullptr;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(!m_trash_list_in_progress);
    m_trash_list_in_progress = true;
  }

  // execute outside of the timer's lock
  m_async_op_tracker.start_op();
  Context *ctx = new LambdaContext([this](int r) {
      create_trash();
      m_async_op_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void TrashWatcher<I>::add_image(const std::string& image_id,
                                const cls::rbd::TrashImageSpec& spec) {
  if (spec.source != cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING) {
    return;
  }

  ceph_assert(ceph_mutex_is_locked(m_lock));
  auto& deferment_end_time = spec.deferment_end_time;
  dout(10) << "image_id=" << image_id << ", "
           << "deferment_end_time=" << deferment_end_time << dendl;

  m_async_op_tracker.start_op();
  auto ctx = new LambdaContext([this, image_id, deferment_end_time](int r) {
      m_trash_listener.handle_trash_image(image_id,
					  deferment_end_time.to_real_time());
      m_async_op_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

} // namespace image_deleter;
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::TrashWatcher<librbd::ImageCtx>;
