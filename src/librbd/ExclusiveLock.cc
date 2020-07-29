// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ImageState.h"
#include "librbd/exclusive_lock/ImageDispatch.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock: " << this << " " \
                           <<  __func__

namespace librbd {

using namespace exclusive_lock;
using librbd::util::create_context_callback;

template <typename I>
using ML = ManagedLock<I>;

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : RefCountedObject(image_ctx.cct),
    ML<I>(image_ctx.md_ctx, *image_ctx.asio_engine, image_ctx.header_oid,
          image_ctx.image_watcher, managed_lock::EXCLUSIVE,
          image_ctx.config.template get_val<bool>("rbd_blacklist_on_break_lock"),
          image_ctx.config.template get_val<uint64_t>("rbd_blacklist_expire_seconds")),
    m_image_ctx(image_ctx) {
  std::lock_guard locker{ML<I>::m_lock};
  ML<I>::set_state_uninitialized();
}

template <typename I>
bool ExclusiveLock<I>::accept_request(OperationRequestType request_type,
                                      int *ret_val) const {
  std::lock_guard locker{ML<I>::m_lock};

  bool accept_request =
    (!ML<I>::is_state_shutdown() && ML<I>::is_state_locked() &&
     (m_request_blocked_count == 0 ||
      m_image_ctx.get_exclusive_lock_policy()->accept_blocked_request(
        request_type)));
  if (ret_val != nullptr) {
    *ret_val = accept_request ? 0 : m_request_blocked_ret_val;
  }

  ldout(m_image_ctx.cct, 20) << "=" << accept_request << " (request_type="
                             << request_type << ")" << dendl;
  return accept_request;
}

template <typename I>
bool ExclusiveLock<I>::accept_ops() const {
  std::lock_guard locker{ML<I>::m_lock};
  bool accept = accept_ops(ML<I>::m_lock);
  ldout(m_image_ctx.cct, 20) << "=" << accept << dendl;
  return accept;
}

template <typename I>
bool ExclusiveLock<I>::accept_ops(const ceph::mutex &lock) const {
  return (!ML<I>::is_state_shutdown() &&
          (ML<I>::is_state_locked() || ML<I>::is_state_post_acquiring()));
}

template <typename I>
void ExclusiveLock<I>::set_require_lock(io::Direction direction,
                                        Context* on_finish) {
  m_image_dispatch->set_require_lock(direction, on_finish);
}

template <typename I>
void ExclusiveLock<I>::unset_require_lock(io::Direction direction) {
  m_image_dispatch->unset_require_lock(direction);
}

template <typename I>
void ExclusiveLock<I>::block_requests(int r) {
  std::lock_guard locker{ML<I>::m_lock};

  m_request_blocked_count++;
  if (m_request_blocked_ret_val == 0) {
    m_request_blocked_ret_val = r;
  }

  ldout(m_image_ctx.cct, 20) << "r=" << r << dendl;
}

template <typename I>
void ExclusiveLock<I>::unblock_requests() {
  std::lock_guard locker{ML<I>::m_lock};

  ceph_assert(m_request_blocked_count > 0);
  m_request_blocked_count--;
  if (m_request_blocked_count == 0) {
    m_request_blocked_ret_val = 0;
  }

  ldout(m_image_ctx.cct, 20) << dendl;
}

template <typename I>
int ExclusiveLock<I>::get_unlocked_op_error() const {
  if (m_image_ctx.image_watcher->is_blacklisted()) {
    return -EBLACKLISTED;
  }
  return -EROFS;
}

template <typename I>
void ExclusiveLock<I>::init(uint64_t features, Context *on_init) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  on_init = create_context_callback<Context>(on_init, this);

  ldout(m_image_ctx.cct, 10) << dendl;

  {
    std::lock_guard locker{ML<I>::m_lock};
    ML<I>::set_state_initializing();
  }

  auto ctx = new LambdaContext([this, features, on_init](int r) {
      handle_init_complete(r, features, on_init);
    });
  m_image_ctx.io_image_dispatcher->block_writes(ctx);
}

template <typename I>
void ExclusiveLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_image_ctx.cct, 10) << dendl;

  on_shut_down = create_context_callback<Context>(on_shut_down, this);

  ML<I>::shut_down(on_shut_down);

  // if stalled in request state machine -- abort
  handle_peer_notification(0);
}

template <typename I>
void ExclusiveLock<I>::handle_peer_notification(int r) {
  std::lock_guard locker{ML<I>::m_lock};
  if (!ML<I>::is_state_waiting_for_lock()) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << dendl;
  ceph_assert(ML<I>::is_action_acquire_lock());

  m_acquire_lock_peer_ret_val = r;
  ML<I>::execute_next_action();
}

template <typename I>
Context *ExclusiveLock<I>::start_op(int* ret_val) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  std::lock_guard locker{ML<I>::m_lock};

  if (!accept_ops(ML<I>::m_lock)) {
    *ret_val = get_unlocked_op_error();
    return nullptr;
  }

  m_async_op_tracker.start_op();
  return new LambdaContext([this](int r) {
      m_async_op_tracker.finish_op();
    });
}

template <typename I>
void ExclusiveLock<I>::handle_init_complete(int r, uint64_t features,
                                            Context* on_finish) {
  if (r < 0) {
    m_image_ctx.io_image_dispatcher->unblock_writes();
    on_finish->complete(r);
    return;
  }

  ldout(m_image_ctx.cct, 10) << ": features=" << features << dendl;

  m_image_dispatch = exclusive_lock::ImageDispatch<I>::create(&m_image_ctx);
  m_image_ctx.io_image_dispatcher->register_dispatch(m_image_dispatch);

  on_finish = new LambdaContext([this, on_finish](int r) {
      m_image_ctx.io_image_dispatcher->unblock_writes();

      {
        std::lock_guard locker{ML<I>::m_lock};
        ML<I>::set_state_unlocked();
      }

      on_finish->complete(r);
    });

  if (m_image_ctx.clone_copy_on_read ||
      (features & RBD_FEATURE_JOURNALING) != 0) {
    m_image_dispatch->set_require_lock(io::DIRECTION_BOTH, on_finish);
  } else {
    m_image_dispatch->set_require_lock(io::DIRECTION_WRITE, on_finish);
  }
}

template <typename I>
void ExclusiveLock<I>::shutdown_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;

  {
    std::unique_lock owner_locker{m_image_ctx.owner_lock};
    m_image_ctx.exclusive_lock = nullptr;
  }

  on_finish = new LambdaContext([this, on_finish](int r) {
      m_image_dispatch = nullptr;
      m_image_ctx.image_watcher->flush(on_finish);
    });
  m_image_ctx.io_image_dispatcher->shut_down_dispatch(
    m_image_dispatch->get_dispatch_layer(), on_finish);
}

template <typename I>
void ExclusiveLock<I>::pre_acquire_lock_handler(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;

  int acquire_lock_peer_ret_val = 0;
  {
    std::lock_guard locker{ML<I>::m_lock};
    std::swap(acquire_lock_peer_ret_val, m_acquire_lock_peer_ret_val);
  }

  if (acquire_lock_peer_ret_val == -EROFS) {
    ldout(m_image_ctx.cct, 10) << ": peer nacked lock request" << dendl;
    on_finish->complete(acquire_lock_peer_ret_val);
    return;
  }

  PreAcquireRequest<I> *req = PreAcquireRequest<I>::create(m_image_ctx,
                                                           on_finish);
  m_image_ctx.op_work_queue->queue(new LambdaContext([req](int r) {
    req->send();
  }));
}

template <typename I>
void ExclusiveLock<I>::post_acquire_lock_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << dendl;

  if (r == -EROFS) {
    // peer refused to release the exclusive lock
    on_finish->complete(r);
    return;
  } else if (r < 0) {
    ML<I>::m_lock.lock();
    ceph_assert(ML<I>::is_state_acquiring());

    // PostAcquire state machine will not run, so we need complete prepare
    m_image_ctx.state->handle_prepare_lock_complete();

    // if lock is in-use by another client, request the lock
    if (ML<I>::is_action_acquire_lock() && (r == -EBUSY || r == -EAGAIN)) {
      ML<I>::set_state_waiting_for_lock();
      ML<I>::m_lock.unlock();

      // request the lock from a peer
      m_image_ctx.image_watcher->notify_request_lock();

      // inform manage lock that we have interrupted the state machine
      r = -ECANCELED;
    } else {
      ML<I>::m_lock.unlock();

      // clear error if peer owns lock
      if (r == -EAGAIN) {
        r = 0;
      }
    }

    on_finish->complete(r);
    return;
  }

  std::lock_guard locker{ML<I>::m_lock};
  m_pre_post_callback = on_finish;
  using EL = ExclusiveLock<I>;
  PostAcquireRequest<I> *req = PostAcquireRequest<I>::create(m_image_ctx,
      util::create_context_callback<EL, &EL::handle_post_acquiring_lock>(this),
      util::create_context_callback<EL, &EL::handle_post_acquired_lock>(this));

  m_image_ctx.op_work_queue->queue(new LambdaContext([req](int r) {
    req->send();
  }));
}

template <typename I>
void ExclusiveLock<I>::handle_post_acquiring_lock(int r) {
  ldout(m_image_ctx.cct, 10) << dendl;

  std::lock_guard locker{ML<I>::m_lock};

  ceph_assert(r == 0);

  // lock is owned at this point
  ML<I>::set_state_post_acquiring();
}

template <typename I>
void ExclusiveLock<I>::handle_post_acquired_lock(int r) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{ML<I>::m_lock};
    ceph_assert(ML<I>::is_state_acquiring() ||
                ML<I>::is_state_post_acquiring());

    assert (m_pre_post_callback != nullptr);
    std::swap(m_pre_post_callback, on_finish);
  }

  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  m_image_ctx.perfcounter->tset(l_librbd_lock_acquired_time,
                                ceph_clock_now());
  m_image_ctx.image_watcher->notify_acquired_lock();
  m_image_dispatch->unset_require_lock(io::DIRECTION_BOTH);

  on_finish->complete(0);
}

template <typename I>
void ExclusiveLock<I>::pre_release_lock_handler(bool shutting_down,
                                                Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;
  std::lock_guard locker{ML<I>::m_lock};

  auto req = PreReleaseRequest<I>::create(
    m_image_ctx, m_image_dispatch, shutting_down, m_async_op_tracker,
    on_finish);
  m_image_ctx.op_work_queue->queue(new LambdaContext([req](int r) {
    req->send();
  }));
}

template <typename I>
void ExclusiveLock<I>::post_release_lock_handler(bool shutting_down, int r,
                                                 Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << " shutting_down="
                             << shutting_down << dendl;
  if (!shutting_down) {
    {
      std::lock_guard locker{ML<I>::m_lock};
      ceph_assert(ML<I>::is_state_pre_releasing() ||
                  ML<I>::is_state_releasing());
    }

    if (r >= 0) {
      m_image_ctx.image_watcher->notify_released_lock();
    }

    on_finish->complete(r);
  } else {
    {
      std::unique_lock owner_locker{m_image_ctx.owner_lock};
      m_image_ctx.exclusive_lock = nullptr;
    }

    on_finish = new LambdaContext([this, r, on_finish](int) {
        m_image_dispatch = nullptr;
        m_image_ctx.image_watcher->notify_released_lock();
        on_finish->complete(r);
      });
    m_image_ctx.io_image_dispatcher->shut_down_dispatch(
      m_image_dispatch->get_dispatch_layer(), on_finish);
  }
}

template <typename I>
void ExclusiveLock<I>::post_reacquire_lock_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;
  if (r >= 0) {
    m_image_ctx.image_watcher->notify_acquired_lock();
  }

  on_finish->complete(r);
}

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
