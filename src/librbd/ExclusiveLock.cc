// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ImageState.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/Utils.h"
#include "common/Mutex.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock: " << this << " " \
                           <<  __func__

namespace librbd {

using namespace exclusive_lock;

template <typename I>
using ML = ManagedLock<I>;

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : ML<I>(image_ctx.md_ctx, image_ctx.op_work_queue, image_ctx.header_oid,
          image_ctx.image_watcher, managed_lock::EXCLUSIVE,
          image_ctx.blacklist_on_break_lock,
          image_ctx.blacklist_expire_seconds),
    m_image_ctx(image_ctx) {
  Mutex::Locker locker(ML<I>::m_lock);
  ML<I>::set_state_uninitialized();
}

template <typename I>
bool ExclusiveLock<I>::accept_requests(int *ret_val) const {
  Mutex::Locker locker(ML<I>::m_lock);

  bool accept_requests = (!ML<I>::is_state_shutdown() &&
                          ML<I>::is_state_locked() &&
                          m_request_blocked_count == 0);
  if (ret_val != nullptr) {
    *ret_val = m_request_blocked_ret_val;
  }

  ldout(m_image_ctx.cct, 20) << "=" << accept_requests << dendl;
  return accept_requests;
}

template <typename I>
bool ExclusiveLock<I>::accept_ops() const {
  Mutex::Locker locker(ML<I>::m_lock);
  bool accept = accept_ops(ML<I>::m_lock);
  ldout(m_image_ctx.cct, 20) << "=" << accept << dendl;
  return accept;
}

template <typename I>
bool ExclusiveLock<I>::accept_ops(const Mutex &lock) const {
  return (!ML<I>::is_state_shutdown() &&
          (ML<I>::is_state_locked() || ML<I>::is_state_post_acquiring()));
}

template <typename I>
void ExclusiveLock<I>::block_requests(int r) {
  Mutex::Locker locker(ML<I>::m_lock);

  m_request_blocked_count++;
  if (m_request_blocked_ret_val == 0) {
    m_request_blocked_ret_val = r;
  }

  ldout(m_image_ctx.cct, 20) << dendl;
}

template <typename I>
void ExclusiveLock<I>::unblock_requests() {
  Mutex::Locker locker(ML<I>::m_lock);

  assert(m_request_blocked_count > 0);
  m_request_blocked_count--;
  if (m_request_blocked_count == 0) {
    m_request_blocked_ret_val = 0;
  }

  ldout(m_image_ctx.cct, 20) << dendl;
}

template <typename I>
void ExclusiveLock<I>::init(uint64_t features, Context *on_init) {
  assert(m_image_ctx.owner_lock.is_locked());
  ldout(m_image_ctx.cct, 10) << dendl;

  {
    Mutex::Locker locker(ML<I>::m_lock);
    ML<I>::set_state_initializing();
  }

  m_image_ctx.io_work_queue->block_writes(new C_InitComplete(this, features,
                                                             on_init));
}

template <typename I>
void ExclusiveLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_image_ctx.cct, 10) << dendl;

  ML<I>::shut_down(on_shut_down);

  // if stalled in request state machine -- abort
  handle_peer_notification(0);
}

template <typename I>
void ExclusiveLock<I>::handle_peer_notification(int r) {
  Mutex::Locker locker(ML<I>::m_lock);
  if (!ML<I>::is_state_waiting_for_lock()) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << dendl;
  assert(ML<I>::is_action_acquire_lock());

  m_acquire_lock_peer_ret_val = r;
  ML<I>::execute_next_action();
}

template <typename I>
Context *ExclusiveLock<I>::start_op() {
  assert(m_image_ctx.owner_lock.is_locked());
  Mutex::Locker locker(ML<I>::m_lock);

  if (!accept_ops(ML<I>::m_lock)) {
    return nullptr;
  }

  m_async_op_tracker.start_op();
  return new FunctionContext([this](int r) {
      m_async_op_tracker.finish_op();
    });
}

template <typename I>
void ExclusiveLock<I>::handle_init_complete(uint64_t features) {
  ldout(m_image_ctx.cct, 10) << ": features=" << features << dendl;

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    if (m_image_ctx.clone_copy_on_read ||
        (features & RBD_FEATURE_JOURNALING) != 0) {
      m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_BOTH, true);
    } else {
      m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_WRITE, true);
    }
  }

  Mutex::Locker locker(ML<I>::m_lock);
  ML<I>::set_state_unlocked();
}

template <typename I>
void ExclusiveLock<I>::shutdown_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_BOTH, false);
    m_image_ctx.exclusive_lock = nullptr;
  }

  m_image_ctx.io_work_queue->unblock_writes();
  m_image_ctx.image_watcher->flush(on_finish);
}

template <typename I>
void ExclusiveLock<I>::pre_acquire_lock_handler(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;

  int acquire_lock_peer_ret_val = 0;
  {
    Mutex::Locker locker(ML<I>::m_lock);
    std::swap(acquire_lock_peer_ret_val, m_acquire_lock_peer_ret_val);
  }

  if (acquire_lock_peer_ret_val == -EROFS) {
    ldout(m_image_ctx.cct, 10) << ": peer nacked lock request" << dendl;
    on_finish->complete(acquire_lock_peer_ret_val);
    return;
  }

  PreAcquireRequest<I> *req = PreAcquireRequest<I>::create(m_image_ctx,
                                                           on_finish);
  m_image_ctx.op_work_queue->queue(new FunctionContext([req](int r) {
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
    ML<I>::m_lock.Lock();
    assert(ML<I>::is_state_acquiring());

    // PostAcquire state machine will not run, so we need complete prepare
    m_image_ctx.state->handle_prepare_lock_complete();

    // if lock is in-use by another client, request the lock
    if (ML<I>::is_action_acquire_lock() && (r == -EBUSY || r == -EAGAIN)) {
      ML<I>::set_state_waiting_for_lock();
      ML<I>::m_lock.Unlock();

      // request the lock from a peer
      m_image_ctx.image_watcher->notify_request_lock();

      // inform manage lock that we have interrupted the state machine
      r = -ECANCELED;
    } else {
      ML<I>::m_lock.Unlock();

      // clear error if peer owns lock
      if (r == -EAGAIN) {
        r = 0;
      }
    }

    on_finish->complete(r);
    return;
  }

  Mutex::Locker locker(ML<I>::m_lock);
  m_pre_post_callback = on_finish;
  using EL = ExclusiveLock<I>;
  PostAcquireRequest<I> *req = PostAcquireRequest<I>::create(m_image_ctx,
      util::create_context_callback<EL, &EL::handle_post_acquiring_lock>(this),
      util::create_context_callback<EL, &EL::handle_post_acquired_lock>(this));

  m_image_ctx.op_work_queue->queue(new FunctionContext([req](int r) {
    req->send();
  }));
}

template <typename I>
void ExclusiveLock<I>::handle_post_acquiring_lock(int r) {
  ldout(m_image_ctx.cct, 10) << dendl;

  Mutex::Locker locker(ML<I>::m_lock);

  assert(r == 0);

  // lock is owned at this point
  ML<I>::set_state_post_acquiring();
}

template <typename I>
void ExclusiveLock<I>::handle_post_acquired_lock(int r) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(ML<I>::m_lock);
    assert(ML<I>::is_state_acquiring() || ML<I>::is_state_post_acquiring());

    assert (m_pre_post_callback != nullptr);
    std::swap(m_pre_post_callback, on_finish);
  }

  if (r >= 0) {
    m_image_ctx.image_watcher->notify_acquired_lock();
    m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_BOTH, false);
    m_image_ctx.io_work_queue->unblock_writes();
  }

  on_finish->complete(r);
}

template <typename I>
void ExclusiveLock<I>::pre_release_lock_handler(bool shutting_down,
                                                Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;
  Mutex::Locker locker(ML<I>::m_lock);

  PreReleaseRequest<I> *req = PreReleaseRequest<I>::create(
    m_image_ctx, shutting_down, m_async_op_tracker, on_finish);
  m_image_ctx.op_work_queue->queue(new FunctionContext([req](int r) {
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
      Mutex::Locker locker(ML<I>::m_lock);
      assert(ML<I>::is_state_pre_releasing() || ML<I>::is_state_releasing());
    }

    if (r >= 0) {
      m_image_ctx.image_watcher->notify_released_lock();
    }
  } else {
    {
      RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
      m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_BOTH, false);
      m_image_ctx.exclusive_lock = nullptr;
    }

    if (r >= 0) {
      m_image_ctx.io_work_queue->unblock_writes();
    }

    m_image_ctx.image_watcher->notify_released_lock();
  }

  on_finish->complete(r);
}

template <typename I>
void ExclusiveLock<I>::post_reacquire_lock_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;
  if (r >= 0) {
    m_image_ctx.image_watcher->notify_acquired_lock();
  }

  on_finish->complete(r);
}

template <typename I>
struct ExclusiveLock<I>::C_InitComplete : public Context {
  ExclusiveLock *exclusive_lock;
  uint64_t features;
  Context *on_init;

  C_InitComplete(ExclusiveLock *exclusive_lock, uint64_t features,
                 Context *on_init)
    : exclusive_lock(exclusive_lock), features(features), on_init(on_init) {
  }
  void finish(int r) override {
    if (r == 0) {
      exclusive_lock->handle_init_complete(features);
    }
    on_init->complete(r);
  }
};

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
