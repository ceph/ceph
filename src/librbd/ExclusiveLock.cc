// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ExclusiveLock.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ImageState.h"
#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "librbd/exclusive_lock/PreReleaseRequest.h"
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
  *ret_val = m_request_blocked_ret_val;

  ldout(m_image_ctx.cct, 20) << "=" << accept_requests << dendl;
  return accept_requests;
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

  m_image_ctx.aio_work_queue->block_writes(new C_InitComplete(this, features,
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
<<<<<<< HEAD
void ExclusiveLock<I>::handle_peer_notification(int r) {
  Mutex::Locker locker(ML<I>::m_lock);
  if (!ML<I>::is_state_waiting_for_lock()) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << dendl;
  assert(ML<I>::is_action_acquire_lock());
=======
void ExclusiveLock<I>::try_lock(Context *on_tried_lock) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_TRY_LOCK, on_tried_lock);
      return;
    }
  }

  on_tried_lock->complete(r);
}

template <typename I>
void ExclusiveLock<I>::request_lock(Context *on_locked) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REQUEST_LOCK, on_locked);
      return;
    }
  }

  if (on_locked != nullptr) {
    on_locked->complete(r);
  }
}

template <typename I>
void ExclusiveLock<I>::release_lock(Context *on_released) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  on_released->complete(r);
}

template <typename I>
void ExclusiveLock<I>::reacquire_lock(Context *on_reacquired) {
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());

    if (m_state == STATE_WAITING_FOR_REGISTER) {
      // restart the acquire lock process now that watch is valid
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": "
                                 << "woke up waiting acquire" << dendl;
      Action active_action = get_active_action();
      assert(active_action == ACTION_TRY_LOCK ||
             active_action == ACTION_REQUEST_LOCK);
      execute_next_action();
    } else if (!is_shutdown() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_POST_ACQUIRING ||
                m_state == STATE_WAITING_FOR_PEER)) {
      // interlock the lock operation with other image state ops
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REACQUIRE_LOCK, on_reacquired);
      return;
    }
  }

  // ignore request if shutdown or not in a locked-related state
  if (on_reacquired != nullptr) {
    on_reacquired->complete(0);
  }
}

template <typename I>
void ExclusiveLock<I>::handle_peer_notification(int r) {
  {
    Mutex::Locker locker(m_lock);
    if (m_state != STATE_WAITING_FOR_PEER) {
      return;
    }

    ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
    assert(get_active_action() == ACTION_REQUEST_LOCK);

    if (r >= 0) {
      execute_next_action();
      return;
    }
  }

  handle_acquire_lock(r);
}

template <typename I>
std::string ExclusiveLock<I>::encode_lock_cookie() const {
  assert(m_lock.is_locked());
>>>>>>> ce8edcfed6cd908779efd229202eab1232d16f1c

  m_acquire_lock_peer_ret_val = r;
  ML<I>::execute_next_action();
}

template <typename I>
void ExclusiveLock<I>::handle_init_complete(uint64_t features) {
  ldout(m_image_ctx.cct, 10) << "features=" << features << dendl;

  if ((features & RBD_FEATURE_JOURNALING) != 0) {
    m_image_ctx.aio_work_queue->set_require_lock_on_read();
  }

  Mutex::Locker locker(ML<I>::m_lock);
  ML<I>::set_state_unlocked();
}

template <typename I>
void ExclusiveLock<I>::shutdown_handler(int r, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.exclusive_lock = nullptr;
  }

  m_image_ctx.aio_work_queue->unblock_writes();
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
<<<<<<< HEAD
    Mutex::Locker locker(ML<I>::m_lock);
    assert(ML<I>::is_state_acquiring() || ML<I>::is_state_post_acquiring());
=======
    m_lock.Lock();
    assert(m_state == STATE_ACQUIRING ||
           m_state == STATE_POST_ACQUIRING ||
           m_state == STATE_WAITING_FOR_PEER);

    Action action = get_active_action();
    assert(action == ACTION_TRY_LOCK || action == ACTION_REQUEST_LOCK);
    if (action == ACTION_REQUEST_LOCK && r < 0 && r != -EBLACKLISTED &&
        r != -EPERM && r != -EROFS) {
      m_state = STATE_WAITING_FOR_PEER;
      m_lock.Unlock();
>>>>>>> ce8edcfed6cd908779efd229202eab1232d16f1c

    assert (m_pre_post_callback != nullptr);
    std::swap(m_pre_post_callback, on_finish);
  }

  if (r >= 0) {
    m_image_ctx.image_watcher->notify_acquired_lock();
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.aio_work_queue->unblock_writes();
  }

  on_finish->complete(r);
}

template <typename I>
void ExclusiveLock<I>::pre_release_lock_handler(bool shutting_down,
                                                Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << dendl;
  Mutex::Locker locker(ML<I>::m_lock);

  PreReleaseRequest<I> *req = PreReleaseRequest<I>::create(
    m_image_ctx, shutting_down, on_finish);
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
      if (m_image_ctx.aio_work_queue->is_lock_request_needed()) {
        // if we have blocked IO -- re-request the lock
        RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
        ML<I>::acquire_lock(nullptr);
      }
    }
  } else {
    {
      RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
      m_image_ctx.aio_work_queue->clear_require_lock_on_read();
      m_image_ctx.exclusive_lock = nullptr;
    }

    if (r >= 0) {
      m_image_ctx.aio_work_queue->unblock_writes();
    }

    m_image_ctx.image_watcher->notify_released_lock();
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
  virtual void finish(int r) override {
    if (r == 0) {
      exclusive_lock->handle_init_complete(features);
    }
    on_init->complete(r);
  }
};

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
