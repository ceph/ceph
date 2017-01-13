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
#define dout_prefix *_dout << "librbd::ExclusiveLock: " << this << " " <<  __func__

namespace librbd {

using namespace exclusive_lock;

template <typename I>
using ML = ManagedLock<I>;

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : ML<I>(image_ctx.md_ctx, image_ctx.op_work_queue, image_ctx.header_oid,
          image_ctx.image_watcher, image_ctx.blacklist_on_break_lock,
          image_ctx.blacklist_expire_seconds),
    m_image_ctx(image_ctx), m_pre_post_callback(nullptr),
    m_shutting_down(false)  {
  ML<I>::m_state = ML<I>::STATE_UNINITIALIZED;
}

template <typename I>
bool ExclusiveLock<I>::accept_requests(int *ret_val) const {
  Mutex::Locker locker(ML<I>::m_lock);

  bool accept_requests = (!ML<I>::is_shutdown_locked() &&
                          ML<I>::m_state == ML<I>::STATE_LOCKED &&
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
    assert(ML<I>::m_state == ML<I>::STATE_UNINITIALIZED);
    ML<I>::m_state = ML<I>::STATE_INITIALIZING;
  }

  m_image_ctx.aio_work_queue->block_writes(new C_InitComplete(this, on_init));
  if ((features & RBD_FEATURE_JOURNALING) != 0) {
    m_image_ctx.aio_work_queue->set_require_lock_on_read();
  }
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
  if (ML<I>::m_state != ML<I>::STATE_WAITING_FOR_LOCK) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << dendl;
  assert(ML<I>::get_active_action() == ML<I>::ACTION_ACQUIRE_LOCK);

  m_acquire_lock_peer_ret_val = r;
  ML<I>::execute_next_action();
}

template <typename I>
void ExclusiveLock<I>::handle_init_complete() {
  ldout(m_image_ctx.cct, 10) << dendl;

  Mutex::Locker locker(ML<I>::m_lock);
  ML<I>::m_state = ML<I>::STATE_UNLOCKED;
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
    assert(ML<I>::m_state == ML<I>::STATE_ACQUIRING);

    // PostAcquire state machine will not run, so we need complete prepare
    m_image_ctx.state->handle_prepare_lock_complete();

    typename ML<I>::Action action = ML<I>::get_active_action();
    if (action == ML<I>::ACTION_ACQUIRE_LOCK && r < 0 && r != -EBLACKLISTED) {
      ML<I>::m_state = ML<I>::STATE_WAITING_FOR_LOCK;
      ML<I>::m_lock.Unlock();

      // request the lock from a peer
      m_image_ctx.image_watcher->notify_request_lock();
      return;
    }

    ML<I>::m_lock.Unlock();
    if (r == -EAGAIN) {
      r = 0;
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
  assert(ML<I>::m_state == ML<I>::STATE_ACQUIRING);

  // lock is owned at this point
  ML<I>::m_state = ML<I>::STATE_POST_ACQUIRING;
}

template <typename I>
void ExclusiveLock<I>::handle_post_acquired_lock(int r) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(ML<I>::m_lock);
    assert(ML<I>::m_state == ML<I>::STATE_ACQUIRING ||
           ML<I>::m_state == ML<I>::STATE_POST_ACQUIRING);

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

  m_shutting_down = shutting_down;

  using EL = ExclusiveLock<I>;
  PreReleaseRequest<I> *req = PreReleaseRequest<I>::create(m_image_ctx,
      util::create_context_callback<EL, &EL::handle_pre_releasing_lock>(this),
      on_finish, shutting_down);

  m_image_ctx.op_work_queue->queue(new FunctionContext([req](int r) {
    req->send();
  }));
}

template <typename I>
void ExclusiveLock<I>::handle_pre_releasing_lock(int r) {
  ldout(m_image_ctx.cct, 10) << dendl;

  Mutex::Locker locker(ML<I>::m_lock);

  assert(r == 0);
  if (!m_shutting_down) {
    assert(ML<I>::m_state == ML<I>::STATE_PRE_RELEASING);

    // all IO and ops should be blocked/canceled by this point
    ML<I>::m_state = ML<I>::STATE_RELEASING;
  } else {
    assert(ML<I>::m_state == ML<I>::STATE_PRE_SHUTTING_DOWN);

    // all IO and ops should be blocked/canceled by this point
    ML<I>::m_state = ML<I>::STATE_SHUTTING_DOWN;
  }
}

template <typename I>
void ExclusiveLock<I>::post_release_lock_handler(bool shutting_down, int r,
                                                   Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << ": r=" << r << " shutting_down="
                             << shutting_down << dendl;
  if (!shutting_down) {
    {
      Mutex::Locker locker(ML<I>::m_lock);
      assert(ML<I>::m_state == ML<I>::STATE_PRE_RELEASING ||
             ML<I>::m_state == ML<I>::STATE_RELEASING);
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

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
