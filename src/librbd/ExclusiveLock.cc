// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ExclusiveLock.h"
#include "librbd/Lock.h"
#include "librbd/managed_lock/LockWatcher.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/ReleaseRequest.h"
#include "librbd/image/RefreshRequest.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock: "

namespace librbd {

using namespace exclusive_lock;
using util::create_context_callback;

namespace {

template <typename R>
struct C_SendRequest : public Context {
  R* request;
  explicit C_SendRequest(R* request) : request(request) {
    }
  virtual void finish(int r) override {
      request->send();
    }
};

} // anonymous namespace

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : m_image_ctx(image_ctx),
    m_managed_lock(new Lock<>(image_ctx.md_ctx, image_ctx.header_oid)),
    m_lock(util::unique_lock_name("librbd::ExclusiveLock::m_lock", this)) {
}

template <typename I>
ExclusiveLock<I>::~ExclusiveLock() {
  delete m_managed_lock;
}

template <typename I>
bool ExclusiveLock<I>::is_lock_owner() const {
  return m_managed_lock->is_lock_owner();
}

template <typename I>
bool ExclusiveLock<I>::accept_requests(int *ret_val) const {
  Mutex::Locker locker(m_lock);

  bool accept_requests = (!m_managed_lock->is_shutdown() &&
                          m_managed_lock->is_locked() &&
                          m_request_blocked_count == 0);
  *ret_val = m_request_blocked_ret_val;

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << "="
                             << accept_requests << dendl;
  return accept_requests;
}

template <typename I>
void ExclusiveLock<I>::block_requests(int r) {
  Mutex::Locker locker(m_lock);
  m_request_blocked_count++;
  if (m_request_blocked_ret_val == 0) {
    m_request_blocked_ret_val = r;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
void ExclusiveLock<I>::unblock_requests() {
  Mutex::Locker locker(m_lock);
  assert(m_request_blocked_count > 0);
  m_request_blocked_count--;
  if (m_request_blocked_count == 0) {
    m_request_blocked_ret_val = 0;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
void ExclusiveLock<I>::init(uint64_t features, Context *on_init) {
  assert(m_image_ctx.owner_lock.is_locked());
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx.aio_work_queue->block_writes(on_init);
  if ((features & RBD_FEATURE_JOURNALING) != 0) {
    m_image_ctx.aio_work_queue->set_require_lock_on_read();
  }
}

template <typename I>
void ExclusiveLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  bool is_locked;
  {
    Mutex::Locker l(m_lock);
    assert(!m_managed_lock->is_shutdown());
    is_locked = m_managed_lock->is_locked();
  }

  FunctionContext *ctx =
    new FunctionContext([this, on_shut_down, is_locked](int r) {
      {
        RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
        m_image_ctx.aio_work_queue->clear_require_lock_on_read();
        m_image_ctx.exclusive_lock = nullptr;
      }
      if (!is_locked || r == 0) {
        m_image_ctx.aio_work_queue->unblock_writes();
      }
      if (!is_locked) {
        m_image_ctx.image_watcher->flush(on_shut_down);
      } else {
        on_shut_down->complete(r);
      }
    });

  if (is_locked) {
    FunctionContext *shutdown_ctx = new FunctionContext([this, ctx](int r) {
        m_managed_lock->shut_down(ctx);
    });
    ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_image_ctx,
                                                       m_managed_lock,
                                                       shutdown_ctx, true);
    Mutex::Locker l(m_lock);
    m_image_ctx.op_work_queue->queue(
        new C_SendRequest<ReleaseRequest<I>>(req), 0);
  } else {
    m_managed_lock->shut_down(ctx);
  }
}

template <typename I>
void ExclusiveLock<I>::try_lock(Context *on_tried_lock) {
  bool is_locked;
  bool is_shutdown;
  {
    Mutex::Locker l(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    is_locked = m_managed_lock->is_locked();
    is_shutdown = m_managed_lock->is_shutdown();
  }

  if (is_locked) {
    on_tried_lock->complete(is_shutdown ? -ESHUTDOWN : 0);
    return;
  }

  FunctionContext *ctx = new FunctionContext([this, on_tried_lock](int r) {
      this->handle_acquire_lock(r);
      if (on_tried_lock != nullptr) {
        on_tried_lock->complete(r);
      }
  });
  AcquireRequest<I>* req = AcquireRequest<I>::create(m_image_ctx,
                                                     m_managed_lock, ctx,
                                                     true);
  Mutex::Locker l(m_lock);
  m_image_ctx.op_work_queue->queue(
      new C_SendRequest<AcquireRequest<I>>(req), 0);
}

template <typename I>
void ExclusiveLock<I>::request_lock(Context *on_locked) {
  bool is_locked;
  bool is_shutdown;
  {
    Mutex::Locker l(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    is_locked = m_managed_lock->is_locked();
    is_shutdown = m_managed_lock->is_shutdown();
  }

  if (is_locked) {
    on_locked->complete(is_shutdown ? -ESHUTDOWN : 0);
    return;
  }

  FunctionContext *ctx = new FunctionContext([this, on_locked](int r) {
      this->handle_acquire_lock(r);
      if (on_locked != nullptr) {
        on_locked->complete(r);
      }
  });
  AcquireRequest<I>* req = AcquireRequest<I>::create(m_image_ctx,
                                                     m_managed_lock, ctx,
                                                     false);

  Mutex::Locker l(m_lock);
  m_image_ctx.op_work_queue->queue(
      new C_SendRequest<AcquireRequest<I>>(req), 0);
}

template <typename I>
void ExclusiveLock<I>::release_lock(Context *on_released) {
  bool is_locked;
  bool is_shutdown;
  {
    Mutex::Locker l(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    is_locked = m_managed_lock->is_locked();
    is_shutdown = m_managed_lock->is_shutdown();
  }

  if (!is_locked) {
    on_released->complete(is_shutdown ? -ESHUTDOWN : 0);
    return;
  }

  FunctionContext *ctx = new FunctionContext([this, on_released](int r) {
      this->handle_release_lock(r);
      if (on_released != nullptr) {
        on_released->complete(r);
      }
  });
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_image_ctx,
                                                     m_managed_lock, ctx,
                                                     false);
  Mutex::Locker l(m_lock);
  m_image_ctx.op_work_queue->queue(
      new C_SendRequest<ReleaseRequest<I>>(req), 0);
}

template <typename I>
void ExclusiveLock<I>::reacquire_lock(Context *on_reacquired) {
  m_managed_lock->request_lock(on_reacquired);
}

template <typename I>
void ExclusiveLock<I>::handle_peer_notification() {
  m_managed_lock->handle_peer_notification();
}

template <typename I>
void ExclusiveLock<I>::assert_header_locked(librados::ObjectWriteOperation *op) {
  m_managed_lock->assert_locked(op, LOCK_EXCLUSIVE);
}

template <typename I>
bool ExclusiveLock<I>::decode_lock_cookie(const std::string &tag,
                                          uint64_t *handle) {
  return managed_lock::LockWatcher::decode_lock_cookie(tag, handle);
}

template <typename I>
void ExclusiveLock<I>::handle_acquire_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r == -EBUSY || r == -EAGAIN) {
    ldout(cct, 5) << "unable to acquire exclusive lock" << dendl;
  } else if (r < 0) {
    lderr(cct) << "failed to acquire exclusive lock:" << cpp_strerror(r)
               << dendl;
  } else {
    ldout(cct, 5) << "successfully acquired exclusive lock" << dendl;
  }

  if (m_managed_lock->is_locked()) {
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.aio_work_queue->unblock_writes();
  }
}

template <typename I>
void ExclusiveLock<I>::handle_release_lock(int r) {
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": r=" << r
                             << dendl;

  if (r >= 0) {
    bool lock_request_needed =
        m_image_ctx.aio_work_queue->is_lock_request_needed();
    if (lock_request_needed) {
      // if we have blocked IO -- re-request the lock
      RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
      request_lock(nullptr);
    }
  }
}

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
