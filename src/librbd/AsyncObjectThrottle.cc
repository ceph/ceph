// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncObjectThrottle.h"
#include "include/rbd/librbd.hpp"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "librbd/AsyncRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

namespace librbd
{

template <typename T>
AsyncObjectThrottle<T>::AsyncObjectThrottle(
    const AsyncRequest<T>* async_request, T &image_ctx,
    const ContextFactory& context_factory, Context *ctx,
    ProgressContext *prog_ctx, uint64_t object_no, uint64_t end_object_no)
  : m_lock(util::unique_lock_name("librbd::AsyncThrottle::m_lock", this)),
    m_async_request(async_request), m_image_ctx(image_ctx),
    m_context_factory(context_factory), m_ctx(ctx), m_prog_ctx(prog_ctx),
    m_object_no(object_no), m_end_object_no(end_object_no), m_current_ops(0),
    m_ret(0)
{
}

template <typename T>
void AsyncObjectThrottle<T>::start_ops(uint64_t max_concurrent) {
  assert(m_image_ctx.owner_lock.is_locked());
  bool complete;
  {
    Mutex::Locker l(m_lock);
    for (uint64_t i = 0; i < max_concurrent; ++i) {
      start_next_op();
      if (m_ret < 0 && m_current_ops == 0) {
	break;
      }
    }
    complete = (m_current_ops == 0);
  }
  if (complete) {
    // avoid re-entrant callback
    m_image_ctx.op_work_queue->queue(m_ctx, m_ret);
    delete this;
  }
}

template <typename T>
void AsyncObjectThrottle<T>::finish_op(int r) {
  bool complete;
  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    Mutex::Locker locker(m_lock);
    --m_current_ops;
    if (r < 0 && r != -ENOENT && m_ret == 0) {
      m_ret = r;
    }

    start_next_op();
    complete = (m_current_ops == 0);
  }
  if (complete) {
    m_ctx->complete(m_ret);
    delete this;
  }
}

template <typename T>
void AsyncObjectThrottle<T>::start_next_op() {
  bool done = false;
  while (!done) {
    if (m_async_request != NULL && m_async_request->is_canceled() &&
        m_ret == 0) {
      // allow in-flight ops to complete, but don't start new ops
      m_ret = -ERESTART;
      return;
    } else if (m_ret != 0 || m_object_no >= m_end_object_no) {
      return;
    }

    uint64_t ono = m_object_no++;
    C_AsyncObjectThrottle<T> *ctx = m_context_factory(*this, ono);

    int r = ctx->send();
    if (r < 0) {
      m_ret = r;
      delete ctx;
      return;
    } else if (r > 0) {
      // op completed immediately
      delete ctx;
    } else {
      ++m_current_ops;
      done = true;
    }
    if (m_prog_ctx != NULL) {
      m_prog_ctx->update_progress(ono, m_end_object_no);
    }
  }
}

} // namespace librbd

template class librbd::AsyncObjectThrottle<librbd::ImageCtx>;
