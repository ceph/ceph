// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncObjectThrottle.h"
#include "include/rbd/librbd.hpp"
#include "librbd/AsyncRequest.h"

namespace librbd
{

AsyncObjectThrottle::AsyncObjectThrottle(const AsyncRequest& async_request,
                                         const ContextFactory& context_factory,
				 	 Context *ctx, ProgressContext &prog_ctx,
					 uint64_t object_no,
					 uint64_t end_object_no)
  : m_lock("librbd::AsyncThrottle::m_lock"),
    m_async_request(async_request), m_context_factory(context_factory),
    m_ctx(ctx), m_prog_ctx(prog_ctx), m_object_no(object_no),
    m_end_object_no(end_object_no), m_current_ops(0), m_ret(0)
{
}

void AsyncObjectThrottle::start_ops(uint64_t max_concurrent) {
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
    m_ctx->complete(m_ret);
    delete this;
  }
}

void AsyncObjectThrottle::finish_op(int r) {
  bool complete;
  {
    Mutex::Locker l(m_lock);
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

void AsyncObjectThrottle::start_next_op() {
  bool done = false;
  while (!done) {
    if (m_async_request.is_canceled() && m_ret == 0) {
      // allow in-flight ops to complete, but don't start new ops
      m_ret = -ERESTART;
      return;
    } else if (m_ret != 0 || m_object_no >= m_end_object_no) {
      return;
    }

    uint64_t ono = m_object_no++;
    C_AsyncObjectThrottle *ctx = m_context_factory(*this, ono);

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
    m_prog_ctx.update_progress(ono, m_end_object_no);
  }
}

} // namespace librbd
