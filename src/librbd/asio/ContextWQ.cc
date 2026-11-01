// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/rbd/asio/ContextWQ.hpp"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include <errno.h>

namespace librbd {
namespace asio {

void ContextWQ::queue(Context* ctx, int r) {
  if (m_shutdown.load(std::memory_order_acquire)) {
    ctx->complete(-ESHUTDOWN);
    return;
  }

  ++m_queued_ops;

  Work work = [this, ctx, r]() {
    ctx->complete(r);
    ceph_assert(m_queued_ops > 0);
    --m_queued_ops;
  };

  // One context for every legacy caller. Do not use current_channel():
  // a synchronous rbd_close/rbd_resize running on that channel would
  // enqueue the continuation onto the thread blocked in C_SaferCond::wait.
  post_serial(std::move(work));
}

} // namespace asio
} // namespace librbd
