// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/asio/ContextWQ.h"
#include "include/Context.h"
#include "common/Cond.h"

namespace librbd {
namespace asio {

ContextWQ::ContextWQ(boost::asio::io_context& io_context)
  : m_io_context(io_context), m_strand(io_context),
    m_queued_ops(0) {
}

void ContextWQ::drain() {
  C_SaferCond ctx;
  drain_handler(&ctx);
  ctx.wait();
}

void ContextWQ::drain_handler(Context* ctx) {
  if (m_queued_ops == 0) {
    ctx->complete(0);
    return;
  }

  // new items might be queued while we are trying to drain, so we
  // might need to post the handler multiple times
  boost::asio::post(m_strand, [this, ctx]() { drain_handler(ctx); });
}

} // namespace asio
} // namespace librbd
