// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/asio/ContextWQ.h"
#include "include/Context.h"
#include "common/Cond.h"
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>

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
  boost::asio::post(m_io_context, boost::asio::bind_executor(
    m_strand, [this, ctx]() { drain_handler(ctx); }));
}

void ContextWQ::queue(Context *ctx, int r) {
  ++m_queued_ops;

  // ensure all legacy ContextWQ users are dispatched sequentially for backwards
  // compatibility (i.e. might not be concurrent thread-safe)
  boost::asio::post(m_io_context, boost::asio::bind_executor(
    m_strand,
    [this, ctx, r]() {
      ctx->complete(r);

      ceph_assert(m_queued_ops > 0);
      --m_queued_ops;
    }));
}

} // namespace asio
} // namespace librbd
