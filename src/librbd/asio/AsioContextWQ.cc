// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/asio/AsioContextWQ.h"
#include "include/Context.h"
#include "common/Cond.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::asio::AsioContextWQ: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace asio {

AsioContextWQ::AsioContextWQ(CephContext* cct, boost::asio::io_context& io_context)
  : ContextWQ(cct),
    m_io_context(&io_context),
    m_strand(std::make_unique<boost::asio::strand<executor_type>>(
      boost::asio::make_strand(io_context))) {
  ldout(get_cct(), 20) << dendl;
}

AsioContextWQ::~AsioContextWQ() {
  ldout(get_cct(), 20) << dendl;
  drain();
  m_strand.reset();
}

void AsioContextWQ::queue(Context *ctx, int r) {
  ++m_queued_ops;

  // ensure all legacy ContextWQ users are dispatched sequentially for
  // backwards compatibility (i.e. might not be concurrent thread-safe)
  boost::asio::post(*m_strand, [this, ctx, r]() {
    ctx->complete(r);

    ceph_assert(m_queued_ops > 0);
    --m_queued_ops;
  });
}

void AsioContextWQ::drain() {
  ldout(get_cct(), 20) << dendl;
  C_SaferCond ctx;
  drain_handler(&ctx);
  ctx.wait();
}

void AsioContextWQ::drain_handler(Context* ctx) {
  if (m_queued_ops == 0) {
    ctx->complete(0);
    return;
  }

  // new items might be queued while we are trying to drain, so we
  // might need to post the handler multiple times
  boost::asio::post(*m_strand, [this, ctx]() { drain_handler(ctx); });
}

} // namespace asio
} // namespace librbd
