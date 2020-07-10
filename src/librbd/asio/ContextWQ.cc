// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/asio/ContextWQ.h"
#include "include/Context.h"
#include "common/Cond.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::asio::ContextWQ: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace asio {

ContextWQ::ContextWQ(CephContext* cct, boost::asio::io_context& io_context)
  : m_cct(cct), m_io_context(io_context), m_strand(io_context),
    m_queued_ops(0) {
  ldout(m_cct, 20) << dendl;
}

ContextWQ::~ContextWQ() {
  ldout(m_cct, 20) << dendl;
  drain();
}

void ContextWQ::drain() {
  ldout(m_cct, 20) << dendl;
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
