// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/asio/AsioContextWQ.h"
#include "include/Context.h"
#include "common/Cond.h"
#include "common/dout.h"
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

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

  boost::asio::post(*m_strand, [this, ctx]() { drain_handler(ctx); });
}

void AsioContextWQ::post(Work fn) {
  boost::asio::post(*m_io_context, std::move(fn));
}

void AsioContextWQ::dispatch(Work fn) {
  boost::asio::dispatch(*m_io_context, std::move(fn));
}

void AsioContextWQ::post_serial(Work fn) {
  boost::asio::post(*m_strand, std::move(fn));
}

void AsioContextWQ::dispatch_serial(Work fn) {
  boost::asio::dispatch(*m_strand, std::move(fn));
}

} // namespace asio
} // namespace librbd
