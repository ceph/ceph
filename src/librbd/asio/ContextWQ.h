// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_CONTEXT_WQ_H
#define CEPH_LIBRBD_ASIO_CONTEXT_WQ_H

#include "include/common_fwd.h"
#include "include/Context.h"
#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/post.hpp>

namespace librbd {
namespace asio {

class ContextWQ {
public:
  explicit ContextWQ(CephContext* cct, boost::asio::io_context& io_context);
  ~ContextWQ();

  void drain();

  void queue(Context *ctx, int r = 0) {
    ++m_queued_ops;

    // ensure all legacy ContextWQ users are dispatched sequentially for
    // backwards compatibility (i.e. might not be concurrent thread-safe)
    boost::asio::post(m_strand, [this, ctx, r]() {
      ctx->complete(r);

      ceph_assert(m_queued_ops > 0);
      --m_queued_ops;
    });
  }

private:
  CephContext* m_cct;
  boost::asio::io_context& m_io_context;
  boost::asio::io_context::strand m_strand;

  std::atomic<uint64_t> m_queued_ops;

  void drain_handler(Context* ctx);

};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_CONTEXT_WQ_H
