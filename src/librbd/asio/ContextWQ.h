// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_CONTEXT_WQ_H
#define CEPH_LIBRBD_ASIO_CONTEXT_WQ_H

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>

struct Context;

namespace librbd {
namespace asio {

class ContextWQ {
public:
  explicit ContextWQ(boost::asio::io_context& io_context);

  void drain();
  void queue(Context *ctx, int r = 0);

private:
  boost::asio::io_context& m_io_context;
  boost::asio::io_context::strand m_strand;

  std::atomic<uint64_t> m_queued_ops;

  void drain_handler(Context* ctx);

};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_CONTEXT_WQ_H
