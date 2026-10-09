// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_ASIO_ASIO_CONTEXT_WQ_H
#define CEPH_LIBRBD_ASIO_ASIO_CONTEXT_WQ_H

#include "librbd/asio/ContextWQ.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/post.hpp>
#include <memory>

namespace librbd {
namespace asio {

/**
 * ASIO-based implementation of ContextWQ.
 *
 * Parallel work uses io_context; serial work (post_serial / queue)
 * shares one strand
 */
class AsioContextWQ : public ContextWQ {
public:
  using executor_type = boost::asio::io_context::executor_type;

  explicit AsioContextWQ(CephContext* cct, boost::asio::io_context& io_context);
  ~AsioContextWQ() override;

  void drain() override;

  void post(Work fn) override;
  void dispatch(Work fn) override;
  void post_serial(Work fn) override;
  void dispatch_serial(Work fn) override;

private:
  boost::asio::io_context* m_io_context;
  std::unique_ptr<boost::asio::strand<executor_type>> m_strand;

  void drain_handler(Context* ctx);

  CephContext* get_cct() const {
    return static_cast<CephContext*>(m_cct);
  }
};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_ASIO_CONTEXT_WQ_H
