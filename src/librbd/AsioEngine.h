// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_ENGINE_H
#define CEPH_LIBRBD_ASIO_ENGINE_H

#include "include/common_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include <memory>
#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>

namespace neorados { struct RADOS; }

namespace librbd {

namespace asio { struct ContextWQ; }

class AsioEngine {
public:
  explicit AsioEngine(std::shared_ptr<librados::Rados> rados);
  explicit AsioEngine(librados::IoCtx& io_ctx);
  ~AsioEngine();

  AsioEngine(AsioEngine&&) = delete;
  AsioEngine(const AsioEngine&) = delete;
  AsioEngine& operator=(const AsioEngine&) = delete;

  inline neorados::RADOS& get_rados_api() {
    return *m_rados_api;
  }

  inline boost::asio::io_context& get_io_context() {
    return m_io_context;
  }
  inline operator boost::asio::io_context&() {
    return m_io_context;
  }
  inline boost::asio::io_context::executor_type get_executor() {
    return m_io_context.get_executor();
  }

  inline boost::asio::io_context::strand& get_api_strand() {
    // API client callbacks should never fire concurrently
    return m_api_strand;
  }

  inline asio::ContextWQ* get_work_queue() {
    return m_context_wq.get();
  }

private:
  std::shared_ptr<neorados::RADOS> m_rados_api;
  CephContext* m_cct;

  boost::asio::io_context& m_io_context;
  boost::asio::io_context::strand m_api_strand;
  std::unique_ptr<asio::ContextWQ> m_context_wq;
};

} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_ENGINE_H
