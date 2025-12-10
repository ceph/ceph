// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_ASIO_ENGINE_H
#define CEPH_LIBRBD_ASIO_ENGINE_H

#include "include/common_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include <memory>
#include <boost/asio/io_context.hpp>

#include "librbd/asio/ContextWQ.h"

struct Context;
namespace neorados { struct RADOS; }

namespace librbd {

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

  using executor_type = boost::asio::io_context::executor_type;
  inline executor_type get_executor() {
    return m_io_context.get_executor();
  }

  inline asio::ContextWQ* get_work_queue() {
    return m_context_wq.get();
  }

  /**
   * Set an external ContextWQ implementation, replaces the
   * default ASIO-based ContextWQ.
   *
   * @param context_wq Shared pointer to external ContextWQ implementation
   */
  void set_context_wq(std::shared_ptr<asio::ContextWQ> context_wq);

  template <typename T>
  void dispatch(T&& t) {
    m_context_wq->dispatch(std::forward<T>(t));
  }
  void dispatch(Context* ctx, int r);

  template <typename T>
  void post(T&& t) {
    m_context_wq->post(std::forward<T>(t));
  }
  void post(Context* ctx, int r);

  template <typename T>
  void post_serial(T&& t) {
    m_context_wq->post_serial(std::forward<T>(t));
  }

  template <typename T>
  void dispatch_serial(T&& t) {
    m_context_wq->dispatch_serial(std::forward<T>(t));
  }

private:
  std::shared_ptr<neorados::RADOS> m_rados_api;
  CephContext* m_cct;

  boost::asio::io_context& m_io_context;
  std::shared_ptr<asio::ContextWQ> m_context_wq;
};

} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_ENGINE_H
