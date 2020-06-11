// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_ENGINE_H
#define CEPH_LIBRBD_ASIO_ENGINE_H

#include "include/common_fwd.h"
#include <memory>
#include <optional>
#include <thread>
#include <vector>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace librbd {

namespace asio { struct ContextWQ; }

class AsioEngine {
public:
  explicit AsioEngine(CephContext* cct);
  ~AsioEngine();

  inline boost::asio::io_context& get_io_context() {
    return m_io_context;
  }

  inline asio::ContextWQ* get_work_queue() {
    return m_work_queue.get();
  }

private:
  typedef std::vector<std::thread> Threads;

  typedef boost::asio::executor_work_guard<
    boost::asio::io_context::executor_type> WorkGuard;

  CephContext* m_cct;
  Threads m_threads;

  boost::asio::io_context m_io_context;
  std::optional<WorkGuard> m_work_guard;

  std::unique_ptr<asio::ContextWQ> m_work_queue;

  void init();
  void shut_down();

};

} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_ENGINE_H
