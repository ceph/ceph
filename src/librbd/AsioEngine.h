// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_ASIO_ENGINE_H
#define CEPH_LIBRBD_ASIO_ENGINE_H

#include "include/common_fwd.h"
#include <optional>
#include <thread>
#include <vector>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace librbd {

class AsioEngine {
public:
  explicit AsioEngine(CephContext* cct);
  ~AsioEngine();

  inline boost::asio::io_context& get_io_context() {
    return m_io_context;
  }

private:
  typedef std::vector<std::thread> Threads;

  typedef boost::asio::executor_work_guard<
    boost::asio::io_context::executor_type> WorkGuard;

  CephContext* m_cct;
  Threads m_threads;

  boost::asio::io_context m_io_context;
  std::optional<WorkGuard> m_work_guard;

  void init();
  void shut_down();

};

} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_ENGINE_H
