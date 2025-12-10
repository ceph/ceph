// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_ASIO_CONTEXT_WQ_EXECUTOR_H
#define CEPH_LIBRBD_ASIO_CONTEXT_WQ_EXECUTOR_H

#include "librbd/asio/ContextWQ.h"

#include <boost/asio/execution.hpp>
#include <type_traits>
#include <utility>

namespace librbd {
namespace asio {

/**
 * Boost.Asio executor that schedules work on a ContextWQ.
 *
 * When bound onto neorados completion tokens (via bind_executor), Objecter's
 * Op::complete dispatches here instead of the default io_context.
 *
 * An optional channel pins the work to one execution context of the queue,
 * e.g. the reactor thread that submitted the op.
 */
class ContextWQExecutor {
public:
  ContextWQExecutor() noexcept = default;
  explicit ContextWQExecutor(ContextWQ* wq) noexcept : m_wq(wq) {}
  ContextWQExecutor(ContextWQ* wq, ContextWQ::Channel channel) noexcept
    : m_wq(wq), m_channel(channel) {}

  ContextWQ* context_wq() const noexcept {
    return m_wq;
  }

  ContextWQ::Channel channel() const noexcept {
    return m_channel;
  }

  template <typename Function>
  void execute(Function&& f) const {
    m_wq->post_channel(m_channel,
                       ContextWQ::Work(std::forward<Function>(f)));
  }

  // --- asio execution properties (work_guard / prefer / require) ---

  static constexpr boost::asio::execution::blocking_t::never_t query(
      boost::asio::execution::blocking_t) noexcept {
    return boost::asio::execution::blocking.never;
  }

  static constexpr boost::asio::execution::relationship_t::fork_t query(
      boost::asio::execution::relationship_t) noexcept {
    return boost::asio::execution::relationship.fork;
  }

  static constexpr boost::asio::execution::outstanding_work_t::untracked_t query(
      boost::asio::execution::outstanding_work_t) noexcept {
    return boost::asio::execution::outstanding_work.untracked;
  }

  static constexpr boost::asio::execution::mapping_t::thread_t query(
      boost::asio::execution::mapping_t) noexcept {
    return boost::asio::execution::mapping.thread;
  }

  ContextWQExecutor require(
      boost::asio::execution::blocking_t::never_t) const {
    return *this;
  }

  ContextWQExecutor prefer(
      boost::asio::execution::blocking_t::never_t) const {
    return *this;
  }

  ContextWQExecutor prefer(
      boost::asio::execution::relationship_t::fork_t) const {
    return *this;
  }

  ContextWQExecutor prefer(
      boost::asio::execution::relationship_t::continuation_t) const {
    return *this;
  }

  ContextWQExecutor prefer(
      boost::asio::execution::outstanding_work_t::tracked_t) const {
    return *this;
  }

  ContextWQExecutor prefer(
      boost::asio::execution::outstanding_work_t::untracked_t) const {
    return *this;
  }

  ContextWQExecutor require(
      boost::asio::execution::outstanding_work_t::tracked_t) const {
    return *this;
  }

  ContextWQExecutor require(
      boost::asio::execution::outstanding_work_t::untracked_t) const {
    return *this;
  }

  friend bool operator==(const ContextWQExecutor& a,
                         const ContextWQExecutor& b) noexcept {
    return a.m_wq == b.m_wq && a.m_channel == b.m_channel;
  }

  friend bool operator!=(const ContextWQExecutor& a,
                         const ContextWQExecutor& b) noexcept {
    return !(a == b);
  }

private:
  ContextWQ* m_wq = nullptr;
  ContextWQ::Channel m_channel = nullptr;
};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_CONTEXT_WQ_EXECUTOR_H
