// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025,2026 IBM Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef CEPH_LIBRBD_ASIO_CONTEXT_WQ_HPP
#define CEPH_LIBRBD_ASIO_CONTEXT_WQ_HPP

#include <atomic>
#include <functional>
#include <memory>

#if defined(__GNUC__) && !defined(__MINGW32__)
#define LIBRBD_ASIO_CONTEXT_WQ_API __attribute__((visibility("default")))
#else
#define LIBRBD_ASIO_CONTEXT_WQ_API
#endif

// Forward declaration only when Context is not already defined
#ifndef CEPH_CONTEXT_H
struct Context;
#endif

namespace librbd {
namespace asio {

/**
 * ContextWQ - interface for work queue execution contexts.
 *
 * This class is part of the public librbd API to allow external modules
 * to provide custom implementations that schedule work on different
 * execution contexts.
 *
 * - post / dispatch: general parallel work
 * - post_serial / dispatch_serial: strictly ordered functor path
 * - queue(Context*, r): legacy Context::complete ordering + drain accounting
 *
 * Exported from librbd.so for out-of-tree implementations
 */
class LIBRBD_ASIO_CONTEXT_WQ_API ContextWQ {
public:
  using Work = std::function<void()>;

  virtual ~ContextWQ() = default;

  /**
   * Drain all pending operations tracked by queue().
   * Blocks until those complete.
   */
  virtual void drain() = 0;

  /**
   * Schedule work on the general executor
   */
  virtual void post(Work fn) = 0;

  /**
   * Dispatch on the general executor
   */
  virtual void dispatch(Work fn) = 0;

  /**
   * Strictly ordered functor path
   */
  virtual void post_serial(Work fn) = 0;

  /**
   * Dispatch on the serial channel
   */
  virtual void dispatch_serial(Work fn) = 0;

  /**
   * Legacy Context completion
   */
  virtual void queue(Context* ctx, int r = 0);

protected:
  // Protected constructor for derived classes
  explicit ContextWQ(void* cct) : m_cct(cct), m_queued_ops(0) {}

  void* m_cct;  // Opaque pointer to CephContext (or nullptr if not needed)
  std::atomic<uint64_t> m_queued_ops;
  std::atomic<bool> m_shutdown{false};
};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_CONTEXT_WQ_HPP
