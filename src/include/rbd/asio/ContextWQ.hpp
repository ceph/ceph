// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM Inc.
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
#include <memory>

class CephContext;
struct Context;

namespace librbd {
namespace asio {

/**
 * ContextWQ - interface for work queue execution contexts.
 *
 * This class is part of the public librbd API to allow external modules
 * to provide custom implementations that schedule work on different
 * execution contexts.
 *
 * External implementations should inherit from this class and implement
 * the pure virtual methods to schedule work on their preferred execution context.
 */
class ContextWQ {
public:
  virtual ~ContextWQ() = default;

  /**
   * Queue a context to be executed on the work queue.
   *
   * @param ctx Context to execute
   * @param r Return value to pass to context
   */
  virtual void queue(Context *ctx, int r = 0) = 0;

  /**
   * Drain all pending operations.
   * Blocks until all queued operations complete.
   */
  virtual void drain() = 0;

protected:
  // Protected constructor for derived classes
  explicit ContextWQ(void* cct) : m_cct(cct), m_queued_ops(0) {}

  void* m_cct;  // Opaque pointer to CephContext (or nullptr if not needed)
  std::atomic<uint64_t> m_queued_ops;
};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_CONTEXT_WQ_HPP
