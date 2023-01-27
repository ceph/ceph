// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <mutex>
#include <boost/asio/execution_context.hpp>
#include <boost/intrusive/list.hpp>

namespace ceph::async {

struct service_tag {};
using service_list_base_hook = boost::intrusive::list_base_hook<
    boost::intrusive::tag<service_tag>>;

/// Service for two-phase execution_context shutdown, which breaks ownership
/// cycles between completion handlers and their io objects. Tracks objects
/// which may have outstanding completion handlers, and calls their member
/// function service_shutdown() when the execution_context is shutting down.
/// This member function should destroy any memory associated with its
/// outstanding completion handlers.
///
/// Requirements for IoObject:
/// * Inherits publicly from service_list_base_hook
/// * Has public member function service_shutdown()
/// * Calls add(*this) on construction and remove(*this) on destruction.
template <typename IoObject>
class service : public boost::asio::execution_context::service {
  using base_hook = boost::intrusive::base_hook<service_list_base_hook>;
  boost::intrusive::list<IoObject, base_hook> entries;
  std::mutex mutex;

  /// Called by the execution_context on shutdown
  void shutdown() override {
    while (!entries.empty()) {
      auto& entry = entries.front();
      entries.pop_front();
      entry.service_shutdown();
    }
  }
 public:
  using key_type = service;
  static inline boost::asio::execution_context::id id;

  explicit service(boost::asio::execution_context& ctx)
      : boost::asio::execution_context::service(ctx) {}

  /// Register an io object for notification of service_shutdown()
  void add(IoObject& entry) {
    auto lock = std::scoped_lock{mutex};
    entries.push_back(entry);
  }
  /// Unregister an object
  void remove(IoObject& entry) {
    auto lock = std::scoped_lock{mutex};
    if (entries.empty()) {
      // already shut down
    } else {
      entries.erase(entries.iterator_to(entry));
    }
  }
};

} // namespace ceph::async
