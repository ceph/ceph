// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <concepts>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <boost/circular_buffer.hpp>
#include "common/dout.h"

namespace rgw::dbstore {

template <typename Connection>
class ConnectionHandle;

/// A thread-safe base class that manages a fixed-size pool of generic database
/// connections and supports the reclamation of ConnectionHandles. This class
/// is the subset of ConnectionPool which doesn't depend on the Factory type.
template <typename Connection>
class ConnectionPoolBase {
 public:
  ConnectionPoolBase(std::size_t max_connections)
      : connections(max_connections)
  {}
 private:
  friend class ConnectionHandle<Connection>;

  // TODO: the caller may detect a connection error that prevents the connection
  // from being reused. allow them to indicate these errors here
  void put(std::unique_ptr<Connection> connection)
  {
    auto lock = std::scoped_lock{mutex};
    connections.push_back(std::move(connection));

    if (connections.size() == 1) { // was empty
      cond.notify_one();
    }
  }
 protected:
  std::mutex mutex;
  std::condition_variable cond;
  boost::circular_buffer<std::unique_ptr<Connection>> connections;
};

/// Handle to a database connection borrowed from the pool. Automatically
/// returns the connection to its pool on the handle's destruction.
template <typename Connection>
class ConnectionHandle {
  ConnectionPoolBase<Connection>* pool = nullptr;
  std::unique_ptr<Connection> conn;
 public:
  ConnectionHandle() noexcept = default;
  ConnectionHandle(ConnectionPoolBase<Connection>* pool,
                   std::unique_ptr<Connection> conn) noexcept
    : pool(pool), conn(std::move(conn)) {}

  ~ConnectionHandle() {
    if (conn) {
      pool->put(std::move(conn));
    }
  }

  ConnectionHandle(ConnectionHandle&&) = default;
  ConnectionHandle& operator=(ConnectionHandle&& o) noexcept {
    if (conn) {
      pool->put(std::move(conn));
    }
    conn = std::move(o.conn);
    pool = o.pool;
    return *this;
  }

  explicit operator bool() const noexcept { return static_cast<bool>(conn); }
  Connection& operator*() const noexcept { return *conn; }
  Connection* operator->() const noexcept { return conn.get(); }
  Connection* get() const noexcept { return conn.get(); }
};


// factory_of concept requires the function signature:
//   F(const DoutPrefixProvider*) -> std::unique_ptr<T>
template <typename F, typename T>
concept factory_of = requires (F factory, const DoutPrefixProvider* dpp) {
  { factory(dpp) } -> std::same_as<std::unique_ptr<T>>;
  requires std::move_constructible<F>;
};


/// Generic database connection pool that enforces a limit on open connections.
template <typename Connection, factory_of<Connection> Factory>
class ConnectionPool : public ConnectionPoolBase<Connection> {
 public:
  ConnectionPool(Factory factory, std::size_t max_connections)
      : ConnectionPoolBase<Connection>(max_connections),
        factory(std::move(factory))
  {}

  /// Borrow a connection from the pool. If all existing connections are in use,
  /// use the connection factory to create another one. If we've reached the
  /// limit on open connections, wait on a condition variable for the next one
  /// returned to the pool.
  auto get(const DoutPrefixProvider* dpp)
      -> ConnectionHandle<Connection>
  {
    auto lock = std::unique_lock{this->mutex};
    std::unique_ptr<Connection> conn;

    if (!this->connections.empty()) {
      // take an existing connection
      conn = std::move(this->connections.front());
      this->connections.pop_front();
    } else if (total < this->connections.capacity()) {
      // add another connection to the pool
      conn = factory(dpp);
      ++total;
    } else {
      // wait for the next put()
      // TODO: support optional_yield
      ldpp_dout(dpp, 4) << "ConnectionPool waiting on a connection" << dendl;
      this->cond.wait(lock, [&] { return !this->connections.empty(); });
      ldpp_dout(dpp, 4) << "ConnectionPool done waiting" << dendl;
      conn = std::move(this->connections.front());
      this->connections.pop_front();
    }

    return {this, std::move(conn)};
  }
 private:
  Factory factory;
  std::size_t total = 0;
};

} // namespace rgw::dbstore
