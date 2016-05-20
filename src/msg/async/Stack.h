// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CEPH_GENERICSOCKET_H
#define CEPH_GENERICSOCKET_H

#include "common/perf_counters.h"
#include "msg/msg_types.h"
#include "msg/async/Event.h"

class ConnectedSocketImpl {
 public:
  virtual ~ConnectedSocketImpl() {}
  virtual int is_connected() = 0;
  virtual ssize_t read(char*, size_t) = 0;
  virtual ssize_t zero_copy_read(bufferptr&) = 0;
  virtual ssize_t send(bufferlist &bl, bool more) = 0;
  virtual void shutdown() = 0;
  virtual void close() = 0;
  virtual int fd() const = 0;
};

class ConnectedSocket;
struct SocketOptions {
  bool nonblock = true;
  bool nodelay = true;
  int rcbuf_size = 0;
};

/// \cond internal
class ServerSocketImpl {
 public:
  virtual ~ServerSocketImpl() {}
  virtual int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out) = 0;
  virtual void abort_accept() = 0;
  /// Get file descriptor
  virtual int fd() const = 0;
};
/// \endcond

/// \addtogroup networking-module
/// @{

/// A TCP (or other stream-based protocol) connection.
///
/// A \c ConnectedSocket represents a full-duplex stream between
/// two endpoints, a local endpoint and a remote endpoint.
class ConnectedSocket {
  std::unique_ptr<ConnectedSocketImpl> _csi;

 public:
  /// Constructs a \c ConnectedSocket not corresponding to a connection
  ConnectedSocket() {};
  /// \cond internal
  explicit ConnectedSocket(std::unique_ptr<ConnectedSocketImpl> csi)
      : _csi(std::move(csi)) {}
  /// \endcond
   ~ConnectedSocket() {
    if (_csi)
      _csi->close();
  }
  /// Moves a \c ConnectedSocket object.
  ConnectedSocket(ConnectedSocket&& cs) = default;
  /// Move-assigns a \c ConnectedSocket object.
  ConnectedSocket& operator=(ConnectedSocket&& cs) = default;

  int is_connected() {
    return _csi->is_connected();
  }
  /// Read the input stream with copy.
  ///
  /// Copy an object returning data sent from the remote endpoint.
  ssize_t read(char* buf, size_t len) {
    return _csi->read(buf, len);
  }
  /// Gets the input stream.
  ///
  /// Gets an object returning data sent from the remote endpoint.
  ssize_t zero_copy_read(bufferptr &data) {
    return _csi->zero_copy_read(data);
  }
  /// Gets the output stream.
  ///
  /// Gets an object that sends data to the remote endpoint.
  ssize_t send(bufferlist &bl, bool more) {
    return _csi->send(bl, more);
  }
  /// Disables output to the socket.
  ///
  /// Current or future writes that have not been successfully flushed
  /// will immediately fail with an error.  This is useful to abort
  /// operations on a socket that is not making progress due to a
  /// peer failure.
  void shutdown() {
    return _csi->shutdown();
  }
  /// Disables input from the socket.
  ///
  /// Current or future reads will immediately fail with an error.
  /// This is useful to abort operations on a socket that is not making
  /// progress due to a peer failure.
  void close() {
    _csi->close();
    _csi.reset();
  }

  /// Get file descriptor
  int fd() const {
    return _csi->fd();
  }

  explicit operator bool() const {
    return _csi.get();
  }
};
/// @}

/// \addtogroup networking-module
/// @{

/// A listening socket, waiting to accept incoming network connections.
class ServerSocket {
  std::unique_ptr<ServerSocketImpl> _ssi;
 public:
  /// Constructs a \c ServerSocket not corresponding to a connection
  ServerSocket() {}
  /// \cond internal
  explicit ServerSocket(std::unique_ptr<ServerSocketImpl> ssi)
      : _ssi(std::move(ssi)) {}
  ~ServerSocket() {
    if (_ssi)
      _ssi->abort_accept();
  }
  /// \endcond
  /// Moves a \c ServerSocket object.
  ServerSocket(ServerSocket&& ss) = default;
  /// Move-assigns a \c ServerSocket object.
  ServerSocket& operator=(ServerSocket&& cs) = default;

  /// Accepts the next connection to successfully connect to this socket.
  ///
  /// \Accepts a \ref ConnectedSocket representing the connection, and
  ///          a \ref entity_addr_t describing the remote endpoint.
  int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out) {
    return _ssi->accept(sock, opt, out);
  }

  /// Stops any \ref accept() in progress.
  ///
  /// Current and future \ref accept() calls will terminate immediately
  /// with an error.
  void abort_accept() {
    _ssi->abort_accept();
    _ssi.reset();
  }

  /// Get file descriptor
  int fd() const {
    return _ssi->fd();
  }

  explicit operator bool() const {
    return _ssi.get();
  }
};
/// @}

class NetworkStack;

enum {
  l_msgr_first = 94000,
  l_msgr_recv_messages,
  l_msgr_send_messages,
  l_msgr_send_messages_inline,
  l_msgr_recv_bytes,
  l_msgr_send_bytes,
  l_msgr_created_connections,
  l_msgr_active_connections,
  l_msgr_last,
};

class Worker {
 public:
  bool init_done = false;
  bool done = false;

  CephContext *cct;
  PerfCounters *perf_logger;
  unsigned id;

  EventCenter center;
  Worker(CephContext *c, unsigned i)
    : cct(c), perf_logger(NULL), id(i), center(c) {
    char name[128];
    sprintf(name, "AsyncMessenger::Worker-%d", id);
    // initialize perf_logger
    PerfCountersBuilder plb(cct, name, l_msgr_first, l_msgr_last);

    plb.add_u64_counter(l_msgr_recv_messages, "msgr_recv_messages", "Network received messages");
    plb.add_u64_counter(l_msgr_send_messages, "msgr_send_messages", "Network sent messages");
    plb.add_u64_counter(l_msgr_send_messages_inline, "msgr_send_messages_inline", "Network sent inline messages");
    plb.add_u64_counter(l_msgr_recv_bytes, "msgr_recv_bytes", "Network received bytes");
    plb.add_u64_counter(l_msgr_send_bytes, "msgr_send_bytes", "Network received bytes");
    plb.add_u64_counter(l_msgr_created_connections, "msgr_active_connections", "Active connection number");
    plb.add_u64_counter(l_msgr_active_connections, "msgr_created_connections", "Created connection number");

    perf_logger = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perf_logger);
  }
  virtual ~Worker() {
    if (perf_logger) {
      cct->get_perfcounters_collection()->remove(perf_logger);
      delete perf_logger;
    }
  }

  virtual int listen(entity_addr_t &addr,
                     const SocketOptions &opts, ServerSocket *) = 0;
  virtual int connect(const entity_addr_t &addr,
                      const SocketOptions &opts, ConnectedSocket *socket) = 0;

  virtual void initialize() {}
  virtual void destroy() {}
  PerfCounters *get_perf_counter() { return perf_logger; }
};

class NetworkStack {
  bool started = false;
  unsigned num_workers = 0;
  uint64_t seq = 0;
  string type;

 protected:
  CephContext *cct;
  vector<Worker*> workers;
  std::vector<std::function<void ()>> threads;
  // Used to indicate whether thread started

  explicit NetworkStack(CephContext *c, const string &t);
 public:
  virtual ~NetworkStack() {
    for (auto &&w : workers)
      delete w;
  }

  static std::shared_ptr<NetworkStack> create(
          CephContext *c, const string &type);

  static Worker* create_worker(
          CephContext *c, const string &t, unsigned i);
  virtual bool support_zero_copy_read() const { return false; }
  virtual bool support_local_listen_table() const { return false; }

  void start();
  void stop();
  virtual Worker *get_worker() {
    return workers[(seq++)%workers.size()];
  }
  Worker *get_worker(unsigned i) {
    return workers[i];
  }
  void barrier();
  uint64_t get_num_worker() const {
    return workers.size();
  }

  // direct is used in tests only
  virtual void spawn_workers(std::vector<std::function<void ()>> &) = 0;
  virtual void join_workers() = 0;

 private:
  NetworkStack(const NetworkStack &);
  NetworkStack& operator=(const NetworkStack &);
};

#endif //CEPH_GENERICSOCKET_H
