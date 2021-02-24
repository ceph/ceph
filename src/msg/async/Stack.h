// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_STACK_H
#define CEPH_MSG_ASYNC_STACK_H

#include "include/spinlock.h"
#include "common/perf_counters.h"
#include "msg/msg_types.h"
#include "msg/async/Event.h"

class Worker;
class ConnectedSocketImpl {
 public:
  virtual ~ConnectedSocketImpl() {}
  virtual int is_connected() = 0;
  virtual ssize_t read(char*, size_t) = 0;
  virtual ssize_t send(ceph::buffer::list &bl, bool more) = 0;
  virtual void shutdown() = 0;
  virtual void close() = 0;
  virtual int fd() const = 0;
};

class ConnectedSocket;
struct SocketOptions {
  bool nonblock = true;
  bool nodelay = true;
  int rcbuf_size = 0;
  int priority = -1;
  entity_addr_t connect_bind_addr;
};

/// \cond internal
class ServerSocketImpl {
 public:
  unsigned addr_type; ///< entity_addr_t::TYPE_*
  unsigned addr_slot; ///< position of our addr in myaddrs().v
  ServerSocketImpl(unsigned type, unsigned slot)
    : addr_type(type), addr_slot(slot) {}
  virtual ~ServerSocketImpl() {}
  virtual int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) = 0;
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
  /// Gets the output stream.
  ///
  /// Gets an object that sends data to the remote endpoint.
  ssize_t send(ceph::buffer::list &bl, bool more) {
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
  int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
    return _ssi->accept(sock, opt, out, w);
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

  /// get listen/bind addr
  unsigned get_addr_slot() {
    return _ssi->addr_slot;
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
  l_msgr_recv_bytes,
  l_msgr_send_bytes,
  l_msgr_created_connections,
  l_msgr_active_connections,

  l_msgr_running_total_time,
  l_msgr_running_send_time,
  l_msgr_running_recv_time,
  l_msgr_running_fast_dispatch_time,

  l_msgr_send_messages_queue_lat,
  l_msgr_handle_ack_lat,

  l_msgr_last,
};

class Worker {
  std::mutex init_lock;
  std::condition_variable init_cond;
  bool init = false;

 public:
  bool done = false;

  CephContext *cct;
  PerfCounters *perf_logger;
  unsigned id;

  std::atomic_uint references;
  EventCenter center;

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  Worker(CephContext *c, unsigned worker_id)
    : cct(c), perf_logger(NULL), id(worker_id), references(0), center(c) {
    char name[128];
    sprintf(name, "AsyncMessenger::Worker-%u", id);
    // initialize perf_logger
    PerfCountersBuilder plb(cct, name, l_msgr_first, l_msgr_last);

    plb.add_u64_counter(l_msgr_recv_messages, "msgr_recv_messages", "Network received messages");
    plb.add_u64_counter(l_msgr_send_messages, "msgr_send_messages", "Network sent messages");
    plb.add_u64_counter(l_msgr_recv_bytes, "msgr_recv_bytes", "Network received bytes", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64_counter(l_msgr_send_bytes, "msgr_send_bytes", "Network sent bytes", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64_counter(l_msgr_active_connections, "msgr_active_connections", "Active connection number");
    plb.add_u64_counter(l_msgr_created_connections, "msgr_created_connections", "Created connection number");

    plb.add_time(l_msgr_running_total_time, "msgr_running_total_time", "The total time of thread running");
    plb.add_time(l_msgr_running_send_time, "msgr_running_send_time", "The total time of message sending");
    plb.add_time(l_msgr_running_recv_time, "msgr_running_recv_time", "The total time of message receiving");
    plb.add_time(l_msgr_running_fast_dispatch_time, "msgr_running_fast_dispatch_time", "The total time of fast dispatch");

    plb.add_time_avg(l_msgr_send_messages_queue_lat, "msgr_send_messages_queue_lat", "Network sent messages lat");
    plb.add_time_avg(l_msgr_handle_ack_lat, "msgr_handle_ack_lat", "Connection handle ack lat");

    perf_logger = plb.create_perf_counters();
    cct->get_perfcounters_collection()->add(perf_logger);
  }
  virtual ~Worker() {
    if (perf_logger) {
      cct->get_perfcounters_collection()->remove(perf_logger);
      delete perf_logger;
    }
  }

  virtual int listen(entity_addr_t &addr, unsigned addr_slot,
                     const SocketOptions &opts, ServerSocket *) = 0;
  virtual int connect(const entity_addr_t &addr,
                      const SocketOptions &opts, ConnectedSocket *socket) = 0;
  virtual void destroy() {}

  virtual void initialize() {}
  PerfCounters *get_perf_counter() { return perf_logger; }
  void release_worker() {
    int oldref = references.fetch_sub(1);
    ceph_assert(oldref > 0);
  }
  void init_done() {
    init_lock.lock();
    init = true;
    init_cond.notify_all();
    init_lock.unlock();
  }
  bool is_init() {
    std::lock_guard<std::mutex> l(init_lock);
    return init;
  }
  void wait_for_init() {
    std::unique_lock<std::mutex> l(init_lock);
    while (!init)
      init_cond.wait(l);
  }
  void reset() {
    init_lock.lock();
    init = false;
    init_cond.notify_all();
    init_lock.unlock();
    done = false;
  }
};

class NetworkStack {
  unsigned num_workers = 0;
  ceph::spinlock pool_spin;
  bool started = false;

  std::function<void ()> add_thread(Worker* w);

  virtual Worker* create_worker(CephContext *c, unsigned i) = 0;

 protected:
  CephContext *cct;
  std::vector<Worker*> workers;

  explicit NetworkStack(CephContext *c);
 public:
  NetworkStack(const NetworkStack &) = delete;
  NetworkStack& operator=(const NetworkStack &) = delete;
  virtual ~NetworkStack() {
    for (auto &&w : workers)
      delete w;
  }

  static std::shared_ptr<NetworkStack> create(
    CephContext *c, const std::string &type);

  // backend need to override this method if backend doesn't support shared
  // listen table.
  // For example, posix backend has in kernel global listen table. If one
  // thread bind a port, other threads also aware this.
  // But for dpdk backend, we maintain listen table in each thread. So we
  // need to let each thread do binding port.
  virtual bool support_local_listen_table() const { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return true; }

  void start();
  void stop();
  virtual Worker *get_worker();
  Worker *get_worker(unsigned worker_id) {
    return workers[worker_id];
  }
  void drain();
  unsigned get_num_worker() const {
    return num_workers;
  }

  // direct is used in tests only
  virtual void spawn_worker(unsigned i, std::function<void ()> &&) = 0;
  virtual void join_worker(unsigned i) = 0;

  virtual bool is_ready() { return true; };
  virtual void ready() { };
};

#endif //CEPH_MSG_ASYNC_STACK_H
