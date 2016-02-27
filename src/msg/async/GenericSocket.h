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

#include "msg/msg_types.h"

class ConnectedSocketImpl {
 public:
  virtual ~ConnectedSocketImpl() {}
  virtual int is_connected() = 0;
  virtual int read(char*, size_t) = 0;
  virtual int zero_copy_read(size_t, bufferptr*) = 0;
  virtual ssize_t send(bufferlist &bl, bool more) = 0;
  virtual void shutdown() = 0;
  virtual void close() = 0;
  virtual int fd() const = 0;
};

class ConnectedSocket;

/// \cond internal
class ServerSocketImpl {
 public:
  virtual ~ServerSocketImpl() {}
  virtual int accept(ConnectedSocket *sock, entity_addr_t *out) = 0;
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
  int read(char* buf, size_t len) {
    return _csi->read(buf, len);
  }
  /// Gets the input stream.
  ///
  /// Gets an object returning data sent from the remote endpoint.
  int zero_copy_read(size_t len, bufferptr *ptr) {
    return _csi->zero_copy_read(len, ptr);
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
    return _csi->close();
  }

  /// Get file descriptor
  int fd() const {
    return _csi->fd();
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
  int accept(ConnectedSocket *sock, entity_addr_t *out) {
    return _ssi->accept(sock, out);
  }

  /// Stops any \ref accept() in progress.
  ///
  /// Current and future \ref accept() calls will terminate immediately
  /// with an error.
  void abort_accept() {
    return _ssi->abort_accept();
  }

  /// Get file descriptor
  int fd() const {
    return _ssi->fd();
  }
};
/// @}

struct SocketOptions {
  bool nonblock = true;
  bool nodelay = true;
  int rcbuf_size = 0;
};

class EventCenter;

class NetworkStack {
 protected:
  NetworkStack(CephContext *c): cct(c) {}
 public:
  CephContext *cct;
  static std::unique_ptr<NetworkStack> create(CephContext *c, const string &type, EventCenter *center, unsigned i);
  virtual ~NetworkStack() {}
  virtual bool support_zero_copy_read() const = 0;
  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) = 0;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) = 0;
  virtual void initialize() {}
};

#endif //CEPH_GENERICSOCKET_H
