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
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CEPH_GENERICSOCKET_H
#define CEPH_GENERICSOCKET_H

class connected_socket_impl {
 public:
  virtual ~connected_socket_impl() {}
  virtual int read(char*, size_t) = 0;
  virtual int sendmsg(struct msghdr &msg, size_t len, bool more) = 0;
  virtual void shutdown() = 0;
  virtual void close() = 0;
  virtual void set_nodelay(bool nodelay) = 0;
  virtual bool get_nodelay() const = 0;
};

/// \cond internal
class server_socket_impl {
 public:
  virtual ~server_socket_impl() {}
  virtual int accept(connected_socket) = 0;
  virtual void abort_accept() = 0;
};
/// \endcond

/// \addtogroup networking-module
/// @{

/// A TCP (or other stream-based protocol) connection.
///
/// A \c connected_socket represents a full-duplex stream between
/// two endpoints, a local endpoint and a remote endpoint.
class connected_socket {
  std::unique_ptr<connected_socket_impl> _csi;
 public:
  /// Constructs a \c connected_socket not corresponding to a connection
  connected_socket() {};
  /// \cond internal
  explicit connected_socket(std::unique_ptr<connected_socket_impl> csi)
          : _csi(std::move(csi)) {}
  /// \endcond
  /// Moves a \c connected_socket object.
  connected_socket(connected_socket&& cs) = default;
  /// Move-assigns a \c connected_socket object.
  connected_socket& operator=(connected_socket&& cs) = default;
  /// Gets the input stream.
  ///
  /// Gets an object returning data sent from the remote endpoint.
  int read(char* buf, size_t len) {
    return _csi->read(buf, len);
  }
  /// Gets the output stream.
  ///
  /// Gets an object that sends data to the remote endpoint.
  int sendmsg(struct msghdr &msg, size_t len, bool more) {
    return _csi->sendmsg(msg, len, more);
  }
  /// Sets the TCP_NODELAY option (disabling Nagle's algorithm)
  void set_nodelay(bool nodelay) {
    return _csi->set_nodelay(nodelay);
  }
  /// Gets the TCP_NODELAY option (Nagle's algorithm)
  ///
  /// \return whether the nodelay option is enabled or not
  bool get_nodelay() const {
    return _csi->get_nodelay();
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
  /// Disables socket input and output.
  ///
  /// Equivalent to \ref shutdown_input() and \ref shutdown_output().
};
/// @}

#endif //CEPH_GENERICSOCKET_H
