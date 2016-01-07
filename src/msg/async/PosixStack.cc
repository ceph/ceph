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

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include "PosixStack.h"
#include "Packet.h"

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  int fd;
  entity_addr_t sa;
  bool connected;

 private:
  explicit PosixConnectedSocketImpl(const entity_addr_t &sa, int f, bool connected)
      : sa(sa), fd(f), connected(connected) {}
 public:

  virtual int is_connected() override {
    if (connected)
      return connected;

    int r = net.reconnect(sa, fd);
    if (r > 0)
      connected = true;
    return r;
  }

  virtual int read(char *buf, size_t len) {
    return ::read(fd, buf, len);
  }
  virtual int sendmsg(struct msghdr &msg, size_t len, bool more) {
    return ::sendmsg(fd, &msg, more);
  }
  virtual void shutdown() {
    ::shutdown(fd, SHUT_RDWR);
  }
  virtual void close() {
    ::close(fd);
  }
  friend class PosixServerSocketImpl;
  friend class PosixNetworkStack;
};

class PosixServerSocketImpl : public ServerSocketImpl {
  entity_addr_t sa;
  int fd;

 public:
  explicit PosixServerSocketImpl(const entity_addr_t &sa, int fd) : sa(sa), fd(fd) {}
  virtual int accept(ConnectedSocket *sock, entity_addr_t *out) override;
  virtual void abort_accept() override {
    ::close(fd);
  }
};

int PosixServerSocketImpl::accept(ConnectedSocket *sock, entity_addr_t *out) {
  assert(sock);
  socklen_t slen = sizeof(sa->ss_addr());
  int sd = ::accept(fd, (sockaddr*)&sa.ss_addr(), &slen);
  if (sd >= 0) {
    ldout(cct, 10) << __func__ << " accepted incoming on sd " << sd << dendl;
    return r;
  }
  std::unique_ptr<PosixServerSocketImpl> csi(new PosixServerSocketImpl(sa, fd));
  *sock = ConnectedSocket(std::move(csi));
  if (out)
    *out = sa;
  return 0;
}

int PosixNetworkStack::listen(const entity_addr_t &sa, const listen_options &opt, ServerSocket *sock) {
  assert(sock);
  int listen_sd = ::socket(sa.get_family(), SOCK_STREAM, 0);
  if (listen_sd < 0) {
    lderr(cct) << __func__ << " unable to create socket: " << cpp_strerror(errno) << dendl;
    return -errno;
  }

  int r = net.set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    return -errno;
  }

  r = net.set_socket_options(listen_sd, opt.nodelay, opt.rcvbuf_size);
  if (r < 0) {
    ::close(listen_sd);
    return -errno;
  }

  r = ::bind(listen_sd, (struct sockaddr *)&sa.ss_addr(), sa.addr_size());
  if (rc < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to bind to " << sa.ss_addr()
               << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, 128);
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(rc) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(std::make_unique<PosixServerSocketImpl>(sa, listen_sd));
  return 0;
}

int PosixNetworkStack::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  int sd = net.create_socket(addr.get_family());
  if (sd < 0)
    return -errno;

  if (opts.nonblock) {
    sd = net.nonblock_connect(addr);
  } else {
    sd = net.connect(addr);
  }

  if (sd < 0) {
    ::close(sd);
    return -errno;
  }

  *sock = ConnectedSocketstd::make_unique<PosixConnectedSocketImpl>(sa, sd, !opts.nonblock));
  return 0;
}
