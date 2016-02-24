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

#include <algorithm>

#include "PosixStack.h"

#include "msg/async/dpdk/Packet.h"

#include "include/buffer.h"
#include "common/errno.h"
#include "common/Tub.h"
#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "dpdk "

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  NetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;

 public:
  explicit PosixConnectedSocketImpl(NetHandler &h, const entity_addr_t &sa, int f, bool connected)
      : handler(h), _fd(f), sa(sa), connected(connected) {}

  virtual int is_connected() override {
    if (connected)
      return 1;

    int r = handler.reconnect(sa, _fd);
    if (r == 0) {
      connected = true;
      return 1;
    } else if (r < 0) {
      return r;
    } else {
      return 0;
    }
  }

  virtual int read(char *buf, size_t len) {
    int r = ::read(_fd, buf, len);
    if (r < 0)
      r = -errno;
    return r;
  }

  // return the sent length
  // < 0 means error occured
  static ssize_t do_sendmsg(int fd, struct msghdr &msg, unsigned len, bool more)
  {
    ssize_t sent = 0;
    while (sent < len) {
      ssize_t r;
  #if defined(MSG_NOSIGNAL)
      r = ::sendmsg(fd, &msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
  #else
      r = ::sendmsg(fd, &msg, (more ? MSG_MORE : 0));
  #endif /* defined(MSG_NOSIGNAL) */

      if (r < 0) {
        if (errno == EINTR) {
          continue;
        } else if (errno == EAGAIN) {
          break;
        }
        return -errno;
      }

      sent += r;
      if (len == sent) break;

      while (r > 0) {
        if (msg.msg_iov[0].iov_len <= (size_t)r) {
          // drain this whole item
          r -= msg.msg_iov[0].iov_len;
          msg.msg_iov++;
          msg.msg_iovlen--;
        } else {
          msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + r;
          msg.msg_iov[0].iov_len -= r;
          break;
        }
      }
    }
    return (ssize_t)sent;
  }

  virtual ssize_t send(bufferlist &bl, bool more) {
    ssize_t sent_bytes = 0;
    std::list<bufferptr>::const_iterator pb = bl.buffers().begin();
    uint64_t left_pbrs = bl.buffers().size();
    while (left_pbrs) {
      struct msghdr msg;
      struct iovec msgvec[IOV_MAX];
      uint64_t size = MIN(left_pbrs, IOV_MAX);
      left_pbrs -= size;
      memset(&msg, 0, sizeof(msg));
      msg.msg_iovlen = 0;
      msg.msg_iov = msgvec;
      unsigned msglen = 0;
      while (size > 0) {
        msgvec[msg.msg_iovlen].iov_base = (void*)(pb->c_str());
        msgvec[msg.msg_iovlen].iov_len = pb->length();
        msg.msg_iovlen++;
        msglen += pb->length();
        ++pb;
        size--;
      }

      ssize_t r = do_sendmsg(_fd, msg, msglen, left_pbrs || more);
      if (r < 0)
        return r;

      // "r" is the remaining length
      sent_bytes += r;
      if (r < msglen)
        break;
      // only "r" == 0 continue
    }

    if (sent_bytes) {
      bufferlist swapped;
      if (sent_bytes < bl.length()) {
        bl.splice(sent_bytes, bl.length()-sent_bytes, &swapped);
        bl.swap(swapped);
      } else {
        bl.clear();
      }
    }

    return sent_bytes;
  }
  virtual void shutdown() {
    ::shutdown(_fd, SHUT_RDWR);
  }
  virtual void close() {
    ::close(_fd);
  }
  virtual int fd() const override {
    return _fd;
  }
  friend class PosixServerSocketImpl;
  friend class PosixNetworkStack;
};

class PosixServerSocketImpl : public ServerSocketImpl {
  NetHandler &handler;
  entity_addr_t sa;
  int _fd;

 public:
  explicit PosixServerSocketImpl(NetHandler &h, const entity_addr_t &sa, int f): handler(h), sa(sa), _fd(f) {}
  virtual int accept(ConnectedSocket *sock, entity_addr_t *out) override;
  virtual void abort_accept() override {
    ::close(_fd);
  }
  virtual int fd() const override {
    return _fd;
  }
};

int PosixServerSocketImpl::accept(ConnectedSocket *sock, entity_addr_t *out) {
  assert(sock);
  socklen_t slen = sizeof(out->ss_addr());
  int sd = ::accept(_fd, (sockaddr*)&out->ss_addr(), &slen);
  if (sd < 0) {
    return -errno;
  }

  int r = handler.set_nonblock(sd);
  if (r < 0) {
    ::close(sd);
    return -errno;
  }

  std::unique_ptr<PosixConnectedSocketImpl> csi(new PosixConnectedSocketImpl(handler, *out, sd, true));
  *sock = ConnectedSocket(std::move(csi));
  if (out)
    *out = sa;
  return 0;
}

int PosixNetworkStack::listen(entity_addr_t &sa, const SocketOptions &opt, ServerSocket *sock)
{
  assert(sock);
  int listen_sd = net.create_socket(sa.get_family(), true);
  if (listen_sd < 0) {
    return -errno;
  }

  int r = net.set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    return -errno;
  }

  r = net.set_socket_options(listen_sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(listen_sd);
    return -errno;
  }

  r = ::bind(listen_sd, (struct sockaddr *)&sa.ss_addr(), sa.addr_size());
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to bind to " << sa.ss_addr()
               << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, 128);
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(std::unique_ptr<PosixServerSocketImpl>(new PosixServerSocketImpl(net, sa, listen_sd)));
  return 0;
}

int PosixNetworkStack::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  int sd;

  if (opts.nonblock) {
    sd = net.nonblock_connect(addr);
  } else {
    sd = net.connect(addr);
  }

  if (sd < 0) {
    ::close(sd);
    return -errno;
  }

  *socket = ConnectedSocket(
      std::unique_ptr<PosixConnectedSocketImpl>(new PosixConnectedSocketImpl(net, addr, sd, !opts.nonblock)));
  return 0;
}
