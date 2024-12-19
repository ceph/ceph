// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * Copyright (C) IBM Corp. 2024
 *
 * Author: Aliaksei Makarau <aliaksei.makarau@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include <algorithm>

#include "SMCDStack.h"

#include "include/buffer.h"
#include "include/str_list.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/dout.h"
#include "msg/Messenger.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "SMCDStack "

class SMCDConnectedSocketImpl final : public ConnectedSocketImpl {
  ceph::smcd::SMCDNetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;

 public:
  explicit SMCDConnectedSocketImpl(ceph::smcd::SMCDNetHandler &h, const entity_addr_t &sa,
                                   int f, bool connected)
      : handler(h), _fd(f), sa(sa), connected(connected) {}

  int is_connected() override {
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

  ssize_t read(char *buf, size_t len) override {
    ssize_t r = ::read(_fd, buf, len);
    if (r < 0)
      r = -ceph_sock_errno();
    return r;
  }

  // return the sent length
  // < 0 means error occurred
  static ssize_t do_sendmsg(int fd, struct msghdr &msg, unsigned len, bool more) {
    size_t sent = 0;
    while (1) {
      MSGR_SIGPIPE_STOPPER;
      ssize_t r;
      r = ::sendmsg(fd, &msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
      if (r < 0) {
        int err = ceph_sock_errno();
        if (err == EINTR) {
          continue;
        } else if (err == EAGAIN) {
          break;
        }
        return -err;
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

  ssize_t send(ceph::buffer::list &bl, bool more) override {
    size_t sent_bytes = 0;
    auto pb = std::cbegin(bl.buffers());
    uint64_t left_pbrs = bl.get_num_buffers();

    while (left_pbrs) {
      struct msghdr msg;
      struct iovec msgvec[IOV_MAX];
      uint64_t size = std::min<uint64_t>(left_pbrs, IOV_MAX);
      left_pbrs -= size;
      // FIPS zeroization audit 20191115: this memset is not security related.
      memset(&msg, 0, sizeof(msg));
      msg.msg_iovlen = size;
      msg.msg_iov = msgvec;
      unsigned msglen = 0;

      for (auto iov = msgvec; iov != msgvec + size; iov++) {
        iov->iov_base = (void*)(pb->c_str());
        iov->iov_len = pb->length();
        msglen += pb->length();
        ++pb;
      }

      ssize_t r = do_sendmsg(_fd, msg, msglen, left_pbrs || more);

      if (r < 0)
        return r;

      // "r" is the remaining length
      sent_bytes += r;

      if (static_cast<unsigned>(r) < msglen)
        break;
      // only "r" == 0 continue
    }

    if (sent_bytes) {
      ceph::buffer::list swapped;
      if (sent_bytes < bl.length()) {
        bl.splice(sent_bytes, bl.length()-sent_bytes, &swapped);
        bl.swap(swapped);
      } else {
        bl.clear();
      }
    }

    return static_cast<ssize_t>(sent_bytes);
  }

  void shutdown() override {
    ::shutdown(_fd, SHUT_RDWR);
  }

  void close() override {
    compat_closesocket(_fd);
  }

  void set_priority(int sd, int prio, int domain) override {
    handler.set_priority(sd, prio, domain);
  }

  int fd() const override {
    return _fd;
  }

  friend class SMCDServerSocketImpl;
  friend class SMCDStack;
};

class SMCDServerSocketImpl : public ServerSocketImpl {
  ceph::smcd::SMCDNetHandler &handler;
  int _fd;

 public:
  explicit SMCDServerSocketImpl(ceph::smcd::SMCDNetHandler &h, int f,
                                const entity_addr_t& listen_addr, unsigned slot)
    : ServerSocketImpl(listen_addr.get_type(), slot),
      handler(h), _fd(f) {}

  int accept(ConnectedSocket *sock, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;

  void abort_accept() override {
    ::close(_fd);
    _fd = -1;
  }

  int fd() const override {
    return _fd;
  }
};

int SMCDServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
  ceph_assert(sock);
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = accept_cloexec(_fd, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -ceph_sock_errno();
  }

  int r = handler.set_nonblock(sd);
  if (r < 0) {
    ::close(sd);
    return -ceph_sock_errno();
  }

  r = handler.set_socket_options(sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(sd);
    return -ceph_sock_errno();
  }

  ceph_assert(NULL != out); //out should not be NULL in accept connection

  out->set_type(addr_type);
  out->set_sockaddr((sockaddr*)&ss);
  handler.set_priority(sd, opt.priority, out->get_family());

  std::unique_ptr<SMCDConnectedSocketImpl> csi(new SMCDConnectedSocketImpl(handler, *out, sd, true));
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void SMCDWorker::initialize()
{
}

int SMCDWorker::listen(entity_addr_t &sa,
                       unsigned addr_slot,
                       const SocketOptions &opt,
                       ServerSocket *sock) {
  int listen_sd = net.create_socket(sa.get_family(), true);

  if (listen_sd < 0) {
    return -ceph_sock_errno();
  }

  int r = net.set_nonblock(listen_sd);
  if (r < 0) {
    ::close(listen_sd);
    return -ceph_sock_errno();
  }

  r = net.set_socket_options(listen_sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(listen_sd);
    return -ceph_sock_errno();
  }

  r = ::bind(listen_sd, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (r < 0) {
    r = -ceph_sock_errno();
    ldout(cct, 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, cct->_conf->ms_tcp_listen_backlog);
  if (r < 0) {
    r = -ceph_sock_errno();
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(
          std::unique_ptr<SMCDServerSocketImpl>(
          new SMCDServerSocketImpl(net, listen_sd, sa, addr_slot)));
  return 0;
}

int SMCDWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  int sd;

  if (opts.nonblock) {
    sd = net.nonblock_connect(addr, opts.connect_bind_addr);
  } else {
    sd = net.connect(addr, opts.connect_bind_addr);
  }

  if (sd < 0) {
    return -ceph_sock_errno();
  }

  net.set_priority(sd, opts.priority, addr.get_family());
  *socket = ConnectedSocket(
      std::unique_ptr<SMCDConnectedSocketImpl>(new SMCDConnectedSocketImpl(net, addr, sd, !opts.nonblock)));
  return 0;
}

SMCDStack::SMCDStack(CephContext *c)
    : NetworkStack(c)
{
}
