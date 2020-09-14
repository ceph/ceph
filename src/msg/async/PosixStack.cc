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

#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include <algorithm>

#include "PosixStack.h"

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
#define dout_prefix *_dout << "PosixStack "

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  ceph::NetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;

 public:
  explicit PosixConnectedSocketImpl(ceph::NetHandler &h, const entity_addr_t &sa,
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
      r = -errno;
    return r;
  }

  // return the sent length
  // < 0 means error occurred
  #ifndef _WIN32
  static ssize_t do_sendmsg(int fd, struct msghdr &msg, unsigned len, bool more)
  {
    size_t sent = 0;
    while (1) {
      MSGR_SIGPIPE_STOPPER;
      ssize_t r;
      r = ::sendmsg(fd, &msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
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
  #else
  ssize_t send(bufferlist &bl, bool more) override
  {
    size_t total_sent_bytes = 0;
    auto pb = std::cbegin(bl.buffers());
    uint64_t left_pbrs = bl.get_num_buffers();
    while (left_pbrs) {
      WSABUF msgvec[IOV_MAX];
      uint64_t size = std::min<uint64_t>(left_pbrs, IOV_MAX);
      left_pbrs -= size;
      unsigned msglen = 0;
      for (auto iov = msgvec; iov != msgvec + size; iov++) {
        iov->buf = const_cast<char*>(pb->c_str());
        iov->len = pb->length();
        msglen += pb->length();
        ++pb;
      }
      DWORD sent_bytes = 0;
      DWORD flags = 0;
      if (more)
        flags |= MSG_PARTIAL;

      int ret_val = WSASend(_fd, msgvec, size, &sent_bytes, flags, NULL, NULL);
      if (ret_val)
        return -ret_val;

      total_sent_bytes += sent_bytes;
      if (static_cast<unsigned>(sent_bytes) < msglen)
        break;
    }

    if (total_sent_bytes) {
      bufferlist swapped;
      if (total_sent_bytes < bl.length()) {
        bl.splice(total_sent_bytes, bl.length()-total_sent_bytes, &swapped);
        bl.swap(swapped);
      } else {
        bl.clear();
      }
    }

    return static_cast<ssize_t>(total_sent_bytes);
  }
  #endif
  void shutdown() override {
    ::shutdown(_fd, SHUT_RDWR);
  }
  void close() override {
    ::close(_fd);
  }
  int fd() const override {
    return _fd;
  }
  friend class PosixServerSocketImpl;
  friend class PosixNetworkStack;
};

class PosixServerSocketImpl : public ServerSocketImpl {
  ceph::NetHandler &handler;
  int _fd;

 public:
  explicit PosixServerSocketImpl(ceph::NetHandler &h, int f,
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

int PosixServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
  ceph_assert(sock);
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = accept_cloexec(_fd, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -errno;
  }

  int r = handler.set_nonblock(sd);
  if (r < 0) {
    ::close(sd);
    return -errno;
  }

  r = handler.set_socket_options(sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(sd);
    return -errno;
  }

  ceph_assert(NULL != out); //out should not be NULL in accept connection

  out->set_type(addr_type);
  out->set_sockaddr((sockaddr*)&ss);
  handler.set_priority(sd, opt.priority, out->get_family());

  std::unique_ptr<PosixConnectedSocketImpl> csi(new PosixConnectedSocketImpl(handler, *out, sd, true));
  *sock = ConnectedSocket(std::move(csi));
  return 0;
}

void PosixWorker::initialize()
{
}

int PosixWorker::listen(entity_addr_t &sa,
			unsigned addr_slot,
			const SocketOptions &opt,
                        ServerSocket *sock)
{
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

  r = ::bind(listen_sd, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (r < 0) {
    r = -errno;
    ldout(cct, 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  r = ::listen(listen_sd, cct->_conf->ms_tcp_listen_backlog);
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(
          std::unique_ptr<PosixServerSocketImpl>(
	    new PosixServerSocketImpl(net, listen_sd, sa, addr_slot)));
  return 0;
}

int PosixWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  int sd;

  if (opts.nonblock) {
    sd = net.nonblock_connect(addr, opts.connect_bind_addr);
  } else {
    sd = net.connect(addr, opts.connect_bind_addr);
  }

  if (sd < 0) {
    return -errno;
  }

  net.set_priority(sd, opts.priority, addr.get_family());
  *socket = ConnectedSocket(
      std::unique_ptr<PosixConnectedSocketImpl>(new PosixConnectedSocketImpl(net, addr, sd, !opts.nonblock)));
  return 0;
}

PosixNetworkStack::PosixNetworkStack(CephContext *c, const std::string &t)
    : NetworkStack(c, t)
{
}
