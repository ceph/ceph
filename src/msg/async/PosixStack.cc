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
#include "include/sock_compat.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/simple_spin.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "PosixStack "

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  NetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;
#if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
  sigset_t sigpipe_mask;
  bool sigpipe_pending;
  bool sigpipe_unblock;
#endif

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

  virtual ssize_t zero_copy_read(bufferptr&) override {
    return -EOPNOTSUPP;
  }

  virtual ssize_t read(char *buf, size_t len) override {
    ssize_t r = ::read(_fd, buf, len);
    if (r < 0)
      r = -errno;
    return r;
  }

  /*
   SIGPIPE suppression - for platforms without SO_NOSIGPIPE or MSG_NOSIGNAL
    http://krokisplace.blogspot.in/2010/02/suppressing-sigpipe-in-library.html 
    http://www.microhowto.info/howto/ignore_sigpipe_without_affecting_other_threads_in_a_process.html 
  */
  static void suppress_sigpipe()
  {
  #if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
    /*
      We want to ignore possible SIGPIPE that we can generate on write.
      SIGPIPE is delivered *synchronously* and *only* to the thread
      doing the write.  So if it is reported as already pending (which
      means the thread blocks it), then we do nothing: if we generate
      SIGPIPE, it will be merged with the pending one (there's no
      queuing), and that suits us well.  If it is not pending, we block
      it in this thread (and we avoid changing signal action, because it
      is per-process).
    */
    sigset_t pending;
    sigemptyset(&pending);
    sigpending(&pending);
    sigpipe_pending = sigismember(&pending, SIGPIPE);
    if (!sigpipe_pending) {
      sigset_t blocked;
      sigemptyset(&blocked);
      pthread_sigmask(SIG_BLOCK, &sigpipe_mask, &blocked);

      /* Maybe is was blocked already?  */
      sigpipe_unblock = ! sigismember(&blocked, SIGPIPE);
    }
  #endif /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
  }

  static void restore_sigpipe()
  {
  #if !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE)
    /*
      If SIGPIPE was pending already we do nothing.  Otherwise, if it
      become pending (i.e., we generated it), then we sigwait() it (thus
      clearing pending status).  Then we unblock SIGPIPE, but only if it
      were us who blocked it.
    */
    if (!sigpipe_pending) {
      sigset_t pending;
      sigemptyset(&pending);
      sigpending(&pending);
      if (sigismember(&pending, SIGPIPE)) {
        /*
          Protect ourselves from a situation when SIGPIPE was sent
          by the user to the whole process, and was delivered to
          other thread before we had a chance to wait for it.
        */
        static const struct timespec nowait = { 0, 0 };
        TEMP_FAILURE_RETRY(sigtimedwait(&sigpipe_mask, NULL, &nowait));
      }

      if (sigpipe_unblock)
        pthread_sigmask(SIG_UNBLOCK, &sigpipe_mask, NULL);
    }
  #endif  /* !defined(MSG_NOSIGNAL) && !defined(SO_NOSIGPIPE) */
  }

  // return the sent length
  // < 0 means error occured
  static ssize_t do_sendmsg(int fd, struct msghdr &msg, unsigned len, bool more)
  {
    suppress_sigpipe();

    size_t sent = 0;
    while (1) {
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
    restore_sigpipe();
    return (ssize_t)sent;
  }

  virtual ssize_t send(bufferlist &bl, bool more) {
    size_t sent_bytes = 0;
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
      if (static_cast<unsigned>(r) < msglen)
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

    return static_cast<ssize_t>(sent_bytes);
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
  virtual int accept(ConnectedSocket *sock, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override {
    ::close(_fd);
  }
  virtual int fd() const override {
    return _fd;
  }
};

int PosixServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
  assert(sock);
  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = ::accept(_fd, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -errno;
  }

  handler.set_close_on_exec(sd);
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
  handler.set_priority(sd, opt.priority);

  std::unique_ptr<PosixConnectedSocketImpl> csi(new PosixConnectedSocketImpl(handler, *out, sd, true));
  *sock = ConnectedSocket(std::move(csi));
  if (out)
    out->set_sockaddr((sockaddr*)&ss);
  return 0;
}

void PosixWorker::initialize()
{
}

int PosixWorker::listen(entity_addr_t &sa, const SocketOptions &opt,
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

  net.set_close_on_exec(listen_sd);
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

  r = ::listen(listen_sd, 128);
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(r) << dendl;
    ::close(listen_sd);
    return r;
  }

  *sock = ServerSocket(
          std::unique_ptr<PosixServerSocketImpl>(
              new PosixServerSocketImpl(net, sa, listen_sd)));
  return 0;
}

int PosixWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
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

  net.set_priority(sd, opts.priority);
  *socket = ConnectedSocket(
      std::unique_ptr<PosixConnectedSocketImpl>(new PosixConnectedSocketImpl(net, addr, sd, !opts.nonblock)));
  return 0;
}

PosixNetworkStack::PosixNetworkStack(CephContext *c, const string &t)
    : NetworkStack(c, t)
{
  vector<string> corestrs;
  get_str_vec(cct->_conf->ms_async_affinity_cores, corestrs);
  for (auto & corestr : corestrs) {
    string err;
    int coreid = strict_strtol(corestr.c_str(), 10, &err);
    if (err == "")
      coreids.push_back(coreid);
    else
      lderr(cct) << __func__ << " failed to parse " << corestr << " in " << cct->_conf->ms_async_affinity_cores << dendl;
  }
}
