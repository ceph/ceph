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
#include <linux/errqueue.h>

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

#ifndef SO_EE_ORIGIN_ZEROCOPY
  #define SO_EE_ORIGIN_ZEROCOPY 5
#endif // SO_EE_ORIGIN_ZEROCOPY

#ifndef MSG_ZEROCOPY
  #define MSG_ZEROCOPY		0x4000000
#endif // MSG_ZEROCOPY


#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "PosixStack "

class PosixConnectedSocketImpl final : public ConnectedSocketImpl {
  const int MERGE_THRESHOLD = 128;
  const int ZEROCOPY_THRESHOLD = 4096;
  CephContext *cct;
  PosixNetworkStack *stack;
  ceph::NetHandler &handler;
  int _fd;
  entity_addr_t sa;
  bool connected;
  bool use_zerocopy;
  uint32_t send_id;
  std::mutex lock;
  uint64_t inflight_bytes;
  std::unordered_map<uint32_t, bufferlist> unacked_map;
 public:
  explicit PosixConnectedSocketImpl(CephContext *cct, PosixNetworkStack *stack,
              NetHandler &h, const entity_addr_t &sa, int f, bool connected)
	  : cct(cct), stack(stack), handler(h), _fd(f), sa(sa),
	    connected(connected), send_id(0), inflight_bytes(0) {
     bool zerocopy = cct->_conf.get_val<bool>("ms_async_tcp_zerocopy");
     if (zerocopy) {
       if (handler.set_zerocopy(_fd, 1))
	 zerocopy = false;
     }
     if (zerocopy) {
       stack->register_poll(_fd, this);
       struct epoll_event ee;
       ee.events = EPOLLET;
       ee.data.u64 = 0; /* avoid valgrind warning */
       ee.data.fd = _fd;
       if (epoll_ctl(stack->get_epfd(),EPOLL_CTL_ADD, _fd, &ee) == -1) {
	 lderr(cct) << __func__<< " epoll_ctl: add fd=" << _fd << " failed "
	            << cpp_strerror(errno) << dendl;
	 zerocopy = false;
       }
     }
     use_zerocopy = zerocopy;
  }

  void free_zerocpy_buffer(uint32_t lo, uint32_t hi)
  {
    std::unique_lock<std::mutex> locker(lock);
    for(uint32_t i = lo; i <=hi; i++) {
      bufferlist &bl = unacked_map[i];
      inflight_bytes -= bl.length();
      bl.clear();
      unacked_map.erase(i);
    }
    ldout(cct, 10) << __func__ << " " << this << " notify free " << lo << "-"
	           << hi << " send_id " << send_id << dendl;
  }

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
    #ifdef _WIN32
    ssize_t r = ::recv(_fd, buf, len, 0);
    #else
    ssize_t r = ::read(_fd, buf, len);
    #endif
    if (r < 0)
      r = -ceph_sock_errno();
    return r;
  }

  // return the sent length
  // < 0 means error occurred
  #ifndef _WIN32
  ssize_t do_sendmsg(bufferlist &zc_bl, int fd, struct msghdr &msg, unsigned len, bool more)
  {
    size_t sent = 0;
    bool zerocopy = !!zc_bl.length();

    while (1) {
      MSGR_SIGPIPE_STOPPER;
      int flags = MSG_NOSIGNAL | (more ? MSG_MORE : 0);
      if (zerocopy && (len - sent) > ZEROCOPY_THRESHOLD)
        flags |= MSG_ZEROCOPY;
      ssize_t r = ::sendmsg(fd, &msg, flags);
      if (r < 0) {
        int err = ceph_sock_errno();
        if (err == EINTR) {
          continue;
        } else if (err == EAGAIN) {
          break;
        }
        return -err;
      }
      if (flags & MSG_ZEROCOPY) {
        lock.lock();
        bufferlist sented;
        zc_bl.splice(0, r, &sented);
        unacked_map[send_id] = std::move(sented);
        send_id++;
        inflight_bytes += r;
        lock.unlock();
        ldout(cct,10) << __func__ << " msglen " << len << " sent " << sent
                      << " send_id " << send_id << " inflight_bytes "
                      << inflight_bytes << dendl;
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

  ceph::buffer::list prepare_zerocopy(bufferlist::buffers_t::const_iterator &pb, unsigned merged_len, struct msghdr &msg)
  {
    ceph::buffer::list zc_bl;

    ceph::buffer::ptr bufptr;
    char *merged_buf = nullptr;
    if (merged_len) {
      bufptr = ceph::buffer::ptr(ceph::buffer::create(merged_len));
      merged_buf = bufptr.c_str();
    }

    unsigned off = 0;
    for (auto iov = msg.msg_iov; iov != msg.msg_iov + msg.msg_iovlen; iov++, pb++) {
      if (pb->length() < MERGE_THRESHOLD && merged_buf) {
        char *data = merged_buf + off;
        ceph::buffer::ptr ptr(bufptr, off, pb->length());
        pb->copy_out(0, pb->length(), data);
        off += round_up_to(pb->length(), sizeof(size_t));
        zc_bl.append(std::move(ptr));
        iov->iov_base = data;
      } else {
        zc_bl.append(*pb);
      }
    }
    return zc_bl;
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
      unsigned merged_len = 0;
      int small_cnt = 0;
      auto pa = pb;
      for (auto iov = msgvec; iov != msgvec + size; iov++) {
	iov->iov_base = (void*)(pb->c_str());
	iov->iov_len = pb->length();
	msglen += pb->length();
	if(use_zerocopy && pb->length() < MERGE_THRESHOLD) {
	  small_cnt++;
	  merged_len += round_up_to(pb->length(), sizeof(size_t));
	}
	++pb;
      }
      ceph::buffer::list zc_bl;
      if (use_zerocopy && msglen > ZEROCOPY_THRESHOLD) {
        if (small_cnt < 2) {
	  merged_len = 0;
	}
        zc_bl = prepare_zerocopy(pa, merged_len, msg);
      }
      ssize_t r = do_sendmsg(zc_bl, _fd, msg, msglen, left_pbrs || more);
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
    if (use_zerocopy) {
      stack->unregister_poll(_fd, this);
      struct epoll_event ee;
      int r = 0;
      ee.events = 0;
      ee.data.u64 = 0;
      if ((r = epoll_ctl(stack->get_epfd(), EPOLL_CTL_DEL, _fd, &ee)) < 0) {
        lderr(cct) << __func__ << " epoll_ctl: delete fd=" << _fd
                   << " failed." << cpp_strerror(errno) << dendl;
      }

      lock.lock();
      for(auto &item : unacked_map) {
	item.second.clear();
      }
      unacked_map.clear();
      lock.unlock();
    }
    compat_closesocket(_fd);
  }
  int fd() const override {
    return _fd;
  }
  friend class PosixServerSocketImpl;
  friend class PosixNetworkStack;
};

class PosixServerSocketImpl : public ServerSocketImpl {
  CephContext *cct;
  PosixNetworkStack *stack;
  ceph::NetHandler &handler;
  int _fd;

 public:
  explicit PosixServerSocketImpl(CephContext *cct, PosixNetworkStack *stack,
                                 ceph::NetHandler &h, int f,
				 const entity_addr_t& listen_addr, unsigned slot)
    : ServerSocketImpl(listen_addr.get_type(), slot),
      cct(cct), stack(stack), handler(h), _fd(f) {}
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

  std::unique_ptr<PosixConnectedSocketImpl> csi(new PosixConnectedSocketImpl(cct, stack, handler, *out, sd, true));
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
          std::unique_ptr<PosixServerSocketImpl>(
	    new PosixServerSocketImpl(cct, stack, net, listen_sd, sa, addr_slot)));
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
    return -ceph_sock_errno();
  }

  net.set_priority(sd, opts.priority, addr.get_family());
  *socket = ConnectedSocket(
      std::unique_ptr<PosixConnectedSocketImpl>(new PosixConnectedSocketImpl(cct,
          stack, net, addr, sd, !opts.nonblock)));
  return 0;
}

PosixNetworkStack::PosixNetworkStack(CephContext *c)
    : NetworkStack(c), cct(c)
{
}

int PosixNetworkStack::polling(void)
{
  const int nevent = 1024;
  struct epoll_event *events = (struct epoll_event *)malloc(sizeof(struct epoll_event) * nevent);
  if (!events) {
    lderr(cct) << __func__ << " unable to malloc memory. " << dendl;
    return -ENOMEM;
  }
  memset(events, 0, sizeof(struct epoll_event) * nevent);

  epfd = epoll_create(nevent);
  if (epfd == -1) {
    lderr(cct) << __func__ << " unable to do epoll_create: " << cpp_strerror(errno) << dendl;
    return -errno;
  }
  if (::fcntl(epfd, F_SETFD, FD_CLOEXEC) == -1) {
    int e = errno;
    ::close(epfd);
    lderr(cct) << __func__ << " unable to set cloexec: " << cpp_strerror(e) << dendl;
    return -e;
  }

  while (!done) {
       int retval = epoll_wait(epfd, events, nevent, 50);
       if (retval <= 0)
	  continue;

       int numevents = retval;
       for (int i = 0; i< numevents; i++) {
         struct epoll_event *e = events +i;
         if (!(e->events & EPOLLERR))
           continue;

         char control[100];
         struct msghdr msg = {};
         msg.msg_control = control;
         msg.msg_controllen = sizeof(control);
         int ret = ::recvmsg(e->data.fd, &msg, MSG_ERRQUEUE);
         if (ret < 0)
             continue;
         struct sock_extended_err *serr;
         struct cmsghdr *cm;
         cm = CMSG_FIRSTHDR(&msg);
         if ((cm->cmsg_level == SOL_IP && cm->cmsg_type == IP_RECVERR) ||
             (cm->cmsg_level == SOL_IPV6 && cm->cmsg_type == IPV6_RECVERR)) {
             serr = (struct sock_extended_err *)CMSG_DATA(cm);
             if (serr->ee_errno == 0 && serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY) {
               uint32_t lo = serr->ee_info;
               uint32_t hi = serr->ee_data;
               std::unique_lock locker(lock);
               auto it = poll_conn.find(e->data.fd);
               if (it == poll_conn.end()) {
                 continue;
               }
               PosixConnectedSocketImpl *sock = static_cast<PosixConnectedSocketImpl *>(it->second);
               sock->free_zerocpy_buffer(lo,hi);
             }
         }
       }
  }
  ::close(epfd);
  return 0;
}

