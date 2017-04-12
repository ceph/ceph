// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
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
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "net_handler.h"
#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "NetHandler "

namespace ceph{

int NetHandler::create_socket(int domain, bool reuse_addr)
{
  int s, on = 1;

  if ((s = ::socket(domain, SOCK_STREAM, 0)) == -1) {
    lderr(cct) << __func__ << " couldn't created socket " << cpp_strerror(errno) << dendl;
    return -errno;
  }

  /* Make sure connection-intensive things like the benchmark
   * will be able to close/open sockets a zillion of times */
  if (reuse_addr) {
    if (::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
      lderr(cct) << __func__ << " setsockopt SO_REUSEADDR failed: "
                 << strerror(errno) << dendl;
      close(s);
      return -errno;
    }
  }

  return s;
}

int NetHandler::set_nonblock(int sd)
{
  int flags;

  /* Set the socket nonblocking.
   * Note that fcntl(2) for F_GETFL and F_SETFL can't be
   * interrupted by a signal. */
  if ((flags = fcntl(sd, F_GETFL)) < 0 ) {
    lderr(cct) << __func__ << " fcntl(F_GETFL) failed: " << strerror(errno) << dendl;
    return -errno;
  }
  if (fcntl(sd, F_SETFL, flags | O_NONBLOCK) < 0) {
    lderr(cct) << __func__ << " fcntl(F_SETFL,O_NONBLOCK): " << strerror(errno) << dendl;
    return -errno;
  }

  return 0;
}

void NetHandler::set_close_on_exec(int sd)
{
  int flags = fcntl(sd, F_GETFD, 0);
  if (flags < 0) {
    int r = errno;
    lderr(cct) << __func__ << " fcntl(F_GETFD): "
	       << cpp_strerror(r) << dendl;
    return;
  }
  if (fcntl(sd, F_SETFD, flags | FD_CLOEXEC)) {
    int r = errno;
    lderr(cct) << __func__ << " fcntl(F_SETFD): "
	       << cpp_strerror(r) << dendl;
  }
}

void NetHandler::set_socket_options(int sd)
{
  // disable Nagle algorithm?
  if (cct->_conf->ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) {
      r = -errno;
      ldout(cct, 0) << "couldn't set TCP_NODELAY: " << cpp_strerror(r) << dendl;
    }
  }
  if (cct->_conf->ms_tcp_rcvbuf) {
    int size = cct->_conf->ms_tcp_rcvbuf;
    int r = ::setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (void*)&size, sizeof(size));
    if (r < 0)  {
      r = -errno;
      ldout(cct, 0) << "couldn't set SO_RCVBUF to " << size << ": " << cpp_strerror(r) << dendl;
    }
  }

  // block ESIGPIPE
#ifdef SO_NOSIGPIPE
  int val = 1;
  int r = ::setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (void*)&val, sizeof(val));
  if (r) {
    r = -errno;
    ldout(cct,0) << "couldn't set SO_NOSIGPIPE: " << cpp_strerror(r) << dendl;
  }
#endif
}

int NetHandler::generic_connect(const entity_addr_t& addr, bool nonblock)
{
  int ret;
  int s = create_socket(addr.get_family());
  if (s < 0)
    return s;

  if (nonblock) {
    ret = set_nonblock(s);
    if (ret < 0) {
      close(s);
      return ret;
    }
  }

  set_socket_options(s);

  ret = ::connect(s, (sockaddr*)&addr.addr, addr.addr_size());
  if (ret < 0) {
    if (errno == EINPROGRESS && nonblock)
      return s;

    ldout(cct, 10) << __func__ << " connect: " << strerror(errno) << dendl;
    close(s);
    return -errno;
  }

  return s;
}

int NetHandler::reconnect(const entity_addr_t &addr, int sd)
{
  int ret = ::connect(sd, (sockaddr*)&addr.addr, addr.addr_size());

  if (ret < 0 && errno != EISCONN) {
    ldout(cct, 10) << __func__ << " reconnect: " << strerror(errno) << dendl;
    if (errno == EINPROGRESS || errno == EALREADY)
      return 1;
    return -errno;
  }

  return 0;
}

int NetHandler::connect(const entity_addr_t &addr)
{
  return generic_connect(addr, false);
}

int NetHandler::nonblock_connect(const entity_addr_t &addr)
{
  return generic_connect(addr, true);
}


}
