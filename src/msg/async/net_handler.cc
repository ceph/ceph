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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "net_handler.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "NetHandler "

namespace ceph{

int NetHandler::create_socket(int domain, bool reuse_addr)
{
  int s;
  int r = 0;

  if ((s = socket_cloexec(domain, SOCK_STREAM, 0)) == -1) {
    r = errno;
    lderr(cct) << __func__ << " couldn't create socket " << cpp_strerror(r) << dendl;
    return -r;
  }

#if !defined(__FreeBSD__)
  /* Make sure connection-intensive things like the benchmark
   * will be able to close/open sockets a zillion of times */
  if (reuse_addr) {
    int on = 1;
    if (::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (SOCKOPT_VAL_TYPE)&on, sizeof(on)) == -1) {
      r = errno;
      lderr(cct) << __func__ << " setsockopt SO_REUSEADDR failed: "
                 << strerror(r) << dendl;
      close(s);
      return -r;
    }
  }
#endif

  return s;
}

int NetHandler::set_nonblock(int sd)
{
  int flags;
  int r = 0;

  #ifdef _WIN32
  ULONG mode = 1;
  r = ioctlsocket(sd, FIONBIO, &mode);
  if (r) {
    lderr(cct) << __func__ << " ioctlsocket(FIONBIO) failed: " << r << dendl;
    return -r;
  }
  #else
  /* Set the socket nonblocking.
   * Note that fcntl(2) for F_GETFL and F_SETFL can't be
   * interrupted by a signal. */
  if ((flags = fcntl(sd, F_GETFL)) < 0 ) {
    r = errno;
    lderr(cct) << __func__ << " fcntl(F_GETFL) failed: " << cpp_strerror(r) << dendl;
    return -r;
  }
  if (fcntl(sd, F_SETFL, flags | O_NONBLOCK) < 0) {
    r = errno;
    lderr(cct) << __func__ << " fcntl(F_SETFL,O_NONBLOCK): " << cpp_strerror(r) << dendl;
    return -r;
  }
  #endif

  return 0;
}

int NetHandler::set_socket_options(int sd, bool nodelay, int size)
{
  int r = 0;
  // disable Nagle algorithm?
  if (nodelay) {
    int flag = 1;
    r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (SOCKOPT_VAL_TYPE)&flag, sizeof(flag));
    if (r < 0) {
      r = errno;
      ldout(cct, 0) << "couldn't set TCP_NODELAY: " << cpp_strerror(r) << dendl;
    }
  }
  if (size) {
    r = ::setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (SOCKOPT_VAL_TYPE)&size, sizeof(size));
    if (r < 0)  {
      r = errno;
      ldout(cct, 0) << "couldn't set SO_RCVBUF to " << size << ": " << cpp_strerror(r) << dendl;
    }
  }

  // block ESIGPIPE
#ifdef CEPH_USE_SO_NOSIGPIPE
  int val = 1;
  r = ::setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (SOCKOPT_VAL_TYPE)&val, sizeof(val));
  if (r) {
    r = errno;
    ldout(cct,0) << "couldn't set SO_NOSIGPIPE: " << cpp_strerror(r) << dendl;
  }
#endif
  return -r;
}

void NetHandler::set_priority(int sd, int prio, int domain)
{
#ifdef SO_PRIORITY
  if (prio < 0) {
    return;
  }
  int r = -1;
#ifdef IPTOS_CLASS_CS6
  int iptos = IPTOS_CLASS_CS6;
  switch (domain) {
  case AF_INET:
    r = ::setsockopt(sd, IPPROTO_IP, IP_TOS, (SOCKOPT_VAL_TYPE)&iptos, sizeof(iptos));
    break;
  case AF_INET6:
    r = ::setsockopt(sd, IPPROTO_IPV6, IPV6_TCLASS, (SOCKOPT_VAL_TYPE)&iptos, sizeof(iptos));
    break;
  default:
    lderr(cct) << "couldn't set ToS of unknown family (" << domain << ")"
	       << " to " << iptos << dendl;
    return;
  }
  if (r < 0) {
    r = errno;
    ldout(cct,0) << "couldn't set TOS to " << iptos
		 << ": " << cpp_strerror(r) << dendl;
  }

#endif	// IPTOS_CLASS_CS6
  // setsockopt(IPTOS_CLASS_CS6) sets the priority of the socket as 0.
  // See http://goo.gl/QWhvsD and http://goo.gl/laTbjT
  // We need to call setsockopt(SO_PRIORITY) after it.
  r = ::setsockopt(sd, SOL_SOCKET, SO_PRIORITY, (SOCKOPT_VAL_TYPE)&prio, sizeof(prio));
  if (r < 0) {
    r = errno;
    ldout(cct, 0) << __func__ << " couldn't set SO_PRIORITY to " << prio
		  << ": " << cpp_strerror(r) << dendl;
  }
#else
  return;
#endif	// SO_PRIORITY
}

int NetHandler::generic_connect(const entity_addr_t& addr, const entity_addr_t &bind_addr, bool nonblock)
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

  set_socket_options(s, cct->_conf->ms_tcp_nodelay, cct->_conf->ms_tcp_rcvbuf);

  {
    entity_addr_t addr = bind_addr;
    if (cct->_conf->ms_bind_before_connect && (!addr.is_blank_ip())) {
      addr.set_port(0);
      ret = ::bind(s, addr.get_sockaddr(), addr.get_sockaddr_len());
      if (ret < 0) {
        ret = errno;
        ldout(cct, 2) << __func__ << " client bind error " << ", " << cpp_strerror(ret) << dendl;
        close(s);
        return -ret;
      }
    }
  }

  ret = ::connect(s, addr.get_sockaddr(), addr.get_sockaddr_len());
  if (ret < 0) {
    ret = errno;
    if (errno == EINPROGRESS && nonblock)
      return s;

    ldout(cct, 10) << __func__ << " connect: " << cpp_strerror(ret) << dendl;
    close(s);
    return -ret;
  }

  return s;
}

int NetHandler::reconnect(const entity_addr_t &addr, int sd)
{
  int r = 0;
  int ret = ::connect(sd, addr.get_sockaddr(), addr.get_sockaddr_len());

  if (ret < 0 && errno != EISCONN) {
    r = errno;
    ldout(cct, 10) << __func__ << " reconnect: " << strerror(r) << dendl;
    if (r == EINPROGRESS || r == EALREADY)
      return 1;
    return -r;
  }

  return 0;
}

int NetHandler::connect(const entity_addr_t &addr, const entity_addr_t& bind_addr)
{
  return generic_connect(addr, bind_addr, false);
}

int NetHandler::nonblock_connect(const entity_addr_t &addr, const entity_addr_t& bind_addr)
{
  return generic_connect(addr, bind_addr, true);
}


}
