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

#ifdef SO_NOSIGPIPE
void NetHandler::set_nosigpipe(int sd)
{
  int val = 1;
  int r = ::setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (void*)&val, sizeof(val));
  if (r) {
    r = -errno;
    ldout(cct,0) << "couldn't set SO_NOSIGPIPE: " << cpp_strerror(r) << dendl;
  }
}
#endif

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

#ifdef SO_NOSIGPIPE
  // block ESIGPIPE
  set_nosigpipe(sd);
#endif
}

void NetHandler::set_unix_socket_options(int sd)
{
  if (cct->_conf->ms_async_sndbuf) {
  // UNIX socket support only SO_SNDBUF.
    int size = cct->_conf->ms_async_sndbuf;
    int r = ::setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (void*)&size, sizeof(size));
    if (r < 0)  {
      r = -errno;
      ldout(cct, 0) << "couldn't set UNIX socket SO_SNDBUF to " << size << ": " << cpp_strerror(r) << dendl;
    }
  }
    
#ifdef SO_NOSIGPIPE
  // block ESIGPIPE
  set_nosigpipe(sd);
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

  if ((addr.get_family() == AF_INET) || (addr.get_family() == AF_INET6)) {
    set_socket_options(s);
  } else {
    set_unix_socket_options(s);
  }

  ret = ::connect(s, (sockaddr*)&addr.addr, addr.addr_size());
  if (ret < 0) {
    if (errno == EINPROGRESS && nonblock)
      return s;

    ldout(cct, 10) << __func__ << " connect to " << addr << " (" << addr.addr << "): " << strerror(errno) << dendl;
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

// converts ip addr to unix equivalent
// catch_all - whether it should try /var/run/ceph/ceph-mon.1aed when
// /var/run/ceph-mon.192.168.40.10.1aed does not exist (because listens on 0.0.0.0)
entity_addr_t NetHandler::ip_to_unix(const entity_addr_t &addr, int type, const string& path, bool catch_all)
{
  assert(addr.addr.ss_family == AF_INET);
  entity_addr_t r;
  r.type = addr.type;
  r.nonce = addr.nonce;
  r.addr.ss_family = AF_UNIX;
  if (addr.addr4.sin_addr.s_addr == 0) {
      // short name without IP
      snprintf(&r.addrun.sun_path[0], sizeof(r.addrun.sun_path), "%s/ceph-%s.%.4x", path.c_str(),
                ceph_entity_type_name(type), ntohs(addr.addr4.sin_port));
  } else {
      // long path with IP
      snprintf(&r.addrun.sun_path[0], sizeof(r.addrun.sun_path), "%s/ceph-%s.%d.%d.%d.%d.%.4x", path.c_str(),
                ceph_entity_type_name(type), (addr.addr4.sin_addr.s_addr & 0xFF), (addr.addr4.sin_addr.s_addr >> 8) & 0xFF,
                (addr.addr4.sin_addr.s_addr >> 16) & 0xFF, (addr.addr4.sin_addr.s_addr >> 24) & 0xFF,
                ntohs(addr.addr4.sin_port));
      if (catch_all) {
          // try something like /var/run/ceph/ceph-mon.1aed when server listens on 0.0.0.0
          struct stat statbuf;
          if (stat(r.addrun.sun_path, &statbuf) || (!S_ISSOCK(statbuf.st_mode)) ) {
              snprintf(&r.addrun.sun_path[0], sizeof(r.addrun.sun_path), "%s/ceph-%s.%.4x", path.c_str(),
                        ceph_entity_type_name(type), ntohs(addr.addr4.sin_port));
              // if that doesn't exist either, upstream (try_unixify) will handle it and
              // revert back to TCP socket
          }
      }
  }
  return r;
}

}
