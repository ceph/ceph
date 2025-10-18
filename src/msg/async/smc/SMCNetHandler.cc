// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * Copyright (C) IBM Corp. 2025
 *
 * Author: Aliaksei Makarau <aliaksei.makarau@ibm.com>
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

#include "common/debug.h"
#include "common/errno.h"
#include "common/dout.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#include "SMCNetHandler.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "SMCNetHandler "

#ifdef HAVE_SMC
  #ifndef SMCPROTO_SMC
    #define SMCPROTO_SMC           0       /* SMC protocol, IPv4 */
    #define SMCPROTO_SMC6          1       /* SMC protocol, IPv6 */
  #endif
#endif

namespace ceph {
namespace smc {

int SMCNetHandler::create_socket(int domain, bool reuse_addr) {
  int s;
  int r = 0;
  int protocol = IPPROTO_TCP;

#ifdef HAVE_SMC
  /* check if socket is eligible for AF_SMC */
  if (domain == AF_INET || domain == AF_INET6) {
    if (domain == AF_INET)
      protocol = SMCPROTO_SMC;
    else /* AF_INET6 */
      protocol = SMCPROTO_SMC6;
    domain = AF_SMC;
  }
#endif

  if ((s = socket_cloexec(domain, SOCK_STREAM, protocol)) == -1) {
    r = ceph_sock_errno();
    lderr(cct) << __func__ << " couldn't create socket " << cpp_strerror(r) << dendl;
    return -r;
  }

#if !defined(__FreeBSD__)
  /* Make sure connection-intensive things like the benchmark
   * will be able to close/open sockets a zillion of times */
  if (reuse_addr) {
    int on = 1;
    if (::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (SOCKOPT_VAL_TYPE)&on, sizeof(on)) == -1) {
      r = ceph_sock_errno();
      lderr(cct) << __func__ << " setsockopt SO_REUSEADDR failed: "
                 << strerror(r) << dendl;
      compat_closesocket(s);
      return -r;
    }
  }
#endif

  return s;
}

}
}
