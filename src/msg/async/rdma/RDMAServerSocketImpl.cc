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

#include "msg/async/net_handler.h"
#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAServerSocketImpl "

int RDMAServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
  server_setup_socket = ::socket(sa.get_family(), SOCK_DGRAM, 0);
  if (server_setup_socket == -1) {
    lderr(cct) << __func__ << " failed to create server socket: "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  int on = 1;
  int rc = ::setsockopt(server_setup_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (rc < 0) {
    lderr(cct) << __func__ << " unable to setsockopt: " << cpp_strerror(errno) << dendl;
    goto err;
  }

  rc = ::bind(server_setup_socket, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (rc < 0) {
    lderr(cct) << __func__ << " unable to bind to " << sa.get_sockaddr()
               << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  rc = net.set_nonblock(server_setup_socket);
  if (rc < 0) {
    goto err;
  }

  net.set_close_on_exec(server_setup_socket);

  ldout(cct, 20) << __func__ << " bind to " << sa.get_sockaddr() << " on port " << sa.get_port()  << dendl;
  return 0;

err:
  ::close(server_setup_socket);
  server_setup_socket = -1;
  return -1;
}

int RDMAServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opts, entity_addr_t *out)
{
  ldout(cct, 15) << __func__ << dendl;
  int r;
  RDMAConnectedSocketImpl* server;
  while (1) {
    IBSYNMsg msg;//TODO
    entity_addr_t addr;
    r = infiniband->recv_udp_msg(server_setup_socket, msg, &addr);
    if (r < 0) {
      r = -errno;
      if (r != -EAGAIN)
        ldout(cct, 10) << __func__ << " recv msg failed:" << cpp_strerror(errno)<< dendl;
      break;
    } else if (r > 0) {
      ldout(cct, 1) << __func__ << " recv msg not whole." << dendl;
      continue;
    } else {
      server = new RDMAConnectedSocketImpl(cct, infiniband, dispatcher, worker, msg);
      msg = server->get_my_msg();
      r = infiniband->send_udp_msg(server_setup_socket, msg, addr);
      server->activate();
      std::unique_ptr<RDMAConnectedSocketImpl> csi(server);
      *sock = ConnectedSocket(std::move(csi));
      if(out)
        *out = sa;
      return r;
    }
  }

  return r;
}
