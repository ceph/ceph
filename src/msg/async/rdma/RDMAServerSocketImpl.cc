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

#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAServerSocketImpl "

RDMAServerSocketImpl::RDMAServerSocketImpl(
  CephContext *cct, Infiniband* i, RDMADispatcher *s, RDMAWorker *w,
  entity_addr_t& a, unsigned slot)
  : ServerSocketImpl(a.get_type(), slot),
    cct(cct), net(cct), server_setup_socket(-1), infiniband(i),
    dispatcher(s), worker(w), sa(a)
{
}

int RDMAServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
  int rc = 0;
  server_setup_socket = net.create_socket(sa.get_family(), true);
  if (server_setup_socket < 0) {
    rc = -errno;
    lderr(cct) << __func__ << " failed to create server socket: "
               << cpp_strerror(errno) << dendl;
    return rc;
  }

  rc = net.set_nonblock(server_setup_socket);
  if (rc < 0) {
    goto err;
  }

  rc = net.set_socket_options(server_setup_socket, opt.nodelay, opt.rcbuf_size);
  if (rc < 0) {
    goto err;
  }

  rc = ::bind(server_setup_socket, sa.get_sockaddr(), sa.get_sockaddr_len());
  if (rc < 0) {
    rc = -errno;
    ldout(cct, 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  rc = ::listen(server_setup_socket, cct->_conf->ms_tcp_listen_backlog);
  if (rc < 0) {
    rc = -errno;
    lderr(cct) << __func__ << " unable to listen on " << sa << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }

  ldout(cct, 20) << __func__ << " bind to " << sa.get_sockaddr() << " on port " << sa.get_port()  << dendl;
  return 0;

err:
  ::close(server_setup_socket);
  server_setup_socket = -1;
  return rc;
}

int RDMAServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w)
{
  ldout(cct, 15) << __func__ << dendl;

  ceph_assert(sock);

  sockaddr_storage ss;
  socklen_t slen = sizeof(ss);
  int sd = accept_cloexec(server_setup_socket, (sockaddr*)&ss, &slen);
  if (sd < 0) {
    return -errno;
  }

  int r = net.set_nonblock(sd);
  if (r < 0) {
    ::close(sd);
    return -errno;
  }

  r = net.set_socket_options(sd, opt.nodelay, opt.rcbuf_size);
  if (r < 0) {
    ::close(sd);
    return -errno;
  }

  ceph_assert(NULL != out); //out should not be NULL in accept connection

  out->set_type(addr_type);
  out->set_sockaddr((sockaddr*)&ss);
  net.set_priority(sd, opt.priority, out->get_family());

  RDMAConnectedSocketImpl* server;
  //Worker* w = dispatcher->get_stack()->get_worker();
  server = new RDMAConnectedSocketImpl(cct, infiniband, dispatcher, dynamic_cast<RDMAWorker*>(w));
  server->set_accept_fd(sd);
  ldout(cct, 20) << __func__ << " accepted a new QP, tcp_fd: " << sd << dendl;
  std::unique_ptr<RDMAConnectedSocketImpl> csi(server);
  *sock = ConnectedSocket(std::move(csi));

  return 0;
}

void RDMAServerSocketImpl::abort_accept()
{
  if (server_setup_socket >= 0)
    ::close(server_setup_socket);
}
