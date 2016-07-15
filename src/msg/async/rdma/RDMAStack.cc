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

#include "RDMAStack.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "RDMAStack "

static Infiniband* global_infiniband;

int RDMAWorker::listen(entity_addr_t &sa, const SocketOptions &opt,ServerSocket *sock)
{
  auto p = new RDMAServerSocketImpl(cct, infiniband, sa);
  int r = p->listen(sa, opt);
  if (r < 0) {
    delete p;
    return r;
  }

  *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
  return 0;
}

int RDMAWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket)
{
  RDMAConnectedSocketImpl* p = new RDMAConnectedSocketImpl(cct, infiniband, this);
  entity_addr_t sa;
  memcpy(&sa, &addr, sizeof(addr));

  IBSYNMsg msg = p->get_my_msg();
  ldout(cct, 20) << __func__ << " connecting to " << sa.get_sockaddr() << " : " << sa.get_port() << dendl;
  ldout(cct, 20) << __func__ << " my syn msg :  < " << msg.qpn << ", " << msg.psn <<  ", " << msg.lid << ">"<< dendl;

  client_setup_socket = ::socket(PF_INET, SOCK_DGRAM, 0);
  if (client_setup_socket == -1) {
    lderr(cct) << __func__ << " failed to create client socket: " << strerror(errno) << dendl;
    return -errno;
  }

  int r = ::connect(client_setup_socket, addr.get_sockaddr(), addr.get_sockaddr_len());
  if (r < 0) {
    lderr(cct) << __func__ << " failed to connect " << addr << ": "
               << strerror(errno) << dendl;
    return -errno;
  }

  r = infiniband->send_udp_msg(client_setup_socket, msg, sa);
  if (r < 0) {
    ldout(cct, 0) << __func__ << " send msg failed." << dendl;
    return r;
  }

  // FIXME: need to make this async
  r = infiniband->recv_udp_msg(client_setup_socket, msg, &sa);
  if (r < 0) {
    ldout(cct, 0) << __func__ << " recv msg failed." << dendl;
    return r;
  }
  p->set_peer_msg(msg);
  ldout(cct, 20) << __func__ << " peer msg :  < " << msg.qpn << ", " << msg.psn <<  ", " << msg.lid << "> " << dendl;
  r = p->activate();
  assert(!r);
  std::unique_ptr<RDMAConnectedSocketImpl> csi(p);
  *socket = ConnectedSocket(std::move(csi));

  return 0;
}

RDMAStack::RDMAStack(CephContext *cct, const string &t): NetworkStack(cct, t)
{
  if (!global_infiniband)
    global_infiniband = new Infiniband(
        this, cct, cct->_conf->ms_async_rdma_device_name);
}

void RDMAWorker::initialize()
{
  infiniband = global_infiniband;
  tx_cc = infiniband->create_comp_channel();
  tx_cq = infiniband->create_comp_queue(tx_cc);
  center.create_file_event(tx_cc->get_fd(), EVENT_READABLE, tx_handler);
  memory_manager = infiniband->get_memory_manager();
}

void RDMAWorker::handle_tx_event()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!tx_cc->get_cq_event())
    return ;

  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];

  bool rearmed = false;
  int n;
 again:
  n = tx_cq->poll_cq(MAX_COMPLETIONS, wc);
  ldout(cct, 20) << __func__ << " pool completion queue got " << n
                 << " responses."<< dendl;
  for (int i = 0; i < n; ++i) {
    ibv_wc* response = &wc[i];
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    ldout(cct, 20) << __func__ << " opcode: " << response->opcode << " len: " << response->byte_len << dendl;

    if (response->status != IBV_WC_SUCCESS) {
      if (response->status == IBV_WC_RETRY_EXC_ERR) {
        lderr(cct) << __func__ << " connection between server and client not working. Disconnect this now" << dendl;
      } else if (response->status == IBV_WC_WR_FLUSH_ERR) {
        lderr(cct) << __func__ << " Work Request Flushed Error: this connection's qp="
                               << response->qp_num << " should be down while this WR=" << response->wr_id
                               << " still in flight." << dendl;
      } else {
        lderr(cct) << __func__ << " send work request returned error for buffer("
                               << response->wr_id << ") status(" << response->status << "): "
                               << infiniband->wc_status_to_string(response->status) << dendl;
      }
      assert(0);
    }

    if (memory_manager->is_tx_chunk(chunk))
      infiniband->get_memory_manager()->return_tx(chunk);
    else
      ldout(cct, 20) << __func__ << " chunk belongs to none " << dendl;
  }

  if (n)
    goto again;

  if (!rearmed) {
    tx_cq->rearm_notify();
    rearmed = true;
    // Clean up cq events after rearm notify ensure no new incoming event
    // arrived between polling and rearm
    goto again;
  }
  ldout(cct, 20) << __func__ << " leaving handle_tx_event. " << dendl;
}
