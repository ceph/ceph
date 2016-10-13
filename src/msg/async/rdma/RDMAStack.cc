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

#include "include/str_list.h"
#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "RDMAStack "

static Infiniband* global_infiniband;

RDMAWorker::RDMAWorker(CephContext *c, unsigned i)
  : Worker(c, i), stack(nullptr), infiniband(NULL),
    tx_handler(new C_handle_cq_tx(this)), memory_manager(NULL)
{}

int RDMAWorker::listen(entity_addr_t &sa, const SocketOptions &opt,ServerSocket *sock)
{
  auto p = new RDMAServerSocketImpl(cct, infiniband, get_stack()->get_dispatcher(), this, sa);
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
  RDMAConnectedSocketImpl* p = new RDMAConnectedSocketImpl(cct, infiniband, get_stack()->get_dispatcher(), this);
  entity_addr_t sa;
  memcpy(&sa, &addr, sizeof(addr));

  IBSYNMsg msg = p->get_my_msg();
  ldout(cct, 20) << __func__ << " connecting to " << sa.get_sockaddr() << " : " << sa.get_port() << dendl;
  ldout(cct, 20) << __func__ << " my syn msg :  < " << msg.qpn << ", " << msg.psn <<  ", " << msg.lid << ">"<< dendl;

  int client_setup_socket = ::socket(PF_INET, SOCK_DGRAM, 0);
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
  ::close(client_setup_socket);

  return 0;
}

RDMAStack::RDMAStack(CephContext *cct, const string &t): NetworkStack(cct, t)
{
  if (!global_infiniband)
    global_infiniband = new Infiniband(
        cct, cct->_conf->ms_async_rdma_device_name);
  dispatcher = new RDMADispatcher(cct, global_infiniband);
}

void RDMAWorker::initialize()
{
  infiniband = global_infiniband;
  tx_cc = infiniband->create_comp_channel();
  tx_cq = infiniband->create_comp_queue(tx_cc);
  center.create_file_event(tx_cc->get_fd(), EVENT_READABLE, tx_handler);
  memory_manager = infiniband->get_memory_manager();
}

int RDMAWorker::reserve_message_buffer(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes)
{
  int r = infiniband->get_tx_buffers(c, bytes);
  if (r == 0) {
    stack->get_dispatcher()->inflight += c.size();
    return r;
  }

  if (pending_sent_conns.back() != o)
    pending_sent_conns.push_back(o);
  return 0;
}

/**
 * Add the given Chunks to the given free queue.
 *
 * \param[in] chunks
 *      The Chunks to enqueue.
 * \return
 *      0 if success or -1 for failure
 */
int RDMAWorker::post_tx_buffer(std::vector<Chunk*> &chunks)
{
  stack->get_dispatcher()->inflight -= chunks.size();
  infiniband->get_memory_manager()->return_tx(chunks);
  while (!pending_sent_conns.empty()) {
    RDMAConnectedSocketImpl *o = pending_sent_conns.front();
    ssize_t r = o->submit(false);
    ldout(cct, 20) << __func__ << " sent pending bl socket=" << o << " r=" << r << dendl;
    if (r < 0) {
      if (r == -EAGAIN)
        break;
      o->fault();
    }
    pending_sent_conns.pop_front();
  }
  return 0;
}

void RDMAWorker::handle_tx_event()
{
  ldout(cct, 20) << __func__ << dendl;
  if (!tx_cc->get_cq_event())
    return ;

  static const int MAX_COMPLETIONS = 16;
  ibv_wc wc[MAX_COMPLETIONS];
  std::vector<Chunk*> tx_chunks;
  tx_chunks.reserve(MAX_COMPLETIONS);

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
      RDMAConnectedSocketImpl *conn = stack->get_dispatcher()->get_conn_by_qp(response->qp_num);
      if (conn) {
        conn->fault();
      } else {
        ldout(cct, 0) << __func__ << " missing qp_num=" << response->qp_num << " discard event" << dendl;
      }
    }

    assert(memory_manager->is_tx_chunk(chunk));
    tx_chunks.push_back(chunk);
  }

  if (n) {
    post_tx_buffer(tx_chunks);
    tx_chunks.clear();
    goto again;
  }

  if (!rearmed) {
    tx_cq->rearm_notify();
    rearmed = true;
    // Clean up cq events after rearm notify ensure no new incoming event
    // arrived between polling and rearm
    goto again;
  }

  ldout(cct, 20) << __func__ << " leaving handle_tx_event. " << dendl;
}

RDMADispatcher::~RDMADispatcher()
{
  done = true;
  t.join();
  assert(qp_conns.empty());
  while (!dead_queue_pairs.empty()) {
    delete dead_queue_pairs.back();
    dead_queue_pairs.pop_back();
  }

  rx_cc->ack_events();
  delete rx_cq;
  delete rx_cc;
  delete async_handler;
}

void RDMADispatcher::handle_async_event()
{
  ldout(cct, 20) << __func__ << dendl;
  while (1) {
    ibv_async_event async_event;
    if (ibv_get_async_event(ib->get_device()->ctxt, &async_event)) {
      if (errno != EAGAIN)
       lderr(cct) << __func__ << " ibv_get_async_event failed. (errno=" << errno
                  << " " << cpp_strerror(errno) << ")" << dendl;
      return;
    }
    // FIXME: Currently we must ensure no other factor make QP in ERROR state,
    // otherwise this qp can't be deleted in current cleanup flow.
    if (async_event.event_type == IBV_EVENT_QP_LAST_WQE_REACHED) {
      uint64_t qpn = async_event.element.qp->qp_num;
      ldout(cct, 10) << __func__ << " event associated qp=" << async_event.element.qp
                     << " evt: " << ibv_event_type_str(async_event.event_type) << dendl;
      RDMAConnectedSocketImpl *conn = get_conn_by_qp(qpn);
      if (!conn) {
        ldout(cct, 1) << __func__ << " missing qp_num=" << qpn << " discard event" << dendl;
      } else {
        ldout(cct, 0) << __func__ << " it's not forwardly stopped by us, reenable=" << conn << dendl;
        conn->fault();
        erase_qpn(qpn);
      }
    } else {
      ldout(cct, 0) << __func__ << " ibv_get_async_event: dev=" << ib->get_device()->ctxt
                    << " evt: " << ibv_event_type_str(async_event.event_type)
                    << dendl;
    }
    ibv_ack_async_event(&async_event);
  }
}


void RDMADispatcher::polling()
{
  static int MAX_COMPLETIONS = 32;
  ibv_wc wc[MAX_COMPLETIONS];

  std::map<RDMAConnectedSocketImpl*, std::vector<ibv_wc> > polled;
  int i, n;
  while (!done) {
    n = rx_cq->poll_cq(MAX_COMPLETIONS, wc);
    if (!n) {
      // NOTE: Has TX just transitioned to idle? We should do it when idle!
      // It's now safe to delete queue pairs (see comment by declaration
      // for dead_queue_pairs).
      // Additionally, don't delete qp while outstanding_buffers isn't empty,
      // because we need to check qp's state before sending
      if (!inflight.load()) {
        while (!dead_queue_pairs.empty()) {
          ldout(cct, 10) << __func__ << " finally delete qp=" << dead_queue_pairs.back() << dendl;
          delete dead_queue_pairs.back();
          dead_queue_pairs.pop_back();
        }
      }
      handle_async_event();
      continue;
    }

    ldout(cct, 20) << __func__ << " pool completion queue got " << n
                   << " responses."<< dendl;
    for (i = 0; i < n; ++i) {
      ibv_wc* response = &wc[i];
      Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
      ldout(cct, 20) << __func__ << " got chunk=" << response->wr_id << " qp:" << wc[i].qp_num << dendl;

      if (response->status != IBV_WC_SUCCESS) {
        lderr(cct) << __func__ << " work request returned error for buffer(" << response->wr_id
                   << ") status(" << response->status << ":"
                   << ib->wc_status_to_string(response->status) << dendl;
        ib->post_chunk(chunk);
        continue;
      }

      RDMAConnectedSocketImpl *conn = get_conn_by_qp(response->qp_num);
      if (!conn) {
        // discard buffer
        ldout(cct, 0) << __func__ << " missing qp_num " << response->qp_num << ", discard bd "
                      << chunk << dendl;
        continue;
      }
      polled[conn].push_back(*response);
    }
    for (auto &&i : polled)
      i.first->pass_wc(std::move(i.second));
    polled.clear();
  }
}
