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
    tx_handler(new C_handle_cq_tx(this)), memory_manager(NULL), lock("RDMAWorker::lock"), pended(false)
{
}

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
  int r = p->try_connect(addr, opts);

  if (r < 0) {
    ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
    return r;
  }
  std::unique_ptr<RDMAConnectedSocketImpl> csi(p);
  *socket = ConnectedSocket(std::move(csi));
  return 0;
}


RDMAStack::RDMAStack(CephContext *cct, const string &t): NetworkStack(cct, t)
{
  if (!global_infiniband)
    global_infiniband = new Infiniband(
      cct, cct->_conf->ms_async_rdma_device_name, cct->_conf->ms_async_rdma_port_num);
  ldout(cct, 20) << __func__ << " constructing RDMAStack..." << dendl;
  dispatcher = new RDMADispatcher(cct, global_infiniband, this);
  unsigned num = get_num_worker();
  for (unsigned i = 0; i < num; ++i) {
    RDMAWorker* w = dynamic_cast<RDMAWorker*>(get_worker(i));
    w->set_ib(global_infiniband);
    w->set_stack(this);
  }
  ldout(cct, 20) << " creating RDMAStack:" << this << " with dispatcher:" << dispatcher << dendl;
}

void RDMAWorker::initialize()
{
  if (!dispatcher) {
    dispatcher = stack->get_dispatcher();
    notify_fd = dispatcher->register_worker(this);
    center.create_file_event(notify_fd, EVENT_READABLE, tx_handler);
    memory_manager = infiniband->get_memory_manager();
  }
}

int RDMAWorker::reserve_message_buffer(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes)
{
  int r = infiniband->get_tx_buffers(c, bytes);
  if (r > 0) {
    stack->get_dispatcher()->inflight += c.size();
    ldout(cct, 30) << __func__ << " reserve " << c.size() << " chunks, inflight " << stack->get_dispatcher()->inflight << dendl;
    return r;
  }
  assert(r == 0);

  if (pending_sent_conns.back() != o)
    pending_sent_conns.push_back(o);
  dispatcher->pending_buffers(this);
  return r;
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
  if (chunks.empty())
    return 0;

  stack->get_dispatcher()->inflight -= chunks.size();
  memory_manager->return_tx(chunks);
  ldout(cct, 30) << __func__ << " release " << chunks.size() << " chunks, inflight " << stack->get_dispatcher()->inflight << dendl;

  pended = false;
  std::set<RDMAConnectedSocketImpl*> done;
  while (!pending_sent_conns.empty()) {
    RDMAConnectedSocketImpl *o = pending_sent_conns.front();
    if (done.count(o) == 0) {
      done.insert(o);
    } else {
      pending_sent_conns.pop_front();
      continue;
    }
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
  std::vector<Chunk*> tx_chunks;
  std::vector<ibv_wc> cqe;
  get_wc(cqe);

  for (size_t i = 0; i < cqe.size(); ++i) {
    ibv_wc* response = &cqe[i];
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    ldout(cct, 25) << __func__ << " QP: " << response->qp_num << " len: " << response->byte_len << " , addr:" << chunk << " " << infiniband->wc_status_to_string(response->status) << dendl;

    if (response->status != IBV_WC_SUCCESS) {
      if (response->status == IBV_WC_RETRY_EXC_ERR) {
        ldout(cct, 1) << __func__ << " connection between server and client not working. Disconnect this now" << dendl;
      } else if (response->status == IBV_WC_WR_FLUSH_ERR) {
        ldout(cct, 1) << __func__ << " Work Request Flushed Error: this connection's qp="
                      << response->qp_num << " should be down while this WR=" << response->wr_id
                      << " still in flight." << dendl;
      } else {
        ldout(cct, 1) << __func__ << " send work request returned error for buffer("
                      << response->wr_id << ") status(" << response->status << "): "
                      << infiniband->wc_status_to_string(response->status) << dendl;
      }
      RDMAConnectedSocketImpl *conn = stack->get_dispatcher()->get_conn_by_qp(response->qp_num);
      if (conn) {
        ldout(cct, 25) << __func__ << " qp state is : " << conn->get_qp_state() << dendl;//wangzhi
        conn->fault();
      } else {
        ldout(cct, 1) << __func__ << " missing qp_num=" << response->qp_num << " discard event" << dendl;
      }
    }

    //assert(memory_manager->is_tx_chunk(chunk));
    if (memory_manager->is_tx_chunk(chunk)) {
      tx_chunks.push_back(chunk);
    } else {
      ldout(cct, 1) << __func__ << " a outter chunk: " << chunk << dendl;//fin
    }
  }

  post_tx_buffer(tx_chunks);

  ldout(cct, 20) << __func__ << " give back " << tx_chunks.size() << " in Worker " << this << dendl;
  dispatcher->notify_pending_workers();
}

RDMADispatcher::~RDMADispatcher()
{
  done = true;
  t.join();
  ldout(cct, 20) << __func__ << " ing..." << dendl;
  auto i = qp_conns.begin();
  while (i != qp_conns.end()) {
    delete i->second.first;
    ++i;
  }

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
        ldout(cct, 1) << __func__ << " it's not forwardly stopped by us, reenable=" << conn << dendl;
        conn->fault();
        erase_qpn(qpn);
      }
    } else {
      ldout(cct, 1) << __func__ << " ibv_get_async_event: dev=" << ib->get_device()->ctxt
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
  std::vector<ibv_wc> tx_cqe;
  RDMAWorker* worker;
  ldout(cct, 20) << __func__ << " going to poll rx cq:" << rx_cq << dendl;
  RDMAConnectedSocketImpl *conn = nullptr;

  while (true) {
    int n = rx_cq->poll_cq(MAX_COMPLETIONS, wc);
    if (!n) {
      // NOTE: Has TX just transitioned to idle? We should do it when idle!
      // It's now safe to delete queue pairs (see comment by declaration
      // for dead_queue_pairs).
      // Additionally, don't delete qp while outstanding_buffers isn't empty,
      // because we need to check qp's state before sending
      if (!inflight.load()) {
        Mutex::Locker l(lock); // FIXME reuse dead qp because creating one qp costs 1 ms
        while (!dead_queue_pairs.empty()) {
          ldout(cct, 10) << __func__ << " finally delete qp=" << dead_queue_pairs.back() << dendl;
          delete dead_queue_pairs.back();
          dead_queue_pairs.pop_back();
        }
      }
      // handle_async_event();
      if (done && !inflight)
        break;
      continue;
    }

    ldout(cct, 20) << __func__ << " pool completion queue got " << n
                   << " responses."<< dendl;
    Mutex::Locker l(lock);//make sure connected socket alive when pass wc
    for (int i = 0; i < n; ++i) {
      ibv_wc* response = &wc[i];
      Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);

      if (response->status != IBV_WC_SUCCESS) {
        ldout(cct, 1) << __func__ << " work request returned error for buffer(" << chunk
                      << ") status(" << response->status << ":"
                      << ib->wc_status_to_string(response->status) << dendl;
        ib->recall_chunk(chunk);
        conn = get_conn_lockless(response->qp_num);
        if (conn && conn->is_connected())
          conn->fault();
        notify_pending_workers();
        continue;
      }

      if (wc[i].opcode == IBV_WC_SEND) {
        tx_cqe.push_back(wc[i]);
        ldout(cct, 25) << " got a tx cqe, bytes:" << wc[i].byte_len << dendl; 
        continue;
      }
      ldout(cct, 25) << __func__ << " got chunk=" << chunk << " bytes:" << response->byte_len << " opcode:" << response->opcode << dendl;
      conn = get_conn_lockless(response->qp_num);
      if (!conn) {
        int ret = ib->recall_chunk(chunk);
        ldout(cct, 1) << __func__ << " csi with qpn " << response->qp_num << " may be dead. chunk " << chunk << " will be back ? " << ret << dendl;
        continue;
      }
      polled[conn].push_back(*response);
    }
    for (auto &&i : polled)
      i.first->pass_wc(std::move(i.second));
    polled.clear();
    if (!tx_cqe.empty()) {
      worker = get_worker_from_list();
      if (worker == nullptr)
        worker = dynamic_cast<RDMAWorker*>(stack->get_worker());
      worker->pass_wc(std::move(tx_cqe));
      tx_cqe.clear();
    }
  }
}

void RDMADispatcher::notify_pending_workers() {
    Mutex::Locker l(w_lock);
    if (pending_workers.empty())
      return ;
    pending_workers.front()->pass_wc(std::move(vector<ibv_wc>()));
    pending_workers.pop_front();
}
