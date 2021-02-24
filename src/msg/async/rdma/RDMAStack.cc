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

#include <poll.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "include/str_list.h"
#include "include/compat.h"
#include "common/Cycles.h"
#include "common/deleter.h"
#include "common/Tub.h"
#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "RDMAStack "

RDMADispatcher::~RDMADispatcher()
{
  ldout(cct, 20) << __func__ << " destructing rdma dispatcher" << dendl;
  polling_stop();

  ceph_assert(qp_conns.empty());
  ceph_assert(num_qp_conn == 0);
  ceph_assert(dead_queue_pairs.empty());
}

RDMADispatcher::RDMADispatcher(CephContext* c, std::shared_ptr<Infiniband>& ib)
  : cct(c), ib(ib)
{
  PerfCountersBuilder plb(cct, "AsyncMessenger::RDMADispatcher", l_msgr_rdma_dispatcher_first, l_msgr_rdma_dispatcher_last);

  plb.add_u64_counter(l_msgr_rdma_polling, "polling", "Whether dispatcher thread is polling");
  plb.add_u64_counter(l_msgr_rdma_inflight_tx_chunks, "inflight_tx_chunks", "The number of inflight tx chunks");
  plb.add_u64_counter(l_msgr_rdma_rx_bufs_in_use, "rx_bufs_in_use", "The number of rx buffers that are holding data and being processed");
  plb.add_u64_counter(l_msgr_rdma_rx_bufs_total, "rx_bufs_total", "The total number of rx buffers");

  plb.add_u64_counter(l_msgr_rdma_tx_total_wc, "tx_total_wc", "The number of tx work comletions");
  plb.add_u64_counter(l_msgr_rdma_tx_total_wc_errors, "tx_total_wc_errors", "The number of tx errors");
  plb.add_u64_counter(l_msgr_rdma_tx_wc_retry_errors, "tx_retry_errors", "The number of tx retry errors");
  plb.add_u64_counter(l_msgr_rdma_tx_wc_wr_flush_errors, "tx_wr_flush_errors", "The number of tx work request flush errors");

  plb.add_u64_counter(l_msgr_rdma_rx_total_wc, "rx_total_wc", "The number of total rx work completion");
  plb.add_u64_counter(l_msgr_rdma_rx_total_wc_errors, "rx_total_wc_errors", "The number of total rx error work completion");
  plb.add_u64_counter(l_msgr_rdma_rx_fin, "rx_fin", "The number of rx finish work request");

  plb.add_u64_counter(l_msgr_rdma_total_async_events, "total_async_events", "The number of async events");
  plb.add_u64_counter(l_msgr_rdma_async_last_wqe_events, "async_last_wqe_events", "The number of last wqe events");

  plb.add_u64_counter(l_msgr_rdma_handshake_errors, "handshake_errors", "The number of handshake errors");


  plb.add_u64_counter(l_msgr_rdma_created_queue_pair, "created_queue_pair", "Active queue pair number");
  plb.add_u64_counter(l_msgr_rdma_active_queue_pair, "active_queue_pair", "Created queue pair number");

  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);
  Cycles::init();
}

void RDMADispatcher::polling_start()
{
  // take lock because listen/connect can happen from different worker threads
  std::lock_guard l{lock};

  if (t.joinable()) 
    return; // dispatcher thread already running 

  ib->get_memory_manager()->set_rx_stat_logger(perf_logger);

  tx_cc = ib->create_comp_channel(cct);
  ceph_assert(tx_cc);
  rx_cc = ib->create_comp_channel(cct);
  ceph_assert(rx_cc);
  tx_cq = ib->create_comp_queue(cct, tx_cc);
  ceph_assert(tx_cq);
  rx_cq = ib->create_comp_queue(cct, rx_cc);
  ceph_assert(rx_cq);

  t = std::thread(&RDMADispatcher::polling, this);
  ceph_pthread_setname(t.native_handle(), "rdma-polling");
}

void RDMADispatcher::polling_stop()
{
  {
    std::lock_guard l{lock};
    done = true;
  }

  if (!t.joinable())
    return;

  t.join();

  tx_cc->ack_events();
  rx_cc->ack_events();
  delete tx_cq;
  delete rx_cq;
  delete tx_cc;
  delete rx_cc;
}

void RDMADispatcher::handle_async_event()
{
  ldout(cct, 30) << __func__ << dendl;
  while (1) {
    ibv_async_event async_event;
    if (ibv_get_async_event(ib->get_device()->ctxt, &async_event)) {
      if (errno != EAGAIN)
       lderr(cct) << __func__ << " ibv_get_async_event failed. (errno=" << errno
                  << " " << cpp_strerror(errno) << ")" << dendl;
      return;
    }
    perf_logger->inc(l_msgr_rdma_total_async_events);
    ldout(cct, 1) << __func__ << "Event : " << ibv_event_type_str(async_event.event_type) << dendl;

    switch (async_event.event_type) {
      /***********************CQ events********************/
      case IBV_EVENT_CQ_ERR:
        lderr(cct) << __func__ << " Fatal Error, effect all QP bound with same CQ, "
                   << " CQ Overflow, dev = " << ib->get_device()->ctxt
                   << " Need destroy and recreate resource " << dendl;
        break;
      /***********************QP events********************/
      case IBV_EVENT_QP_FATAL:
        {
          /* Error occurred on a QP and it transitioned to error state */
          ibv_qp* ib_qp = async_event.element.qp;
          uint32_t qpn = ib_qp->qp_num;
          QueuePair* qp = get_qp(qpn);
          lderr(cct) << __func__ << " Fatal Error, event associate qp number: " << qpn
                     << " Queue Pair status: " << Infiniband::qp_state_string(qp->get_state())
                     << " Event : " << ibv_event_type_str(async_event.event_type) << dendl;
        }
        break;
      case IBV_EVENT_QP_LAST_WQE_REACHED:
        {
          /*
           * 1. The QP bound with SRQ is in IBV_QPS_ERR state & no more WQE on the RQ of the QP
           *    Reason: QP is force switched into Error before posting Beacon WR.
           *            The QP's WRs will be flushed into CQ with IBV_WC_WR_FLUSH_ERR status
           *            For SRQ, only WRs on the QP which is switched into Error status will be flushed.
           *    Handle: Only confirm that qp enter into dead queue pairs
           * 2. The CQE with error was generated for the last WQE
           *    Handle: output error log
           */
          perf_logger->inc(l_msgr_rdma_async_last_wqe_events);
          ibv_qp* ib_qp = async_event.element.qp;
          uint32_t qpn = ib_qp->qp_num;
          std::lock_guard l{lock};
          RDMAConnectedSocketImpl *conn = get_conn_lockless(qpn);
          QueuePair* qp = get_qp_lockless(qpn);

          if (qp && !qp->is_dead()) {
            lderr(cct) << __func__ << " QP not dead, event associate qp number: " << qpn
                       << " Queue Pair status: " << Infiniband::qp_state_string(qp->get_state())
                       << " Event : " << ibv_event_type_str(async_event.event_type) << dendl;
          }
          if (!conn) {
            ldout(cct, 20) << __func__ << " Connection's QP maybe entered into dead status. "
                           << " qp number: " << qpn << dendl;
          } else {
             conn->fault();
             if (qp) {
                if (!cct->_conf->ms_async_rdma_cm) {
                  enqueue_dead_qp_lockless(qpn);
                }
             }
          }
        }
        break;
      case IBV_EVENT_QP_REQ_ERR:
        /* Invalid Request Local Work Queue Error */
        [[fallthrough]];
      case IBV_EVENT_QP_ACCESS_ERR:
        /* Local access violation error */
        [[fallthrough]];
      case IBV_EVENT_COMM_EST:
        /* Communication was established on a QP */
        [[fallthrough]];
      case IBV_EVENT_SQ_DRAINED:
        /* Send Queue was drained of outstanding messages in progress */
        [[fallthrough]];
      case IBV_EVENT_PATH_MIG:
        /* A connection has migrated to the alternate path */
        [[fallthrough]];
      case IBV_EVENT_PATH_MIG_ERR:
        /* A connection failed to migrate to the alternate path */
        break;
      /***********************SRQ events*******************/
      case IBV_EVENT_SRQ_ERR:
        /* Error occurred on an SRQ */
        [[fallthrough]];
      case IBV_EVENT_SRQ_LIMIT_REACHED:
        /* SRQ limit was reached */
        break;
      /***********************Port events******************/
      case IBV_EVENT_PORT_ACTIVE:
        /* Link became active on a port */
        [[fallthrough]];
      case IBV_EVENT_PORT_ERR:
        /* Link became unavailable on a port */
        [[fallthrough]];
      case IBV_EVENT_LID_CHANGE:
        /* LID was changed on a port */
        [[fallthrough]];
      case IBV_EVENT_PKEY_CHANGE:
        /* P_Key table was changed on a port */
        [[fallthrough]];
      case IBV_EVENT_SM_CHANGE:
        /* SM was changed on a port */
        [[fallthrough]];
      case IBV_EVENT_CLIENT_REREGISTER:
        /* SM sent a CLIENT_REREGISTER request to a port */
        [[fallthrough]];
      case IBV_EVENT_GID_CHANGE:
        /* GID table was changed on a port */
        break;

      /***********************CA events******************/
      //CA events:
      case IBV_EVENT_DEVICE_FATAL:
        /* CA is in FATAL state */
        lderr(cct) << __func__ << " ibv_get_async_event: dev = " << ib->get_device()->ctxt
                   << " evt: " << ibv_event_type_str(async_event.event_type) << dendl;
        break;
      default:
        lderr(cct) << __func__ << " ibv_get_async_event: dev = " << ib->get_device()->ctxt
                   << " unknown event: " << async_event.event_type << dendl;
        break;
    }
    ibv_ack_async_event(&async_event);
  }
}

void RDMADispatcher::post_chunk_to_pool(Chunk* chunk)
{
  std::lock_guard l{lock};
  ib->post_chunk_to_pool(chunk);
  perf_logger->dec(l_msgr_rdma_rx_bufs_in_use);
}

int RDMADispatcher::post_chunks_to_rq(int num, QueuePair *qp)
{
  std::lock_guard l{lock};
  return ib->post_chunks_to_rq(num, qp);
}

void RDMADispatcher::polling()
{
  static int MAX_COMPLETIONS = 32;
  ibv_wc wc[MAX_COMPLETIONS];

  std::map<RDMAConnectedSocketImpl*, std::vector<ibv_wc> > polled;
  std::vector<ibv_wc> tx_cqe;
  ldout(cct, 20) << __func__ << " going to poll tx cq: " << tx_cq << " rx cq: " << rx_cq << dendl;
  uint64_t last_inactive = Cycles::rdtsc();
  bool rearmed = false;
  int r = 0;

  while (true) {
    int tx_ret = tx_cq->poll_cq(MAX_COMPLETIONS, wc);
    if (tx_ret > 0) {
      ldout(cct, 20) << __func__ << " tx completion queue got " << tx_ret
                     << " responses."<< dendl;
      handle_tx_event(wc, tx_ret);
    }

    int rx_ret = rx_cq->poll_cq(MAX_COMPLETIONS, wc);
    if (rx_ret > 0) {
      ldout(cct, 20) << __func__ << " rx completion queue got " << rx_ret
                     << " responses."<< dendl;
      handle_rx_event(wc, rx_ret);
    }

    if (!tx_ret && !rx_ret) {
      perf_logger->set(l_msgr_rdma_inflight_tx_chunks, inflight);
      //
      // Clean up dead QPs when rx/tx CQs are in idle. The thing is that
      // we can destroy QPs even earlier, just when beacon has been received,
      // but we have two CQs (rx & tx), thus beacon WC can be poped from tx
      // CQ before other WCs are fully consumed from rx CQ. For safety, we
      // wait for beacon and then "no-events" from CQs.
      //
      // Calling size() on vector without locks is totally fine, since we
      // use it as a hint (accuracy is not important here)
      //
      if (!dead_queue_pairs.empty()) {
        decltype(dead_queue_pairs) dead_qps;
        {
          std::lock_guard l{lock};
          dead_queue_pairs.swap(dead_qps);
        }

        for (auto& qp: dead_qps) {
          perf_logger->dec(l_msgr_rdma_active_queue_pair);
          ldout(cct, 10) << __func__ << " finally delete qp = " << qp << dendl;
          delete qp;
        }
      }

      if (!num_qp_conn && done && dead_queue_pairs.empty())
        break;

      uint64_t now = Cycles::rdtsc();
      if (Cycles::to_microseconds(now - last_inactive) > cct->_conf->ms_async_rdma_polling_us) {
        handle_async_event();
        if (!rearmed) {
          // Clean up cq events after rearm notify ensure no new incoming event
          // arrived between polling and rearm
          tx_cq->rearm_notify();
          rx_cq->rearm_notify();
          rearmed = true;
          continue;
        }

        struct pollfd channel_poll[2];
        channel_poll[0].fd = tx_cc->get_fd();
        channel_poll[0].events = POLLIN;
        channel_poll[0].revents = 0;
        channel_poll[1].fd = rx_cc->get_fd();
        channel_poll[1].events = POLLIN;
        channel_poll[1].revents = 0;
        r = 0;
        perf_logger->set(l_msgr_rdma_polling, 0);
        while (!done && r == 0) {
          r = TEMP_FAILURE_RETRY(poll(channel_poll, 2, 100));
          if (r < 0) {
            r = -errno;
            lderr(cct) << __func__ << " poll failed " << r << dendl;
            ceph_abort();
          }
        }
        if (r > 0 && tx_cc->get_cq_event())
          ldout(cct, 20) << __func__ << " got tx cq event." << dendl;
        if (r > 0 && rx_cc->get_cq_event())
          ldout(cct, 20) << __func__ << " got rx cq event." << dendl;
        last_inactive = Cycles::rdtsc();
        perf_logger->set(l_msgr_rdma_polling, 1);
        rearmed = false;
      }
    }
  }
}

void RDMADispatcher::notify_pending_workers() {
  if (num_pending_workers) {
    RDMAWorker *w = nullptr;
    {
      std::lock_guard l{w_lock};
      if (!pending_workers.empty()) {
        w = pending_workers.front();
        pending_workers.pop_front();
        --num_pending_workers;
      }
    }
    if (w)
      w->notify_worker();
  }
}

void RDMADispatcher::register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi)
{
  std::lock_guard l{lock};
  ceph_assert(!qp_conns.count(qp->get_local_qp_number()));
  qp_conns[qp->get_local_qp_number()] = std::make_pair(qp, csi);
  ++num_qp_conn;
}

RDMAConnectedSocketImpl* RDMADispatcher::get_conn_lockless(uint32_t qp)
{
  auto it = qp_conns.find(qp);
  if (it == qp_conns.end())
    return nullptr;
  if (it->second.first->is_dead())
    return nullptr;
  return it->second.second;
}

Infiniband::QueuePair* RDMADispatcher::get_qp_lockless(uint32_t qp)
{
  // Try to find the QP in qp_conns firstly.
  auto it = qp_conns.find(qp);
  if (it != qp_conns.end())
    return it->second.first;

  // Try again in dead_queue_pairs.
  for (auto &i: dead_queue_pairs)
    if (i->get_local_qp_number() == qp)
      return i;

  return nullptr;
}

Infiniband::QueuePair* RDMADispatcher::get_qp(uint32_t qp)
{
  std::lock_guard l{lock};
  return get_qp_lockless(qp);
}

void RDMADispatcher::enqueue_dead_qp_lockless(uint32_t qpn)
{
  auto it = qp_conns.find(qpn);
  if (it == qp_conns.end()) {
    lderr(cct) << __func__ << " QP [" << qpn << "] is not registered." << dendl;
    return ;
  }
  QueuePair *qp = it->second.first;
  dead_queue_pairs.push_back(qp);
  qp_conns.erase(it);
  --num_qp_conn;
}

void RDMADispatcher::enqueue_dead_qp(uint32_t qpn)
{
  std::lock_guard l{lock};
  enqueue_dead_qp_lockless(qpn);
}

void RDMADispatcher::schedule_qp_destroy(uint32_t qpn)
{
  std::lock_guard l{lock};
  auto it = qp_conns.find(qpn);
  if (it == qp_conns.end()) {
    lderr(cct) << __func__ << " QP [" << qpn << "] is not registered." << dendl;
    return;
  }
  QueuePair *qp = it->second.first;
  if (qp->to_dead()) {
    //
    // Failed to switch to dead. This is abnormal, but we can't
    // do anything, so just destroy QP.
    //
    dead_queue_pairs.push_back(qp);
    qp_conns.erase(it);
    --num_qp_conn;
  } else {
    //
    // Successfully switched to dead, thus keep entry in the map.
    // But only zero out socked pointer in order to return null from
    // get_conn_lockless();
    it->second.second = nullptr;
  }
}

void RDMADispatcher::handle_tx_event(ibv_wc *cqe, int n)
{
  std::vector<Chunk*> tx_chunks;

  for (int i = 0; i < n; ++i) {
    ibv_wc* response = &cqe[i];

    // If it's beacon WR, enqueue the QP to be destroyed later
    if (response->wr_id == BEACON_WRID) {
      enqueue_dead_qp(response->qp_num);
      continue;
    }

    ldout(cct, 20) << __func__ << " QP number: " << response->qp_num << " len: " << response->byte_len
                   << " status: " << ib->wc_status_to_string(response->status) << dendl;

    if (response->status != IBV_WC_SUCCESS) {
      switch(response->status) {
        case IBV_WC_RETRY_EXC_ERR:
          {
            perf_logger->inc(l_msgr_rdma_tx_wc_retry_errors);

            ldout(cct, 1) << __func__ << " Responder ACK timeout, possible disconnect, or Remote QP in bad state "
                          << " WCE status(" << response->status << "): " << ib->wc_status_to_string(response->status)
                          << " WCE QP number " << response->qp_num << " Opcode " << response->opcode
                          << " wr_id: 0x" << std::hex << response->wr_id << std::dec << dendl;

            std::lock_guard l{lock};
            RDMAConnectedSocketImpl *conn = get_conn_lockless(response->qp_num);
            if (conn) {
              ldout(cct, 1) << __func__ << " SQ WR return error, remote Queue Pair, qp number: "
                            << conn->get_peer_qpn() << dendl;
            }
          }
          break;
        case IBV_WC_WR_FLUSH_ERR:
          {
            perf_logger->inc(l_msgr_rdma_tx_wc_wr_flush_errors);

            std::lock_guard l{lock};
            QueuePair *qp = get_qp_lockless(response->qp_num);
            if (qp) {
              ldout(cct, 20) << __func__ << " qp state is " << Infiniband::qp_state_string(qp->get_state()) << dendl;
            }
            if (qp && qp->is_dead()) {
              ldout(cct, 20) << __func__ << " outstanding SQ WR is flushed into CQ since QueuePair is dead " << dendl;
            } else {
              lderr(cct) << __func__ << " Invalid/Unsupported request to consume outstanding SQ WR,"
                         << " WCE status(" << response->status << "): " << ib->wc_status_to_string(response->status)
                         << " WCE QP number " << response->qp_num << " Opcode " << response->opcode
                         << " wr_id: 0x" << std::hex << response->wr_id << std::dec << dendl;

              RDMAConnectedSocketImpl *conn = get_conn_lockless(response->qp_num);
              if (conn) {
                ldout(cct, 1) << __func__ << " SQ WR return error, remote Queue Pair, qp number: "
                              << conn->get_peer_qpn() << dendl;
              }
            }
          }
          break;

        default:
          {
            lderr(cct) << __func__ << " SQ WR return error,"
                       << " WCE status(" << response->status << "): " << ib->wc_status_to_string(response->status)
                       << " WCE QP number " << response->qp_num << " Opcode " << response->opcode
                       << " wr_id: 0x" << std::hex << response->wr_id << std::dec << dendl;

            std::lock_guard l{lock};
            RDMAConnectedSocketImpl *conn = get_conn_lockless(response->qp_num);
            if (conn && conn->is_connected()) {
              ldout(cct, 20) << __func__ << " SQ WR return error Queue Pair error state is : " << conn->get_qp_state()
                             << " remote Queue Pair, qp number: " << conn->get_peer_qpn() << dendl;
              conn->fault();
            } else {
              ldout(cct, 1) << __func__ << " Disconnected, qp_num = " << response->qp_num << " discard event" << dendl;
            }
          }
          break;
      }
    }

    auto chunk = reinterpret_cast<Chunk *>(response->wr_id);
    //TX completion may come either from
    // 1) regular send message, WCE wr_id points to chunk
    // 2) 'fin' message, wr_id points to the QP
    if (ib->get_memory_manager()->is_valid_chunk(chunk)) {
      tx_chunks.push_back(chunk);
    } else if (reinterpret_cast<QueuePair*>(response->wr_id)->get_local_qp_number() == response->qp_num ) {
      ldout(cct, 1) << __func__ << " sending of the disconnect msg completed" << dendl;
    } else {
      ldout(cct, 1) << __func__ << " not tx buffer, chunk " << chunk << dendl;
      ceph_abort();
    }
  }

  perf_logger->inc(l_msgr_rdma_tx_total_wc, n);
  post_tx_buffer(tx_chunks);
}

/**
 * Add the given Chunks to the given free queue.
 *
 * \param[in] chunks
 *      The Chunks to enqueue.
 * \return
 *      0 if success or -1 for failure
 */
void RDMADispatcher::post_tx_buffer(std::vector<Chunk*> &chunks)
{
  if (chunks.empty())
    return ;

  inflight -= chunks.size();
  ib->get_memory_manager()->return_tx(chunks);
  ldout(cct, 30) << __func__ << " release " << chunks.size()
                 << " chunks, inflight " << inflight << dendl;
  notify_pending_workers();
}

void RDMADispatcher::handle_rx_event(ibv_wc *cqe, int rx_number)
{
  perf_logger->inc(l_msgr_rdma_rx_total_wc, rx_number);
  perf_logger->inc(l_msgr_rdma_rx_bufs_in_use, rx_number);

  std::map<RDMAConnectedSocketImpl*, std::vector<ibv_wc> > polled;
  std::lock_guard l{lock};//make sure connected socket alive when pass wc

  for (int i = 0; i < rx_number; ++i) {
    ibv_wc* response = &cqe[i];
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    RDMAConnectedSocketImpl *conn = get_conn_lockless(response->qp_num);
    QueuePair *qp = get_qp_lockless(response->qp_num);

    switch (response->status) {
      case IBV_WC_SUCCESS:
        ceph_assert(response->opcode == IBV_WC_RECV);
        if (!conn) {
          ldout(cct, 1) << __func__ << " csi with qpn " << response->qp_num << " may be dead. chunk 0x"
                        << std::hex << chunk << " will be back." << std::dec << dendl;
          ib->post_chunk_to_pool(chunk);
          perf_logger->dec(l_msgr_rdma_rx_bufs_in_use);
        } else {
          conn->post_chunks_to_rq(1);
          polled[conn].push_back(*response);

          if (qp != nullptr && !qp->get_srq()) {
            qp->remove_rq_wr(chunk);
            chunk->clear_qp();
          }
        }
        break;

      case IBV_WC_WR_FLUSH_ERR:
        perf_logger->inc(l_msgr_rdma_rx_total_wc_errors);

        if (qp) {
          ldout(cct, 20) << __func__ << " qp state is " << Infiniband::qp_state_string(qp->get_state()) << dendl;
        }
        if (qp && qp->is_dead()) {
          ldout(cct, 20) << __func__ << " outstanding RQ WR is flushed into CQ since QueuePair is dead " << dendl;
        } else {
          ldout(cct, 1) << __func__ << " RQ WR return error,"
                     << " WCE status(" << response->status << "): " << ib->wc_status_to_string(response->status)
                     << " WCE QP number " << response->qp_num << " Opcode " << response->opcode
                     << " wr_id: 0x" << std::hex << response->wr_id << std::dec << dendl;
          if (conn) {
            ldout(cct, 1) << __func__ << " RQ WR return error, remote Queue Pair, qp number: "
                       << conn->get_peer_qpn() << dendl;
          }
        }

        ib->post_chunk_to_pool(chunk);
        perf_logger->dec(l_msgr_rdma_rx_bufs_in_use);
        break;

      default:
        perf_logger->inc(l_msgr_rdma_rx_total_wc_errors);

        ldout(cct, 1) << __func__ << " RQ WR return error,"
                      << " WCE status(" << response->status << "): " << ib->wc_status_to_string(response->status)
                      << " WCE QP number " << response->qp_num << " Opcode " << response->opcode
                      << " wr_id: 0x" << std::hex << response->wr_id << std::dec << dendl;
        if (conn && conn->is_connected())
          conn->fault();

        ib->post_chunk_to_pool(chunk);
        perf_logger->dec(l_msgr_rdma_rx_bufs_in_use);
        break;
    }
  }

  for (auto &i : polled)
    i.first->pass_wc(std::move(i.second));
  polled.clear();
}

RDMAWorker::RDMAWorker(CephContext *c, unsigned worker_id)
  : Worker(c, worker_id),
    tx_handler(new C_handle_cq_tx(this))
{
  // initialize perf_logger
  char name[128];
  sprintf(name, "AsyncMessenger::RDMAWorker-%u", id);
  PerfCountersBuilder plb(cct, name, l_msgr_rdma_first, l_msgr_rdma_last);

  plb.add_u64_counter(l_msgr_rdma_tx_no_mem, "tx_no_mem", "The count of no tx buffer");
  plb.add_u64_counter(l_msgr_rdma_tx_parital_mem, "tx_parital_mem", "The count of parital tx buffer");
  plb.add_u64_counter(l_msgr_rdma_tx_failed, "tx_failed_post", "The number of tx failed posted");

  plb.add_u64_counter(l_msgr_rdma_tx_chunks, "tx_chunks", "The number of tx chunks transmitted");
  plb.add_u64_counter(l_msgr_rdma_tx_bytes, "tx_bytes", "The bytes of tx chunks transmitted", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_msgr_rdma_rx_chunks, "rx_chunks", "The number of rx chunks transmitted");
  plb.add_u64_counter(l_msgr_rdma_rx_bytes, "rx_bytes", "The bytes of rx chunks transmitted", NULL, 0, unit_t(UNIT_BYTES));
  plb.add_u64_counter(l_msgr_rdma_pending_sent_conns, "pending_sent_conns", "The count of pending sent conns");

  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);
}

RDMAWorker::~RDMAWorker()
{
  delete tx_handler;
}

void RDMAWorker::initialize()
{
  ceph_assert(dispatcher);
}

int RDMAWorker::listen(entity_addr_t &sa, unsigned addr_slot,
		       const SocketOptions &opt,ServerSocket *sock)
{
  ib->init();
  dispatcher->polling_start();

  RDMAServerSocketImpl *p;
  if (cct->_conf->ms_async_rdma_type == "iwarp") {
    p = new RDMAIWARPServerSocketImpl(cct, ib, dispatcher, this, sa, addr_slot);
  } else {
    p = new RDMAServerSocketImpl(cct, ib, dispatcher, this, sa, addr_slot);
  }
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
  ib->init();
  dispatcher->polling_start();

  RDMAConnectedSocketImpl* p;
  if (cct->_conf->ms_async_rdma_type == "iwarp") {
    p = new RDMAIWARPConnectedSocketImpl(cct, ib, dispatcher, this);
  } else {
    p = new RDMAConnectedSocketImpl(cct, ib, dispatcher, this);
  }
  int r = p->try_connect(addr, opts);

  if (r < 0) {
    ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
    delete p;
    return r;
  }
  std::unique_ptr<RDMAConnectedSocketImpl> csi(p);
  *socket = ConnectedSocket(std::move(csi));
  return 0;
}

int RDMAWorker::get_reged_mem(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes)
{
  ceph_assert(center.in_thread());
  int r = ib->get_tx_buffers(c, bytes);
  size_t got = ib->get_memory_manager()->get_tx_buffer_size() * r;
  ldout(cct, 30) << __func__ << " need " << bytes << " bytes, reserve " << got << " registered  bytes, inflight " << dispatcher->inflight << dendl;
  dispatcher->inflight += r;
  if (got >= bytes)
    return r;

  if (o) {
    if (!o->is_pending()) {
      pending_sent_conns.push_back(o);
      perf_logger->inc(l_msgr_rdma_pending_sent_conns, 1);
      o->set_pending(1);
    }
    dispatcher->make_pending_worker(this);
  }
  return r;
}


void RDMAWorker::handle_pending_message()
{
  ldout(cct, 20) << __func__ << " pending conns " << pending_sent_conns.size() << dendl;
  while (!pending_sent_conns.empty()) {
    RDMAConnectedSocketImpl *o = pending_sent_conns.front();
    pending_sent_conns.pop_front();
    ssize_t r = o->submit(false);
    ldout(cct, 20) << __func__ << " sent pending bl socket=" << o << " r=" << r << dendl;
    if (r < 0) {
      if (r == -EAGAIN) {
        pending_sent_conns.push_back(o);
        dispatcher->make_pending_worker(this);
        return ;
      }
      o->fault();
    }
    o->set_pending(0);
    perf_logger->dec(l_msgr_rdma_pending_sent_conns, 1);
  }
  dispatcher->notify_pending_workers();
}

RDMAStack::RDMAStack(CephContext *cct)
  : NetworkStack(cct), ib(std::make_shared<Infiniband>(cct)),
    rdma_dispatcher(std::make_shared<RDMADispatcher>(cct, ib))
{
  ldout(cct, 20) << __func__ << " constructing RDMAStack..." << dendl;
  ldout(cct, 20) << " creating RDMAStack:" << this << " with dispatcher:" << rdma_dispatcher.get() << dendl;
}

RDMAStack::~RDMAStack()
{
  if (cct->_conf->ms_async_rdma_enable_hugepage) {
    unsetenv("RDMAV_HUGEPAGES_SAFE");	//remove env variable on destruction
  }
}

Worker* RDMAStack::create_worker(CephContext *c, unsigned worker_id)
{
  auto w = new RDMAWorker(c, worker_id);
  w->set_dispatcher(rdma_dispatcher);
  w->set_ib(ib);
  return w;
}

void RDMAStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
  threads.resize(i+1);
  threads[i] = std::thread(func);
}

void RDMAStack::join_worker(unsigned i)
{
  ceph_assert(threads.size() > i && threads[i].joinable());
  threads[i].join();
}
