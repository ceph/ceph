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

#include <sys/time.h>
#include <sys/resource.h>

#include "include/str_list.h"
#include "common/deleter.h"
#include "common/Tub.h"
#include "RDMAStack.h"
#include "RDMAConnTCP.h"
#include "Device.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "RDMAStack "

static Tub<Infiniband> global_infiniband;

RDMADispatcher::~RDMADispatcher()
{
  polling_stop();

  ldout(cct, 20) << __func__ << " destructing rdma dispatcher" << dendl;

  global_infiniband->set_dispatcher(nullptr);

  assert(qp_conns.empty());
  assert(num_qp_conn == 0);
  assert(dead_queue_pairs.empty());
  assert(num_dead_queue_pair == 0);
}

RDMADispatcher::RDMADispatcher(CephContext* c, RDMAStack* s)
  : cct(c), lock("RDMADispatcher::lock"),
  w_lock("RDMADispatcher::for worker pending list"), stack(s)
{
  PerfCountersBuilder plb(cct, "AsyncMessenger::RDMADispatcher", l_msgr_rdma_dispatcher_first, l_msgr_rdma_dispatcher_last);

  plb.add_u64_counter(l_msgr_rdma_polling, "polling", "Whether dispatcher thread is polling");
  plb.add_u64_counter(l_msgr_rdma_inflight_tx_chunks, "inflight_tx_chunks", "The number of inflight tx chunks");
  plb.add_u64_counter(l_msgr_rdma_inqueue_rx_chunks, "inqueue_rx_chunks", "The number of inqueue rx chunks");

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
}

void RDMADispatcher::polling_start()
{
  t = std::thread(&RDMADispatcher::polling, this);
}

void RDMADispatcher::polling_stop()
{
  if (!t.joinable())
    return;

  done = true;
  t.join();
}

void RDMADispatcher::process_async_event(Device *ibdev, ibv_async_event &async_event)
{
  perf_logger->inc(l_msgr_rdma_total_async_events);
  // FIXME: Currently we must ensure no other factor make QP in ERROR state,
  // otherwise this qp can't be deleted in current cleanup flow.
  if (async_event.event_type == IBV_EVENT_QP_LAST_WQE_REACHED) {
    perf_logger->inc(l_msgr_rdma_async_last_wqe_events);
    uint64_t qpn = async_event.element.qp->qp_num;
    ldout(cct, 10) << __func__ << " event associated qp=" << async_event.element.qp
      << " evt: " << ibv_event_type_str(async_event.event_type) << dendl;
    Mutex::Locker l(lock);
    RDMAConnectedSocketImpl *conn = get_conn_lockless(qpn);
    if (!conn) {
      ldout(cct, 1) << __func__ << " missing qp_num=" << qpn << " discard event" << dendl;
    } else {
      ldout(cct, 1) << __func__ << " it's not forwardly stopped by us, reenable=" << conn << dendl;
      conn->fault();
      erase_qpn_lockless(qpn);
    }
  } else {
    ldout(cct, 1) << __func__ << " ibv_get_async_event: dev=" << *ibdev
      << " evt: " << ibv_event_type_str(async_event.event_type)
      << dendl;
  }
}

void RDMADispatcher::polling()
{
  static int MAX_COMPLETIONS = 32;
  ibv_wc wc[MAX_COMPLETIONS];

  std::map<RDMAConnectedSocketImpl*, std::vector<ibv_wc> > polled;
  std::vector<ibv_wc> tx_cqe;
  RDMAConnectedSocketImpl *conn = nullptr;
  utime_t last_inactive = ceph_clock_now();
  bool rearmed = false;
  int r = 0;

  while (true) {
    Device *ibdev;

    int tx_ret = global_infiniband->poll_tx(MAX_COMPLETIONS, &ibdev, wc);
    if (tx_ret > 0) {
      ldout(cct, 20) << __func__ << " tx completion queue got " << tx_ret
                     << " responses."<< dendl;
      handle_tx_event(ibdev, wc, tx_ret);
    }

    int rx_ret = global_infiniband->poll_rx(MAX_COMPLETIONS, &ibdev, wc);
    if (rx_ret > 0) {
      ldout(cct, 20) << __func__ << " rx completion queue got " << rx_ret
                     << " responses."<< dendl;
      perf_logger->inc(l_msgr_rdma_rx_total_wc, rx_ret);

      Mutex::Locker l(lock);//make sure connected socket alive when pass wc
      for (int i = 0; i < rx_ret; ++i) {
        ibv_wc* response = &wc[i];
        Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
        ldout(cct, 25) << __func__ << " got chunk=" << chunk << " bytes:" << response->byte_len << " opcode:" << response->opcode << dendl;

        assert(wc[i].opcode == IBV_WC_RECV);

        if (response->status == IBV_WC_SUCCESS) {
          conn = get_conn_lockless(response->qp_num);
          if (!conn) {
            assert(ibdev->is_rx_buffer(chunk->buffer));
            r = ibdev->post_chunk(chunk);
            ldout(cct, 1) << __func__ << " csi with qpn " << response->qp_num << " may be dead. chunk " << chunk << " will be back ? " << r << dendl;
            assert(r == 0);
          } else {
            polled[conn].push_back(*response);
          }
        } else {
          perf_logger->inc(l_msgr_rdma_rx_total_wc_errors);
          ldout(cct, 1) << __func__ << " work request returned error for buffer(" << chunk
              << ") status(" << response->status << ":"
              << Infiniband::wc_status_to_string(response->status) << ")" << dendl;
          assert(ibdev->is_rx_buffer(chunk->buffer));
          r = ibdev->post_chunk(chunk);
          if (r) {
            ldout(cct, 0) << __func__ << " post chunk failed, error: " << cpp_strerror(r) << dendl;
            assert(r == 0);
          }

          conn = get_conn_lockless(response->qp_num);
          if (conn && conn->is_connected())
            conn->fault();
        }
      }

      for (auto &&i : polled) {
        perf_logger->inc(l_msgr_rdma_inqueue_rx_chunks, i.second.size());
        i.first->pass_wc(std::move(i.second));
      }
      polled.clear();
    }

    if (!tx_ret && !rx_ret) {
      // NOTE: Has TX just transitioned to idle? We should do it when idle!
      // It's now safe to delete queue pairs (see comment by declaration
      // for dead_queue_pairs).
      // Additionally, don't delete qp while outstanding_buffers isn't empty,
      // because we need to check qp's state before sending
      perf_logger->set(l_msgr_rdma_inflight_tx_chunks, inflight);
      if (num_dead_queue_pair) {
        Mutex::Locker l(lock); // FIXME reuse dead qp because creating one qp costs 1 ms
        while (!dead_queue_pairs.empty()) {
          ldout(cct, 10) << __func__ << " finally delete qp=" << dead_queue_pairs.back() << dendl;
          delete dead_queue_pairs.back();
          perf_logger->dec(l_msgr_rdma_active_queue_pair);
          dead_queue_pairs.pop_back();
          --num_dead_queue_pair;
        }
      }
      if (!num_qp_conn && done)
        break;

      if ((ceph_clock_now() - last_inactive).to_nsec() / 1000 > cct->_conf->ms_async_rdma_polling_us) {
        if (!rearmed) {
          // Clean up cq events after rearm notify ensure no new incoming event
          // arrived between polling and rearm
	  global_infiniband->rearm_notify();
          rearmed = true;
          continue;
        }

        perf_logger->set(l_msgr_rdma_polling, 0);

	r = global_infiniband->poll_blocking(done);
        if (r > 0)
          ldout(cct, 20) << __func__ << " got a cq event." << dendl;

        last_inactive = ceph_clock_now();
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
      Mutex::Locker l(w_lock);
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

int RDMADispatcher::register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi)
{
  int fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
  assert(fd >= 0);
  Mutex::Locker l(lock);
  assert(!qp_conns.count(qp->get_local_qp_number()));
  qp_conns[qp->get_local_qp_number()] = std::make_pair(qp, csi);
  ++num_qp_conn;
  return fd;
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

void RDMADispatcher::erase_qpn_lockless(uint32_t qpn)
{
  auto it = qp_conns.find(qpn);
  if (it == qp_conns.end())
    return ;
  ++num_dead_queue_pair;
  dead_queue_pairs.push_back(it->second.first);
  qp_conns.erase(it);
  --num_qp_conn;
}

void RDMADispatcher::erase_qpn(uint32_t qpn)
{
  Mutex::Locker l(lock);
  erase_qpn_lockless(qpn);
}

void RDMADispatcher::handle_tx_event(Device *ibdev, ibv_wc *cqe, int n)
{
  std::vector<Chunk*> tx_chunks;

  for (int i = 0; i < n; ++i) {
    ibv_wc* response = &cqe[i];
    Chunk* chunk = reinterpret_cast<Chunk *>(response->wr_id);
    ldout(cct, 25) << __func__ << " QP: " << response->qp_num
                   << " len: " << response->byte_len << " , addr:" << chunk
                   << " " << global_infiniband->wc_status_to_string(response->status) << dendl;

    if (response->status != IBV_WC_SUCCESS) {
      perf_logger->inc(l_msgr_rdma_tx_total_wc_errors);
      if (response->status == IBV_WC_RETRY_EXC_ERR) {
        ldout(cct, 1) << __func__ << " connection between server and client not working. Disconnect this now" << dendl;
        perf_logger->inc(l_msgr_rdma_tx_wc_retry_errors);
      } else if (response->status == IBV_WC_WR_FLUSH_ERR) {
        ldout(cct, 1) << __func__ << " Work Request Flushed Error: this connection's qp="
                      << response->qp_num << " should be down while this WR=" << response->wr_id
                      << " still in flight." << dendl;
        perf_logger->inc(l_msgr_rdma_tx_wc_wr_flush_errors);
      } else {
        ldout(cct, 1) << __func__ << " send work request returned error for buffer("
                      << response->wr_id << ") status(" << response->status << "): "
                      << global_infiniband->wc_status_to_string(response->status) << dendl;
      }

      Mutex::Locker l(lock);//make sure connected socket alive when pass wc
      RDMAConnectedSocketImpl *conn = get_conn_lockless(response->qp_num);

      if (conn && conn->is_connected()) {
        ldout(cct, 25) << __func__ << " qp state is : " << conn->get_qp_state() << dendl;//wangzhi
        conn->fault();
      } else {
        ldout(cct, 1) << __func__ << " missing qp_num=" << response->qp_num << " discard event" << dendl;
      }
    }

    // FIXME: why not tx?
    if (ibdev->get_memory_manager()->is_tx_buffer(chunk->buffer))
      tx_chunks.push_back(chunk);
    else
      ldout(cct, 1) << __func__ << " not tx buffer, chunk " << chunk << dendl;
  }

  perf_logger->inc(l_msgr_rdma_tx_total_wc, n);
  post_tx_buffer(ibdev, tx_chunks);
}

/**
 * Add the given Chunks to the given free queue.
 *
 * \param[in] chunks
 *      The Chunks to enqueue.
 * \return
 *      0 if success or -1 for failure
 */
void RDMADispatcher::post_tx_buffer(Device *ibdev, std::vector<Chunk*> &chunks)
{
  if (chunks.empty())
    return ;

  inflight -= chunks.size();
  ibdev->get_memory_manager()->return_tx(chunks);
  ldout(cct, 30) << __func__ << " release " << chunks.size()
                 << " chunks, inflight " << inflight << dendl;
  notify_pending_workers();
}


RDMAWorker::RDMAWorker(CephContext *c, unsigned i)
  : Worker(c, i), stack(nullptr),
    tx_handler(new C_handle_cq_tx(this)), lock("RDMAWorker::lock")
{
  // initialize perf_logger
  char name[128];
  sprintf(name, "AsyncMessenger::RDMAWorker-%u", id);
  PerfCountersBuilder plb(cct, name, l_msgr_rdma_first, l_msgr_rdma_last);

  plb.add_u64_counter(l_msgr_rdma_tx_no_mem, "tx_no_mem", "The count of no tx buffer");
  plb.add_u64_counter(l_msgr_rdma_tx_parital_mem, "tx_parital_mem", "The count of parital tx buffer");
  plb.add_u64_counter(l_msgr_rdma_tx_failed, "tx_failed_post", "The number of tx failed posted");
  plb.add_u64_counter(l_msgr_rdma_rx_no_registered_mem, "rx_no_registered_mem", "The count of no registered buffer when receiving");

  plb.add_u64_counter(l_msgr_rdma_tx_chunks, "tx_chunks", "The number of tx chunks transmitted");
  plb.add_u64_counter(l_msgr_rdma_tx_bytes, "tx_bytes", "The bytes of tx chunks transmitted");
  plb.add_u64_counter(l_msgr_rdma_rx_chunks, "rx_chunks", "The number of rx chunks transmitted");
  plb.add_u64_counter(l_msgr_rdma_rx_bytes, "rx_bytes", "The bytes of rx chunks transmitted");

  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);
}

RDMAWorker::~RDMAWorker()
{
  delete tx_handler;
}

void RDMAWorker::initialize()
{
  if (!dispatcher) {
    dispatcher = stack->get_dispatcher();
  }
}

int RDMAWorker::listen(entity_addr_t &sa, const SocketOptions &opt,ServerSocket *sock)
{
  global_infiniband->init();

  auto p = new RDMAServerConnTCP(cct, global_infiniband.get(), get_stack()->get_dispatcher(), this, sa);
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
  global_infiniband->init();

  RDMAConnectedSocketImpl* p = new RDMAConnectedSocketImpl(cct, global_infiniband.get(), get_stack()->get_dispatcher(), this);
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
  Device *ibdev = o->get_device();

  assert(center.in_thread());
  int r = ibdev->get_tx_buffers(c, bytes);
  assert(r >= 0);
  size_t got = ibdev->get_memory_manager()->get_tx_buffer_size() * r;
  ldout(cct, 30) << __func__ << " need " << bytes << " bytes, reserve " << got << " registered  bytes, inflight " << dispatcher->inflight << dendl;
  stack->get_dispatcher()->inflight += r;
  if (got == bytes)
    return r;

  if (o) {
    if (pending_sent_conns.back() != o)
      pending_sent_conns.push_back(o);
    dispatcher->make_pending_worker(this);
  }
  return r;
}


void RDMAWorker::handle_pending_message()
{
  ldout(cct, 20) << __func__ << " pending conns " << pending_sent_conns.size() << dendl;
  std::set<RDMAConnectedSocketImpl*> done;
  while (!pending_sent_conns.empty()) {
    RDMAConnectedSocketImpl *o = pending_sent_conns.front();
    pending_sent_conns.pop_front();
    if (!done.count(o)) {
      done.insert(o);
      ssize_t r = o->submit(false);
      ldout(cct, 20) << __func__ << " sent pending bl socket=" << o << " r=" << r << dendl;
      if (r < 0) {
        if (r == -EAGAIN) {
          pending_sent_conns.push_front(o);
          dispatcher->make_pending_worker(this);
          return ;
        }
        o->fault();
      }
    }
  }

  dispatcher->notify_pending_workers();
}

RDMAStack::RDMAStack(CephContext *cct, const string &t): NetworkStack(cct, t)
{
  //
  //On RDMA MUST be called before fork
  //
  int rc = ibv_fork_init();
  if (rc) {
     lderr(cct) << __func__ << " failed to call ibv_for_init(). On RDMA must be called before fork. Application aborts." << dendl;
     ceph_abort();
  }

  //Check ulimit
  struct rlimit limit;
  getrlimit(RLIMIT_MEMLOCK, &limit);
  if (limit.rlim_cur != RLIM_INFINITY || limit.rlim_max != RLIM_INFINITY) {
     lderr(cct) << __func__ << "!!! WARNING !!! For RDMA to work properly user memlock (ulimit -l) must be big enough to allow large amount of registered memory."
				  " We recommend setting this parameter to infinity" << dendl;
  }

  if (!global_infiniband)
    global_infiniband.construct(cct);
  ldout(cct, 20) << __func__ << " constructing RDMAStack..." << dendl;
  dispatcher = new RDMADispatcher(cct, this);
  global_infiniband->set_dispatcher(dispatcher);

  unsigned num = get_num_worker();
  for (unsigned i = 0; i < num; ++i) {
    RDMAWorker* w = dynamic_cast<RDMAWorker*>(get_worker(i));
    w->set_stack(this);
  }

  ldout(cct, 20) << " creating RDMAStack:" << this << " with dispatcher:" << dispatcher << dendl;
}

RDMAStack::~RDMAStack()
{
  delete dispatcher;
}

void RDMAStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
  threads.resize(i+1);
  threads[i] = std::thread(func);
}

void RDMAStack::join_worker(unsigned i)
{
  assert(threads.size() > i && threads[i].joinable());
  threads[i].join();
}
