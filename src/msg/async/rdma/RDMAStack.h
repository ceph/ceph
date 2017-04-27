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

#ifndef CEPH_MSG_RDMASTACK_H
#define CEPH_MSG_RDMASTACK_H

#include <sys/eventfd.h>

#include <list>
#include <vector>
#include <thread>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"
#include "Infiniband.h"
#include "RDMAConnectedSocketImpl.h"

class RDMAConnectedSocketImpl;
class RDMAServerSocketImpl;
class RDMAStack;
class RDMAWorker;

enum {
  l_msgr_rdma_dispatcher_first = 94000,

  l_msgr_rdma_polling,
  l_msgr_rdma_inflight_tx_chunks,
  l_msgr_rdma_inqueue_rx_chunks,

  l_msgr_rdma_tx_total_wc,
  l_msgr_rdma_tx_total_wc_errors,
  l_msgr_rdma_tx_wc_retry_errors,
  l_msgr_rdma_tx_wc_wr_flush_errors,

  l_msgr_rdma_rx_total_wc,
  l_msgr_rdma_rx_total_wc_errors,
  l_msgr_rdma_rx_fin,

  l_msgr_rdma_handshake_errors,

  l_msgr_rdma_total_async_events,
  l_msgr_rdma_async_last_wqe_events,

  l_msgr_rdma_created_queue_pair,
  l_msgr_rdma_active_queue_pair,

  l_msgr_rdma_dispatcher_last,
};


class RDMADispatcher {
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::QueuePair QueuePair;

  std::thread t;
  CephContext *cct;
  bool done = false;
  std::atomic<uint64_t> num_dead_queue_pair = {0};
  std::atomic<uint64_t> num_qp_conn = {0};
  Mutex lock; // protect `qp_conns`, `dead_queue_pairs`
  // qp_num -> InfRcConnection
  // The main usage of `qp_conns` is looking up connection by qp_num,
  // so the lifecycle of element in `qp_conns` is the lifecycle of qp.
  //// make qp queue into dead state
  /**
   * 1. Connection call mark_down
   * 2. Move the Queue Pair into the Error state(QueuePair::to_dead)
   * 3. Wait for the affiliated event IBV_EVENT_QP_LAST_WQE_REACHED(handle_async_event)
   * 4. Wait for CQ to be empty(handle_tx_event)
   * 5. Destroy the QP by calling ibv_destroy_qp()(handle_tx_event)
   *
   * @param qp The qp needed to dead
   */
  ceph::unordered_map<uint32_t, std::pair<QueuePair*, RDMAConnectedSocketImpl*> > qp_conns;

  /// if a queue pair is closed when transmit buffers are active
  /// on it, the transmit buffers never get returned via tx_cq.  To
  /// work around this problem, don't delete queue pairs immediately. Instead,
  /// save them in this vector and delete them at a safe time, when there are
  /// no outstanding transmit buffers to be lost.
  std::vector<QueuePair*> dead_queue_pairs;

  std::atomic<uint64_t> num_pending_workers = {0};
  Mutex w_lock; // protect pending workers
  // fixme: lockfree
  std::list<RDMAWorker*> pending_workers;
  RDMAStack* stack;

 public:
  PerfCounters *perf_logger;

  explicit RDMADispatcher(CephContext* c, RDMAStack* s);
  virtual ~RDMADispatcher();

  void process_async_event(Device *ibdev, ibv_async_event &async_event);

  void polling_start();
  void polling_stop();
  void polling();

  int register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi);
  void make_pending_worker(RDMAWorker* w) {
    Mutex::Locker l(w_lock);
    if (pending_workers.back() != w) {
      pending_workers.push_back(w);
      ++num_pending_workers;
    }
  }
  RDMAStack* get_stack() { return stack; }
  RDMAConnectedSocketImpl* get_conn_lockless(uint32_t qp);
  void erase_qpn_lockless(uint32_t qpn);
  void erase_qpn(uint32_t qpn);
  void notify_pending_workers();
  void handle_tx_event(Device *ibdev, ibv_wc *cqe, int n);
  void post_tx_buffer(Device *ibdev, std::vector<Chunk*> &chunks);

  std::atomic<uint64_t> inflight = {0};
};


enum {
  l_msgr_rdma_first = 95000,

  l_msgr_rdma_tx_no_mem,
  l_msgr_rdma_tx_parital_mem,
  l_msgr_rdma_tx_failed,
  l_msgr_rdma_rx_no_registered_mem,

  l_msgr_rdma_tx_chunks,
  l_msgr_rdma_tx_bytes,
  l_msgr_rdma_rx_chunks,
  l_msgr_rdma_rx_bytes,

  l_msgr_rdma_last,
};

class RDMAWorker : public Worker {
  typedef Infiniband::CompletionQueue CompletionQueue;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::MemoryManager MemoryManager;
  typedef std::vector<Chunk*>::iterator ChunkIter;
  RDMAStack *stack;
  EventCallbackRef tx_handler;
  std::list<RDMAConnectedSocketImpl*> pending_sent_conns;
  RDMADispatcher* dispatcher = nullptr;
  Mutex lock;

  class C_handle_cq_tx : public EventCallback {
    RDMAWorker *worker;
    public:
    C_handle_cq_tx(RDMAWorker *w): worker(w) {}
    void do_request(int fd) {
      worker->handle_pending_message();
    }
  };

 public:
  PerfCounters *perf_logger;
  explicit RDMAWorker(CephContext *c, unsigned i);
  virtual ~RDMAWorker();
  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
  RDMAStack *get_stack() { return stack; }
  int get_reged_mem(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes);
  void remove_pending_conn(RDMAConnectedSocketImpl *o) {
    assert(center.in_thread());
    pending_sent_conns.remove(o);
  }
  void handle_pending_message();
  void set_stack(RDMAStack *s) { stack = s; }
  void notify_worker() {
    center.dispatch_event_external(tx_handler);
  }
};


class RDMAStack : public NetworkStack {
  vector<std::thread> threads;
  RDMADispatcher *dispatcher;
  PerfCounters *perf_counter;

  std::atomic<bool> fork_finished = {false};

 public:
  explicit RDMAStack(CephContext *cct, const string &t);
  virtual ~RDMAStack();
  virtual bool support_zero_copy_read() const override { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return false; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;
  RDMADispatcher *get_dispatcher() { return dispatcher; }

  virtual bool is_ready() override { return fork_finished.load(); };
  virtual void ready() override { fork_finished = true; };
};
#endif
