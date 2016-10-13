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

class RDMAConnectedSocketImpl;
class RDMAStack;

class RDMADispatcher {
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::QueuePair QueuePair;

  std::thread t;
  CephContext *cct;
  Infiniband* ib;
  Infiniband::CompletionQueue* rx_cq;           // common completion queue for all transmits
  Infiniband::CompletionChannel* rx_cc;
  EventCallbackRef async_handler;
  bool done = false;
  Mutex lock; // protect `qp_conns
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

  class C_handle_cq_async : public EventCallback {
    RDMADispatcher *dispatcher;
   public:
    C_handle_cq_async(RDMADispatcher *w): dispatcher(w) {}
    void do_request(int fd) {
      // worker->handle_tx_event();
      dispatcher->handle_async_event();
    }
  };

 public:
  std::atomic_ulong inflight;
  explicit RDMADispatcher(CephContext* cct, Infiniband* i)
    : t(&RDMADispatcher::polling, this), cct(cct), ib(i), async_handler(new C_handle_cq_async(this)), lock("RDMADispatcher::lock") {
    rx_cc = ib->create_comp_channel();
    assert(rx_cc);
    rx_cq = ib->create_comp_queue(rx_cc);
    assert(rx_cq);
  }
  virtual ~RDMADispatcher();

  void handle_async_event();
  void polling();
  int register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi) {
    int fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
    assert(fd >= 0);
    Mutex::Locker l(lock);
    assert(!qp_conns.count(qp->get_local_qp_number()));
    qp_conns[qp->get_local_qp_number()] = std::make_pair(qp, csi);
    return fd;
  }
  RDMAConnectedSocketImpl* get_conn_by_qp(uint32_t qp) {
    Mutex::Locker l(lock);
    auto it = qp_conns.find(qp);
    if (it == qp_conns.end())
      return NULL;
    if (it->second.first->is_dead())
      return NULL;
    return it->second.second;
  }
  void erase_qpn(uint32_t qpn) {
    Mutex::Locker l(lock);
    auto it = qp_conns.find(qpn);
    if (it == qp_conns.end())
      return ;
    dead_queue_pairs.push_back(it->second.first);
    qp_conns.erase(it);
  }
};


class RDMAWorker : public Worker {
  typedef Infiniband::CompletionQueue CompletionQueue;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::MemoryManager MemoryManager;
  RDMAStack *stack;
  Infiniband *infiniband;
  CompletionQueue *tx_cq;           // common completion queue for all transmits
  CompletionChannel *tx_cc;
  EventCallbackRef tx_handler;
  MemoryManager *memory_manager;
  std::list<RDMAConnectedSocketImpl*> pending_sent_conns;

  class C_handle_cq_tx : public EventCallback {
    RDMAWorker *worker;
    public:
    C_handle_cq_tx(RDMAWorker *w): worker(w) {}
    void do_request(int fd) {
      worker->handle_tx_event();
    }
  };

 public:
  explicit RDMAWorker(CephContext *c, unsigned i);
  virtual ~RDMAWorker() {
    tx_cc->ack_events();
    delete tx_cq;
    delete tx_cc;
    delete tx_handler;
  }

  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
  CompletionQueue* get_tx_cq() { return tx_cq; }
  RDMAStack *get_stack() {
    return stack;
  }
  int reserve_message_buffer(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes);
  int post_tx_buffer(std::vector<Chunk*> &chunks);
  void remove_pending_conn(RDMAConnectedSocketImpl *o) {
    pending_sent_conns.remove(o);
  }
  void handle_tx_event();
};

class RDMAConnectedSocketImpl : public ConnectedSocketImpl {
 public:
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::CompletionQueue CompletionQueue;

 private:
  CephContext *cct;
  Infiniband::QueuePair *qp;
  IBSYNMsg peer_msg;
  IBSYNMsg my_msg;
  int connected;
  int error;
  Infiniband* infiniband;
  RDMADispatcher* dispatcher;
  RDMAWorker* worker;
  std::vector<Chunk*> buffers;
  int notify_fd;
  bufferlist pending_bl;

  Mutex lock;
  std::vector<ibv_wc> wc;

  void notify() {
    uint64_t i = 1;
    assert(write(notify_fd, &i, sizeof(i)) == sizeof(i));
  }
  ssize_t read_buffers(char* buf, size_t len);
  int post_work_request(std::vector<Chunk*>&);

 public:
  RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
                          RDMAWorker *w, IBSYNMsg im = IBSYNMsg())
    : cct(cct), peer_msg(im), connected(0), error(0), infiniband(ib),
      dispatcher(s), worker(w), lock("RDMAConnectedSocketImpl::lock") {
    qp = infiniband->create_queue_pair(w->get_tx_cq(), IBV_QPT_RC);
    my_msg.qpn = qp->get_local_qp_number();
    my_msg.psn = qp->get_initial_psn();
    my_msg.lid = infiniband->get_lid();
    my_msg.gid = infiniband->get_gid();
    notify_fd = dispatcher->register_qp(qp, this);
  }
  virtual ~RDMAConnectedSocketImpl() {
    worker->remove_pending_conn(this);
  }

  void pass_wc(std::vector<ibv_wc> &&v) {
    Mutex::Locker l(lock);
    if (wc.empty())
      wc = std::move(v);
    else
      wc.insert(wc.end(), v.begin(), v.end());
    notify();
  }
  void get_wc(std::vector<ibv_wc> &w) {
    Mutex::Locker l(lock);
    if (wc.empty())
      return ;
    w.swap(wc);
  }
  virtual int is_connected() override {
    return connected;
  }
  virtual ssize_t read(char* buf, size_t len) override;
  virtual ssize_t zero_copy_read(bufferptr &data) override;
  virtual ssize_t send(bufferlist &bl, bool more) override;
  virtual void shutdown() override {
    if (qp) {
      qp->to_dead();
      qp = NULL;
    }
  }
  virtual void close() override {
    if (qp) {
      qp->to_dead();
      qp = NULL;
    }
  }
  virtual int fd() const override {
    return notify_fd;
  }
  void fault() {
    if (qp) {
      qp->to_dead();
      qp = NULL;
    }
    error = ECONNRESET;
    notify();
  }
  ssize_t submit(bool more);
  int activate();
  IBSYNMsg get_my_msg() { return my_msg; }
  void set_peer_msg(IBSYNMsg m) { peer_msg = m ;}
};


class RDMAServerSocketImpl : public ServerSocketImpl {
  CephContext *cct;
  NetHandler net;
  int server_setup_socket;
  Infiniband* infiniband;
  RDMADispatcher *dispatcher;
  RDMAWorker *worker;
  entity_addr_t sa;

 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a)
    : cct(cct), net(cct), server_setup_socket(-1), infiniband(i), dispatcher(s), worker(w), sa(a) {}
  int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out) override;
  virtual void abort_accept() override {
    if (server_setup_socket >= 0)
      ::close(server_setup_socket);
  }
  virtual int fd() const override {
    return server_setup_socket;
  }
};


class RDMAStack : public NetworkStack {
  vector<std::thread> threads;
  RDMADispatcher *dispatcher;

 public:
  explicit RDMAStack(CephContext *cct, const string &t);
  virtual ~RDMAStack() { delete dispatcher; }
  virtual bool support_zero_copy_read() const override { return true; }
  //virtual bool support_local_listen_table() const { return true; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override {
    threads.resize(i+1);
    threads[i] = std::move(std::thread(func));
  }
  virtual void join_worker(unsigned i) override {
    assert(threads.size() > i && threads[i].joinable());
    threads[i].join();
  }
  RDMADispatcher *get_dispatcher() { return dispatcher; }
};

#endif
