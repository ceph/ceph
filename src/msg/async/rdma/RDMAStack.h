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
class RDMAServerSocketImpl;
class RDMAStack;
class RDMAWorker;

class RDMADispatcher {
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::QueuePair QueuePair;

  std::thread t;
  CephContext *cct;
  Infiniband::CompletionQueue* tx_cq = nullptr;
  Infiniband::CompletionQueue* rx_cq = nullptr;
  Infiniband::CompletionChannel *tx_cc = nullptr, *rx_cc = nullptr;
  EventCallbackRef async_handler;
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

  class C_handle_cq_async : public EventCallback {
    RDMADispatcher *dispatcher;
   public:
    explicit C_handle_cq_async(RDMADispatcher *w): dispatcher(w) {}
    void do_request(uint64_t fd) {
      // worker->handle_tx_event();
      dispatcher->handle_async_event();
    }
  };

 public:
  PerfCounters *perf_logger;

  explicit RDMADispatcher(CephContext* c, RDMAStack* s);
  virtual ~RDMADispatcher();
  void handle_async_event();

  void polling_start();
  void polling_stop();
  void polling();
  void register_qp(QueuePair *qp, RDMAConnectedSocketImpl* csi);
  void make_pending_worker(RDMAWorker* w) {
    Mutex::Locker l(w_lock);
    auto it = std::find(pending_workers.begin(), pending_workers.end(), w);
    if (it != pending_workers.end())
      return;
    pending_workers.push_back(w);
    ++num_pending_workers;
  }
  RDMAStack* get_stack() { return stack; }
  RDMAConnectedSocketImpl* get_conn_lockless(uint32_t qp);
  QueuePair* get_qp(uint32_t qp);
  void erase_qpn_lockless(uint32_t qpn);
  void erase_qpn(uint32_t qpn);
  Infiniband::CompletionQueue* get_tx_cq() const { return tx_cq; }
  Infiniband::CompletionQueue* get_rx_cq() const { return rx_cq; }
  void notify_pending_workers();
  void handle_tx_event(ibv_wc *cqe, int n);
  void post_tx_buffer(std::vector<Chunk*> &chunks);

  std::atomic<uint64_t> inflight = {0};

  void post_chunk_to_pool(Chunk* chunk);
  int post_chunks_to_rq(int num, ibv_qp *qp=NULL);
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
    explicit C_handle_cq_tx(RDMAWorker *w): worker(w) {}
    void do_request(uint64_t fd) {
      worker->handle_pending_message();
    }
  };

 public:
  PerfCounters *perf_logger;
  explicit RDMAWorker(CephContext *c, unsigned i);
  virtual ~RDMAWorker();
  virtual int listen(entity_addr_t &addr,
		     unsigned addr_slot,
		     const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
  RDMAStack *get_stack() { return stack; }
  int get_reged_mem(RDMAConnectedSocketImpl *o, std::vector<Chunk*> &c, size_t bytes);
  void remove_pending_conn(RDMAConnectedSocketImpl *o) {
    ceph_assert(center.in_thread());
    pending_sent_conns.remove(o);
  }
  void handle_pending_message();
  void set_stack(RDMAStack *s) { stack = s; }
  void notify_worker() {
    center.dispatch_event_external(tx_handler);
  }
};

struct RDMACMInfo {
  RDMACMInfo(rdma_cm_id *cid, rdma_event_channel *cm_channel_, uint32_t qp_num_)
    : cm_id(cid), cm_channel(cm_channel_), qp_num(qp_num_) {}
  rdma_cm_id *cm_id;
  rdma_event_channel *cm_channel;
  uint32_t qp_num;
};

class RDMAConnectedSocketImpl : public ConnectedSocketImpl {
 public:
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::CompletionQueue CompletionQueue;

 protected:
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
  int notify_fd = -1;
  bufferlist pending_bl;

  Mutex lock;
  std::vector<ibv_wc> wc;
  bool is_server;
  EventCallbackRef con_handler;
  int tcp_fd = -1;
  bool active;// qp is active ?
  bool pending;
  int post_backlog = 0;

  void notify();
  ssize_t read_buffers(char* buf, size_t len);
  int post_work_request(std::vector<Chunk*>&);

 public:
  RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
                          RDMAWorker *w);
  virtual ~RDMAConnectedSocketImpl();

  void pass_wc(std::vector<ibv_wc> &&v);
  void get_wc(std::vector<ibv_wc> &w);
  virtual int is_connected() override { return connected; }

  virtual ssize_t read(char* buf, size_t len) override;
  virtual ssize_t zero_copy_read(bufferptr &data) override;
  virtual ssize_t send(bufferlist &bl, bool more) override;
  virtual void shutdown() override;
  virtual void close() override;
  virtual int fd() const override { return notify_fd; }
  virtual int socket_fd() const override { return tcp_fd; }
  void fault();
  const char* get_qp_state() { return Infiniband::qp_state_string(qp->get_state()); }
  ssize_t submit(bool more);
  int activate();
  void fin();
  void handle_connection();
  void cleanup();
  void set_accept_fd(int sd);
  virtual int try_connect(const entity_addr_t&, const SocketOptions &opt);
  bool is_pending() {return pending;}
  void set_pending(bool val) {pending = val;}
  void post_chunks_to_rq(int num);
  void update_post_backlog();

  class C_handle_connection : public EventCallback {
    RDMAConnectedSocketImpl *csi;
    bool active;
   public:
    explicit C_handle_connection(RDMAConnectedSocketImpl *w): csi(w), active(true) {}
    void do_request(uint64_t fd) {
      if (active)
        csi->handle_connection();
    }
    void close() {
      active = false;
    }
  };
};

enum RDMA_CM_STATUS {
  IDLE = 1,
  RDMA_ID_CREATED,
  CHANNEL_FD_CREATED,
  RESOURCE_ALLOCATED,
  ADDR_RESOLVED,
  ROUTE_RESOLVED,
  CONNECTED,
  DISCONNECTED,
  ERROR
};

class RDMAIWARPConnectedSocketImpl : public RDMAConnectedSocketImpl {
  public:
    RDMAIWARPConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
                          RDMAWorker *w, RDMACMInfo *info = nullptr);
    ~RDMAIWARPConnectedSocketImpl();
    virtual int try_connect(const entity_addr_t&, const SocketOptions &opt) override;
    virtual void close() override;
    virtual void shutdown() override;
    virtual void handle_cm_connection();
    uint32_t get_local_qpn() const { return local_qpn; }
    void activate();
    int alloc_resource();
    void close_notify();

  private:
    rdma_cm_id *cm_id;
    rdma_event_channel *cm_channel;
    uint32_t local_qpn;
    uint32_t remote_qpn;
    EventCallbackRef cm_con_handler;
    bool is_server;
    std::mutex close_mtx;
    std::condition_variable close_condition;
    bool closed;
    RDMA_CM_STATUS status;


  class C_handle_cm_connection : public EventCallback {
    RDMAIWARPConnectedSocketImpl *csi;
    public:
      C_handle_cm_connection(RDMAIWARPConnectedSocketImpl *w): csi(w) {}
      void do_request(uint64_t fd) {
        csi->handle_cm_connection();
      }
  };
};

class RDMAServerSocketImpl : public ServerSocketImpl {
  protected:
    CephContext *cct;
    NetHandler net;
    int server_setup_socket;
    Infiniband* infiniband;
    RDMADispatcher *dispatcher;
    RDMAWorker *worker;
    entity_addr_t sa;

 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, RDMADispatcher *s,
		       RDMAWorker *w, entity_addr_t& a, unsigned slot);

  virtual int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override;
  virtual int fd() const override { return server_setup_socket; }
  int get_fd() { return server_setup_socket; }
};

class RDMAIWARPServerSocketImpl : public RDMAServerSocketImpl {
  public:
    RDMAIWARPServerSocketImpl(
      CephContext *cct, Infiniband *i, RDMADispatcher *s, RDMAWorker *w,
      entity_addr_t& addr, unsigned addr_slot);
    virtual int listen(entity_addr_t &sa, const SocketOptions &opt) override;
    virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
    virtual void abort_accept() override;
  private:
    rdma_cm_id *cm_id;
    rdma_event_channel *cm_channel;
};

class RDMAStack : public NetworkStack {
  vector<std::thread> threads;
  PerfCounters *perf_counter;
  Infiniband ib;
  RDMADispatcher dispatcher;

  std::atomic<bool> fork_finished = {false};

 public:
  explicit RDMAStack(CephContext *cct, const string &t);
  virtual ~RDMAStack();
  virtual bool support_zero_copy_read() const override { return false; }
  virtual bool nonblock_connect_need_writable_event() const override { return false; }

  virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;
  RDMADispatcher &get_dispatcher() { return dispatcher; }
  Infiniband &get_infiniband() { return ib; }
  virtual bool is_ready() override { return fork_finished.load(); };
  virtual void ready() override { fork_finished = true; };
};


#endif
