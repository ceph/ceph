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

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"
#include <thread>
#include "Infiniband.h"

class RDMAConnectedSocketImpl;

class RDMAWorker : public Worker {
  typedef Infiniband::CompletionQueue CompletionQueue;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::MemoryManager MemoryManager;
  int client_setup_socket;
  Infiniband* infiniband;
  CompletionQueue* tx_cq;           // common completion queue for all transmits
  CompletionChannel* tx_cc;
  EventCallbackRef tx_handler;
  MemoryManager* memory_manager;
  vector<RDMAConnectedSocketImpl*> to_delete;
  class C_handle_cq_tx : public EventCallback {
    RDMAWorker *worker;
    public:
    C_handle_cq_tx(RDMAWorker *w): worker(w) {}
    void do_request(int fd) {
      worker->handle_tx_event();
    }
  };

 public:
  explicit RDMAWorker(CephContext *c, unsigned i)
    : Worker(c, i), infiniband(NULL), tx_handler(new C_handle_cq_tx(this)) {}
  virtual ~RDMAWorker() {
    tx_cc->ack_events();
    delete tx_cq;
    delete tx_cc;
    delete tx_handler;
  }

  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  virtual void initialize() override;
  void handle_tx_event();
  CompletionQueue* get_tx_cq() { return tx_cq; }
  void remove_to_delete(RDMAConnectedSocketImpl* csi) {
    if (to_delete.empty())
      return ;
    vector<RDMAConnectedSocketImpl*>::iterator iter = to_delete.begin();
    for (; iter != to_delete.end(); ++iter) {
      if(csi == *iter) {
        to_delete.erase(iter);
      }
    }
  }
  void add_to_delete(RDMAConnectedSocketImpl* csi) {
    to_delete.push_back(csi);
  }
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
  Infiniband* infiniband;
  RDMAWorker* worker;
  std::vector<Chunk*> buffers;
  CompletionChannel* rx_cc;
  CompletionQueue* rx_cq;
  bool wait_close;

 public:
  RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMAWorker* w, IBSYNMsg im = IBSYNMsg()) : cct(cct), peer_msg(im), infiniband(ib), worker(w), wait_close(false) {
    qp = infiniband->create_queue_pair(IBV_QPT_RC);
    rx_cq = qp->get_rx_cq();
    rx_cc = rx_cq->get_cc();
    my_msg.qpn = qp->get_local_qp_number();
    my_msg.psn = qp->get_initial_psn();
    my_msg.lid = infiniband->get_lid();
    my_msg.gid = infiniband->get_gid();
  }

  virtual int is_connected() override {
    return connected;
  }
  virtual ssize_t read(char* buf, size_t len) override;
  virtual ssize_t zero_copy_read(bufferptr &data) override;
  virtual ssize_t send(bufferlist &bl, bool more) override;
  virtual void shutdown() override {
  }
  virtual void close() override {
    if (!wait_close) {
      fin();
      worker->add_to_delete(this);
    } else {
      clear_all();
    }
  }
  virtual int fd() const override {
    return rx_cc->get_fd();
  }
  void clear_all() {
    delete qp;
    rx_cc->ack_events();
    delete rx_cq;
    rx_cq = NULL;
    if (!wait_close)
      worker->remove_to_delete(this);
  }
  int activate();
  ssize_t read_buffers(char* buf, size_t len);
  int poll_cq(int num_entries, ibv_wc *ret_wc_array);
  IBSYNMsg get_my_msg() { return my_msg; }
  IBSYNMsg get_peer_msg() { return peer_msg; }
  void set_peer_msg(IBSYNMsg m) { peer_msg = m ;}
  int post_work_request(std::vector<Chunk*>&);
  void fin();
};


class RDMAServerSocketImpl : public ServerSocketImpl {
  CephContext *cct;
  NetHandler net;
  int server_setup_socket;
  Infiniband* infiniband;
  entity_addr_t sa;
 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, entity_addr_t& a)
    : cct(cct), net(cct), infiniband(i), sa(a) {}
  int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out) override;
  virtual void abort_accept() override {}
  virtual int fd() const override {
    return server_setup_socket;
  }
};


class RDMAStack : public NetworkStack {
  vector<std::thread> threads;

 public:
  explicit RDMAStack(CephContext *cct, const string &t);
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
};

#endif
