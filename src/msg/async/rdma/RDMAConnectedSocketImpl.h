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

#ifndef CEPH_MSG_RDMA_CONNECTED_SOCKET_IMPL_H
#define CEPH_MSG_RDMA_CONNECTED_SOCKET_IMPL_H

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"
#include "Infiniband.h"

class RDMAWorker;
class RDMADispatcher;

class RDMAConnectedSocketImpl : public ConnectedSocketImpl {
 public:
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::CompletionQueue CompletionQueue;

 private:
  CephContext *cct;
  Infiniband::QueuePair *qp;
  Device *ibdev;
  int ibport;
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

  void notify();
  ssize_t read_buffers(char* buf, size_t len);
  int post_work_request(std::vector<Chunk*>&);

 public:
  RDMAConnectedSocketImpl(CephContext *cct, Infiniband* ib, RDMADispatcher* s,
                          RDMAWorker *w);
  virtual ~RDMAConnectedSocketImpl();

  Device *get_device() { return ibdev; }

  void pass_wc(std::vector<ibv_wc> &&v);
  void get_wc(std::vector<ibv_wc> &w);
  virtual int is_connected() override { return connected; }

  virtual ssize_t read(char* buf, size_t len) override;
  virtual ssize_t zero_copy_read(bufferptr &data) override;
  virtual ssize_t send(bufferlist &bl, bool more) override;
  virtual void shutdown() override;
  virtual void close() override;
  virtual int fd() const override { return notify_fd; }
  void fault();
  const char* get_qp_state() { return Infiniband::qp_state_string(qp->get_state()); }
  ssize_t submit(bool more);
  int activate();
  void fin();
  void handle_connection();
  void cleanup();
  void set_accept_fd(int sd);
  int try_connect(const entity_addr_t&, const SocketOptions &opt);

  class C_handle_connection : public EventCallback {
    RDMAConnectedSocketImpl *csi;
    bool active;
   public:
    C_handle_connection(RDMAConnectedSocketImpl *w): csi(w), active(true) {}
    void do_request(int fd) {
      if (active)
        csi->handle_connection();
    }
    void close() {
      active = false;
    }
  };
};

class RDMAServerSocketImpl : public ServerSocketImpl {
  CephContext *cct;
  Device *ibdev;
  int ibport;
  NetHandler net;
  int server_setup_socket;
  Infiniband* infiniband;
  RDMADispatcher *dispatcher;
  RDMAWorker *worker;
  entity_addr_t sa;

 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a);

  int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override;
  virtual int fd() const override { return server_setup_socket; }
  int get_fd() { return server_setup_socket; }
};

#endif

