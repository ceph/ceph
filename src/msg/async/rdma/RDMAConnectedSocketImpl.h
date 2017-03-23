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

typedef Infiniband::QueuePair QueuePair;

class RDMADisconnector {
public:
  virtual ~RDMADisconnector() { };
  virtual void disconnect() = 0;
  virtual ostream &print(ostream &o) = 0;
};

inline ostream& operator<<(ostream& out, RDMADisconnector &d)
{
  return d.print(out);
}


class RDMAConnectedSocketImpl : public ConnectedSocketImpl {
protected:
  CephContext *cct;

  Infiniband* infiniband;
  RDMADispatcher* dispatcher;
  RDMAWorker* worker;
  Device *ibdev;
  int ibport;
  Infiniband::QueuePair *qp;
  bool is_server;
  bool active;// qp is active ?
  int connected;
  int error;
  Mutex lock;
  std::vector<ibv_wc> wc;

  void register_qp(QueuePair *qp);
  void notify();
  void fin(uint64_t wr_id);

 public:
  typedef Infiniband::MemoryManager::Chunk Chunk;
  typedef Infiniband::CompletionChannel CompletionChannel;
  typedef Infiniband::CompletionQueue CompletionQueue;

 private:
  std::vector<Chunk*> buffers;
  int notify_fd = -1;
  bufferlist pending_bl;

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
  virtual void fin() = 0;
  virtual void cleanup() = 0;
  virtual int try_connect(const entity_addr_t&, const SocketOptions &opt) = 0;
  virtual ssize_t submit(bool more);
  virtual int remote_qpn() = 0;
  int local_qpn() { return qp ? qp->get_local_qp_number() : -1; };

  virtual ostream &print(ostream &o) = 0;
};

class RDMAServerSocketImpl : public ServerSocketImpl {
protected:
  CephContext *cct;
  Device *ibdev;
  int ibport;
  Infiniband* infiniband;
  RDMADispatcher *dispatcher;
  RDMAWorker *worker;
  entity_addr_t sa;

 public:
  RDMAServerSocketImpl(CephContext *cct, Infiniband* i, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a);

  virtual int listen(entity_addr_t &sa, const SocketOptions &opt) = 0;
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) = 0;
  virtual void abort_accept() = 0;
  virtual int fd() const = 0;
};

inline ostream& operator<<(ostream& out, RDMAConnectedSocketImpl &s)
{
  return s.print(out);
}

#endif

