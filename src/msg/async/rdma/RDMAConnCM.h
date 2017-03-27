// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Mellanox Ltd.
 *
 * Author: Amir Vadai <amirva@mellanox.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_RDMA_CONNECTED_SOCKET_CM_H
#define CEPH_MSG_RDMA_CONNECTED_SOCKET_CM_H

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/async/Stack.h"
#include "Infiniband.h"
#include "RDMAConnectedSocketImpl.h"

#include <queue>

class RDMAConnCM;

class CMHandler : public RDMADisconnector {
  class C_handle_cm_event : public EventCallback {
    CMHandler *cm;
  public:
    C_handle_cm_event(CMHandler *cm): cm(cm) {}
    void do_request(int fd) {
      cm->handle_cm_event();
    }
  };

public:
  struct rdma_cm_id *id;

  CMHandler(CephContext *cct, RDMAConnCM *s, RDMAWorker *w, struct rdma_cm_id *_id = NULL);
  virtual ~CMHandler();
  void put();

  void handle_cm_event(int fd = -1);
  virtual void disconnect() override;
  void set_orphan() { csc = nullptr; };
  RDMAConnCM *get_socket() { return csc; };

  virtual ostream &print(ostream &out) override {
    return out << "cm_id: { " <<
      " id: " << id <<
      " channel->fd: " << (id->channel ? id->channel->fd : -1) <<
      "}";
  };

private:
  CephContext *cct;
  RDMAConnCM *csc;
  RDMAWorker *worker;
  rdma_event_channel *channel;

 /* We need ref count because, CMHandler need to be alive until both events
  * happened: last cm event and IBV_EVENT_QP_LAST_WQE_REACHED.
  * last cm event is RDMA_CM_EVENT_DISCONNECTED on active side or
  * RDMA_CM_EVENT_TIMEWAIT_EXIT on passive side.
  * Only after LAST_WQE_REACHED destroy_qp() could be called, which can't be
  * called after rdma_destroy_id()
  */
  atomic_t refs;

  EventCallbackRef cm_handler;
};

inline ostream &operator<<(ostream &out, CMHandler &cm)
{
  return cm.print(out);
}


class RDMAConnCM : public RDMAConnectedSocketImpl  {
public:
  RDMAConnCM(CephContext *cct, Infiniband *ib,
			    RDMADispatcher* s, RDMAWorker *w);
  RDMAConnCM(CephContext *cct, Infiniband *ib,
                        RDMADispatcher* s, RDMAWorker *w,
                        struct rdma_cm_id *cma_id,
                        uint32_t peer_qpn);
  virtual ~RDMAConnCM();

  virtual void fin(); 
  virtual int try_connect(const entity_addr_t&, const SocketOptions &opt) override;
  int alloc_resources();
  virtual void shutdown() override;
  virtual struct sockaddr *get_peer_addr();
  virtual struct sockaddr *get_my_addr();

  void cm_addr_resolved();
  void cm_route_resolved();
  void cm_established(uint32_t qpn);
  void cm_disconnected();
  void cm_rejected();
  void cm_error();
  virtual int remote_qpn() override;
  virtual void cleanup() override;
  virtual int activate() override;

  virtual ostream &print(ostream &o) override;

private:
  void handle_cm_event();

  CMHandler *cm_handler;
  Infiniband *ib;
  uint32_t peer_qpn = 0;
};


class RDMAServerConnCM : public RDMAServerSocketImpl {
  struct rdma_event_channel *channel;
  struct rdma_cm_id *listen_id;

 public:
  RDMAServerConnCM(CephContext *cct, Infiniband *ib, RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a);
  ~RDMAServerConnCM();

  int listen(entity_addr_t &sa, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override;
  virtual int fd() const override;
  void handle_cm_event();

 private:
  int on_connect_request(struct rdma_cm_event *event);
  EventCallbackRef cm_handler;

  int accept_fd;
  Mutex lock; // protect accepted_sockets
  std::queue<RDMAConnCM *> accepted_sockets;

  class C_handle_cm_event : public EventCallback {
    RDMAServerConnCM *ss;
  public:
    C_handle_cm_event(RDMAServerConnCM *ss): ss(ss) {}
    void do_request(int fd) {
      ss->handle_cm_event();
    }
  };
};

#endif
