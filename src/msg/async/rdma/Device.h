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

#ifndef CEPH_RDMA_DEVICE_H
#define CEPH_RDMA_DEVICE_H

#include <infiniband/verbs.h>

#include <string>
#include <vector>

#include "include/int_types.h"
#include "include/page.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/msg_types.h"
#include "msg/async/net_handler.h"
#include "common/Mutex.h"
#include "msg/async/Event.h"

typedef Infiniband::QueuePair QueuePair;
typedef Infiniband::CompletionChannel CompletionChannel;
typedef Infiniband::CompletionQueue CompletionQueue;
typedef Infiniband::ProtectionDomain ProtectionDomain;
typedef Infiniband::MemoryManager::Cluster Cluster;
typedef Infiniband::MemoryManager::Chunk Chunk;
typedef Infiniband::MemoryManager MemoryManager;

class Port {
  struct ibv_context* ctxt;
  int port_num;
  struct ibv_port_attr* port_attr;
  uint16_t lid;
  int gid_idx = 0;
  union ibv_gid gid;

 public:
  explicit Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn);
  ~Port();
  uint16_t get_lid() { return lid; }
  ibv_gid  get_gid() { return gid; }
  int get_port_num() { return port_num; }
  ibv_port_attr* get_port_attr() { return port_attr; }
  int get_gid_idx() { return gid_idx; }
};


class Device {
  class C_handle_cq_async : public EventCallback {
    Device *device;
  public:
    C_handle_cq_async(Device *d): device(d) {}
    void do_request(int fd) {
      device->handle_async_event();
    }
  };

  CephContext *cct;
  ibv_device *device;
  const char *name;

  Port **ports; // Array of Port objects. index is 1 based (IB port #1 is in
                // index 1). Index 0 is not used

  int port_cnt;

  uint32_t max_send_wr;
  uint32_t max_recv_wr;
  uint32_t max_sge;

  Mutex lock; // Protects from concurrent intialization of the device
  bool initialized = false;
  EventCallbackRef async_handler;
  Infiniband *infiniband;

  void verify_port(int port_num);

 public:
  explicit Device(CephContext *c, Infiniband *ib, ibv_device* d);
  ~Device();

  void init(int ibport = -1);
  void uninit();

  void handle_async_event();

  const char* get_name() const { return name;}

  Port *get_port(int ibport);
  uint16_t get_lid(int p) { return get_port(p)->get_lid(); }
  ibv_gid get_gid(int p) { return get_port(p)->get_gid(); }
  int get_gid_idx(int p) { return get_port(p)->get_gid_idx(); }

  QueuePair *create_queue_pair(int port,
			       ibv_qp_type type);
  ibv_srq* create_shared_receive_queue(uint32_t max_wr, uint32_t max_sge);
  CompletionChannel *create_comp_channel(CephContext *c);
  CompletionQueue *create_comp_queue(CephContext *c, CompletionChannel *cc=NULL);
  int post_chunk(Chunk* chunk);
  int post_channel_cluster();

  MemoryManager* get_memory_manager() { return memory_manager; }
  bool is_tx_buffer(const char* c) { return memory_manager->is_tx_buffer(c);}
  bool is_rx_buffer(const char* c) { return memory_manager->is_rx_buffer(c);}
  Chunk *get_tx_chunk_by_buffer(const char *c) { return memory_manager->get_tx_chunk_by_buffer(c); }
  int get_tx_buffers(std::vector<Chunk*> &c, size_t bytes);
  int poll_tx_cq(int n, ibv_wc *wc);
  int poll_rx_cq(int n, ibv_wc *wc);
  void rearm_cqs();

  struct ibv_context *ctxt;
  ibv_device_attr *device_attr;

  MemoryManager* memory_manager = nullptr;
  ibv_srq *srq = nullptr;
  Infiniband::CompletionQueue *rx_cq = nullptr;
  Infiniband::CompletionChannel *rx_cc = nullptr;
  Infiniband::CompletionQueue *tx_cq = nullptr;
  Infiniband::CompletionChannel *tx_cc = nullptr;
  ProtectionDomain *pd = nullptr;
};

inline ostream& operator<<(ostream& out, const Device &d)
{
    return out << d.get_name();
}


class DeviceList {
  CephContext *cct;
  struct ibv_device ** device_list;
  int num;
  Device** devices;

  unsigned last_poll_dev = 0;
  struct pollfd *poll_fds;

 public:
  DeviceList(CephContext *cct, Infiniband *ib);
  ~DeviceList();

  Device* get_device(const char* device_name);
  Device* get_device(const struct ibv_context *ctxt);

  void uninit();

  void rearm_notify();
  int poll_tx(int n, Device **d, ibv_wc *wc);
  int poll_rx(int n, Device **d, ibv_wc *wc);
  int poll_blocking(bool &done);

  void handle_async_event();
};

#endif
