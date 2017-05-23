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

class Port {
  struct ibv_context* ctxt;
  int port_num;
  struct ibv_port_attr* port_attr;
  uint16_t lid;
  int gid_idx;
  union ibv_gid gid;

 public:
  explicit Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn);
  uint16_t get_lid() { return lid; }
  ibv_gid  get_gid() { return gid; }
  int get_port_num() { return port_num; }
  ibv_port_attr* get_port_attr() { return port_attr; }
  int get_gid_idx() { return gid_idx; }
};


class Device {
  ibv_device *device;
  const char* name;
  uint8_t  port_cnt;
 public:
  explicit Device(CephContext *c, ibv_device* d);
  ~Device();

  const char* get_name() { return name;}
  uint16_t get_lid() { return active_port->get_lid(); }
  ibv_gid get_gid() { return active_port->get_gid(); }
  int get_gid_idx() { return active_port->get_gid_idx(); }
  void binding_port(CephContext *c, int port_num);
  struct ibv_context *ctxt;
  ibv_device_attr *device_attr;
  Port* active_port;
};


class DeviceList {
  struct ibv_device ** device_list;
  int num;
  Device** devices;
 public:
  DeviceList(CephContext *cct);
  ~DeviceList();

  Device* get_device(const char* device_name);
};

#endif
