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

#include "Infiniband.h"
#include "Device.h"
#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "IBDevice "

Port::Port(CephContext *cct, struct ibv_context* ictxt, uint8_t ipn): ctxt(ictxt), port_num(ipn), port_attr(new ibv_port_attr)
{
#ifdef HAVE_IBV_EXP
  union ibv_gid cgid;
  struct ibv_exp_gid_attr gid_attr;
  bool malformed = false;

  ldout(cct,1) << __func__ << " using experimental verbs for gid" << dendl;
  int r = ibv_query_port(ctxt, port_num, port_attr);
  if (r == -1) {
    lderr(cct) << __func__  << " query port failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }

  lid = port_attr->lid;

  // search for requested GID in GIDs table
  ldout(cct, 1) << __func__ << " looking for local GID " << (cct->_conf->ms_async_rdma_local_gid)
    << " of type " << (cct->_conf->ms_async_rdma_roce_ver) << dendl;
  r = sscanf(cct->_conf->ms_async_rdma_local_gid.c_str(),
	     "%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx"
	     ":%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx:%02hhx%02hhx",
	     &cgid.raw[ 0], &cgid.raw[ 1],
	     &cgid.raw[ 2], &cgid.raw[ 3],
	     &cgid.raw[ 4], &cgid.raw[ 5],
	     &cgid.raw[ 6], &cgid.raw[ 7],
	     &cgid.raw[ 8], &cgid.raw[ 9],
	     &cgid.raw[10], &cgid.raw[11],
	     &cgid.raw[12], &cgid.raw[13],
	     &cgid.raw[14], &cgid.raw[15]);

  if (r != 16) {
    ldout(cct, 1) << __func__ << " malformed or no GID supplied, using GID index 0" << dendl;
    malformed = true;
  }

  gid_attr.comp_mask = IBV_EXP_QUERY_GID_ATTR_TYPE;

  for (gid_idx = 0; gid_idx < port_attr->gid_tbl_len; gid_idx++) {
    r = ibv_query_gid(ctxt, port_num, gid_idx, &gid);
    if (r) {
      lderr(cct) << __func__  << " query gid of port " << port_num << " index " << gid_idx << " failed  " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }
    r = ibv_exp_query_gid_attr(ctxt, port_num, gid_idx, &gid_attr);
    if (r) {
      lderr(cct) << __func__  << " query gid attributes of port " << port_num << " index " << gid_idx << " failed  " << cpp_strerror(errno) << dendl;
      ceph_abort();
    }

    if (malformed) break; // stay with gid_idx=0
    if ( (gid_attr.type == cct->_conf->ms_async_rdma_roce_ver) &&
	 (memcmp(&gid, &cgid, 16) == 0) ) {
      ldout(cct, 1) << __func__ << " found at index " << gid_idx << dendl;
      break;
    }
  }

  if (gid_idx == port_attr->gid_tbl_len) {
    lderr(cct) << __func__ << " Requested local GID was not found in GID table" << dendl;
    ceph_abort();
  }
#else
  int r = ibv_query_port(ctxt, port_num, port_attr);
  if (r == -1) {
    lderr(cct) << __func__  << " query port failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }

  lid = port_attr->lid;
  r = ibv_query_gid(ctxt, port_num, 0, &gid);
  if (r) {
    lderr(cct) << __func__  << " query gid failed  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
#endif
}


Device::Device(CephContext *cct, ibv_device* d): device(d), device_attr(new ibv_device_attr), active_port(nullptr)
{
  if (device == NULL) {
    lderr(cct) << __func__ << " device == NULL" << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
  name = ibv_get_device_name(device);
  ctxt = ibv_open_device(device);
  if (ctxt == NULL) {
    lderr(cct) << __func__ << " open rdma device failed. " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
  int r = ibv_query_device(ctxt, device_attr);
  if (r == -1) {
    lderr(cct) << __func__ << " failed to query rdma device. " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
}

Device::~Device()
{
  if (active_port) {
    delete active_port;
    assert(ibv_close_device(ctxt) == 0);
  }
}

void Device::binding_port(CephContext *cct, int port_num) {
  port_cnt = device_attr->phys_port_cnt;
  for (uint8_t i = 0; i < port_cnt; ++i) {
    Port *port = new Port(cct, ctxt, i+1);
    if (i + 1 == port_num && port->get_port_attr()->state == IBV_PORT_ACTIVE) {
      active_port = port;
      ldout(cct, 1) << __func__ << " found active port " << i+1 << dendl;
      break;
    } else {
      ldout(cct, 10) << __func__ << " port " << i+1 << " is not what we want. state: " << port->get_port_attr()->state << ")"<< dendl;
    }
    delete port;
  }
  if (nullptr == active_port) {
    lderr(cct) << __func__ << "  port not found" << dendl;
    assert(active_port);
  }
}


DeviceList::DeviceList(CephContext *cct)
  : device_list(ibv_get_device_list(&num))
{
  if (device_list == NULL || num == 0) {
    lderr(cct) << __func__ << " failed to get rdma device list.  " << cpp_strerror(errno) << dendl;
    ceph_abort();
  }
  devices = new Device*[num];

  for (int i = 0;i < num; ++i) {
    devices[i] = new Device(cct, device_list[i]);
  }
}

DeviceList::~DeviceList()
{
  for (int i=0; i < num; ++i) {
    delete devices[i];
  }
  delete []devices;
  ibv_free_device_list(device_list);
}

Device* DeviceList::get_device(const char* device_name)
{
  assert(devices);
  for (int i = 0; i < num; ++i) {
    if (!strlen(device_name) || !strcmp(device_name, devices[i]->get_name())) {
      return devices[i];
    }
  }
  return NULL;
}
