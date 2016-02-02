// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "DPDKStack.h"
#include "DPDK.h"
#include "dpdk_rte.h"
#include "IP.h"
#include "TCP-Stack.h"

#define dout_subsys ceph_subsys_dpdk

static std::shared_ptr<DPDKDevice> sdev;

std::unique_ptr<NetworkStack> DPDKStack::create(CephContext *cct, EventCenter *center, unsigned i) {
  static enum {
    WAIT_DEVICE_STAGE,
    WAIT_PORT_FIN_STAGE,
    DONE
  } create_stage = WAIT_DEVICE_STAGE;
  static Mutex lock("DPDKStack::lock");
  static Cond cond;
  int cores = cct->_conf->ms_dpdk_num_cores;
  if (i == 0) {
    dpdk::eal::init(cct);
    // Hardcoded port index 0.
    // TODO: Inherit it from the opts
    std::unique_ptr<DPDKDevice> dev = create_dpdk_net_device(
        cct, cores,
        0, cct->_conf->ms_dpdk_num_queues,
        cct->_conf->ms_dpdk_lro,
        cct->_conf->ms_dpdk_hw_flow_control);
    sdev = std::shared_ptr<DPDKDevice>(dev.release());
    sdev->stacks.resize(cores);

    Mutex::Locker l(lock);
    create_stage = WAIT_PORT_FIN_STAGE;
    cond.Signal();
  } else {
    Mutex::Locker l(lock);
    while (create_stage <= WAIT_DEVICE_STAGE)
      cond.Wait(lock);
  }
  assert(sdev);
  if (i < sdev->hw_queues_count()) {
    auto qp = sdev->init_local_queue(cct, center, cct->_conf->ms_dpdk_hugepages, i);
    std::map<unsigned, float> cpu_weights;
    for (unsigned j = sdev->hw_queues_count() + i % sdev->hw_queues_count();
         j < cct->_conf->ms_dpdk_num_cores; j+= sdev->hw_queues_count())
      cpu_weights[i] = 1;
    cpu_weights[i] = cct->_conf->ms_dpdk_hw_queue_weight;
    qp->configure_proxies(cpu_weights);
    sdev->set_local_queue(i, std::move(qp));
  } else {
    // auto master = qid % sdev->hw_queues_count();
    // sdev->set_local_queue(create_proxy_net_device(master, sdev.get()));
    assert(0);
  }
  if (i == 0) {
    if (sdev->init_port_fini() < 0)
      return nullptr;
    Mutex::Locker l(lock);
    create_stage = DONE;
    cond.Signal();
  } else {
    Mutex::Locker l(lock);
    while (create_stage <= WAIT_PORT_FIN_STAGE)
      cond.Wait(lock);
  }

  sdev->stacks[i] = new DPDKStack(cct, center, sdev, cores);
  return std::unique_ptr<DPDKStack>(sdev->stacks[i]);
}

DPDKStack::DPDKStack(CephContext *cct, EventCenter *c,
                     std::shared_ptr<DPDKDevice> dev, unsigned cores)
    : NetworkStack(cct), _netif(cct, std::move(dev), c), _inet(cct, c, &_netif),
      cores(cores), center(c)
{
  _inet.set_host_address(ipv4_address(cct->_conf->ms_dpdk_host_ipv4_addr));
  _inet.set_gw_address(ipv4_address(cct->_conf->ms_dpdk_gateway_ipv4_addr));
  _inet.set_netmask_address(ipv4_address(cct->_conf->ms_dpdk_netmask_ipv4_addr));
}

int DPDKStack::listen(entity_addr_t &sa, const SocketOptions &opt, ServerSocket *sock) {
  assert(sa.get_family() == AF_INET);
  assert(sock);
  *sock = tcpv4_listen(_inet.get_tcp(), sa.get_port(), opt);
  return 0;
}

int DPDKStack::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  assert(addr.get_family() == AF_INET);
  *socket = tcpv4_connect(_inet.get_tcp(), addr);
  return 0;
}

class C_arp_learn : public EventCallback {
  DPDKStack *stack;
  ethernet_address l2_addr;
  ipv4_address l3_addr;

 public:
  C_arp_learn(DPDKStack *s, ethernet_address l2, ipv4_address l3)
      : stack(s), l2_addr(l2), l3_addr(l3) {}
  void do_request(int id) {
    stack->arp_learn(l2_addr, l3_addr);
    delete this;
  }
};

void arp_learn(ethernet_address l2, ipv4_address l3)
{
  assert(sdev);
  for (auto &&s: sdev->stacks) {
    s->center->dispatch_event_external(
        new C_arp_learn(s, l2, l3));
  }
}
