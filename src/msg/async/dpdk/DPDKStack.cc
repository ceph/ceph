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
#include <queue>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "DPDKStack.h"
#include "IP.h"
#include "DPDK.h"

#define dout_subsys ceph_subsys_dpdk

static NetworkStack **stacks[];

static std::unique_ptr<NetworkStack> create(CephContext *cct, unsigned i) {
  static bool created = false;
  static Mutex lock("DPDKStack::lock");
  static Cond cond;
  if (i == 0) {
    dpdk::eal::cpuset cpus;
    cpus[0] = true;

    dpdk::eal::init(cpus, cct);
    int cores = cct->_conf->ms_dpdk_num_cores;
    stacks = new NetworkStack[cores];
    std::unique_ptr<device> dev;

    // Hardcoded port index 0.
    // TODO: Inherit it from the opts
    dev = create_dpdk_net_device(
        cct, 0, cct->_conf->ms_dpdk_num_queues,
        cores,
        cct->_conf->ms_dpdk_lro,
        cct->_conf->ms_dpdk_hw_flow_control);

    auto sem = std::make_shared<semaphore>(0);
    std::shared_ptr<device> sdev(dev.release());
    for (unsigned i = 0; i < cct->_conf->ms_dpdk_num_cores; i++) {
      if (i < sdev->hw_queues_count()) {
        auto qp = sdev->init_local_queue(stacks[i]->center, cct->_conf->ms_dpdk_hugepages, i);
        std::map<unsigned, float> cpu_weights;
        for (unsigned j = sdev->hw_queues_count() + qid % sdev->hw_queues_count();
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
    }
    sdev->init_port_fini();
    for (unsigned i = 0; i < cct->_conf->ms_dpdk_num_cores; i++)
      stacks[i] = std::make_unique<DPDKStack>(cct, std::move(dev).get(), cores, i);
    Mutex::Locker l(lock);
    created = true;
    cond.Signal();
  } else {
    Mutex::Locker l(lock);
    while (!created) {
      cond.Wait(lock);
    }
  }
  return stacks[i];
}

DPDKStack::DPDKStack(CephContext *cct, std::shared_ptr<device> dev, unsigned i)
    : NetworkStack(cct), _netif(cct, std::move(dev), i), _inet(cct, &_netif),
      cores(), cpu_id(i)
{
  _inet.set_host_address(ipv4_address(cct->_conf->ms_dpdk_host_ipv4_addr));
  _inet.set_gw_address(ipv4_address(cct->_conf->ms_dpdk_gateway_ipv4_addr));
  _inet.set_netmask_address(ipv4_address(cct->_conf->ms_dpdk_netmask_ipv4_addr));
}

int listen(const entity_addr_t &sa, const SocketOptions &opt, ServerSocket *sock) {
  assert(sa.as_posix_sockaddr().sa_family == AF_INET);
  assert(sock);
  *sock = tcpv4_listen(_inet.get_tcp(), sa.get_port(), opt);
  return 0;
}

int DPDKStack::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) {
  assert(addr.get_family() == AF_INET);
  return tcpv4_connect(_inet.get_tcp(), addr);
}

class C_arp_learn : public EventCallback {
  DPDKStack *s;
  ethernet_address l2_addr;
  ipv4_address l3_addr;

 public:
  C_arp_learn(DPDKStack *s, ethernet_address l2, ipv4_address l3)
      : stack(s), l2_addr(l2), l3_addr(l3) {}
  void do_request(int id) {
    s->arp_learn(l2_addr, l3_addr);
    delete this;
  }
};

void arp_learn(ethernet_address l2, ipv4_address l3)
{
  for (unsigned i = 0; i < cores; i++) {
    stacks[i]->center->create_external_event(
        new C_arp_learn(stacks[i], l2, l3));
  }
}
