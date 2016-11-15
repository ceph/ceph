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

#include <tuple>

#include "common/ceph_argparse.h"
#include "DPDKStack.h"
#include "DPDK.h"
#include "IP.h"
#include "TCP-Stack.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "dpdkstack "

static int dpdk_thread_adaptor(void* f)
{
  (*static_cast<std::function<void ()>*>(f))();
  return 0;
}

void DPDKWorker::initialize()
{
  static enum {
    WAIT_DEVICE_STAGE,
    WAIT_PORT_FIN_STAGE,
    DONE
  } create_stage = WAIT_DEVICE_STAGE;
  static Mutex lock("DPDKStack::lock");
  static Cond cond;
  static unsigned queue_init_done = 0;
  static unsigned cores = 0;
  static std::shared_ptr<DPDKDevice> sdev;

  unsigned i = center.get_id();
  if (i == 0) {
    // Hardcoded port index 0.
    // TODO: Inherit it from the opts
    cores = cct->_conf->ms_async_op_threads;
    std::unique_ptr<DPDKDevice> dev = create_dpdk_net_device(
        cct, cores, cct->_conf->ms_dpdk_port_id,
        cct->_conf->ms_dpdk_lro,
        cct->_conf->ms_dpdk_hw_flow_control);
    sdev = std::shared_ptr<DPDKDevice>(dev.release());
    sdev->workers.resize(cores);
    ldout(cct, 1) << __func__ << " using " << cores << " cores " << dendl;

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
    auto qp = sdev->init_local_queue(cct, &center, cct->_conf->ms_dpdk_hugepages, i);
    std::map<unsigned, float> cpu_weights;
    for (unsigned j = sdev->hw_queues_count() + i % sdev->hw_queues_count();
         j < cores; j+= sdev->hw_queues_count())
      cpu_weights[i] = 1;
    cpu_weights[i] = cct->_conf->ms_dpdk_hw_queue_weight;
    qp->configure_proxies(cpu_weights);
    sdev->set_local_queue(i, std::move(qp));
    Mutex::Locker l(lock);
    ++queue_init_done;
    cond.Signal();
  } else {
    // auto master = qid % sdev->hw_queues_count();
    // sdev->set_local_queue(create_proxy_net_device(master, sdev.get()));
    assert(0);
  }
  if (i == 0) {
    {
      Mutex::Locker l(lock);
      while (queue_init_done < cores)
        cond.Wait(lock);
    }

    if (sdev->init_port_fini() < 0) {
      lderr(cct) << __func__ << " init_port_fini failed " << dendl;
      assert(0);
    }
    Mutex::Locker l(lock);
    create_stage = DONE;
    cond.Signal();
  } else {
    Mutex::Locker l(lock);
    while (create_stage <= WAIT_PORT_FIN_STAGE)
      cond.Wait(lock);
  }

  sdev->workers[i] = this;
  _impl = std::unique_ptr<DPDKWorker::Impl>(
          new DPDKWorker::Impl(cct, i, &center, sdev));
  {
    Mutex::Locker l(lock);
    if (!--queue_init_done) {
      create_stage = WAIT_DEVICE_STAGE;
      sdev.reset();
    }
  }
}

using AvailableIPAddress = std::tuple<string, string, string>;
static bool parse_available_address(
        const string &ips, const string &gates, const string &masks, vector<AvailableIPAddress> &res)
{
  vector<string> ip_vec, gate_vec, mask_vec;
  string_to_vec(ip_vec, ips);
  string_to_vec(gate_vec, gates);
  string_to_vec(mask_vec, masks);
  if (ip_vec.empty() || ip_vec.size() != gate_vec.size() || ip_vec.size() != mask_vec.size())
    return false;

  for (size_t i = 0; i < ip_vec.size(); ++i) {
    res.push_back(AvailableIPAddress{ip_vec[i], gate_vec[i], mask_vec[i]});
  }
  return true;
}

static bool match_available_address(const vector<AvailableIPAddress> &avails,
                                    const entity_addr_t &ip, int &res)
{
  for (size_t i = 0; i < avails.size(); ++i) {
    entity_addr_t addr;
    auto a = std::get<0>(avails[i]).c_str();
    if (!addr.parse(a))
      continue;
    if (addr.is_same_host(ip)) {
      res = i;
      return true;
    }
  }
  return false;
}

DPDKWorker::Impl::Impl(CephContext *cct, unsigned i, EventCenter *c, std::shared_ptr<DPDKDevice> dev)
    : id(i), _netif(cct, dev, c), _dev(dev), _inet(cct, c, &_netif)
{
  vector<AvailableIPAddress> tuples;
  bool parsed = parse_available_address(cct->_conf->ms_dpdk_host_ipv4_addr,
                                        cct->_conf->ms_dpdk_gateway_ipv4_addr,
                                        cct->_conf->ms_dpdk_netmask_ipv4_addr, tuples);
  if (!parsed) {
    lderr(cct) << __func__ << " no available address "
               << cct->_conf->ms_dpdk_host_ipv4_addr << ", "
               << cct->_conf->ms_dpdk_gateway_ipv4_addr << ", "
               << cct->_conf->ms_dpdk_netmask_ipv4_addr << ", "
               << dendl;
    assert(0);
  }
  _inet.set_host_address(ipv4_address(std::get<0>(tuples[0])));
  _inet.set_gw_address(ipv4_address(std::get<1>(tuples[0])));
  _inet.set_netmask_address(ipv4_address(std::get<2>(tuples[0])));
}

int DPDKWorker::listen(entity_addr_t &sa, const SocketOptions &opt,
                       ServerSocket *sock)
{
  assert(sa.get_family() == AF_INET);
  assert(sock);

  ldout(cct, 10) << __func__ << " addr " << sa << dendl;
  // vector<AvailableIPAddress> tuples;
  // bool parsed = parse_available_address(cct->_conf->ms_dpdk_host_ipv4_addr,
  //                                       cct->_conf->ms_dpdk_gateway_ipv4_addr,
  //                                       cct->_conf->ms_dpdk_netmask_ipv4_addr, tuples);
  // if (!parsed) {
  //   lderr(cct) << __func__ << " no available address "
  //              << cct->_conf->ms_dpdk_host_ipv4_addr << ", "
  //              << cct->_conf->ms_dpdk_gateway_ipv4_addr << ", "
  //              << cct->_conf->ms_dpdk_netmask_ipv4_addr << ", "
  //              << dendl;
  //   return -EINVAL;
  // }
  // int idx;
  // parsed = match_available_address(tuples, sa, idx);
  // if (!parsed) {
  //   lderr(cct) << __func__ << " no matched address for " << sa << dendl;
  //   return -EINVAL;
  // }
  // _inet.set_host_address(ipv4_address(std::get<0>(tuples[idx])));
  // _inet.set_gw_address(ipv4_address(std::get<1>(tuples[idx])));
  // _inet.set_netmask_address(ipv4_address(std::get<2>(tuples[idx])));
  return tcpv4_listen(_impl->_inet.get_tcp(), sa.get_port(), opt, sock);
}

int DPDKWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket)
{
  // assert(addr.get_family() == AF_INET);
  int r =  tcpv4_connect(_impl->_inet.get_tcp(), addr, socket);
  ldout(cct, 10) << __func__ << " addr " << addr << dendl;
  return r;
}

void DPDKStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
  // create a extra master thread
  //
  funcs[i] = std::move(func);
  int r = 0;
  r = dpdk::eal::init(cct);
  if (r < 0) {
    lderr(cct) << __func__ << " init dpdk rte failed, r=" << r << dendl;
    assert(0);
  }
  // if dpdk::eal::init already called by NVMEDevice, we will select 1..n
  // cores
  assert(rte_lcore_count() >= i + 1);
  dpdk::eal::execute_on_master([&]() {
    r = rte_eal_remote_launch(dpdk_thread_adaptor, static_cast<void*>(&funcs[i]), i+1);
    if (r < 0) {
      lderr(cct) << __func__ << " remote launch failed, r=" << r << dendl;
      assert(0);
    }
  });
}
