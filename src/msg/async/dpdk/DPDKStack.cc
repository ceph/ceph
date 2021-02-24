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
#include "dpdk_rte.h"
#include "DPDKStack.h"
#include "DPDK.h"
#include "IP.h"
#include "TCP-Stack.h"

#include "common/dout.h"
#include "include/ceph_assert.h"
#include "common/Cond.h"

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
  static ceph::mutex lock = ceph::make_mutex("DPDKStack::lock");
  static ceph::condition_variable cond;
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

    std::lock_guard l{lock};
    create_stage = WAIT_PORT_FIN_STAGE;
    cond.notify_all();
  } else {
    std::unique_lock l{lock};
    cond.wait(l, [] { return create_stage > WAIT_DEVICE_STAGE; });
  }
  ceph_assert(sdev);
  if (i < sdev->hw_queues_count()) {
    auto qp = sdev->init_local_queue(cct, &center, cct->_conf->ms_dpdk_hugepages, i);
    std::map<unsigned, float> cpu_weights;
    for (unsigned j = sdev->hw_queues_count() + i % sdev->hw_queues_count();
         j < cores; j+= sdev->hw_queues_count())
      cpu_weights[i] = 1;
    cpu_weights[i] = cct->_conf->ms_dpdk_hw_queue_weight;
    qp->configure_proxies(cpu_weights);
    sdev->set_local_queue(i, std::move(qp));
    std::lock_guard l{lock};
    ++queue_init_done;
    cond.notify_all();
  } else {
    // auto master = qid % sdev->hw_queues_count();
    // sdev->set_local_queue(create_proxy_net_device(master, sdev.get()));
    ceph_abort();
  }
  if (i == 0) {
    {
      std::unique_lock l{lock};
      cond.wait(l, [] { return queue_init_done >= cores; });
    }

    if (sdev->init_port_fini() < 0) {
      lderr(cct) << __func__ << " init_port_fini failed " << dendl;
      ceph_abort();
    }
    std::lock_guard l{lock};
    create_stage = DONE;
    cond.notify_all();
  } else {
    std::unique_lock  l{lock};
    cond.wait(l, [&] { return create_stage > WAIT_PORT_FIN_STAGE; });
  }

  sdev->workers[i] = this;
  _impl = std::unique_ptr<DPDKWorker::Impl>(
          new DPDKWorker::Impl(cct, i, &center, sdev));
  {
    std::lock_guard l{lock};
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
  bool parsed = parse_available_address(cct->_conf.get_val<std::string>("ms_dpdk_host_ipv4_addr"),
                                        cct->_conf.get_val<std::string>("ms_dpdk_gateway_ipv4_addr"),
                                        cct->_conf.get_val<std::string>("ms_dpdk_netmask_ipv4_addr"), tuples);
  if (!parsed) {
    lderr(cct) << __func__ << " no available address "
               << cct->_conf.get_val<std::string>("ms_dpdk_host_ipv4_addr") << ", "
               << cct->_conf.get_val<std::string>("ms_dpdk_gateway_ipv4_addr") << ", "
               << cct->_conf.get_val<std::string>("ms_dpdk_netmask_ipv4_addr") << ", "
               << dendl;
    ceph_abort();
  }
  _inet.set_host_address(ipv4_address(std::get<0>(tuples[0])));
  _inet.set_gw_address(ipv4_address(std::get<1>(tuples[0])));
  _inet.set_netmask_address(ipv4_address(std::get<2>(tuples[0])));
}

DPDKWorker::Impl::~Impl()
{
  _dev->unset_local_queue(id);
}

int DPDKWorker::listen(entity_addr_t &sa,
		       unsigned addr_slot,
		       const SocketOptions &opt,
                       ServerSocket *sock)
{
  ceph_assert(sa.get_family() == AF_INET);
  ceph_assert(sock);

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
  return tcpv4_listen(_impl->_inet.get_tcp(), sa.get_port(), opt, sa.get_type(),
		      addr_slot, sock);
}

int DPDKWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket)
{
  // ceph_assert(addr.get_family() == AF_INET);
  int r =  tcpv4_connect(_impl->_inet.get_tcp(), addr, socket);
  ldout(cct, 10) << __func__ << " addr " << addr << dendl;
  return r;
}

void DPDKStack::spawn_worker(std::function<void ()> &&func)
{
  // create a extra master thread
  //
  funcs.push_back(std::move(func));
  int r = 0;
  r = dpdk::eal::init(cct);
  if (r < 0) {
    lderr(cct) << __func__ << " init dpdk rte failed, r=" << r << dendl;
    ceph_abort();
  }
  // if dpdk::eal::init already called by NVMEDevice, we will select 1..n
  // cores
  unsigned nr_worker = funcs.size();
  ceph_assert(rte_lcore_count() >= nr_worker);
  unsigned core_id;
  RTE_LCORE_FOREACH_SLAVE(core_id) {
    if (--nr_worker == 0) {
      break;
    }
  }
  void *adapted_func = static_cast<void*>(funcs.back());
  dpdk::eal::execute_on_master([adapted_func, core_id, this]() {
    int r = rte_eal_remote_launch(dpdk_thread_adaptor, adapted_func, core_id);
    if (r < 0) {
      lderr(cct) << __func__ << " remote launch failed, r=" << r << dendl;
      ceph_abort();
    }
  });
}

void DPDKStack::join_worker(unsigned i)
{
  dpdk::eal::execute_on_master([&]() {
    rte_eal_wait_lcore(i+1);
  });
}
