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

#include <rte_config.h>
#include <rte_common.h>
#include <rte_ethdev.h>
#include <rte_version.h>

#include "DPDK.h"
#include "dpdk_rte.h"

namespace dpdk {

  static inline std::vector<char> string2vector(std::string str) {
    auto v = std::vector<char>(str.begin(), str.end());
    v.push_back('\0');
    return v;
  }

  bool eal::initialized = false;
  std::thread eal::t;
  std::mutex eal::lock;
  std::condition_variable eal::cond;
  std::list<std::function<void()>> eal::funcs;

  static int bitcount(unsigned n)
  {
    unsigned int c =0 ;
    for (c = 0; n; ++c)
      n &= (n -1);
    return c;
  }

  int eal::init(CephContext *c)
  {
    if (initialized) {
      return 1;
    }

    bool done = false;
    t = std::thread([&]() {
      // TODO: Inherit these from the app parameters - "opts"
      std::vector<std::vector<char>> args {
          string2vector(string("ceph")),
          string2vector("-c"), string2vector(c->_conf->ms_dpdk_coremask),
          string2vector("-n"), string2vector(c->_conf->ms_dpdk_memory_channel),
      };

      Tub<std::string> hugepages_path;
      if (!c->_conf->ms_dpdk_hugepages.empty()) {
        hugepages_path.construct(c->_conf->ms_dpdk_hugepages);
      }

      // If "hugepages" is not provided and DPDK PMD drivers mode is requested -
      // use the default DPDK huge tables configuration.
      if (hugepages_path) {
        args.push_back(string2vector("--huge-dir"));
        args.push_back(string2vector(*hugepages_path));

        //
        // We don't know what is going to be our networking configuration so we
        // assume there is going to be a queue per-CPU. Plus we'll give a DPDK
        // 64MB for "other stuff".
        //
        unsigned int x;
        std::stringstream ss;
        ss << std::hex << "fffefffe";
        ss >> x;
        size_t size_MB = mem_size(bitcount(x)) >> 20;
        std::stringstream size_MB_str;
        size_MB_str << size_MB;

        args.push_back(string2vector("-m"));
        args.push_back(string2vector(size_MB_str.str()));
      } else if (!c->_conf->ms_dpdk_pmd.empty()) {
        args.push_back(string2vector("--no-huge"));
      }

      std::vector<char*> cargs;

      for (auto&& a: args) {
        cargs.push_back(a.data());
      }
      /* initialise the EAL for all */
      int ret = rte_eal_init(cargs.size(), cargs.data());
      if (ret < 0)
        return ret;

      std::unique_lock<std::mutex> l(lock);
      initialized = true;
      done = true;
      cond.notify_all();
      while (true) {
        if (!funcs.empty()) {
          auto f = std::move(funcs.front());
          funcs.pop_front();
          f();
          cond.notify_all();
        } else {
          cond.wait(l);
        }
      }
    });
    t.detach();
    std::unique_lock<std::mutex> l(lock);
    while (!done)
      cond.wait(l);
    return 0;
  }

  size_t eal::mem_size(int num_cpus)
  {
    size_t memsize = 0;
    //
    // PMD mempool memory:
    //
    // We don't know what is going to be our networking configuration so we
    // assume there is going to be a queue per-CPU.
    //
    memsize += num_cpus * qp_mempool_obj_size();

    // Plus we'll give a DPDK 64MB for "other stuff".
    memsize += (64UL << 20);

    return memsize;
  }

} // namespace dpdk
