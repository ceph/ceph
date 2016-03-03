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
#ifndef CEPH_DPDK_RTE_H_
#define CEPH_DPDK_RTE_H_

#include <bitset>
#include <rte_config.h>
#include <rte_version.h>
#include <boost/program_options.hpp>

/*********************** Compat section ***************************************/
// We currently support only versions 2.0 and above.
#if (RTE_VERSION < RTE_VERSION_NUM(2,0,0,0))
#error "DPDK version above 2.0.0 is required"
#endif

#if defined(RTE_MBUF_REFCNT_ATOMIC)
#warning "CONFIG_RTE_MBUF_REFCNT_ATOMIC should be disabled in DPDK's " \
         "config/common_linuxapp"
#endif
/******************************************************************************/

namespace dpdk {

// DPDK Environment Abstraction Layer
class eal {
public:
    using cpuset = std::bitset<RTE_MAX_LCORE>;

    static int init(CephContext *c);
    /**
     * Returns the amount of memory needed for DPDK
     * @param num_cpus Number of CPUs the application is going to use
     *
     * @return
     */
    static size_t mem_size(int num_cpus);
    static bool initialized;
};

} // namespace dpdk
#endif // CEPH_DPDK_RTE_H_
