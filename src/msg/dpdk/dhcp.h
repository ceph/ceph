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
 * Copyright 2014 Cloudius Systems
 */

#ifndef CEPH_DPDK_DHCP_H_
#define CEPH_DPDK_DHCP_H_

#include "IP.h"

/*
 * Simplistic DHCP query class.
 * Due to the nature of the native stack,
 * it operates on an "ipv4" object instead of,
 * for example, an interface.
 */
class dhcp {
 public:
  dhcp(ipv4 &);
  dhcp(dhcp &&);
  ~dhcp();

  static const utime_t default_timeout;

  struct lease {
    ipv4_address ip;
    ipv4_address netmask;
    ipv4_address broadcast;

    ipv4_address gateway;
    ipv4_address dhcp_server;

    std::vector<ipv4_address> name_servers;

    std::chrono::seconds lease_time;
    std::chrono::seconds renew_time;
    std::chrono::seconds rebind_time;

    uint16_t mtu = 0;
  };

  std::pair<bool, lease> result_type;

  /**
   * Runs a discover/request sequence on the ipv4 "stack".
   * During this execution the ipv4 will be "hijacked"
   * more or less (through packet filter), and while not
   * inoperable, most likely quite less efficient.
   *
   * Please note that this does _not_ modify the ipv4 object bound.
   * It only makes queries and records replys for the related NIC.
   * It is up to caller to use the returned information as he se fit.
   */
  result_type discover(const utime_t & = default_timeout);
  result_type renew(const lease &, const utime_t & = default_timeout);
  ip_packet_filter* get_ipv4_filter();
 private:
  class impl;
  std::unique_ptr<impl> _impl;
};

#endif /* CEPH_DPDK_DHCP_H_ */
