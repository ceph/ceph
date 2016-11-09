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
 *
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

#ifndef CEPH_IP_TYPES_H_H
#define CEPH_IP_TYPES_H_H

#include <boost/asio/ip/address_v4.hpp>
#include <string>

class Packet;
class ethernet_address;
using resolution_cb = std::function<void (const ethernet_address&, Packet, int)>;

struct ipv4_addr {
  uint32_t ip;
  uint16_t port;

  ipv4_addr() : ip(0), port(0) {}
  ipv4_addr(uint32_t ip, uint16_t port) : ip(ip), port(port) {}
  ipv4_addr(uint16_t port) : ip(0), port(port) {}
  ipv4_addr(const std::string &addr);
  ipv4_addr(const std::string &addr, uint16_t port);

  ipv4_addr(const entity_addr_t &ad) {
    ip = ntoh(ad.in4_addr().sin_addr.s_addr);
    port = ad.get_port();
  }

  ipv4_addr(entity_addr_t &&addr) : ipv4_addr(addr) {}
};

struct ipv4_address {
  ipv4_address() : ip(0) {}
  explicit ipv4_address(uint32_t ip) : ip(ip) {}
  explicit ipv4_address(const std::string& addr) {
    ip = static_cast<uint32_t>(boost::asio::ip::address_v4::from_string(addr).to_ulong());
  }
  ipv4_address(ipv4_addr addr) {
    ip = addr.ip;
  }

  uint32_t ip;

  ipv4_address hton() {
    ipv4_address addr;
    addr.ip = ::hton(ip);
    return addr;
  }
  ipv4_address ntoh() {
    ipv4_address addr;
    addr.ip = ::ntoh(ip);
    return addr;
  }

  friend bool operator==(ipv4_address x, ipv4_address y) {
    return x.ip == y.ip;
  }
  friend bool operator!=(ipv4_address x, ipv4_address y) {
    return x.ip != y.ip;
  }
} __attribute__((packed));

static inline bool is_unspecified(ipv4_address addr) { return addr.ip == 0; }

std::ostream& operator<<(std::ostream& os, ipv4_address a);

namespace std {

  template <>
  struct hash<ipv4_address> {
    size_t operator()(ipv4_address a) const { return a.ip; }
  };

}

#endif //CEPH_IP_TYPES_H_H
