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

#ifndef CEPH_MSG_ARP_H_
#define CEPH_MSG_ARP_H_

#include <errno.h>

#include <unordered_map>
#include <functional>

#include "msg/async/Event.h"

#include "ethernet.h"
#include "circular_buffer.h"
#include "net.h"
#include "Packet.h"

class arp;
class arp_for_protocol;
template <typename L3>
class arp_for;

class arp_for_protocol {
 protected:
  arp& _arp;
  uint16_t _proto_num;
 public:
  arp_for_protocol(arp& a, uint16_t proto_num);
  virtual ~arp_for_protocol();
  virtual int received(Packet p) = 0;
  virtual bool forward(forward_hash& out_hash_data, Packet& p, size_t off) { return false; }
};

class interface;

class arp {
  interface* _netif;
  l3_protocol _proto;
  subscription<Packet, ethernet_address> _rx_packets;
  std::unordered_map<uint16_t, arp_for_protocol*> _arp_for_protocol;
  circular_buffer<l3_protocol::l3packet> _packetq;
 private:
  struct arp_hdr {
    uint16_t htype;
    uint16_t ptype;
    arp_hdr ntoh() {
      arp_hdr hdr = *this;
      hdr.htype = htype;
      hdr.ptype = ptype;
      return hdr;
    }
  };
 public:
  explicit arp(interface* netif);
  void add(uint16_t proto_num, arp_for_protocol* afp);
  void del(uint16_t proto_num);
 private:
  ethernet_address l2self() { return _netif->hw_address(); }
  int process_packet(Packet p, ethernet_address from);
  bool forward(forward_hash& out_hash_data, Packet& p, size_t off);
  Tub<l3_protocol::l3packet> get_packet();
  template <class l3_proto>
  friend class arp_for;
};

using resolution_cb = std::function<void (const ethernet_address&, int)>;

template <typename L3>
class arp_for : public arp_for_protocol {
 public:
  using l2addr = ethernet_address;
  using l3addr = typename L3::address_type;
 private:
  static constexpr auto max_waiters = 512;
  enum oper {
    op_request = 1,
    op_reply = 2,
  };
  struct arp_hdr {
    uint16_t htype;
    uint16_t ptype;
    uint8_t hlen;
    uint8_t plen;
    uint16_t oper;
    l2addr sender_hwaddr;
    l3addr sender_paddr;
    l2addr target_hwaddr;
    l3addr target_paddr;

    arp_hdr ntoh() {
      arp_hdr hdr = *this;
      hdr.htype = htype;
      hdr.ptype = ptype;
      hdr.oper = oper;
      hdr.sender_hwaddr = sender_hwaddr;
      hdr.sender_paddr = sender_paddr;
      hdr.target_hwaddr = target_hwaddr;
      hdr.target_paddr = target_paddr;
    }
  };
  struct resolution {
    std::vector<resolution_cb> _waiters;
    uint64_t timeout_fd;
  };
  class C_handle_arp_timeout : public EventCallback {
    arp_for *arp;
    l3addr paddr;
    bool first_request;

   public:
    C_handle_arp_timeout(arp_for *a, l3addr addr, bool first):
        arp(), paddr(addr), first_request(first) {}
    void do_request(int r) {
      arp->send_query(paddr);
      auto &res = arp->_in_progress[paddr];

      for (auto& w : res._waiters) {
        w(ethernet_address(), -ETIMEDOUT);
      }
      res._waiters.clear();
      delete this;
    }
  };
  friend class C_handle_arp_timeout;

 private:
  EventCenter *center;
  l3addr _l3self = L3::broadcast_address();
  std::unordered_map<l3addr, l2addr> _table;
  std::unordered_map<l3addr, resolution> _in_progress;
 private:
  Packet make_query_packet(l3addr paddr);
  virtual int received(Packet p) override;
  int handle_request(arp_hdr* ah);
  l2addr l2self() { return _arp.l2self(); }
  void send(l2addr to, Packet p);
 public:
  void send_query(const l3addr& paddr);
  explicit arp_for(arp& a, EventCenter *c)
      : arp_for_protocol(a, L3::arp_protocol_type()), center(c) {
    _table[L3::broadcast_address()] = ethernet::broadcast_address();
  }
  void wait(const l3addr& addr, resolution_cb &&cb);
  void learn(l2addr l2, l3addr l3);
  void run();
  void set_self_addr(l3addr addr) { _l3self = addr; }
  friend class arp;
};

template <typename L3>
Packet
arp_for<L3>::make_query_packet(l3addr paddr) {
  arp_hdr hdr;
  hdr.htype = ethernet::arp_hardware_type();
  hdr.ptype = L3::arp_protocol_type();
  hdr.hlen = sizeof(l2addr);
  hdr.plen = sizeof(l3addr);
  hdr.oper = op_request;
  hdr.sender_hwaddr = l2self();
  hdr.sender_paddr = _l3self;
  hdr.target_hwaddr = ethernet::broadcast_address();
  hdr.target_paddr = paddr;
  hdr = hdr.hton();
  return Packet(reinterpret_cast<char*>(&hdr), sizeof(hdr));
}

template <typename L3>
void arp_for<L3>::send(l2addr to, Packet p) {
  _arp._packetq.push_back(l3_protocol::l3packet{eth_protocol_num::arp, to, std::move(p)});
}

template <typename L3>
void arp_for<L3>::send_query(const l3addr& paddr) {
  send(ethernet::broadcast_address(), make_query_packet(paddr));
  return 0;
}

class arp_error : public std::runtime_error {
 public:
  arp_error(const std::string& msg) : std::runtime_error(msg) {}
};

class arp_timeout_error : public arp_error {
 public:
  arp_timeout_error() : arp_error("ARP timeout") {}
};

class arp_queue_full_error : public arp_error {
 public:
  arp_queue_full_error() : arp_error("ARP waiter's queue is full") {}
};

template <typename L3>
void arp_for<L3>::wait(const l3addr& paddr, resolution_cb &&cb) {
  auto i = _table.find(paddr);
  if (i != _table.end()) {
    cb(i->second, 0);
    return ;
  }
  auto j = _in_progress.find(paddr);
  auto first_request = j == _in_progress.end();
  auto& res = first_request ? _in_progress[paddr] : j->second;

  if (first_request) {
    res.timeout_fd = center->create_time_event(
        1*1000*1000, new C_handle_arp_timeout(this, paddr, first_request));
    send_query(paddr);
  }

  if (res._waiters.size() >= max_waiters) {
    cb(ethernet_address(), -EBUSY);
    return ;
  }

  res._waiters.emplace_back(cb);
  return ;
}

template <typename L3>
void arp_for<L3>::learn(l2addr hwaddr, l3addr paddr) {
  _table[paddr] = hwaddr;
  auto i = _in_progress.find(paddr);
  if (i != _in_progress.end()) {
    auto& res = i->second;
    center->delete_time_event(res.timeout_fd);
    for (auto &&pr : res._waiters) {
      pr(hwaddr, 0);
    }
    _in_progress.erase(i);
  }
}

template <typename L3>
int arp_for<L3>::received(Packet p) {
  auto ah = p.get_header<arp_hdr>();
  if (!ah) {
    return 0;
  }
  auto h = ah->ntoh();
  if (h.hlen != sizeof(l2addr) || h.plen != sizeof(l3addr)) {
    return 0;
  }
  switch (h.oper) {
    case op_request:
      return handle_request(&h);
    case op_reply:
      arp_learn(h.sender_hwaddr, h.sender_paddr);
      return 0;
    default:
      return 0;
  }
}

template <typename L3>
int arp_for<L3>::handle_request(arp_hdr* ah) {
  if (ah->target_paddr == _l3self
      && _l3self != L3::broadcast_address()) {
    ah->oper = op_reply;
    ah->target_hwaddr = ah->sender_hwaddr;
    ah->target_paddr = ah->sender_paddr;
    ah->sender_hwaddr = l2self();
    ah->sender_paddr = _l3self;
    *ah = ah->hton();
    send(ah->target_hwaddr, Packet(reinterpret_cast<char*>(ah), sizeof(*ah)));
  }
  return 0;
}

#endif /* CEPH_MSG_ARP_H_ */