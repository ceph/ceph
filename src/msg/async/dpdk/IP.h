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

#ifndef CEPH_MSG_IP_H_
#define CEPH_MSG_IP_H_

#include <boost/asio/ip/address_v4.hpp>
#include <arpa/inet.h>
#include <unordered_map>
#include <cstdint>
#include <array>
#include <map>
#include <list>
#include <chrono>

#include "array_map.h"
#include "ARP.h"
#include "IPChecksum.h"
#include "const.h"
#include "PacketUtil.h"
#include "toeplitz.h"
#include "Event.h"

class ipv4;
template <ip_protocol_num ProtoNum>
class ipv4_l4;

template <typename InetTraits>
class tcp;

struct ipv4_address {
  ipv4_address() : ip(0) {}
  explicit ipv4_address(uint32_t ip) : ip(ip) {}
  explicit ipv4_address(const std::string& addr) {
    ip = static_cast<uint32_t>(boost::asio::ip::address_v4::from_string(addr).to_ulong());
  }
  ipv4_address(ipv4_addr addr) {
    ip = addr.ip;
  }

  unaligned<uint32_t> ip;

  template <typename Adjuster>
  auto adjust_endianness(Adjuster a) { return a(ip); }

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
  struct hash<net::ipv4_address> {
    size_t operator()(net::ipv4_address a) const { return a.ip; }
  };

}

struct ipv4_traits {
  using address_type = ipv4_address;
  using inet_type = ipv4_l4<ip_protocol_num::tcp>;
  struct l4packet {
    ipv4_address to;
    Packet p;
    ethernet_address e_dst;
    ip_protocol_num proto_num;
  };
  using packet_provider_type = std::function<Tub<l4packet> ()>;
  static void tcp_pseudo_header_checksum(checksummer& csum, ipv4_address src, ipv4_address dst, uint16_t len) {
    csum.sum_many(src.ip.raw, dst.ip.raw, uint8_t(0), uint8_t(ip_protocol_num::tcp), len);
  }
  static constexpr uint8_t ip_hdr_len_min = net::ipv4_hdr_len_min;
};

template <ip_protocol_num ProtoNum>
class ipv4_l4 {
 public:
  ipv4& _inet;
 public:
  ipv4_l4(ipv4& inet) : _inet(inet) {}
  void register_packet_provider(ipv4_traits::packet_provider_type func);
  void wait_l2_dst_address(ipv4_address to, resolution_cb &&cb);
};

class ip_protocol {
 public:
  virtual ~ip_protocol() {}
  virtual void received(Packet p, ipv4_address from, ipv4_address to) = 0;
  virtual bool forward(forward_hash& out_hash_data, Packet& p, size_t off) { return true; }
};

template <typename InetTraits>
struct l4connid {
  using ipaddr = typename InetTraits::address_type;
  using inet_type = typename InetTraits::inet_type;
  struct connid_hash;

  ipaddr local_ip;
  ipaddr foreign_ip;
  uint16_t local_port;
  uint16_t foreign_port;

  bool operator==(const l4connid& x) const {
    return local_ip == x.local_ip
           && foreign_ip == x.foreign_ip
           && local_port == x.local_port
           && foreign_port == x.foreign_port;
  }

  uint32_t hash(const rss_key_type& rss_key) {
    forward_hash hash_data;
    hash_data.push_back(hton(foreign_ip.ip));
    hash_data.push_back(hton(local_ip.ip));
    hash_data.push_back(hton(foreign_port));
    hash_data.push_back(hton(local_port));
    return toeplitz_hash(rss_key, hash_data);
  }
};

class ipv4_tcp final : public ip_protocol {
  ipv4_l4<ip_protocol_num::tcp> _inet_l4;
  std::unique_ptr<tcp<ipv4_traits>> _tcp;
 public:
  ipv4_tcp(ipv4& inet);
  ~ipv4_tcp();
  virtual void received(Packet p, ipv4_address from, ipv4_address to);
  virtual bool forward(forward_hash& out_hash_data, Packet& p, size_t off) override;
  friend class ipv4;
};

struct icmp_hdr {
  enum class msg_type : uint8_t {
    echo_reply = 0,
    echo_request = 8,
  };
  msg_type type;
  uint8_t code;
  unaligned<uint16_t> csum;
  unaligned<uint32_t> rest;
  template <typename Adjuster>
  auto adjust_endianness(Adjuster a) {
    return a(csum);
  }
} __attribute__((packed));


class icmp {
 public:
  using ipaddr = ipv4_address;
  using inet_type = ipv4_l4<ip_protocol_num::icmp>;
  explicit icmp(inet_type& inet)
      : _inet(inet), _queue_space(inet.cct, "DPDK::icmp::_queue_space", 212992) {
    _inet.register_packet_provider([this] {
      Tub<ipv4_traits::l4packet> l4p;
      if (!_packetq.empty()) {
        l4p.construct(_packetq.front());
        _packetq.pop_front();
        _queue_space.put(l4p.value().p.len());
      }
      return l4p;
    });
  }
  void received(Packet p, ipaddr from, ipaddr to);

 private:
  inet_type& _inet;
  circular_buffer<ipv4_traits::l4packet> _packetq;
  Throttler _queue_space;
};

class ipv4_icmp final : public ip_protocol {
  ipv4_l4<ip_protocol_num::icmp> _inet_l4;
  icmp _icmp;
 public:
  ipv4_icmp(ipv4& inet) : _inet_l4(inet), _icmp(_inet_l4) {}
  virtual void received(Packet p, ipv4_address from, ipv4_address to) {
    _icmp.received(std::move(p), from, to);
  }
  friend class ipv4;
};

struct ip_hdr;

struct ip_packet_filter {
  virtual ~ip_packet_filter() {};
  virtual void handle(Packet& p, ip_hdr* iph, ethernet_address from, bool & handled) = 0;
};

struct ipv4_frag_id {
  struct hash;
  ipv4_address src_ip;
  ipv4_address dst_ip;
  uint16_t identification;
  uint8_t protocol;
  bool operator==(const ipv4_frag_id& x) const {
    return src_ip == x.src_ip &&
           dst_ip == x.dst_ip &&
           identification == x.identification &&
           protocol == x.protocol;
  }
};

struct ipv4_frag_id::hash : private std::hash<ipv4_address>,
                            private std::hash<uint16_t>, private std::hash<uint8_t> {
  size_t operator()(const ipv4_frag_id& id) const noexcept {
    using h1 = std::hash<ipv4_address>;
    using h2 = std::hash<uint16_t>;
    using h3 = std::hash<uint8_t>;
    return h1::operator()(id.src_ip) ^
           h1::operator()(id.dst_ip) ^
           h2::operator()(id.identification) ^
           h3::operator()(id.protocol);
  }
};

struct ipv4_tag {};
using ipv4_packet_merger = packet_merger<uint32_t, ipv4_tag>;

class ipv4 {
 public:
  using address_type = ipv4_address;
  using proto_type = uint16_t;
  static address_type broadcast_address() { return ipv4_address(0xffffffff); }
  static proto_type arp_protocol_type() { return proto_type(eth_protocol_num::ipv4); }
  CephContext *cct;
  EventCenter *center;

 private:
  interface* _netif;
  std::vector<ipv4_traits::packet_provider_type> _pkt_providers;
  Tub<uint64_t> frag_timefd;
  EventCallbackRef frag_handler;
  ARP _global_arp;
  arp_for<ipv4> _arp;
  ipv4_address _host_address;
  ipv4_address _gw_address;
  ipv4_address _netmask;
  l3_protocol _l3;
  subscription<Packet, ethernet_address> _rx_packets;
  ipv4_tcp _tcp;
  ipv4_icmp _icmp;
  array_map<ip_protocol*, 256> _l4;
  ip_packet_filter *_packet_filter;
  struct frag {
    Packet header;
    ipv4_packet_merger data;
    utime_t rx_time;
    uint32_t mem_size = 0;
    // fragment with MF == 0 inidates it is the last fragment
    bool last_frag_received = false;

    Packet get_assembled_packet(ethernet_address from, ethernet_address to);
    int32_t merge(ip_hdr &h, uint16_t offset, Packet p);
    bool is_complete();
  };
  std::unordered_map<ipv4_frag_id, frag, ipv4_frag_id::hash> _frags;
  std::list<ipv4_frag_id> _frags_age;
  static constexpr utime_t _frag_timeout{30};
  static constexpr uint32_t _frag_low_thresh{3 * 1024 * 1024};
  static constexpr uint32_t _frag_high_thresh{4 * 1024 * 1024};
  uint32_t _frag_mem = 0;
  circular_buffer<l3_protocol::l3packet> _packetq;
  unsigned _pkt_provider_idx = 0;
  PerfCounters *perf_logger;
 private:
  int handle_received_packet(Packet p, ethernet_address from);
  bool forward(forward_hash& out_hash_data, Packet& p, size_t off);
  Tub<l3_protocol::l3packet> get_packet();
  bool in_my_netmask(ipv4_address a) const {
    return !((a.ip ^ _host_address.ip) & _netmask.ip);
  }
  void frag_limit_mem();
  void frag_timeout();
  void frag_drop(ipv4_frag_id frag_id, uint32_t dropped_size) {
    _frags.erase(frag_id);
    _frag_mem -= dropped_size;
  }
  void frag_arm(utime_t now) {
    auto tp = now + _frag_timeout;
    frag_timefd.construct(center->create_time_event(tp.to_nsec() / 1000, frag_handler));
  }
  void frag_arm() {
    auto now = ceph_clock_now(cct);
    frag_timefd.construct(center->create_time_event(now.to_nsec() / 1000, frag_handler));
  }

 public:
  explicit ipv4(interface* netif);
  ~ipv4() {
    delete frag_handler;
  }
  void set_host_address(ipv4_address ip) {
    _host_address = ip;
    _arp.set_self_addr(ip);
  }
  ipv4_address host_address() {
    return _host_address;
  }
  void set_gw_address(ipv4_address ip) {
    _gw_address = ip;
  }
  ipv4_address gw_address() const {
    return _gw_address;
  }
  void set_netmask_address(ipv4_address ip) {
    _netmask = ip;
  }
  ipv4_address netmask_address() const {
    return _netmask;
  }
  interface *netif() const {
    return _netif;
  }
  // TODO or something. Should perhaps truly be a list
  // of filters. With ordering. And blackjack. Etc.
  // But for now, a simple single raw pointer suffices
  void set_packet_filter(ip_packet_filter *) {
    _packet_filter = f;
  }
  ip_packet_filter * packet_filter() const {
    return _packet_filter;
  }
  void send(ipv4_address to, ip_protocol_num proto_num, Packet p, ethernet_address e_dst);
  tcp<ipv4_traits>& get_tcp() { return *_tcp._tcp; }
  void register_l4(proto_type id, ip_protocol* handler);
  const hw_features& hw_features() const { return _netif->hw_features(); }
  static bool needs_frag(Packet& p, ip_protocol_num proto_num, net::hw_features hw_features) {
    if (p.len() + ipv4_hdr_len_min <= hw_features.mtu)
      return false;

    if ((prot_num == ip_protocol_num::tcp && hw_features.tx_tso))
      return false;

    return true;
  }
  void learn(ethernet_address l2, ipv4_address l3) {
    _arp.learn(l2, l3);
  }
  void register_packet_provider(ipv4_traits::packet_provider_type&& func) {
    _pkt_providers.push_back(std::move(func));
  }
  void wait_l2_dst_address(ipv4_address to, resolution_cb &&cb);
};

template <ip_protocol_num ProtoNum>
inline void ipv4_l4<ProtoNum>::register_packet_provider(
    ipv4_traits::packet_provider_type func) {
  auto func = std::move(func);
  _inet.register_packet_provider([func] {
    auto l4p = func();
    if (l4p) {
      l4p.value().proto_num = ProtoNum;
    }
    return l4p;
  });
}

template <ip_protocol_num ProtoNum>
inline void ipv4_l4<ProtoNum>::wait_l2_dst_address(ipv4_address to, resolution_cb &&cb) {
  _inet.wait_l2_dst_address(to, cb);
}

struct ip_hdr {
  uint8_t ihl : 4;
  uint8_t ver : 4;
  uint8_t dscp : 6;
  uint8_t ecn : 2;
  unaligned<uint16_t> len;
  unaligned<uint16_t> id;
  unaligned<uint16_t> frag;
  enum class frag_bits : uint8_t { mf = 13, df = 14, reserved = 15, offset_shift = 3 };
  uint8_t ttl;
  uint8_t ip_proto;
  unaligned<uint16_t> csum;
  ipv4_address src_ip;
  ipv4_address dst_ip;
  uint8_t options[0];
  template <typename Adjuster>
  auto adjust_endianness(Adjuster a) {
    return a(len, id, frag, csum, src_ip, dst_ip);
  }
  bool mf() { return frag & (1 << uint8_t(frag_bits::mf)); }
  bool df() { return frag & (1 << uint8_t(frag_bits::df)); }
  uint16_t offset() { return frag << uint8_t(frag_bits::offset_shift); }
} __attribute__((packed));

template <typename InetTraits>
struct l4connid<InetTraits>::connid_hash : private std::hash<ipaddr>, private std::hash<uint16_t> {
  size_t operator()(const l4connid<InetTraits>& id) const noexcept {
    using h1 = std::hash<ipaddr>;
    using h2 = std::hash<uint16_t>;
    return h1::operator()(id.local_ip)
           ^ h1::operator()(id.foreign_ip)
           ^ h2::operator()(id.local_port)
           ^ h2::operator()(id.foreign_port);
  }
};

void arp_learn(ethernet_address l2, ipv4_address l3);

#endif /* CEPH_MSG_IP_H */
