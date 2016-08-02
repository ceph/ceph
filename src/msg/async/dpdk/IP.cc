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

#include "common/perf_counters.h"

#include "capture.h"
#include "IP.h"
#include "shared_ptr.h"
#include "toeplitz.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "dpdk "

std::ostream& operator<<(std::ostream& os, ipv4_address a) {
  auto ip = a.ip;
  return os << ((ip >> 24) & 0xff) << "." << ((ip >> 16) & 0xff)
            << "." << ((ip >> 8) & 0xff) << "." << ((ip >> 0) & 0xff);
}

utime_t ipv4::_frag_timeout = utime_t(30, 0);
constexpr uint32_t ipv4::_frag_low_thresh;
constexpr uint32_t ipv4::_frag_high_thresh;

class C_handle_frag_timeout : public EventCallback {
  ipv4 *_ipv4;

 public:
  C_handle_frag_timeout(ipv4 *i): _ipv4(i) {}
  void do_request(int fd_or_id) {
    _ipv4->frag_timeout();
  }
};

enum {
  l_dpdk_qp_first = 99000,
  l_dpdk_total_linearize_operations,
  l_dpdk_qp_last
};

ipv4::ipv4(CephContext *c, EventCenter *cen, interface* netif)
  : cct(c), center(cen), _netif(netif), _global_arp(netif),
    _arp(c, _global_arp, cen),
    _host_address(0), _gw_address(0), _netmask(0),
    _l3(netif, eth_protocol_num::ipv4, [this] { return get_packet(); }),
    _rx_packets(
      _l3.receive(
        [this] (Packet p, ethernet_address ea) {
          return handle_received_packet(std::move(p), ea);
        },
        [this] (forward_hash& out_hash_data, Packet& p, size_t off) {
          return forward(out_hash_data, p, off);
        }
      )
    ),
    _tcp(*this, cen), _icmp(c, *this),
    _l4({{ uint8_t(ip_protocol_num::tcp), &_tcp },
         { uint8_t(ip_protocol_num::icmp), &_icmp }}),
    _packet_filter(nullptr)
{
  PerfCountersBuilder plb(cct, "ipv4", l_dpdk_qp_first, l_dpdk_qp_last);
  plb.add_u64_counter(l_dpdk_total_linearize_operations, "dpdk_ip_linearize_operations", "DPDK IP Packet linearization operations");
  perf_logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf_logger);
  frag_handler = new C_handle_frag_timeout(this);
}

bool ipv4::forward(forward_hash& out_hash_data, Packet& p, size_t off)
{
  auto iph = p.get_header<ip_hdr>(off);

  out_hash_data.push_back(iph->src_ip.ip);
  out_hash_data.push_back(iph->dst_ip.ip);

  auto h = iph->ntoh();
  auto l4 = _l4[h.ip_proto];
  if (l4) {
    if (h.mf() == false && h.offset() == 0) {
      // This IP datagram is atomic, forward according to tcp connection hash
      l4->forward(out_hash_data, p, off + sizeof(ip_hdr));
    }
    // else forward according to ip fields only
  }
  return true;
}

int ipv4::handle_received_packet(Packet p, ethernet_address from)
{
  auto iph = p.get_header<ip_hdr>(0);
  if (!iph) {
    return 0;
  }

  // Skip checking csum of reassembled IP datagram
  if (!get_hw_features().rx_csum_offload && !p.offload_info_ref().reassembled) {
    checksummer csum;
    csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
    if (csum.get() != 0) {
      return 0;
    }
  }

  auto h = iph->ntoh();
  unsigned ip_len = h.len;
  unsigned ip_hdr_len = h.ihl * 4;
  unsigned pkt_len = p.len();
  auto offset = h.offset();

  ldout(cct, 10) << __func__ << " get " << std::hex << int(h.ip_proto)
                 << std::dec << " packet from "
                 << h.src_ip << " -> " << h.dst_ip << " id=" << h.id
                 << " ip_len=" << ip_len << " ip_hdr_len=" << ip_hdr_len
                 << " pkt_len=" << pkt_len << " offset=" << offset << dendl;

  if (pkt_len > ip_len) {
    // Trim extra data in the packet beyond IP total length
    p.trim_back(pkt_len - ip_len);
  } else if (pkt_len < ip_len) {
    // Drop if it contains less than IP total length
    return 0;
  }
  // Drop if the reassembled datagram will be larger than maximum IP size
  if (offset + p.len() > ip_packet_len_max) {
    return 0;
  }

  // FIXME: process options
  if (in_my_netmask(h.src_ip) && h.src_ip != _host_address) {
    ldout(cct, 20) << __func__ << " learn mac " << from << " with " << h.src_ip << dendl;
    _arp.learn(from, h.src_ip);
  }

  if (_packet_filter) {
    bool handled = false;
    _packet_filter->handle(p, &h, from, handled);
    if (handled) {
      return 0;
    }
  }

  if (h.dst_ip != _host_address) {
    // FIXME: forward
    return 0;
  }

  // Does this IP datagram need reassembly
  auto mf = h.mf();
  if (mf == true || offset != 0) {
    frag_limit_mem();
    auto frag_id = ipv4_frag_id{h.src_ip, h.dst_ip, h.id, h.ip_proto};
    auto& frag = _frags[frag_id];
    if (mf == false) {
      frag.last_frag_received = true;
    }
    // This is a newly created frag_id
    if (frag.mem_size == 0) {
      _frags_age.push_back(frag_id);
      frag.rx_time = ceph_clock_now(cct);
    }
    auto added_size = frag.merge(h, offset, std::move(p));
    _frag_mem += added_size;
    if (frag.is_complete()) {
      // All the fragments are received
      auto dropped_size = frag.mem_size;
      auto& ip_data = frag.data.map.begin()->second;
      // Choose a cpu to forward this packet
      auto cpu_id = center->get_id();
      auto l4 = _l4[h.ip_proto];
      if (l4) {
        size_t l4_offset = 0;
        forward_hash hash_data;
        hash_data.push_back(hton(h.src_ip.ip));
        hash_data.push_back(hton(h.dst_ip.ip));
        l4->forward(hash_data, ip_data, l4_offset);
        cpu_id = _netif->hash2cpu(toeplitz_hash(_netif->rss_key(), hash_data));
      }

      // No need to forward if the dst cpu is the current cpu
      if (cpu_id == center->get_id()) {
        l4->received(std::move(ip_data), h.src_ip, h.dst_ip);
      } else {
        auto to = _netif->hw_address();
        auto pkt = frag.get_assembled_packet(from, to);
        _netif->forward(center, cpu_id, std::move(pkt));
      }

      // Delete this frag from _frags and _frags_age
      frag_drop(frag_id, dropped_size);
      _frags_age.remove(frag_id);
      perf_logger->set(l_dpdk_total_linearize_operations,
                       ipv4_packet_merger::linearizations());
    } else {
      // Some of the fragments are missing
      if (frag_timefd) {
        frag_arm();
      }
    }
    return 0;
  }

  auto l4 = _l4[h.ip_proto];
  if (l4) {
    // Trim IP header and pass to upper layer
    p.trim_front(ip_hdr_len);
    l4->received(std::move(p), h.src_ip, h.dst_ip);
  }
  return 0;
}

void ipv4::wait_l2_dst_address(ipv4_address to, Packet p, resolution_cb cb) {
  // Figure out where to send the packet to. If it is a directly connected
  // host, send to it directly, otherwise send to the default gateway.
  ipv4_address dst;
  if (in_my_netmask(to)) {
    dst = to;
  } else {
    dst = _gw_address;
  }

  _arp.wait(std::move(dst), std::move(p), std::move(cb));
}

const hw_features& ipv4::get_hw_features() const
{
  return _netif->get_hw_features();
}

void ipv4::send(ipv4_address to, ip_protocol_num proto_num,
        Packet p, ethernet_address e_dst) {
  auto needs_frag = this->needs_frag(p, proto_num, get_hw_features());

  auto send_pkt = [this, to, proto_num, needs_frag, e_dst] (Packet& pkt, uint16_t remaining, uint16_t offset) mutable  {
    static uint16_t id = 0;
    auto iph = pkt.prepend_header<ip_hdr>();
    iph->ihl = sizeof(*iph) / 4;
    iph->ver = 4;
    iph->dscp = 0;
    iph->ecn = 0;
    iph->len = pkt.len();
    // FIXME: a proper id
    iph->id = id++;
    if (needs_frag) {
      uint16_t mf = remaining > 0;
      // The fragment offset is measured in units of 8 octets (64 bits)
      auto off = offset / 8;
      iph->frag = (mf << uint8_t(ip_hdr::frag_bits::mf)) | off;
    } else {
      iph->frag = 0;
    }
    iph->ttl = 64;
    iph->ip_proto = (uint8_t)proto_num;
    iph->csum = 0;
    iph->src_ip = _host_address;
    iph->dst_ip = to;
    ldout(cct, 20) << " ipv4::send " << " id=" << iph->id << " " << _host_address << " -> " << to
                   << " len " << pkt.len() << dendl;
    *iph = iph->hton();

    if (get_hw_features().tx_csum_ip_offload) {
      iph->csum = 0;
      pkt.offload_info_ref().needs_ip_csum = true;
    } else {
      checksummer csum;
      csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
      iph->csum = csum.get();
    }

    _packetq.push_back(
            l3_protocol::l3packet{eth_protocol_num::ipv4, e_dst, std::move(pkt)});
  };

  if (needs_frag) {
    uint16_t offset = 0;
    uint16_t remaining = p.len();
    auto mtu = get_hw_features().mtu;

    while (remaining) {
      auto can_send = std::min(uint16_t(mtu - ipv4_hdr_len_min), remaining);
      remaining -= can_send;
      auto pkt = p.share(offset, can_send);
      send_pkt(pkt, remaining, offset);
      offset += can_send;
    }
  } else {
    // The whole packet can be send in one shot
    send_pkt(p, 0, 0);
  }
}

Tub<l3_protocol::l3packet> ipv4::get_packet() {
  // _packetq will be mostly empty here unless it hold remnants of previously
  // fragmented packet
  if (_packetq.empty()) {
    for (size_t i = 0; i < _pkt_providers.size(); i++) {
      auto l4p = _pkt_providers[_pkt_provider_idx++]();
      if (_pkt_provider_idx == _pkt_providers.size()) {
        _pkt_provider_idx = 0;
      }
      if (l4p) {
        ldout(cct, 20) << " ipv4::get_packet len " << l4p->p.len() << dendl;
        send(l4p->to, l4p->proto_num, std::move(l4p->p), l4p->e_dst);
        break;
      }
    }
  }

  Tub<l3_protocol::l3packet> p;
  if (!_packetq.empty()) {
    p = std::move(_packetq.front());
    _packetq.pop_front();
  }
  return p;
}

void ipv4::frag_limit_mem() {
  if (_frag_mem <= _frag_high_thresh) {
    return;
  }
  auto drop = _frag_mem - _frag_low_thresh;
  while (drop) {
    if (_frags_age.empty()) {
      return;
    }
    // Drop the oldest frag (first element) from _frags_age
    auto frag_id = _frags_age.front();
    _frags_age.pop_front();

    // Drop from _frags as well
    auto& frag = _frags[frag_id];
    auto dropped_size = frag.mem_size;
    frag_drop(frag_id, dropped_size);

    drop -= std::min(drop, dropped_size);
  }
}

void ipv4::frag_timeout() {
  if (_frags.empty()) {
    return;
  }
  auto now = ceph_clock_now(cct);
  for (auto it = _frags_age.begin(); it != _frags_age.end();) {
    auto frag_id = *it;
    auto& frag = _frags[frag_id];
    if (now > frag.rx_time + _frag_timeout) {
      auto dropped_size = frag.mem_size;
      // Drop from _frags
      frag_drop(frag_id, dropped_size);
      // Drop from _frags_age
      it = _frags_age.erase(it);
    } else {
      // The further items can only be younger
      break;
    }
  }
  if (_frags.size() != 0) {
    frag_arm(now);
  } else {
    _frag_mem = 0;
  }
}

int32_t ipv4::frag::merge(ip_hdr &h, uint16_t offset, Packet p) {
  uint32_t old = mem_size;
  unsigned ip_hdr_len = h.ihl * 4;
  // Store IP header
  if (offset == 0) {
    header = p.share(0, ip_hdr_len);
  }
  // Sotre IP payload
  p.trim_front(ip_hdr_len);
  data.merge(offset, std::move(p));
  // Update mem size
  mem_size = header.memory();
  for (const auto& x : data.map) {
    mem_size += x.second.memory();
  }
  auto added_size = mem_size - old;
  return added_size;
}

bool ipv4::frag::is_complete() {
  // If all the fragments are received, ipv4::frag::merge() should merge all
  // the fragments into a single packet
  auto offset = data.map.begin()->first;
  auto nr_packet = data.map.size();
  return last_frag_received && nr_packet == 1 && offset == 0;
}

Packet ipv4::frag::get_assembled_packet(ethernet_address from, ethernet_address to) {
  auto& ip_header = header;
  auto& ip_data = data.map.begin()->second;
  // Append a ethernet header, needed for forwarding
  auto eh = ip_header.prepend_header<eth_hdr>();
  eh->src_mac = from;
  eh->dst_mac = to;
  eh->eth_proto = uint16_t(eth_protocol_num::ipv4);
  *eh = eh->hton();
  // Prepare a packet contains both ethernet header, ip header and ip data
  ip_header.append(std::move(ip_data));
  auto pkt = std::move(ip_header);
  auto iph = pkt.get_header<ip_hdr>(sizeof(eth_hdr));
  // len is the sum of each fragment
  iph->len = hton(uint16_t(pkt.len() - sizeof(eth_hdr)));
  // No fragmentation for the assembled datagram
  iph->frag = 0;
  // Since each fragment's csum is checked, no need to csum
  // again for the assembled datagram
  offload_info oi;
  oi.reassembled = true;
  pkt.set_offload_info(oi);
  return pkt;
}

void icmp::received(Packet p, ipaddr from, ipaddr to) {
  auto hdr = p.get_header<icmp_hdr>(0);
  if (!hdr || hdr->type != icmp_hdr::msg_type::echo_request) {
    return;
  }
  hdr->type = icmp_hdr::msg_type::echo_reply;
  hdr->code = 0;
  hdr->csum = 0;
  checksummer csum;
  csum.sum(reinterpret_cast<char*>(hdr), p.len());
  hdr->csum = csum.get();

  if (_queue_space.get_or_fail(p.len())) { // drop packets that do not fit the queue
    auto cb = [this, from] (const ethernet_address e_dst, Packet p, int r) mutable {
        if (r == 0) {
          _packetq.emplace_back(ipv4_traits::l4packet{from, std::move(p), e_dst, ip_protocol_num::icmp});
        }
    };
    _inet.wait_l2_dst_address(from, std::move(p), cb);
  }
}
