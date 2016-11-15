// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#ifndef CEPH_MSG_DPDK_NET_H
#define CEPH_MSG_DPDK_NET_H

#include "const.h"
#include "ethernet.h"
#include "Packet.h"
#include "stream.h"
#include "toeplitz.h"

struct hw_features {
  // Enable tx ip header checksum offload
  bool tx_csum_ip_offload = false;
  // Enable tx l4 (TCP or UDP) checksum offload
  bool tx_csum_l4_offload = false;
  // Enable rx checksum offload
  bool rx_csum_offload = false;
  // LRO is enabled
  bool rx_lro = false;
  // Enable tx TCP segment offload
  bool tx_tso = false;
  // Enable tx UDP fragmentation offload
  bool tx_ufo = false;
  // Maximum Transmission Unit
  uint16_t mtu = 1500;
  // Maximun packet len when TCP/UDP offload is enabled
  uint16_t max_packet_len = ip_packet_len_max - eth_hdr_len;
};

class forward_hash {
  uint8_t data[64];
  size_t end_idx = 0;
 public:
  size_t size() const {
    return end_idx;
  }
  void push_back(uint8_t b) {
    assert(end_idx < sizeof(data));
    data[end_idx++] = b;
  }
  void push_back(uint16_t b) {
    push_back(uint8_t(b));
    push_back(uint8_t(b >> 8));
  }
  void push_back(uint32_t b) {
    push_back(uint16_t(b));
    push_back(uint16_t(b >> 16));
  }
  const uint8_t& operator[](size_t idx) const {
    return data[idx];
  }
};

class interface;

class l3_protocol {
 public:
  struct l3packet {
    eth_protocol_num proto_num;
    ethernet_address to;
    Packet p;
  };
  using packet_provider_type = std::function<Tub<l3packet> ()>;

 private:
  interface* _netif;
  eth_protocol_num _proto_num;

 public:
  explicit l3_protocol(interface* netif, eth_protocol_num proto_num, packet_provider_type func);
  subscription<Packet, ethernet_address> receive(
      std::function<int (Packet, ethernet_address)> rx_fn,
      std::function<bool (forward_hash &h, Packet &p, size_t s)> forward);

 private:
  friend class interface;
};

class DPDKDevice;
struct ipv4_address;

class interface {
  CephContext *cct;
  struct l3_rx_stream {
    stream<Packet, ethernet_address> packet_stream;
    std::function<bool (forward_hash&, Packet&, size_t)> forward;
    bool ready() { return packet_stream.started(); }
    l3_rx_stream(std::function<bool (forward_hash&, Packet&, size_t)>&& fw) : forward(fw) {}
  };
  std::unordered_map<uint16_t, l3_rx_stream> _proto_map;
  std::shared_ptr<DPDKDevice> _dev;
  subscription<Packet> _rx;
  ethernet_address _hw_address;
  struct hw_features _hw_features;
  std::vector<l3_protocol::packet_provider_type> _pkt_providers;

 private:
  int dispatch_packet(EventCenter *c, Packet p);
 public:
  explicit interface(CephContext *cct, std::shared_ptr<DPDKDevice> dev, EventCenter *c);
  ethernet_address hw_address() { return _hw_address; }
  const struct hw_features& get_hw_features() const { return _hw_features; }
  subscription<Packet, ethernet_address> register_l3(
      eth_protocol_num proto_num,
      std::function<int (Packet, ethernet_address)> next,
      std::function<bool (forward_hash&, Packet&, size_t)> forward);
  void forward(EventCenter *source, unsigned target, Packet p);
  unsigned hash2cpu(uint32_t hash);
  void register_packet_provider(l3_protocol::packet_provider_type func) {
    _pkt_providers.push_back(std::move(func));
  }
  const rss_key_type& rss_key() const;
  uint16_t hw_queues_count() const;
  void arp_learn(ethernet_address l2, ipv4_address l3);
  friend class l3_protocol;
};

#endif //CEPH_MSG_DPDK_NET_H
