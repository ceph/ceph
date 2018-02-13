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

#include "net.h"
#include "DPDK.h"
#include "DPDKStack.h"

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "net "

interface::interface(CephContext *c, std::shared_ptr<DPDKDevice> dev, EventCenter *center)
    : cct(c), _dev(dev),
      _rx(_dev->receive(
          center->get_id(),
          [center, this] (Packet p) {
            return dispatch_packet(center, std::move(p));
          }
      )),
      _hw_address(_dev->hw_address()),
      _hw_features(_dev->get_hw_features()) {
  auto idx = 0u;
  unsigned qid = center->get_id();
  dev->queue_for_cpu(center->get_id()).register_packet_provider([this, idx, qid] () mutable {
    Tub<Packet> p;
    for (size_t i = 0; i < _pkt_providers.size(); i++) {
      auto l3p = _pkt_providers[idx++]();
      if (idx == _pkt_providers.size())
        idx = 0;
      if (l3p) {
        auto l3pv = std::move(*l3p);
        auto eh = l3pv.p.prepend_header<eth_hdr>();
        eh->dst_mac = l3pv.to;
        eh->src_mac = _hw_address;
        eh->eth_proto = uint16_t(l3pv.proto_num);
        *eh = eh->hton();
        ldout(cct, 10) << "=== tx === proto " << std::hex << uint16_t(l3pv.proto_num)
                       << " " << _hw_address << " -> " << l3pv.to
                       << " length " << std::dec << l3pv.p.len() << dendl;
        p = std::move(l3pv.p);
        return p;
      }
    }
    return p;
  });
}

subscription<Packet, ethernet_address> interface::register_l3(
    eth_protocol_num proto_num,
    std::function<int (Packet p, ethernet_address from)> next,
    std::function<bool (forward_hash&, Packet& p, size_t)> forward)
{
  auto i = _proto_map.emplace(std::piecewise_construct, std::make_tuple(uint16_t(proto_num)), std::forward_as_tuple(std::move(forward)));
  assert(i.second);
  l3_rx_stream& l3_rx = i.first->second;
  return l3_rx.packet_stream.listen(std::move(next));
}

unsigned interface::hash2cpu(uint32_t hash) {
  return _dev->hash2cpu(hash);
}

const rss_key_type& interface::rss_key() const {
  return _dev->rss_key();
}

uint16_t interface::hw_queues_count() const {
  return _dev->hw_queues_count();
}

class C_handle_l2forward : public EventCallback {
  std::shared_ptr<DPDKDevice> sdev;
  unsigned &queue_depth;
  Packet p;
  unsigned dst;

 public:
  C_handle_l2forward(std::shared_ptr<DPDKDevice> &p, unsigned &qd, Packet pkt, unsigned target)
      : sdev(p), queue_depth(qd), p(std::move(pkt)), dst(target) {}
  void do_request(uint64_t fd) {
    sdev->l2receive(dst, std::move(p));
    queue_depth--;
    delete this;
  }
};

void interface::forward(EventCenter *source, unsigned target, Packet p) {
  static __thread unsigned queue_depth;

  if (queue_depth < 1000) {
    queue_depth++;
    // FIXME: need ensure this event not be called after EventCenter destruct
    _dev->workers[target]->center.dispatch_event_external(
        new C_handle_l2forward(_dev, queue_depth, std::move(p.free_on_cpu(source)), target));
  }
}

int interface::dispatch_packet(EventCenter *center, Packet p) {
  auto eh = p.get_header<eth_hdr>();
  if (eh) {
    auto i = _proto_map.find(ntoh(eh->eth_proto));
    auto hwrss = p.rss_hash();
    if (hwrss) {
      ldout(cct, 10) << __func__ << " === rx === proto " << std::hex << ::ntoh(eh->eth_proto)
                     << " "<< eh->src_mac.ntoh() << " -> " << eh->dst_mac.ntoh()
                     << " length " << std::dec << p.len() << " rss_hash " << *p.rss_hash() << dendl;
    } else {
      ldout(cct, 10) << __func__ << " === rx === proto " << std::hex << ::ntoh(eh->eth_proto)
                     << " "<< eh->src_mac.ntoh() << " -> " << eh->dst_mac.ntoh()
                     << " length " << std::dec << p.len() << dendl;
    }
    if (i != _proto_map.end()) {
      l3_rx_stream& l3 = i->second;
      auto fw = _dev->forward_dst(center->get_id(), [&p, &l3, this] () {
        auto hwrss = p.rss_hash();
        if (hwrss) {
          return *hwrss;
        } else {
          forward_hash data;
          if (l3.forward(data, p, sizeof(eth_hdr))) {
            return toeplitz_hash(rss_key(), data);
          }
          return 0u;
        }
      });
      if (fw != center->get_id()) {
        ldout(cct, 1) << __func__ << " forward to " << fw << dendl;
        forward(center, fw, std::move(p));
      } else {
        auto h = eh->ntoh();
        auto from = h.src_mac;
        p.trim_front(sizeof(*eh));
        // avoid chaining, since queue length is unlimited
        // drop instead.
        if (l3.ready()) {
          return l3.packet_stream.produce(std::move(p), from);
        }
      }
    }
  }
  return 0;
}

class C_arp_learn : public EventCallback {
  DPDKWorker *worker;
  ethernet_address l2_addr;
  ipv4_address l3_addr;

 public:
  C_arp_learn(DPDKWorker *w, ethernet_address l2, ipv4_address l3)
      : worker(w), l2_addr(l2), l3_addr(l3) {}
  void do_request(uint64_t id) {
    worker->arp_learn(l2_addr, l3_addr);
    delete this;
  }
};

void interface::arp_learn(ethernet_address l2, ipv4_address l3)
{
  for (auto &&w : _dev->workers) {
    w->center.dispatch_event_external(
        new C_arp_learn(w, l2, l3));
  }
}

l3_protocol::l3_protocol(interface* netif, eth_protocol_num proto_num, packet_provider_type func)
    : _netif(netif), _proto_num(proto_num)  {
  _netif->register_packet_provider(std::move(func));
}

subscription<Packet, ethernet_address> l3_protocol::receive(
    std::function<int (Packet, ethernet_address)> rx_fn,
    std::function<bool (forward_hash &h, Packet &p, size_t s)> forward) {
  return _netif->register_l3(_proto_num, std::move(rx_fn), std::move(forward));
};
