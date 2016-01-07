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

interface::interface(CephContext *c, std::shared_ptr<device> dev, unsigned cpuid)
    : cct(c), _dev(dev),
      _rx(_dev->receive(
          cpuid,
          [cpuid, this] (Packet p) {
            return dispatch_packet(cpuid, std::move(p));
          }
      )),
      _hw_address(_dev->hw_address()),
      _hw_features(_dev->hw_features()) {
  auto idx = 0u;
  dev->queue_for_cpu(cpuid).register_packet_provider([this, idx] () mutable {
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
        p.construct(l3pv.p);
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

class C_handle_l2forward : public EventCallback {
  std::shared_ptr<DPDKDevice> sdev;
  unsigned &queue_depth;

 public:
  C_handle_l2forward(std::shared_ptr<DPDKDevice> &p, unsigned &qd)
      : sdev(p), queue_depth(qd) {}
  void do_request(int fd) {
    _dev->l2receive(p.free_on_cpu(src_cpu));
    queue_depth--;
    delete this;
  }
};

void interface::forward(unsigned cpuid, Packet p) {
  static __thread unsigned queue_depth;

  if (queue_depth < 1000) {
    queue_depth++;
    stacks[cpudid]->center->create_external_event(new C_handle_l2forward(_dev));
  }
}

int interface::dispatch_packet(unsigned cpuid, Packet p) {
  auto eh = p.get_header<eth_hdr>();
  if (eh) {
    auto i = _proto_map.find(ntoh(eh->eth_proto));
    if (i != _proto_map.end()) {
      l3_rx_stream& l3 = i->second;
      auto fw = _dev->forward_dst(cpuid, [&p, &l3, this] () {
        auto hwrss = p.rss_hash();
        if (hwrss) {
          return hwrss.value();
        } else {
          forward_hash data;
          if (l3.forward(data, p, sizeof(eth_hdr))) {
            return toeplitz_hash(rss_key(), data);
          }
          return 0u;
        }
      });
      if (fw != cpuid) {
        forward(fw, std::move(p));
      } else {
        auto h = ntoh(*eh);
        auto from = h.src_mac;
        p.trim_front(sizeof(*eh));
        // avoid chaining, since queue length is unlimited
        // drop instead.
        if (l3.ready()) {
          l3.ready = l3.packet_stream.produce(std::move(p), from);
        }
      }
    }
  }
}
