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
#ifndef CEPH_MSG_DPDKSTACK_H
#define CEPH_MSG_DPDKSTACK_H

#include <functional>

#include "common/ceph_context.h"
#include "common/Tub.h"

#include "msg/async/GenericSocket.h"
#include "const.h"
#include "Packet.h"

class interface;

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
  explicit l3_protocol(interface* netif, eth_protocol_num proto_num, packet_provider_type func) : _netif(netif), _proto_num(proto_num)  {
    _netif->register_packet_provider(std::move(func));
  }
  subscription<Packet, ethernet_address> l3_protocol::receive(
      std::function<int (Packet, ethernet_address)> rx_fn,
      std::function<bool (forward_hash &h, Packet &p, size_t s)> forward) {
    return _netif->register_l3(_proto_num, std::move(rx_fn), std::move(forward));
  };

 private:
  friend class interface;
};


template <typename Protocol>
class NativeConnectedSocketImpl;

// DPDKServerSocketImpl
template <typename Protocol>
class DPDKServerSocketImpl : public ServerSocketImpl {
  typename Protocol::listener _listener;
 public:
  DPDKServerSocketImpl(Protocol& proto, uint16_t port, listen_options opt);
  virtual int accept(ConnectedSocket *s) override;
  virtual void abort_accept() override;
};

template <typename Protocol>
DPDKServerSocketImpl<Protocol>::DPDKServerSocketImpl(
        Protocol& proto, uint16_t port, listen_options opt)
        : _listener(proto.listen(port)) {}

template <typename Protocol>
int DPDKServerSocketImpl<Protocol>::accept(ConnectedSocket *s) {
  if (_listener.errno() < 0)
    return _listener.errno();
  auto c = _listener.accept();
  if (c)
    return -EAGAIN;
  *s = std::make_unique<NativeConnectedSocketImpl<Protocol>>(std::move(conn));
  return 0;
}

template <typename Protocol>
void DPDKServerSocketImpl<Protocol>::abort_accept() {
  _listener.abort_accept();
}

// NativeConnectedSocketImpl
template <typename Protocol>
class NativeConnectedSocketImpl : public ConnectedSocketImpl {
  typename Protocol::connection _conn;
  size_t _cur_frag = 0;
  Packet _buf;

 public:
  explicit NativeConnectedSocketImpl(typename Protocol::connection conn)
          : _conn(std::move(conn)) {}
  virtual int connected() override {
    return 1;
  }
  virtual int read(char *buf, size_t len) override {
    if (_conn.errno() <= 0)
      return _conn.errno();

    if (_cur_frag == _buf.nr_frags()) {
      _buf = _conn.read();
      if (_buf) {
        _cur_frag = 0;
      }
    }
    auto& f = _buf.fragments()[_cur_frag++];
    auto p = _buf.share();
    assert(f.size <= len);
    memcpy(buf, f.base, f.size);
    return f.size;
  }
  virtual int sendmsg(struct msghdr &msg, size_t len, bool more) override {
    Packet p;

    while (len > 0) {
      if (msg.msg_iov[0].iov_len <= len) {
        assert(msg.msg_iov[0].iov_len);
        p = Packet(std::move(p),
                   fragment{msg.msg_iov[0].iov_base, msg.msg_iov[0].iov_len},
                   deleter());
        len -= msg.msg_iov[0].iov_len;
        msg.msg_iov++;
        msg.msg_iovlen--;
      } else {
        p = Packet(std::move(p),
                   fragment{msg.msg_iov[0].iov_base, len},
                   deleter());
        msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + len;
        msg.msg_iov[0].iov_len -= len;
        break;
      }
    }
    return _conn.send(std::move(p));
  }
  virtual void shutdown() override {
    _conn.close_read();
    _conn.close_write();
  }
  // FIXME need to impl close
  virtual void close() override { return ; }
  virtual int set_nodelay() override { return 0; }
  virtual int set_rcvbuf(size_t s) override { return 0; }
};

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
  hw_features _hw_features;
  std::vector<l3_protocol::packet_provider_type> _pkt_providers;

 private:
  void dispatch_packet(Packet p);
 public:
  explicit interface(CephContext *cct, std::shared_ptr<device> dev, unsigned cpuid);
  ethernet_address hw_address() { return _hw_address; }
  const hw_features& hw_features() const { return _hw_features; }
  subscription<Packet, ethernet_address> register_l3(
      eth_protocol_num proto_num,
      std::function<int (Packet, ethernet_address)> next,
      std::function<bool (forward_hash&, Packet&, size_t)> forward);
  void forward(unsigned cpuid, Packet p);
  unsigned hash2cpu(uint32_t hash);
  void register_packet_provider(l3_protocol::packet_provider_type func) {
    _pkt_providers.push_back(std::move(func));
  }
  const rss_key_type& rss_key() const;
  friend class l3_protocol;
};

static NetworkStack* stacks[];

class DPDKStack : public NetworkStack {
  static bool created = false;

  interface _netif;
  ipv4 _inet;
  EventCenter *center;
  unsigned cores;
  unsigned cpu_id;

  void set_ipv4_packet_filter(ip_packet_filter* filter) {
    _inet.set_packet_filter(filter);
  }
  using tcp4 = tcp<ipv4_traits>;

 public:
  explicit DPDKStack(CephContext *cct, std::shared_ptr<device> dev, unsigned cores, unsigned i);
  virtual int listen(const entity_addr_t &sa, const listen_options &opt, ServerSocket *sock) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  static std::unique_ptr<NetworkStack> create(CephContext *cct, unsigned i);
  void arp_learn(ethernet_address l2, ipv4_address l3) {
    _inet.learn(l2, l3);
  }
  friend class DPDKServerSocketImpl<tcp4>;
};

#endif
