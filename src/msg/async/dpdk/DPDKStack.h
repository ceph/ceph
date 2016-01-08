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
#include "DPDK.h"
#include "net.h"
#include "const.h"
#include "IP.h"
#include "Packet.h"

class interface;



template <typename Protocol>
class NativeConnectedSocketImpl;

// DPDKServerSocketImpl
template <typename Protocol>
class DPDKServerSocketImpl : public ServerSocketImpl {
  typename Protocol::listener _listener;
 public:
  DPDKServerSocketImpl(Protocol& proto, uint16_t port, const SocketOptions &opt);
  virtual int accept(ConnectedSocket *s, entity_addr_t *out) override;
  virtual void abort_accept() override;
};


// NativeConnectedSocketImpl
template <typename Protocol>
class NativeConnectedSocketImpl : public ConnectedSocketImpl {
  typename Protocol::connection _conn;
  size_t _cur_frag = 0;
  Packet _buf;

 public:
  explicit NativeConnectedSocketImpl(typename Protocol::connection conn)
          : _conn(std::move(conn)) {}
  virtual int is_connected() override {
    return 1;
  }
  virtual int read(char *buf, size_t len) override {
    if (_conn.get_errno() <= 0)
      return _conn.get_errno();

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

template <typename Protocol>
DPDKServerSocketImpl<Protocol>::DPDKServerSocketImpl(
        Protocol& proto, uint16_t port, const SocketOptions &opt)
        : _listener(proto.listen(port)) {}

template <typename Protocol>
int DPDKServerSocketImpl<Protocol>::accept(ConnectedSocket *s, entity_addr_t *out){
  if (_listener.get_errno() < 0)
    return _listener.get_errno();
  auto c = _listener.accept();
  if (!c)
    return -EAGAIN;

  std::unique_ptr<NativeConnectedSocketImpl<Protocol>> csi(new NativeConnectedSocketImpl<Protocol>(*c));
  *s = ConnectedSocket(std::move(csi));
  if (out)
    *out = c->remote_addr();
  return 0;
}

template <typename Protocol>
void DPDKServerSocketImpl<Protocol>::abort_accept() {
  _listener.abort_accept();
}

class DPDKStack : public NetworkStack {
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
  explicit DPDKStack(CephContext *cct, std::shared_ptr<DPDKDevice> dev, unsigned cores, unsigned i);
  virtual int listen(const entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  static std::unique_ptr<NetworkStack> create(CephContext *cct, unsigned i);
  void arp_learn(ethernet_address l2, ipv4_address l3) {
    _inet.learn(l2, l3);
  }
  friend class DPDKServerSocketImpl<tcp4>;
};

#endif
