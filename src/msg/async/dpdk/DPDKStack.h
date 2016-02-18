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
  virtual int fd() const override {
    return _listener.fd();
  }
};


// NativeConnectedSocketImpl
template <typename Protocol>
class NativeConnectedSocketImpl : public ConnectedSocketImpl {
  typename Protocol::connection _conn;
  size_t _cur_frag = 0;
  size_t _cur_frag_off = 0;
  Tub<Packet> _buf;

 public:
  explicit NativeConnectedSocketImpl(typename Protocol::connection conn)
          : _conn(std::move(conn)) {}
  NativeConnectedSocketImpl(NativeConnectedSocketImpl &&rhs)
      : _conn(std::move(rhs._conn)), _cur_frag(rhs._cur_frag),
        _buf(std::move(rhs.buf))  {}
  virtual int is_connected() override {
    return _conn.is_connected();
  }
  virtual int read(char *buf, size_t len) override {
    auto err = _conn.get_errno();
    if (err <= 0)
      return err;

    size_t copied = 0;
    while (copied < len) {
      if (!_buf || _cur_frag == _buf->nr_frags()) {
        _buf = std::move(_conn.read());
        if (_buf) {
          _cur_frag = 0;
        } else {
          return copied == 0 ? -EAGAIN : copied;
        }
      }

      auto& f = _buf->fragments()[_cur_frag];
      size_t buf_left = len - copied;
      if (f.size - _cur_frag_off <= buf_left) {
        memcpy(buf+copied, f.base+_cur_frag_off, f.size-_cur_frag_off);
        ++_cur_frag;
        copied += f.size-_cur_frag_off;
        _cur_frag_off = 0;
      } else {
        memcpy(buf+copied, f.base+_cur_frag_off, buf_left);
        copied += buf_left;
        _cur_frag_off += buf_left;
      }
    }
    return copied;
  }
  virtual int sendmsg(const struct msghdr &msg, bool more) override {
    auto err = _conn.get_errno();
    if (err <= 0)
      return err;

    size_t available = _conn.peek_sent_available();
    if (available == 0)
      return -EAGAIN;

    Packet p;
    size_t idx = 0;
    while (idx < msg.msg_iovlen && available > 0) {
      if (msg.msg_iov[idx].iov_len <= available) {
        assert(msg.msg_iov[idx].iov_len);
        p = Packet(std::move(p),
                   fragment{(char*)msg.msg_iov[idx].iov_base, msg.msg_iov[idx].iov_len},
                   deleter());
        available -= msg.msg_iov[idx].iov_len;
        ++idx;
      } else {
        p = Packet(std::move(p),
                   fragment{(char*)msg.msg_iov[idx].iov_base, available},
                   deleter());
        break;
      }
    }
    return _conn.send(std::move(p));
  }
  virtual void shutdown() override {
    _conn.close_write();
  }
  // FIXME need to impl close
  virtual void close() override {
    _conn.close_write();
  }
  virtual int fd() const override {
    return _conn.fd();
  }
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

  if (out)
    *out = c->remote_addr();
  std::unique_ptr<NativeConnectedSocketImpl<Protocol>> csi(
          new NativeConnectedSocketImpl<Protocol>(std::move(*c)));
  *s = ConnectedSocket(std::move(csi));
  return 0;
}

template <typename Protocol>
void DPDKServerSocketImpl<Protocol>::abort_accept() {
  _listener.abort_accept();
}

class DPDKStack : public NetworkStack {
  interface _netif;
  ipv4 _inet;
  unsigned cores;

  void set_ipv4_packet_filter(ip_packet_filter* filter) {
    _inet.set_packet_filter(filter);
  }
  using tcp4 = tcp<ipv4_traits>;

 public:
  EventCenter *center;

  explicit DPDKStack(CephContext *cct, EventCenter *c,
                     std::shared_ptr<DPDKDevice> dev, unsigned cores);
  virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  static std::unique_ptr<NetworkStack> create(CephContext *cct, EventCenter *center, unsigned i);
  void arp_learn(ethernet_address l2, ipv4_address l3) {
    _inet.learn(l2, l3);
  }
  friend class DPDKServerSocketImpl<tcp4>;
};

#endif
