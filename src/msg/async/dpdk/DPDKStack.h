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
#include <optional>

#include "common/ceph_context.h"

#include "msg/async/Stack.h"
#include "net.h"
#include "const.h"
#include "IP.h"
#include "Packet.h"
#include "dpdk_rte.h"

class interface;

template <typename Protocol>
class NativeConnectedSocketImpl;

// DPDKServerSocketImpl
template <typename Protocol>
class DPDKServerSocketImpl : public ServerSocketImpl {
  typename Protocol::listener _listener;
 public:
  DPDKServerSocketImpl(Protocol& proto, uint16_t port, const SocketOptions &opt,
		       int type, unsigned addr_slot);
  int listen() {
    return _listener.listen();
  }
  virtual int accept(ConnectedSocket *s, const SocketOptions &opts, entity_addr_t *out, Worker *w) override;
  virtual void abort_accept() override;
  virtual int fd() const override {
    return _listener.fd();
  }
  virtual void set_priority(int sd, int prio, int domain) override {}
};

// NativeConnectedSocketImpl
template <typename Protocol>
class NativeConnectedSocketImpl : public ConnectedSocketImpl {
  typename Protocol::connection _conn;
  uint32_t _cur_frag = 0;
  uint32_t _cur_off = 0;
  std::optional<Packet> _buf;
  std::optional<bufferptr> _cache_ptr;

 public:
  explicit NativeConnectedSocketImpl(typename Protocol::connection conn)
          : _conn(std::move(conn)) {}
  NativeConnectedSocketImpl(NativeConnectedSocketImpl &&rhs)
      : _conn(std::move(rhs._conn)), _buf(std::move(rhs.buf))  {}
  virtual int is_connected() override {
    return _conn.is_connected();
  }

  virtual ssize_t read(char *buf, size_t len) override {
    size_t left = len;
    ssize_t r = 0;
    size_t off = 0;
    while (left > 0) {
      if (!_cache_ptr) {
        _cache_ptr.emplace();
        r = zero_copy_read(*_cache_ptr);
        if (r <= 0) {
          _cache_ptr.reset();
          if (r == -EAGAIN)
            break;
          return r;
        }
      }
      if (_cache_ptr->length() <= left) {
        _cache_ptr->copy_out(0, _cache_ptr->length(), buf+off);
        left -= _cache_ptr->length();
        off += _cache_ptr->length();
        _cache_ptr.reset();
      } else {
        _cache_ptr->copy_out(0, left, buf+off);
        _cache_ptr->set_offset(_cache_ptr->offset() + left);
        _cache_ptr->set_length(_cache_ptr->length() - left);
        left = 0;
        break;
      }
    }
    return len - left ? len - left : -EAGAIN;
  }

private:
  ssize_t zero_copy_read(bufferptr &data) {
    auto err = _conn.get_errno();
    if (err <= 0)
      return err;

    if (!_buf) {
      _buf = std::move(_conn.read());
      if (!_buf)
        return -EAGAIN;
    }

    fragment &f = _buf->frag(_cur_frag);
    Packet p = _buf->share(_cur_off, f.size);
    auto del = std::bind(
            [](Packet &p) {}, std::move(p));
    data = buffer::claim_buffer(
            f.size, f.base, make_deleter(std::move(del)));
    if (++_cur_frag == _buf->nr_frags()) {
      _cur_frag = 0;
      _cur_off = 0;
      _buf.reset();
    } else {
      _cur_off += f.size;
    }
    ceph_assert(data.length());
    return data.length();
  }
  virtual ssize_t send(bufferlist &bl, bool more) override {
    auto err = _conn.get_errno();
    if (err < 0)
      return (ssize_t)err;

    size_t available = _conn.peek_sent_available();
    if (available == 0) {
      return 0;
    }

    std::vector<fragment> frags;
    auto pb = bl.buffers().begin();
    uint64_t len = 0;
    uint64_t seglen = 0;
    while (len < available && pb != bl.buffers().end()) {
      seglen = pb->length();
      // Buffer length is zero, no need to send, so skip it
      if (seglen == 0) {
        ++pb;
        continue;
      }
      if (len + seglen > available) {
        // don't continue if we enough at least 1 fragment since no available
        // space for next ptr.
        if (len > 0)
          break;
        seglen = std::min(seglen, available);
      }
      len += seglen;
      frags.push_back(fragment{(char*)pb->c_str(), seglen});
      ++pb;
    }

    if (len != bl.length()) {
      bufferlist swapped;
      bl.splice(0, len, &swapped);
      auto del = std::bind(
              [](bufferlist &bl) {}, std::move(swapped));
      return _conn.send(Packet(std::move(frags), make_deleter(std::move(del))));
    } else {
      auto del = std::bind(
              [](bufferlist &bl) {}, std::move(bl));

      return _conn.send(Packet(std::move(frags), make_deleter(std::move(del))));
    }
  }

public:
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
  Protocol& proto, uint16_t port, const SocketOptions &opt,
  int type, unsigned addr_slot)
  : ServerSocketImpl(type, addr_slot), _listener(proto.listen(port)) {}

template <typename Protocol>
int DPDKServerSocketImpl<Protocol>::accept(ConnectedSocket *s, const SocketOptions &options, entity_addr_t *out, Worker *w) {
  if (_listener.get_errno() < 0)
    return _listener.get_errno();
  auto c = _listener.accept();
  if (!c)
    return -EAGAIN;

  if (out) {
    *out = c->remote_addr();
    out->set_type(addr_type);
  }
  std::unique_ptr<NativeConnectedSocketImpl<Protocol>> csi(
          new NativeConnectedSocketImpl<Protocol>(std::move(*c)));
  *s = ConnectedSocket(std::move(csi));
  return 0;
}

template <typename Protocol>
void DPDKServerSocketImpl<Protocol>::abort_accept() {
  _listener.abort_accept();
}

class DPDKWorker : public Worker {
  struct Impl {
    unsigned id;
    interface _netif;
    std::shared_ptr<DPDKDevice> _dev;
    ipv4 _inet;
    Impl(CephContext *cct, unsigned i, EventCenter *c, std::shared_ptr<DPDKDevice> dev);
    ~Impl();
  };
  std::unique_ptr<Impl> _impl;

  virtual void initialize() override;
  void set_ipv4_packet_filter(ip_packet_filter* filter) {
    _impl->_inet.set_packet_filter(filter);
  }
  using tcp4 = tcp<ipv4_traits>;

 public:
  explicit DPDKWorker(CephContext *c, unsigned i): Worker(c, i) {}
  virtual int listen(entity_addr_t &addr, unsigned addr_slot,
		     const SocketOptions &opts, ServerSocket *) override;
  virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;
  void arp_learn(ethernet_address l2, ipv4_address l3) {
    _impl->_inet.learn(l2, l3);
  }
  virtual void destroy() override {
    _impl.reset();
  }

  friend class DPDKServerSocketImpl<tcp4>;
};

using namespace dpdk;
class DPDKStack : public NetworkStack {
  std::vector<std::function<void()> > funcs;

  virtual Worker* create_worker(CephContext *c, unsigned worker_id) override {
    return new DPDKWorker(c, worker_id);
  }
  virtual void rename_thread(unsigned id) override {}

 public:
  explicit DPDKStack(CephContext *cct): NetworkStack(cct), eal(cct) {
    funcs.reserve(cct->_conf->ms_async_op_threads);
  }
  virtual bool support_local_listen_table() const override { return true; }

  virtual void spawn_worker(std::function<void ()> &&func) override;
  virtual void join_worker(unsigned i) override;
 private:
  dpdk::eal eal;
};

#endif
