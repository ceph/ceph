// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <seastar/core/future.hh>

#include "Fwd.h"
#include "crimson/thread/Throttle.h"
#include "msg/Policy.h"

class AuthAuthorizer;

namespace ceph::net {

using Throttle = ceph::thread::Throttle;
using SocketPolicy = ceph::net::Policy<Throttle>;

class Messenger {
  entity_name_t my_name;
  entity_addrvec_t my_addrs;
  uint32_t global_seq = 0;
  uint32_t crc_flags = 0;

 public:
  Messenger(const entity_name_t& name)
    : my_name(name)
  {}
  virtual ~Messenger() {}

  const entity_name_t& get_myname() const { return my_name; }
  const entity_addrvec_t& get_myaddrs() const { return my_addrs; }
  entity_addr_t get_myaddr() const { return my_addrs.front(); }
  virtual seastar::future<> set_myaddrs(const entity_addrvec_t& addrs) {
    my_addrs = addrs;
    return seastar::now();
  }

  /// bind to the given address
  virtual seastar::future<> bind(const entity_addrvec_t& addr) = 0;

  /// try to bind to the first unused port of given address
  virtual seastar::future<> try_bind(const entity_addrvec_t& addr,
                                     uint32_t min_port, uint32_t max_port) = 0;

  /// start the messenger
  virtual seastar::future<> start(Dispatcher *dispatcher) = 0;

  /// either return an existing connection to the peer,
  /// or a new pending connection
  virtual seastar::future<ConnectionXRef>
  connect(const entity_addr_t& peer_addr,
          const entity_type_t& peer_type) = 0;

  // wait for messenger shutdown
  virtual seastar::future<> wait() = 0;

  /// stop listenening and wait for all connections to close. safe to destruct
  /// after this future becomes available
  virtual seastar::future<> shutdown() = 0;

  uint32_t get_global_seq(uint32_t old=0) {
    if (old > global_seq) {
      global_seq = old;
    }
    return ++global_seq;
  }

  uint32_t get_crc_flags() const {
    return crc_flags;
  }
  void set_crc_data() {
    crc_flags |= MSG_CRC_DATA;
  }
  void set_crc_header() {
    crc_flags |= MSG_CRC_HEADER;
  }

  // get the local messenger shard if it is accessed by another core
  virtual Messenger* get_local_shard() {
    return this;
  }

  virtual void print(ostream& out) const = 0;

  virtual void set_default_policy(const SocketPolicy& p) = 0;

  virtual void set_policy(entity_type_t peer_type, const SocketPolicy& p) = 0;

  virtual void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) = 0;

  static seastar::future<Messenger*>
  create(const entity_name_t& name,
         const std::string& lname,
         const uint64_t nonce,
         const int master_sid=-1);
};

inline ostream& operator<<(ostream& out, const Messenger& msgr) {
  out << "[";
  msgr.print(out);
  out << "]";
  return out;
}

} // namespace ceph::net
