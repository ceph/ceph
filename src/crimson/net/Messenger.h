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

class AuthAuthorizer;

namespace ceph::net {

class Messenger {
  entity_name_t my_name;
  entity_addr_t my_addr;
  uint32_t global_seq = 0;
  uint32_t crc_flags = 0;

 public:
  Messenger(const entity_name_t& name)
    : my_name(name)
  {}
  virtual ~Messenger() {}

  const entity_name_t& get_myname() const { return my_name; }
  const entity_addr_t& get_myaddr() const { return my_addr; }
  void set_myaddr(const entity_addr_t& addr) {
    my_addr = addr;
  }

  /// bind to the given address
  virtual void bind(const entity_addr_t& addr) = 0;

  /// start the messenger
  virtual seastar::future<> start(Dispatcher *dispatcher) = 0;

  /// establish a client connection and complete a handshake
  virtual seastar::future<ConnectionRef> connect(const entity_addr_t& addr,
						 entity_type_t peer_type) = 0;

  /// stop listenening and wait for all connections to close. safe to destruct
  /// after this future becomes available
  virtual seastar::future<> shutdown() = 0;

  uint32_t get_global_seq(uint32_t old=0) {
    if (old > global_seq) {
      global_seq = old;
    }
    return ++global_seq;
  }
  virtual ConnectionRef lookup_conn(const entity_addr_t&) = 0;
  virtual void register_conn(ConnectionRef) = 0;
  virtual void unregister_conn(ConnectionRef) = 0;

  // @returns a tuple of <is_valid, auth_reply, session_key>
  virtual seastar::future<msgr_tag_t,    /// tag for error, 0 if authorized
                          bufferlist>    /// auth_reply
  verify_authorizer(peer_type_t peer_type,
		    auth_proto_t protocol,
		    bufferlist& auth) = 0;
  virtual seastar::future<std::unique_ptr<AuthAuthorizer>>
  get_authorizer(peer_type_t peer_type,
		 bool force_new) = 0;
  uint32_t get_crc_flags() const {
    return crc_flags;
  }
  void set_crc_data() {
    crc_flags |= MSG_CRC_DATA;
  }
  void set_crc_header() {
    crc_flags |= MSG_CRC_HEADER;
  }
};

} // namespace ceph::net
