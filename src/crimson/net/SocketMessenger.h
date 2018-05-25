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

#include <list>
#include <boost/optional.hpp>
#include <core/reactor.hh>

#include "Messenger.h"

namespace ceph::net {

class SocketMessenger final : public Messenger {
  boost::optional<seastar::server_socket> listener;
  Dispatcher *dispatcher = nullptr;
  uint32_t global_seq = 0;

  std::list<ConnectionRef> connections;

  seastar::future<> dispatch(ConnectionRef conn);

  seastar::future<> accept(seastar::connected_socket socket,
                           seastar::socket_address paddr);

 public:
  SocketMessenger(const entity_name_t& myname);

  void bind(const entity_addr_t& addr) override;

  seastar::future<> start(Dispatcher *dispatcher) override;

  seastar::future<ConnectionRef> connect(const entity_addr_t& addr,
					 entity_type_t peer_type,
                                         const entity_addr_t& myaddr,
					 entity_type_t host_type) override;

  seastar::future<> shutdown() override;
  seastar::future<msgr_tag_t, bufferlist>
  verify_authorizer(peer_type_t peer_type,
		    auth_proto_t protocol,
		    bufferlist& auth) override;
  seastar::future<std::unique_ptr<AuthAuthorizer>>
  get_authorizer(peer_type_t peer_type,
		 bool force_new) override;
};

} // namespace ceph::net
