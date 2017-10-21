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

class SocketMessenger : public Messenger {
  boost::optional<seastar::server_socket> listener;
  Dispatcher *dispatcher = nullptr;
  std::list<ConnectionRef> connections;

  seastar::future<> dispatch(ConnectionRef conn);

  seastar::future<> accept(seastar::connected_socket socket,
                           seastar::socket_address paddr);

 public:
  SocketMessenger(const entity_name_t& myname);

  void bind(const entity_addr_t& addr) override;

  seastar::future<> start(Dispatcher *dispatcher) override;

  seastar::future<ConnectionRef> connect(const entity_addr_t& addr,
                                         const entity_addr_t& myaddr) override;

  seastar::future<> shutdown() override;
};

} // namespace ceph::net
