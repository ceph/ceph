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

#include <seastar/core/shared_future.hh>

#include "SocketConnection.h"
#include "SocketMessenger.h"

using namespace ceph::net;

SocketConnection::SocketConnection(SocketMessenger &messenger,
                                   Dispatcher &disp,
                                   const entity_addr_t& my_addr)
  : Connection(&messenger), s(my_addr),
    protocol(std::make_unique<ProtocolV1>(s, *this, disp, messenger))
{
}

void SocketConnection::requeue_sent()
{
  s.out_seq -= s.sent.size();
  while (!s.sent.empty()) {
    auto m = s.sent.front();
    s.sent.pop();
    s.out_q.push(std::move(m));
  }
}
