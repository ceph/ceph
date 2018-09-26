// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "Protocols.h"

namespace ceph::net {

class Workspace
{
  NoneProtocol none_protocol;
  AcceptProtocol accept_protocol;
  ConnectProtocol connect_protocol;
  OpenProtocol open_protocol;
  CloseProtocol close_protocol;
  Protocol *const protocols[static_cast<int>(state_t::LAST)] = {
    &none_protocol,
    &accept_protocol,
    &connect_protocol,
    &open_protocol,
    &close_protocol
  };

 public:
  Workspace(Session *_s,
            Connection *conn,
            Dispatcher *disp,
            Messenger *msgr)
    : none_protocol(protocols, _s, conn, disp, msgr),
      accept_protocol(protocols, _s, conn, disp, msgr),
      connect_protocol(protocols, _s, conn, disp, msgr),
      open_protocol(protocols, _s, conn, disp, msgr),
      close_protocol(protocols, _s, conn, disp, msgr) {
    _s->protocol = &none_protocol;
  }

  void start_accept() {
    none_protocol.start_accept();
  }
  void start_connect(entity_type_t peer_type) {
    none_protocol.start_connect(peer_type);
  }
};

} // namespace ceph::net
