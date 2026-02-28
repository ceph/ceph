// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#pragma once

#include "msg/Connection.h"
#include "msg/Message.h"
#include "global/global_context.h"

//MockConnection - simple stub. Required because PeeringState needs
//to know the features of the peer OSD which sent a peering message
class MockConnection : public Connection {
 private:
  int peer_osd;
  
 public:
  MockConnection(int peer = -1) : Connection(g_ceph_context, nullptr), peer_osd(peer) {
    set_features(CEPH_FEATURES_ALL);
  }

  int get_peer_osd() const {
    return peer_osd;
  }

  bool is_connected() override {
    return true;
  }

  int send_message(Message *m) override {
    m->put();
    return 0;
  }

  void send_keepalive() override {
  }

  void mark_down() override {
  }

  void mark_disposable() override {
  }

  entity_addr_t get_peer_socket_addr() const override {
    return entity_addr_t();
  }
};
