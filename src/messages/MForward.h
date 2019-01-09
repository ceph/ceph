// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 * Client requests often need to get forwarded from some monitor
 * to the leader. This class encapsulates the original message
 * along with the client's caps so the leader can do proper permissions
 * checking.
 */

#ifndef CEPH_MFORWARD_H
#define CEPH_MFORWARD_H

#include "msg/Message.h"
#include "mon/MonCap.h"
#include "include/encoding.h"
#include "include/stringify.h"

class MForward : public MessageInstance<MForward> {
public:
  friend factory;

  uint64_t tid;
  uint8_t client_type;
  entity_addrvec_t client_addrs;
  entity_addr_t client_socket_addr;
  MonCap client_caps;
  uint64_t con_features;
  EntityName entity_name;
  PaxosServiceMessage *msg;   // incoming or outgoing message

  string msg_desc;  // for operator<< only
  
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 4;

  MForward() : MessageInstance(MSG_FORWARD, HEAD_VERSION, COMPAT_VERSION),
               tid(0), con_features(0), msg(NULL) {}
  MForward(uint64_t t, PaxosServiceMessage *m, uint64_t feat,
           const MonCap& caps) :
    MessageInstance(MSG_FORWARD, HEAD_VERSION, COMPAT_VERSION),
    tid(t), client_caps(caps), msg(NULL) {
    client_type = m->get_source().type();
    client_addrs = m->get_source_addrs();
    if (auto con = m->get_connection()) {
      client_socket_addr = con->get_peer_socket_addr();
    }
    con_features = feat;
    msg = (PaxosServiceMessage*)m->get();
  }
private:
  ~MForward() override {
    if (msg) {
      // message was unclaimed
      msg->put();
      msg = NULL;
    }
  }

public:
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      header.version = 3;
      header.compat_version = 3;
      encode(tid, payload);
      entity_inst_t client;
      client.name = entity_name_t(client_type, -1);
      client.addr = client_addrs.legacy_addr();
      encode(client, payload, features);
      encode(client_caps, payload, features);
      // Encode client message with intersection of target and source
      // features.  This could matter if the semantics of the encoded
      // message are changed when reencoding with more features than the
      // client had originally.  That should never happen, but we may as
      // well be defensive here.
      if (con_features != features) {
	msg->clear_payload();
      }
      encode_message(msg, features & con_features, payload);
      encode(con_features, payload);
      encode(entity_name, payload);
      return;
    }
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(tid, payload);
    encode(client_type, payload, features);
    encode(client_addrs, payload, features);
    encode(client_socket_addr, payload, features);
    encode(client_caps, payload, features);
    // Encode client message with intersection of target and source
    // features.  This could matter if the semantics of the encoded
    // message are changed when reencoding with more features than the
    // client had originally.  That should never happen, but we may as
    // well be defensive here.
    if (con_features != features) {
      msg->clear_payload();
    }
    encode_message(msg, features & con_features, payload);
    encode(con_features, payload);
    encode(entity_name, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(tid, p);
    if (header.version < 4) {
      entity_inst_t client;
      decode(client, p);
      client_type = client.name.type();
      client_addrs = entity_addrvec_t(client.addr);
      client_socket_addr = client.addr;
    } else {
      decode(client_type, p);
      decode(client_addrs, p);
      decode(client_socket_addr, p);
    }
    decode(client_caps, p);
    msg = (PaxosServiceMessage *)decode_message(NULL, 0, p);
    decode(con_features, p);
    decode(entity_name, p);
  }

  PaxosServiceMessage *claim_message() {
    // let whoever is claiming the message deal with putting it.
    ceph_assert(msg);
    msg_desc = stringify(*msg);
    PaxosServiceMessage *m = msg;
    msg = NULL;
    return m;
  }

  std::string_view get_type_name() const override { return "forward"; }
  void print(ostream& o) const override {
    o << "forward(";
    if (msg) {
      o << *msg;
    } else {
      o << msg_desc;
    }
    o << " caps " << client_caps
      << " tid " << tid
      << " con_features " << con_features << ")";
  }
};
  
#endif
