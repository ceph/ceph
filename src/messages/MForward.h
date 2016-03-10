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

struct MForward : public Message {
  uint64_t tid;
  entity_inst_t client;
  MonCap client_caps;
  uint64_t con_features;
  EntityName entity_name;
  PaxosServiceMessage *msg;   // incoming message
  bufferlist msg_bl;          // outgoing message

  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 1;

  MForward() : Message(MSG_FORWARD, HEAD_VERSION, COMPAT_VERSION),
               tid(0), con_features(0), msg(NULL) {}
  //the message needs to have caps filled in!
  MForward(uint64_t t, PaxosServiceMessage *m, uint64_t feat) :
    Message(MSG_FORWARD, HEAD_VERSION, COMPAT_VERSION),
    tid(t), msg(NULL) {
    client = m->get_source_inst();
    client_caps = m->get_session()->caps;
    con_features = feat;
    set_message(m, feat);
  }
  MForward(uint64_t t, PaxosServiceMessage *m, uint64_t feat,
           const MonCap& caps) :
    Message(MSG_FORWARD, HEAD_VERSION, COMPAT_VERSION),
    tid(t), client_caps(caps), msg(NULL) {
    client = m->get_source_inst();
    con_features = feat;
    set_message(m, feat);
  }
private:
  ~MForward() {
    if (msg) {
      // message was unclaimed
      msg->put();
      msg = NULL;
    }
  }

  PaxosServiceMessage *get_msg_from_bl() {
    bufferlist::iterator p = msg_bl.begin();
    return (msg_bl.length() ?
        (PaxosServiceMessage*)decode_message(NULL, 0, p) : NULL);
  }

public:
  void encode_payload(uint64_t features) {
    ::encode(tid, payload);
    ::encode(client, payload);
    ::encode(client_caps, payload, features);
    payload.append(msg_bl);
    ::encode(con_features, payload);
    ::encode(entity_name, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(tid, p);
    ::decode(client, p);
    ::decode(client_caps, p);
    msg = (PaxosServiceMessage *)decode_message(NULL, 0, p);
    if (header.version >= 2) {
      ::decode(con_features, p);
    } else {
      con_features = 0;
    }
    if (header.version >= 3) {
      ::decode(entity_name, p);
    } else {
      // we are able to know the entity type, obtaining it from the
      // entity_name_t on 'client', but we have no idea about the
      // entity name, so we'll just use a friendly '?' instead.
      entity_name.set(client.name.type(), "?");
    }

  }

  void set_message(PaxosServiceMessage *m, uint64_t features) {
    // get a reference to the message.  We will not use it except for print(),
    // and we will put it in the dtor if it is not claimed.
    // we could avoid doing this if only we had a const bufferlist iterator :)
    msg = (PaxosServiceMessage*)m->get();

    encode_message(m, features, msg_bl);
  }

  PaxosServiceMessage *claim_message() {
    if (!msg) {
      return get_msg_from_bl();
    }

    // let whoever is claiming the message deal with putting it.
    PaxosServiceMessage *m = msg;
    msg = NULL;
    return m;
  }

  const char *get_type_name() const { return "forward"; }
  void print(ostream& o) const {
    if (msg) {
      o << "forward(" << *msg << " caps " << client_caps
	<< " tid " << tid
        << " con_features " << con_features << ") to leader";
    } else {
      o << "forward(??? ) to leader";
    }
  }
};
REGISTER_MESSAGE(MForward, MSG_FORWARD);
#endif
