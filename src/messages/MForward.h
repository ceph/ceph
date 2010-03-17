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

#ifndef __MFORWARD_H
#define __MFORWARD_H

#include "msg/Message.h"
#include "mon/MonCaps.h"
#include "include/encoding.h"

struct MForward : public Message {
  PaxosServiceMessage *msg;
  entity_inst_t client;
  MonCaps client_caps;
  bool has_caps;
  
  MForward() : Message(MSG_FORWARD), msg(NULL), has_caps(false) {}
  //the message needs to have caps filled in!
  MForward(PaxosServiceMessage *m) :
    Message(MSG_FORWARD), msg(m) {
    client = m->get_source_inst();
    if (m->caps) {
      client_caps = *m->caps;
      has_caps = true;
    } else {
      has_caps = false;
      generic_dout(10) << "creating MForward without caps on message " 
		       << m << dendl;
    }
  }

  ~MForward() {
    delete msg;
  }
  
  void encode_payload() {
    ::encode(client, payload);
    ::encode(has_caps, payload);
    ::encode(client_caps, payload);
    encode_message(msg, payload);
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(client, p);
    ::decode(has_caps, p);
    ::decode(client_caps, p);
    msg = (PaxosServiceMessage *)decode_message(p);
    if (has_caps) msg->caps = &client_caps;
    else {
      msg->caps = NULL;
      generic_dout(10) << "Decoding MForward without any caps!" << dendl;
    }
    generic_dout(20) << "MForward decoded! " << *msg << client_caps << dendl;
  }

  const char *get_type_name() { return "forward"; }
  void print(ostream& o) {
    if (msg)
      o << "forward(" << *msg << ") to leader";
    else o << "forward(??? ) to leader";
  }
};
  
#endif
