// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MAUTH_H
#define __MAUTH_H

#include "messages/PaxosServiceMessage.h"

struct MAuth : public PaxosServiceMessage {
  bufferlist auth_payload;

  MAuth() : PaxosServiceMessage(CEPH_MSG_AUTH, 0) { }

  const char *get_type_name() { return "client_auth"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(auth_payload, p);
  }
  void encode_payload() {
    paxos_encode();
    ::encode(auth_payload, payload);
  }
  bufferlist& get_auth_payload() { return auth_payload; }
};

#endif
