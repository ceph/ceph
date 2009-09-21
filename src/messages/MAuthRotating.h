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

#ifndef __MAUTHROTATING_H
#define __MAUTHROTATING_H

#include "messages/PaxosServiceMessage.h"
#include "auth/Auth.h"

class MAuthRotating : public PaxosServiceMessage {
public:
  bufferlist response_bl;
  uint32_t status;
  EntityName entity_name;

  MAuthRotating() : PaxosServiceMessage(MSG_AUTH_ROTATING, 0) { }

  const char *get_type_name() { return "get auth_rotating_secret request/response"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(status, p);
    ::decode(entity_name, p);
    ::decode(response_bl, p);
  }
  void encode_payload() {
    paxos_encode();
    ::encode(status, payload);
    ::encode(entity_name, payload);
    ::encode(response_bl, payload);
  }
};

#endif
