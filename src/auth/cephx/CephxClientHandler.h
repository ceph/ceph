// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CEPHXLIENTHANDLER_H
#define __CEPHXCLIENTHANDLER_H

#include "../AuthClientHandler.h"
#include "CephxProtocol.h"

class CephxClientHandler : public AuthClientHandler {
  enum {
    STATE_START,
    STATE_GETTING_MON_KEY,
    STATE_GETTING_SESSION_KEYS,
    STATE_DONE
  } state;
  
  /* envelope protocol parameters */
  uint64_t server_challenge;
  
  CephXAuthorizer *authorizer;
  CephXTicketManager tickets;

public:
  CephxClientHandler() : authorizer(0) {
    reset();
  }

  void reset() {
    delete authorizer;
    authorizer = 0;
    state = STATE_START;
  }
  int build_request(bufferlist& bl);
  int handle_response(int ret, bufferlist::iterator& iter);
  void build_rotating_request(bufferlist& bl);

  int get_protocol() { return CEPH_AUTH_CEPHX; }
  
  void tick() {}

  AuthAuthorizer *build_authorizer(uint32_t service_id);

};

#endif
