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

#ifndef CEPH_CEPHXCLIENTHANDLER_H
#define CEPH_CEPHXCLIENTHANDLER_H

#include "../AuthClientHandler.h"
#include "CephxProtocol.h"

class CephContext;

class CephxClientHandler : public AuthClientHandler {
  bool starting;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  CephXTicketManager tickets;
  CephXTicketHandler* ticket_handler;

  RotatingKeyRing *rotating_secrets;
  KeyRing *keyring;

public:
  CephxClientHandler(CephContext *cct_, RotatingKeyRing *rsecrets) 
    : AuthClientHandler(cct_),
      starting(false),
      server_challenge(0),
      tickets(cct_),
      ticket_handler(NULL),
      rotating_secrets(rsecrets),
      keyring(rsecrets->get_keyring())
  {
    reset();
  }

  void reset() {
    RWLock::WLocker l(lock);
    starting = true;
    server_challenge = 0;
  }
  void prepare_build_request();
  int build_request(bufferlist& bl) const;
  int handle_response(int ret, bufferlist::iterator& iter);
  bool build_rotating_request(bufferlist& bl) const;

  int get_protocol() const { return CEPH_AUTH_CEPHX; }

  AuthAuthorizer *build_authorizer(uint32_t service_id) const;

  bool need_tickets();

  void set_global_id(uint64_t id) {
    RWLock::WLocker l(lock);
    global_id = id;
    tickets.global_id = id;
  }
private:
  void validate_tickets();
};

#endif
