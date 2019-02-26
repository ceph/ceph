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

#include "auth/AuthClientHandler.h"
#include "CephxProtocol.h"
#include "auth/RotatingKeyRing.h"

class CephContext;
class KeyRing;

class CephxClientHandler : public AuthClientHandler {
  bool starting;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  CephXTicketManager tickets;
  CephXTicketHandler* ticket_handler;

  RotatingKeyRing* rotating_secrets;
  KeyRing *keyring;

public:
  CephxClientHandler(CephContext *cct_,
		     RotatingKeyRing *rsecrets)
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

  void reset() override;
  void prepare_build_request() override;
  int build_request(bufferlist& bl) const override;
  int handle_response(int ret, bufferlist::const_iterator& iter,
		      CryptoKey *session_key,
		      std::string *connection_secret) override;
  bool build_rotating_request(bufferlist& bl) const override;

  int get_protocol() const override { return CEPH_AUTH_CEPHX; }

  AuthAuthorizer *build_authorizer(uint32_t service_id) const override;

  bool need_tickets() override;

  void set_global_id(uint64_t id) override {
    global_id = id;
    tickets.global_id = id;
  }
private:
  void validate_tickets() override;
  bool _need_tickets() const;
};

#endif
