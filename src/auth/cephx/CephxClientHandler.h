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

template<LockPolicy lock_policy>
class CephxClientHandler : public AuthClientHandler<lock_policy> {
  bool starting;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  CephXTicketManager tickets;
  CephXTicketHandler* ticket_handler;

  RotatingKeyRing<lock_policy> *rotating_secrets;
  KeyRing *keyring;

  using AuthClientHandler<lock_policy>::cct;
  using AuthClientHandler<lock_policy>::global_id;
  using AuthClientHandler<lock_policy>::want;
  using AuthClientHandler<lock_policy>::have;
  using AuthClientHandler<lock_policy>::need;
  using AuthClientHandler<lock_policy>::lock;

public:
  CephxClientHandler(CephContext *cct_,
		     RotatingKeyRing<lock_policy> *rsecrets)
    : AuthClientHandler<lock_policy>(cct_),
      starting(false),
      server_challenge(0),
      tickets(cct_),
      ticket_handler(NULL),
      rotating_secrets(rsecrets),
      keyring(rsecrets->get_keyring())
  {
    reset();
  }

  void reset() override {
    std::unique_lock l{lock};
    starting = true;
    server_challenge = 0;
  }
  void prepare_build_request() override;
  int build_request(bufferlist& bl) const override;
  int handle_response(int ret, bufferlist::const_iterator& iter) override;
  bool build_rotating_request(bufferlist& bl) const override;

  int get_protocol() const override { return CEPH_AUTH_CEPHX; }

  AuthAuthorizer *build_authorizer(uint32_t service_id) const override;

  bool need_tickets() override;

  void set_global_id(uint64_t id) override {
    std::unique_lock l{lock};
    global_id = id;
    tickets.global_id = id;
  }
private:
  void validate_tickets() override;
  bool _need_tickets() const;
};

#endif
