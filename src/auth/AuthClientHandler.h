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

#ifndef __AUTHCLIENTHANDLER_H
#define __AUTHCLIENTHANDLER_H


#include "auth/Auth.h"

#include "config.h"

class AuthClientHandler {
  uint32_t want_keys;
  uint32_t have_keys;
  int request_state;
  int response_state;

  int status;

  int cephx_request_state;
  int cephx_response_state;

  EntityName name;
  entity_addr_t addr;

  /* envelope protocol parameters */
  uint64_t server_challenge;

  /* ceph-x protocol */
  utime_t auth_ts;
  AuthTicketHandler ticket_handler;

  CryptoKey secret;

  int generate_cephx_protocol_request(bufferlist& bl);
  int handle_cephx_protocol_response(bufferlist::iterator& indata);
public:
  AuthClientHandler() : want_keys(0), request_state(0), response_state(0),
                        status(0),
                        cephx_request_state(0), cephx_response_state(0) {}
  int set_request_keys(uint32_t keys) {
    want_keys = keys;
    return 0;
  }

  int generate_request(bufferlist& bl);
  int handle_response(int ret, bufferlist& bl);

  bool request_pending();
};


#endif

