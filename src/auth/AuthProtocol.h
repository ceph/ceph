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

#ifndef __AUTHPROTOCOL_H
#define __AUTHPROTOCOL_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"

#include "config.h"

class Monitor;

/*
  Ceph X-Envelope protocol
*/
struct CephXEnvRequest1 {
  map<uint32_t, bool> auth_types;

  void encode(bufferlist& bl) const {
    uint32_t num_auth = auth_types.size();
    ::encode(num_auth, bl);

    map<uint32_t, bool>::const_iterator iter = auth_types.begin();

    for (iter = auth_types.begin(); iter != auth_types.end(); ++iter) {
       uint32_t auth_type = iter->first;
       ::encode(auth_type, bl);
    }
  }
  void decode(bufferlist::iterator& bl) {
    uint32_t num_auth;
    ::decode(num_auth, bl);

    dout(0) << "num_auth=" << num_auth << dendl;

    auth_types.clear();

    for (uint32_t i=0; i<num_auth; i++) {
      uint32_t auth_type;
      ::decode(auth_type, bl);
    dout(0) << "auth_type[" << i << "] = " << auth_type << dendl;
      auth_types[auth_type] = true;
    }
  }

  bool supports(uint32_t auth_type) {
    return (auth_types.find(auth_type) != auth_types.end());
  }

  void init() {
    auth_types.clear();
    auth_types[CEPH_AUTH_CEPH] = true;
  }
};
WRITE_CLASS_ENCODER(CephXEnvRequest1)

struct CephXEnvResponse1 {
  uint64_t server_challenge;

  void encode(bufferlist& bl) const {
    ::encode(server_challenge, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(server_challenge, bl);
  }
};
WRITE_CLASS_ENCODER(CephXEnvResponse1);

struct CephXEnvRequest2 {
  uint64_t client_challenge;
  uint64_t key;
  char piggyback; /* do we piggyback X protocol */

  void encode(bufferlist& bl) const {
    ::encode(client_challenge, bl);
    ::encode(key, bl);
    ::encode(piggyback, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(client_challenge, bl);
    ::decode(key, bl);
    ::decode(piggyback, bl);
  }
};
WRITE_CLASS_ENCODER(CephXEnvRequest2);

/*
  Ceph X protocol

  First, the principle has to authenticate with the authenticator. A
  shared-secret mechanism is being used, and the negotitaion goes like this:

  A = Authenticator
  P = Principle
  S = Service

  1. Obtaining principle/auth session key

  P->A : principle_name, principle_addr.  authenticate me!

 ...authenticator does lookup in database...

  M->P : A= {principle/auth session key, validity}^principle_secret (*)
         B= {principle ticket, validity, principle/auth session key}^authsecret

  (*) annotation: ^ signifies 'encrypted by'

  At this point, if is genuine, the principle should have the principle/auth
  session key at hand. The next step would be to request an authorization to
  use some other service:

  2. Obtaining principle/service session key

  P->M : B, {principle_addr, timestamp}^principle/auth session key.  authorize
         me!
  M->P : E= {service ticket}^svcsecret
         F= {principle/service session key, validity}^principle/auth session key

  Note that steps 1 and 2 are pretty much the same thing; contacting the
  authenticator and requesting for a key.

  Following this the principle should have a principle/service session key that
  could be used later on for creating a session:

  3. Opening a session to a service

  P->S : E + {principle_addr, timestamp}^principle/service session key
  S->P : {timestamp+1}^principle/service/session key

  Now, the principle is fully authenticated with the service. So, logically we
  have 2 main actions here. The first one would be to obtain a session key to
  the service (steps 1 and 2), and the second one would be to authenticate with
  the service, using that ticket.
*/

#define CEPHX_PRINCIPAL_MON             0x0001
#define CEPHX_PRINCIPAL_OSD             0x0002
#define CEPHX_PRINCIPAL_MDS             0x0004

#define CEPHX_PRINCIPAL_TYPE_MASK       0x00FF

#define CEPHX_GET_AUTH_SESSION_KEY      0x0100    /* Get TGT */
#define CEPHX_GET_PRINCIPAL_SESSION_KEY 0x0200
#define CEPHX_OPEN_SESSION              0x0300

#define CEPHX_REQUEST_TYPE_MASK         0x0F00


struct CephXRequestHeader {
  uint16_t request_type;

  void encode(bufferlist& bl) const {
    ::encode(request_type, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(request_type, bl);
  }
};
WRITE_CLASS_ENCODER(CephXRequestHeader);

struct CephXResponseHeader {
  uint16_t request_type;
  int32_t status;

  void encode(bufferlist& bl) const {
    ::encode(request_type, bl);
    ::encode(status, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(request_type, bl);
    ::decode(status, bl);
  }
};
WRITE_CLASS_ENCODER(CephXResponseHeader);


#endif
