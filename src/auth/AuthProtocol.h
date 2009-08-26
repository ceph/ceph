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

  void encode(bufferlist& bl) const {
    ::encode(client_challenge, bl);
    ::encode(key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(client_challenge, bl);
    ::decode(key, bl);
  }
};
WRITE_CLASS_ENCODER(CephXEnvRequest2);


#endif
