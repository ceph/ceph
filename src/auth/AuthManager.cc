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


#include "AuthManager.h"

#include <errno.h>
#include <sstream>

#include "config.h"

struct CephXResponse {
  uint64_t server_challenge;

  void encode(bufferlist& bl) const {
    ::encode(server_challenge, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(server_challenge, bl);
  }
};
WRITE_CLASS_ENCODER(CephXResponse);

class CephAuth_X  : public AuthHandler {
  int state;
public:
  CephAuth_X() : state(0) {}
  int handle_request(bufferlist& bl, bufferlist& result_bl);
};

int CephAuth_X::handle_request(bufferlist& bl, bufferlist& result_bl)
{
  switch(state) {
  case 0:
    {
      CephXResponse response;
      response.server_challenge = 0x1234ffff;
      ::encode(response, result_bl);
    }
    break;
  case 1:
    break;
  default:
    return -EINVAL;
  }
  state++;
  return 0;
}


struct AuthInitReq {
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
};

WRITE_CLASS_ENCODER(AuthInitReq)


AuthHandler::~AuthHandler()
{
  if (instance)
    delete instance;
}

AuthHandler *AuthHandler::get_instance() {
  if (instance)
    return instance;
  return this;
}

int AuthHandler::handle_request(bufferlist& bl, bufferlist& result)
{
  bufferlist::iterator iter = bl.begin();
  AuthInitReq req;
  try {
    req.decode(iter);
  } catch (buffer::error *e) {
    dout(0) << "failed to decode message auth message" << dendl;
    delete e;
    return -EINVAL;
  }

  if (req.supports(CEPH_AUTH_CEPH)) {
    CephAuth_X *auth = new CephAuth_X();
    if (!auth)
      return -ENOMEM;
    instance = auth;
    return -EAGAIN;
  }

  return -EINVAL;
}


AuthHandler *AuthManager::get_auth_handler(entity_addr_t& addr)
{
  AuthHandler& handler = m[addr];

  return handler.get_instance();
}


