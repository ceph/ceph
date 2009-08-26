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


#include "AuthServiceManager.h"
#include "AuthProtocol.h"

#include <errno.h>
#include <sstream>

#include "config.h"

/*
   the first X request is empty, we then send a response and get another request which
   is not empty and contains the client challenge and the key
*/
class CephAuthService_X  : public AuthServiceHandler {
  int state;
  uint64_t server_challenge;
public:
  CephAuthService_X() : state(0) {}
  int handle_request(bufferlist& bl, bufferlist& result_bl);
};

int CephAuthService_X::handle_request(bufferlist& bl, bufferlist& result_bl)
{
  int ret = 0;
  dout(0) << "CephAuthService_X: handle request" << dendl;
  switch(state) {
  case 0:
    {
      CephXResponse1 response;
      server_challenge = 0x1234ffff;
      response.server_challenge = server_challenge;
      ::encode(response, result_bl);
      ret = -EAGAIN;
    }
    break;
  case 1:
    {
      CephXRequest2 req;
      bufferlist::iterator iter = bl.begin();
      req.decode(iter);
      if (req.key != (server_challenge ^ req.client_challenge))
        ret = -EPERM;
      else
	ret = 0;
    }
    break;
  default:
    return -EINVAL;
  }
  state++;
  return ret;
}



AuthServiceHandler::~AuthServiceHandler()
{
  if (instance)
    delete instance;
}

AuthServiceHandler *AuthServiceHandler::get_instance() {
  if (instance)
    return instance;
  return this;
}

int AuthServiceHandler::handle_request(bufferlist& bl, bufferlist& result)
{
  bufferlist::iterator iter = bl.begin();
  CephAuthService_X *auth = NULL;
  CephXRequest1 req;
  try {
    req.decode(iter);
  } catch (buffer::error *e) {
    dout(0) << "failed to decode message auth message" << dendl;
    delete e;
    return -EINVAL;
  }

  if (req.supports(CEPH_AUTH_CEPH)) {
    auth = new CephAuthService_X();
    if (!auth)
      return -ENOMEM;
    instance = auth;
  }

  if (auth)
    return auth->handle_request(bl, result);

  return -EINVAL;
}


AuthServiceHandler *AuthServiceManager::get_auth_handler(entity_addr_t& addr)
{
  AuthServiceHandler& handler = m[addr];

  return handler.get_instance();
}


