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


#include <errno.h>

#include "AuthProtocol.h"

#include "config.h"



#define AUTH_CAP_MON_ACCESS 0x01
#define AUTH_CAP_OSD_ACCESS 0x02
#define AUTH_CAP_MDS_ACCESS 0x04

class AuthClientHandler {
  int request_state;
  int response_state;

  bufferlist tgt;

  uint32_t requested_caps;
  int generate_auth_protocol_request(bufferlist& bl);
  int handle_auth_protocol_response(bufferlist& bl);
public:
  AuthClientHandler() : requested_caps(0) {}
  int get_caps(uint32_t flags) {
    requested_caps = flags;
    return 0;
  }

  int generate_request(bufferlist& bl);
  int handle_response(bufferlist& bl);
  
};

int AuthClientHandler::generate_request(bufferlist& bl)
{
  if (request_state != response_state) {
    dout(0) << "can't generate request while waiting for response" << dendl;
    return -EINVAL;
  }

  switch(request_state) {
  case 0:
    /* initialize  */
    { 
      CephXRequest1 req;
      req.init();

      ::encode(req, bl);
    }
    break;
  case 1:
   /* authenticate */
    {
      /* FIXME: init req fields */
      CephXRequest2 req;
      ::encode(req, bl);
    }
    break;
  default:
    return generate_auth_protocol_request(bl);
  }
  request_state++;
  return 0;
}


int AuthClientHandler::handle_response(bufferlist& bl)
{
  return 0;
}

int AuthClientHandler::generate_auth_protocol_request(bufferlist& bl)
{
  return 0;
}

int AuthClientHandler::handle_auth_protocol_response(bufferlist& bl)
{
  return 0;
}
