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
#include "AuthClientHandler.h"

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
      CephXEnvRequest1 req;
      req.init();

      ::encode(req, bl);
    }
    break;
  case 1:
   /* authenticate */
    {
      /* FIXME: init req fields */
      CephXEnvRequest2 req;
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
  bufferlist::iterator iter = bl.begin();
  switch(response_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvResponse1 response;

      ::decode(response, iter);
    }
    break;
  case 1:
   /* authenticate */
    {
      /* FIXME: init req fields */
    }
    break;
  default:
    return handle_auth_protocol_response(bl);
  }
  response_state++;
  return  -EAGAIN;
}

int AuthClientHandler::generate_auth_protocol_request(bufferlist& bl)
{
  return 0;
}

int AuthClientHandler::handle_auth_protocol_response(bufferlist& bl)
{
  return 0;
}
