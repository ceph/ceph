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
  dout(0) << "status=" << status << dendl;
  if (status < 0) {
    return status;
  }
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
      memset(&req.client_challenge, 0x88, sizeof(req.client_challenge));
      req.key = req.client_challenge ^ server_challenge;
      req.piggyback = 1;
      ::encode(req, bl);
      request_state++;
      return generate_cephx_protocol_request(bl);
    }
    break;
  default:
    return generate_cephx_protocol_request(bl);
  }
  request_state++;
  return 0;
}


int AuthClientHandler::handle_response(int ret, bufferlist& bl)
{
char buf[1024];
      const char *s = bl.c_str();
      int pos = 0;
      for (unsigned int i=0; i<bl.length(); i++) {
        pos += snprintf(&buf[pos], 1024-pos, "%.2x ", (int)(unsigned char)s[i]);
        if (i && !(i%8))
          pos += snprintf(&buf[pos], 1024-pos, " ");
        if (i && !(i%16))
          pos += snprintf(&buf[pos], 1024-pos, "\n");
      }
      dout(0) << "result_buf=" << buf << dendl;


  bufferlist::iterator iter = bl.begin();

  if (ret != 0 && ret != -EAGAIN) {
    response_state = request_state;
    cephx_response_state = cephx_request_state;
    status = ret;
    return ret;
  }

  dout(0) << "AuthClientHandler::handle_response()" << dendl;
  switch(response_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvResponse1 response;

      ::decode(response, iter);
      server_challenge = response.server_challenge;
    }
    break;
  case 1:
    /* authenticate */
    {
      response_state++;
      return handle_cephx_protocol_response(iter);
    }
    break;
  default:
    return handle_cephx_protocol_response(iter);
  }
  response_state++;
  return  -EAGAIN;
}

int AuthClientHandler::generate_cephx_protocol_request(bufferlist& bl)
{
  CephXRequestHeader header;

  if (!ticket_handler.has_key()) {
    dout(0) << "auth ticket: doesn't have key" << dendl;
    /* we first need to get the principle/auth session key */

    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;

   ::encode(header, bl);
    CryptoKey key;
    AuthBlob blob;
    build_authenticate_request(name, addr, CEPHX_PRINCIPAL_AUTH,
                               false, key, blob, bl);
    cephx_request_state = 1;
    return 0;
  }

  dout(0) << "want_keys=" << hex << want_keys << " have_keys=" << have_keys << dec << dendl;

  cephx_request_state = 2;

  if (want_keys == have_keys) {
    cephx_response_state = 2;
    return 0;
  }

  header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;

  ::encode(header, bl);
  build_authenticate_request(name, addr, want_keys,
                             true, ticket_handler.session_key, ticket_handler.ticket, bl);
  
  return 0;
}

int AuthClientHandler::handle_cephx_protocol_response(bufferlist::iterator& indata)
{
  int ret = 0;
  struct CephXResponseHeader header;
  ::decode(header, indata);

  dout(0) << "request_type=" << hex << header.request_type << dec << dendl;
  dout(0) << "handle_cephx_response()" << dendl;

  switch (header.request_type & CEPHX_REQUEST_TYPE_MASK) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    cephx_response_state = 1;
    dout(0) << "CEPHX_GET_AUTH_SESSION_KEY" << dendl;

#define PRINCIPAL_SECRET "123456789ABCDEF0"
    {
      bufferptr p(PRINCIPAL_SECRET, sizeof(PRINCIPAL_SECRET) - 1);
      secret.set_secret(CEPH_SECRET_AES, p);
  
      if (!ticket_handler.verify_service_ticket_reply(secret, indata)) {
        dout(0) << "could not verify authenticate reply" << dendl;
        return -EPERM;
      }

      if (want_keys)
        ret = -EAGAIN;
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    cephx_response_state = 2;
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY" << dendl;
    {
    }
    ret = 0;
    break;

  case CEPHX_OPEN_SESSION:
    cephx_response_state = 3;
    dout(0) << "FIXME: CEPHX_OPEN_SESSION" << dendl;
    break;
  default:
    dout(0) << "header.request_type = " << hex << header.request_type << dec << dendl;
    ret = -EINVAL;
    break;
  }

  return ret;
}

bool AuthClientHandler::request_pending() {
  dout(0) << "request_pending(): cephx_request_state=" << cephx_request_state << " cephx_response_state=" << cephx_response_state << dendl;
  return (request_state != response_state) || (cephx_request_state != cephx_response_state);
}
