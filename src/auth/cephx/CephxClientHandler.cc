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

#include "CephxClientHandler.h"
#include "CephxProtocol.h"

#include "../KeyRing.h"

int CephxClientHandler::build_request(bufferlist& bl)
{
  dout(0) << "state=" << state << dendl;

  switch (state) {
  case STATE_START:
    break;

  case STATE_GETTING_MON_KEY:
    /* authenticate */
    {
      CephXRequestHeader header;
      header.request_type = CEPHX_GET_AUTH_SESSION_KEY;
      ::encode(header, bl);

      CephXAuthenticate req;
      req.name = name;
      CryptoKey secret;
      g_keyring.get_master(secret);
      bufferlist key, key_enc;
      get_random_bytes((char *)&req.client_challenge, sizeof(req.client_challenge));
      ::encode(server_challenge, key);
      ::encode(req.client_challenge, key);
      int ret = encode_encrypt(key, secret, key_enc);
      if (ret < 0)
        return ret;
      req.key = 0;
      const uint64_t *p = (const uint64_t *)key_enc.c_str();
      for (int pos = 0; pos + sizeof(req.key) <= key_enc.length(); pos+=sizeof(req.key), p++) {
        req.key ^= *p;
      }
      ::encode(req, bl);
    }
    break;

  case STATE_GETTING_SESSION_KEYS:
    /* get service tickets */
    {
      dout(0) << "want=" << hex << want << " have=" << have << dec << dendl;

      CephXRequestHeader header;
      header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;
      ::encode(header, bl);

      AuthTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
      if (!ticket_handler.build_authorizer(authorizer))
	return -EINVAL;
      bl.claim_append(authorizer.bl);

      build_service_ticket_request(want, bl);
    }
    break;

  case STATE_DONE:
    break;

  default:
    assert(0);
  }
  return 0;
}

int CephxClientHandler::handle_response(int ret, bufferlist::iterator& indata)
{
  dout(0) << "cephx handle_response ret = " << ret << " state " << state << dendl;
  
  if (ret < 0)
    return ret; // hrm!

  if (state == STATE_START) {
    CephXServerChallenge ch;
    ::decode(ch, indata);
    server_challenge = ch.server_challenge;
    state = STATE_GETTING_MON_KEY;
    return -EAGAIN;
  }

  struct CephXResponseHeader header;
  ::decode(header, indata);

  switch (state) {
  case STATE_GETTING_MON_KEY:
    {
      dout(0) << "request_type=" << hex << header.request_type << dec << dendl;
      dout(0) << "handle_cephx_response()" << dendl;

      dout(0) << "CEPHX_GET_AUTH_SESSION_KEY" << dendl;
      
      CryptoKey secret;
      g_keyring.get_master(secret);
	
      if (!tickets.verify_service_ticket_reply(secret, indata)) {
	dout(0) << "could not verify service_ticket reply" << dendl;
	return -EPERM;
      }
      dout(0) << "want=" << want << " have=" << have << dendl;
      if (want != have) {
	state = STATE_GETTING_SESSION_KEYS;
	ret = -EAGAIN;
      } else {
	state = STATE_DONE;
	ret = 0;
      }
    }
    break;

  case STATE_GETTING_SESSION_KEYS:
    {
      AuthTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
      dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY session_key " << ticket_handler.session_key << dendl;
  
      if (!tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
      if (want == have) {
	state = STATE_DONE;
	ret = 0;
      }
    }
    break;

  case STATE_DONE:
    // ignore?
    ret = 0;
    break;

  default:
    assert(0);
  }
  return ret;
}



AuthAuthorizer *CephxClientHandler::build_authorizer(uint32_t service_id)
{
  dout(0) << "going to build authorizer for peer_id=" << service_id << " service_id=" << service_id << dendl;
  
  AuthAuthorizer *a = new AuthAuthorizer;
  if (!tickets.build_authorizer(service_id, authorizer))
    return 0;

  dout(0) << "authorizer built successfully" << dendl;
  return a;
}

