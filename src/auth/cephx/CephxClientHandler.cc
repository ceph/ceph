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

#include "config.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "cephx client: "


int CephxClientHandler::build_request(bufferlist& bl)
{
  dout(10) << "build_request" << dendl;

  dout(10) << "validate_tickets: want=" << want << " need=" << need << " have=" << have << dendl;
  validate_tickets();

  dout(10) << "want=" << want << " need=" << need << " have=" << have << dendl;

  if (need & CEPH_ENTITY_TYPE_AUTH) {
    /* authenticate */
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

    dout(10) << "get auth session key: client_challenge " << req.client_challenge << dendl;
    return 0;
  }

  if (need) {
    /* get service tickets */
    dout(10) << "get service keys: want=" << want << " need=" << need << " have=" << have << dendl;

    CephXRequestHeader header;
    header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;
    ::encode(header, bl);

    CephXTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
    authorizer = ticket_handler.build_authorizer();
    if (!authorizer)
      return -EINVAL;
    bl.claim_append(authorizer->bl);

    CephXServiceTicketRequest req;
    req.keys = need;
    ::encode(req, bl);
  }

  return 0;
}

int CephxClientHandler::handle_response(int ret, bufferlist::iterator& indata)
{
  dout(10) << "handle_response ret = " << ret << dendl;
  
  if (ret < 0)
    return ret; // hrm!

  if (starting) {
    CephXServerChallenge ch;
    ::decode(ch, indata);
    server_challenge = ch.server_challenge;
    dout(10) << " got initial server challenge " << server_challenge << dendl;
    starting = false;
    return -EAGAIN;
  }

  struct CephXResponseHeader header;
  ::decode(header, indata);

  switch (header.request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      dout(10) << " get_auth_session_key" << dendl;
      CryptoKey secret;
      g_keyring.get_master(secret);
	
      if (!tickets.verify_service_ticket_reply(secret, indata)) {
	dout(0) << "could not verify service_ticket reply" << dendl;
	return -EPERM;
      }
      dout(10) << " want=" << want << " need=" << need << " have=" << have << dendl;
      validate_tickets();
      if (need)
	ret = -EAGAIN;
      else
	ret = 0;
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    {
      CephXTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
      dout(10) << " get_principal_session_key session_key " << ticket_handler.session_key << dendl;
  
      if (!tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
      validate_tickets();
      if (!need) {
	ret = 0;
      }
    }
    break;

  case CEPHX_GET_ROTATING_KEY:
    {
      dout(10) << " get_rotating_key" << dendl;
      RotatingSecrets secrets;
      CryptoKey secret_key;
      g_keyring.get_master(secret_key);
      if (decode_decrypt(secrets, secret_key, indata) == 0) {
	g_keyring.set_rotating(secrets);
      } else {
	derr(0) << "could not set rotating key: decode_decrypt failed" << dendl;
      }
    }
    break;

  default:
   dout(0) << " unknown request_type " << header.request_type << dendl;
   assert(0);
  }
  return ret;
}



AuthAuthorizer *CephxClientHandler::build_authorizer(uint32_t service_id)
{
  dout(10) << "build_authorizer for service " << service_id << dendl;
  return tickets.build_authorizer(service_id);
}


void CephxClientHandler::build_rotating_request(bufferlist& bl)
{
  dout(10) << "build_rotating_request" << dendl;
  CephXRequestHeader header;
  header.request_type = CEPHX_GET_ROTATING_KEY;
  ::encode(header, bl);
}

void CephxClientHandler::validate_tickets()
{
  tickets.validate_tickets(want, have, need);
}

bool CephxClientHandler::need_tickets()
{
  validate_tickets();

  dout(20) << "need_tickets: want=" << want << " need=" << need << " have=" << have << dendl;

  return (need != 0);
}

