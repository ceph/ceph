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

  CephXTicketHandler& ticket_handler = tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);

  if (need & CEPH_ENTITY_TYPE_AUTH) {
    /* authenticate */
    CephXRequestHeader header;
    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;
    ::encode(header, bl);

    CryptoKey secret;
    g_keyring.get_master(secret);

    CephXAuthenticate req;
    get_random_bytes((char *)&req.client_challenge, sizeof(req.client_challenge));
    cephx_calc_client_server_challenge(secret, server_challenge, req.client_challenge, &req.key);

    req.old_ticket = ticket_handler.ticket;

    if (req.old_ticket.blob.length()) {
      dout(0) << "old ticket len=" << req.old_ticket.blob.length() << dendl;
      hexdump("encoded ticket:", req.old_ticket.blob.c_str(), req.old_ticket.blob.length());
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

    authorizer = ticket_handler.build_authorizer(global_id);
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
      if (rotating_secrets) {
	RotatingSecrets secrets;
	CryptoKey secret_key;
	g_keyring.get_master(secret_key);
	if (decode_decrypt(secrets, secret_key, indata) == 0) {
	  rotating_secrets->set_secrets(secrets);
	} else {
	  derr(0) << "could not set rotating key: decode_decrypt failed" << dendl;
	}
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
  dout(10) << "build_authorizer for service " << ceph_entity_type_name(service_id) << dendl;
  return tickets.build_authorizer(service_id);
}


bool CephxClientHandler::build_rotating_request(bufferlist& bl)
{
  dout(10) << "build_rotating_request" << dendl;
  CephXRequestHeader header;
  header.request_type = CEPHX_GET_ROTATING_KEY;
  ::encode(header, bl);
  return true;
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

