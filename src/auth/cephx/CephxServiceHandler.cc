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


#include "CephxServiceHandler.h"
#include "CephxProtocol.h"

#include "../Auth.h"

#include "mon/Monitor.h"

#include <errno.h>
#include <sstream>

#include "config.h"


int CephxServiceHandler::start_session(bufferlist& result_bl)
{
  CephXServerChallenge ch;
  get_random_bytes((char *)&server_challenge, sizeof(server_challenge));
  ch.server_challenge = server_challenge;
  ::encode(ch, result_bl);
  state = 1;
  return CEPH_AUTH_CEPHX;
}

int CephxServiceHandler::handle_request(bufferlist::iterator& indata, bufferlist& result_bl)
{
  int ret = 0;

  dout(0) << "CephxServiceHandler: handle request" << dendl;
  dout(0) << "state=" << state << dendl;

  struct CephXRequestHeader cephx_header;
  ::decode(cephx_header, indata);

  dout(0) << "op = " << cephx_header.request_type << dendl;

  switch (state) {
  case 0:
    assert(0);
    break;
  case 1:
    {
      CephXAuthenticate req;
      ::decode(req, indata);

      entity_name = req.name;

      CryptoKey secret;
      dout(0) << "entity_name=" << entity_name.to_str() << dendl;
      if (!key_server->get_secret(entity_name, secret)) {
        dout(0) << "couldn't find entity name: " << entity_name.to_str() << dendl;
	ret = -EPERM;
	break;
      }

      bufferlist key, key_enc;
      ::encode(server_challenge, key);
      ::encode(req.client_challenge, key);
      ret = encode_encrypt(key, secret, key_enc);
      if (ret < 0)
        break;
      uint64_t expected_key = 0;
      const uint64_t *p = (const uint64_t *)key_enc.c_str();
      for (int pos = 0; pos + sizeof(req.key) <= key_enc.length(); pos+=sizeof(req.key), p++) {
        expected_key ^= *p;
      }
      dout(0) << "checking key: req.key=" << hex << req.key << " expected_key=" << expected_key << dec << dendl;
      if (req.key != expected_key) {
        dout(0) << "unexpected key: req.key=" << req.key << " expected_key=" << expected_key << dendl;
        ret = -EPERM;
	break;
      }

      dout(0) << "CEPHX_GET_AUTH_SESSION_KEY" << dendl;

      CryptoKey session_key;
      SessionAuthInfo info;

      CryptoKey principal_secret;
      if (key_server->get_secret(req.name, principal_secret) < 0) {
	ret = -EPERM;
	break;
      }

      info.ticket.name = req.name;
      info.ticket.init_timestamps(g_clock.now(), g_conf.auth_mon_ticket_ttl);

      key_server->generate_secret(session_key);

      info.session_key = session_key;
      info.service_id = CEPH_ENTITY_TYPE_AUTH;
      if (!key_server->get_service_secret(CEPH_ENTITY_TYPE_AUTH, info.service_secret, info.secret_id)) {
        dout(0) << "could not get service secret for auth subsystem" << dendl;
        ret = -EIO;
        break;
      }

      vector<SessionAuthInfo> info_vec;
      info_vec.push_back(info);

      build_cephx_response_header(cephx_header.request_type, 0, result_bl);
      if (!cephx_build_service_ticket_reply(principal_secret, info_vec, result_bl)) {
        ret = -EIO;
        break;
      }
    }
    break;

  case 2:
    {
      dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY " << cephx_header.request_type << dendl;

      bufferlist tmp_bl;
      CephXServiceTicketInfo auth_ticket_info;
      if (!cephx_verify_authorizer(*key_server, indata, auth_ticket_info, tmp_bl)) {
        ret = -EPERM;
      }

      CephXServiceTicketRequest ticket_req;
      ::decode(ticket_req, indata);
      dout(0) << " ticket_req.keys = " << ticket_req.keys << dendl;

      ret = 0;
      vector<SessionAuthInfo> info_vec;
      for (uint32_t service_id = 1; service_id <= ticket_req.keys; service_id <<= 1) {
        if (ticket_req.keys & service_id) {
	  dout(0) << " adding key for service " << service_id << dendl;
          SessionAuthInfo info;
          int r = key_server->build_session_auth_info(service_id, auth_ticket_info, info);
          if (r < 0) {
            ret = r;
            break;
          }
          info_vec.push_back(info);
        }
      }
      build_cephx_response_header(cephx_header.request_type, ret, result_bl);
      cephx_build_service_ticket_reply(auth_ticket_info.session_key, info_vec, result_bl);
    }
    break;

  default:
    return -EINVAL;
  }

  if (!ret || (ret == -EAGAIN)) {
    state++;
  }
  dout(0) << "returning with state=" << state << dendl;
  return ret;
}

void CephxServiceHandler::build_cephx_response_header(int request_type, int status, bufferlist& bl)
{
  struct CephXResponseHeader header;
  header.request_type = request_type;
  header.status = status;
  ::encode(header, bl);
}
