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


#include "AuthServiceHandler.h"
#include "AuthProtocol.h"
#include "Auth.h"

#include "mon/Monitor.h"

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
  EntityName entity_name;

public:
  CephAuthService_X(Monitor *m) : AuthServiceHandler(m), state(0) {}
  ~CephAuthService_X() {}

  int handle_request(bufferlist::iterator& indata, bufferlist& result_bl);
  int handle_cephx_protocol(bufferlist::iterator& indata, bufferlist& result_bl);
  void build_cephx_response_header(int request_type, int status, bufferlist& bl);
};


int CephAuthService_X::handle_request(bufferlist::iterator& indata, bufferlist& result_bl)
{
  int ret = 0;
  bool piggyback = false;

  dout(0) << "CephAuthService_X: handle request" << dendl;
  dout(0) << "state=" << state << dendl;

  switch(state) {
  case 0:
    {
      CephXEnvRequest1 req;
      ::decode(req, indata);
      entity_name = req.name;
      CephXEnvResponse1 response;
      get_random_bytes((char *)&server_challenge, sizeof(server_challenge));
      response.server_challenge = server_challenge;
      ::encode(response, result_bl);
      ret = -EAGAIN;
    }
    break;
  case 1:
    {
      CephXEnvRequest2 req;
      ::decode(req, indata);

      CryptoKey secret;
      dout(0) << "entity_name=" << entity_name.to_str() << dendl;
      if (!mon->key_server.get_secret(entity_name, secret)) {
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
      } else {
	ret = 0;
        piggyback = req.piggyback;
      }
    }
    break;

  case 2:
    return handle_cephx_protocol(indata, result_bl);
  default:
    return -EINVAL;
  }

  if (!ret && piggyback) {
    ret = handle_cephx_protocol(indata, result_bl);
  }

  if (!ret || (ret == -EAGAIN)) {
    state++;
  }
  dout(0) << "returning with state=" << state << dendl;
  return ret;
}

int CephAuthService_X::handle_cephx_protocol(bufferlist::iterator& indata, bufferlist& result_bl)
{
  struct CephXRequestHeader cephx_header;

  ::decode(cephx_header, indata);

  uint16_t request_type = cephx_header.request_type & CEPHX_REQUEST_TYPE_MASK;
  int ret = -EAGAIN;

  dout(0) << "request_type=" << request_type << dendl;

  switch (request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      dout(0) << "CEPHX_GET_AUTH_SESSION_KEY" << dendl;

      AuthAuthenticateRequest req;
      ::decode(req, indata);

      CryptoKey session_key;
      SessionAuthInfo info;

      CryptoKey principal_secret;
      if (mon->key_server.get_secret(req.name, principal_secret) < 0) {
	ret = -EPERM;
	break;
      }

      info.ticket.name = req.name;
      info.ticket.init_timestamps(g_clock.now(), g_conf.auth_mon_ticket_ttl);

      mon->key_server.generate_secret(session_key);

      info.session_key = session_key;
      info.service_id = CEPH_ENTITY_TYPE_AUTH;
      if (!mon->key_server.get_service_secret(CEPH_ENTITY_TYPE_AUTH, info.service_secret, info.secret_id)) {
        dout(0) << "could not get service secret for auth subsystem" << dendl;
        ret = -EIO;
        break;
      }

      vector<SessionAuthInfo> info_vec;
      info_vec.push_back(info);

      build_cephx_response_header(request_type, 0, result_bl);
      if (!build_service_ticket_reply(principal_secret, info_vec, result_bl)) {
        ret = -EIO;
        break;
      }
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY " << cephx_header.request_type << dendl;
    {
      bufferlist tmp_bl;
      AuthServiceTicketInfo auth_ticket_info;
      if (!verify_authorizer(mon->key_server, indata, auth_ticket_info, tmp_bl)) {
        ret = -EPERM;
      }

      AuthServiceTicketRequest ticket_req;
      if (!verify_service_ticket_request(ticket_req, indata)) {
        ret = -EPERM;
        break;
      }

      ret = 0;
      vector<SessionAuthInfo> info_vec;
      for (uint32_t service_id = 1; service_id <= ticket_req.keys; service_id <<= 1) {
        if (ticket_req.keys & service_id) {
          SessionAuthInfo info;
          int r = mon->key_server.build_session_auth_info(service_id, auth_ticket_info, info);
          if (r < 0) {
            ret = r;
            break;
          }

          info_vec.push_back(info);
        }
      }
      build_cephx_response_header(request_type, ret, result_bl);
      build_service_ticket_reply(auth_ticket_info.session_key, info_vec, result_bl);
    }
    break;
  default:
    ret = -EINVAL;
    build_cephx_response_header(request_type, -EINVAL, result_bl);
    break;
  }

  return ret;
}

void CephAuthService_X::build_cephx_response_header(int request_type, int status, bufferlist& bl)
{
  struct CephXResponseHeader header;
  header.request_type = request_type;
  header.status = status;
  ::encode(header, bl);
}


// --------------

AuthServiceHandler *get_auth_handler(Monitor *mon, set<__u32>& supported)
{
  if (supported.count(CEPH_AUTH_CEPHX))
    return new CephAuthService_X(mon);
  return NULL;
}


