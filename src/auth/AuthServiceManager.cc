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
#include "Auth.h"

#include "mon/Monitor.h"

#include <errno.h>
#include <sstream>

#include "config.h"

/* FIXME */
#define SERVICE_SECRET   "0123456789ABCDEF"
#define AUTH_SESSION_KEY "23456789ABCDEF01"
#define SESSION_KEY "456789ABCDEF0123"

#define PRINCIPAL_CLIENT_SECRET "123456789ABCDEF0"
#define PRINCIPAL_OSD_SECRET "3456789ABCDEF012"

static inline void hexdump(string msg, const char *s, int len)
{
  int buf_len = len*4;
  char buf[buf_len];
  int pos = 0;
  for (int i=0; i<len && pos<buf_len - 8; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], buf_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], buf_len-pos, "\n");
    pos += snprintf(&buf[pos], buf_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
  dout(0) << msg << ":\n" << buf << dendl;
}

/*
   the first X request is empty, we then send a response and get another request which
   is not empty and contains the client challenge and the key
*/
class CephAuthService_X  : public AuthServiceHandler {
  int state;
  uint64_t server_challenge;

public:
  CephAuthService_X(Monitor *m) : AuthServiceHandler(m), state(0) {}
  ~CephAuthService_X() {}

  int handle_request(bufferlist& bl, bufferlist& result_bl);
  int handle_cephx_protocol(bufferlist::iterator& indata, bufferlist& result_bl);
  void build_cephx_response_header(int request_type, int status, bufferlist& bl);
};


int CephAuthService_X::handle_request(bufferlist& bl, bufferlist& result_bl)
{
  int ret = 0;
  bool piggyback = false;
  bufferlist::iterator indata = bl.begin();

  dout(0) << "CephAuthService_X: handle request" << dendl;
  if (state != 0) {
    CephXPremable pre;
    ::decode(pre, indata);
    dout(0) << "CephXPremable id=" << pre.trans_id << dendl;
    ::encode(pre, result_bl);
  }

  dout(0) << "state=" << state << dendl;

  switch(state) {
  case 0:
    {
      CephXEnvResponse1 response;
      server_challenge = 0x1234ffff;
      response.server_challenge = server_challenge;
      ::encode(response, result_bl);
      ret = -EAGAIN;
    }
    break;
  case 1:
    {
      CephXEnvRequest2 req;
      ::decode(req, indata);
      uint64_t expected_key = (server_challenge ^ req.client_challenge); /* FIXME: obviously not */
      if (req.key != expected_key) {
        dout(0) << "unexpected key: req.key=" << req.key << " expected_key=" << expected_key << dendl;
        ret = -EPERM;
      } else {
	ret = 0;
      }

      piggyback = req.piggyback;
      break;
    }

  case 2:
    return handle_cephx_protocol(indata, result_bl);
  default:
    return -EINVAL;
  }
  state++;

  if (!ret && piggyback) {
    ret = handle_cephx_protocol(indata, result_bl);
  }
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
      if (mon->keys_server.get_secret(req.name, principal_secret, info.ticket.caps) < 0) {
	ret = -EPERM;
	break;
      }

      info.ticket.name = req.name;
      info.ticket.addr = req.addr;
      info.ticket.created = g_clock.now();
      info.ticket.expires = info.ticket.created;
      info.ticket.expires += g_conf.auth_mon_ticket_ttl;
      info.ticket.renew_after = info.ticket.created;
      info.ticket.renew_after += g_conf.auth_mon_ticket_ttl / 2.0;

      generate_random_string(info.ticket.nonce, g_conf.auth_nonce_len);
      mon->keys_server.generate_secret(session_key);

      info.session_key = session_key;
      info.service_id = CEPHX_PRINCIPAL_AUTH;
      mon->keys_server.get_service_secret(CEPHX_PRINCIPAL_AUTH, info.service_secret);

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
      CryptoKey auth_secret;
      if (mon->keys_server.get_service_secret(CEPHX_PRINCIPAL_AUTH, auth_secret) < 0) {
        ret = -EPERM;
      }

      AuthServiceTicketRequest ticket_req;
      AuthServiceTicketInfo ticket_info;
      EntityName name;
      bufferlist tmp_bl;
      CryptoKey auth_session_key;
      if (!verify_authorizer(auth_secret, indata, auth_session_key, tmp_bl)) {
        ret = -EPERM;
      }

      if (!verify_service_ticket_request(ticket_req, indata)) {
        ret = -EPERM;
        break;
      }

      vector<SessionAuthInfo> info_vec;
      for (uint32_t service_id = 1; service_id != (CEPHX_PRINCIPAL_TYPE_MASK + 1); service_id <<= 1) {
        if (ticket_req.keys & service_id) {
	  CryptoKey service_secret;
          if (mon->keys_server.get_service_secret(service_id, service_secret) < 0) {
            ret = -EPERM;
            break;
          }

          SessionAuthInfo info;

          AuthTicket service_ticket;
          /* FIXME: initialize service_ticket */

          CryptoKey session_key;

          mon->keys_server.generate_secret(session_key);

          info.service_id = service_id;
          info.ticket = service_ticket;
          info.session_key = session_key;
          info.service_secret = service_secret;

          info_vec.push_back(info);
        }
      }

      build_cephx_response_header(request_type, ret, result_bl);
      build_service_ticket_reply(auth_session_key, info_vec, result_bl);

      ret = 0;
    }
    break;
  case CEPHX_OPEN_SESSION:
    {
      CryptoKey service_secret;

      if (mon->keys_server.get_service_secret(CEPHX_PRINCIPAL_MON, service_secret) < 0) {
        ret = -EPERM;
        break;
      }

      ret = 0;
      bufferlist tmp_bl;
      CryptoKey session_key;
      if (!verify_authorizer(service_secret, indata, session_key, tmp_bl)) {
        ret = -EPERM;
      }
      build_cephx_response_header(request_type, ret, result_bl);
      result_bl.claim_append(tmp_bl);

      break;
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

AuthServiceHandler *AuthServiceManager::get_auth_handler(set<__u32>& supported)
{
  if (supported.count(CEPH_AUTH_CEPH)) {
    return new CephAuthService_X(mon);
  }
  return NULL;
}


