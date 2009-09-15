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
  for (unsigned int i=0; i<len && pos<buf_len - 8; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], buf_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], buf_len-pos, "\n");
    pos += snprintf(&buf[pos], buf_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
  dout(0) << msg << ":\n" << buf << dendl;
}


class CephAuthServer {
  /* FIXME: this is all temporary */
  AuthTicket ticket;
  CryptoKey client_secret;
  CryptoKey auth_session_key;
  CryptoKey service_secret;
  
public:
  CephAuthServer() {
    bufferptr ptr1(SERVICE_SECRET, sizeof(SERVICE_SECRET) - 1);
    service_secret.set_secret(CEPH_SECRET_AES, ptr1);

    bufferptr ptr2(PRINCIPAL_CLIENT_SECRET, sizeof(PRINCIPAL_CLIENT_SECRET) - 1);
    client_secret.set_secret(CEPH_SECRET_AES, ptr2);

    bufferptr ptr4(AUTH_SESSION_KEY, sizeof(AUTH_SESSION_KEY) - 1);
    auth_session_key.set_secret(CEPH_SECRET_AES, ptr4);
   }

/* FIXME: temporary stabs */
  int get_client_secret(CryptoKey& secret) {
     secret = client_secret;
     return 0;
  }

  int get_service_secret(CryptoKey& secret, uint32_t service_id) {
    char buf[16];
    memcpy(buf, SERVICE_SECRET, 16);
    buf[0] = service_id & 0xFF;
    bufferptr ptr(buf, 16);
    secret.set_secret(CEPH_SECRET_AES, ptr);

    return 0;
  }

  int get_service_session_key(CryptoKey& secret, uint32_t service_id) {
    char buf[16];
    memcpy(buf, SERVICE_SECRET, 16);
    buf[0] = service_id & 0xFF;
    bufferptr ptr(buf, 16);
    secret.set_secret(CEPH_SECRET_AES, ptr);

    return 0;
  }
};


static CephAuthServer auth_server;

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
      EntityName name; /* FIXME should take it from the request */
      entity_addr_t addr;
      AuthTicket ticket;
      CryptoKey principal_secret;
      CryptoKey session_key;
      CryptoKey auth_secret;

      ticket.expires = g_clock.now();

      uint32_t keys;

      auth_server.get_client_secret(principal_secret);
      auth_server.get_service_session_key(session_key, CEPHX_PRINCIPAL_AUTH);
      auth_server.get_service_secret(auth_secret, CEPHX_PRINCIPAL_AUTH);

      if (!verify_service_ticket_request(false, auth_secret,
                                     session_key, keys, indata)) {
         ret = -EPERM;
         break;
       }

      build_cephx_response_header(request_type, 0, result_bl);
      vector<SessionAuthInfo> info_vec;
      SessionAuthInfo info;
      info.ticket = ticket;
      info.service_id = CEPHX_PRINCIPAL_AUTH;
      info.session_key = session_key;
      info.service_secret = auth_secret;
      info_vec.push_back(info);
      if (!build_service_ticket_reply(principal_secret, info_vec, result_bl)) {
        ret = -EIO;
        break;
      }
    }
    break;
  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY " << cephx_header.request_type << dendl;
    {
      EntityName name; /* FIXME should take it from the request */
      entity_addr_t addr;
      CryptoKey auth_secret;
      CryptoKey auth_session_key;
      CryptoKey session_key;
      CryptoKey service_secret;
      uint32_t keys;

      auth_server.get_service_secret(auth_secret, CEPHX_PRINCIPAL_AUTH);
      auth_server.get_service_session_key(auth_session_key, CEPHX_PRINCIPAL_AUTH);

      vector<SessionAuthInfo> info_vec;

      if (!verify_service_ticket_request(true, auth_secret,
                                     auth_session_key, keys, indata)) {
        ret = -EPERM;
        break;
      }

      auth_server.get_service_session_key(auth_session_key, CEPHX_PRINCIPAL_AUTH);

      for (uint32_t service_id = 1; service_id != (CEPHX_PRINCIPAL_TYPE_MASK + 1); service_id <<= 1) {
        if (keys & service_id) {
          auth_server.get_service_secret(service_secret, service_id);

          SessionAuthInfo info;

          AuthTicket service_ticket;
          /* FIXME: initialize service_ticket */

          auth_server.get_service_secret(service_secret, service_id);
          auth_server.get_service_session_key(session_key, service_id);

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

      auth_server.get_service_secret(service_secret, CEPHX_PRINCIPAL_MON);

      ret = 0;
      bufferlist tmp_bl;
      if (!verify_authorizer(service_secret, indata, tmp_bl)) {
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
  CephXEnvRequest1 req;

  try {
    CephXPremable pre;
    ::decode(pre, iter);
    dout(0) << "CephXPremable id=" << pre.trans_id << dendl;
    ::encode(pre, result);

    ::decode(req, iter);
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


