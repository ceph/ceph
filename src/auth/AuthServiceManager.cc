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

#define PRINCIPAL_CLIENT_SECRET "123456789ABCDEF0"
#define PRINCIPAL_OSD_SECRET "3456789ABCDEF012"

class CephAuthServer {
  /* FIXME: this is all temporary */
  PrincipalTicket principal_ticket;
  CryptoKey client_secret;
  CryptoKey osd_secret;
  CryptoKey auth_session_key;
  CryptoKey service_secret;
  
public:
  CephAuthServer() {
    bufferptr ptr1(SERVICE_SECRET, sizeof(SERVICE_SECRET) - 1);
    service_secret.set_secret(CEPH_SECRET_AES, ptr1);

    bufferptr ptr2(PRINCIPAL_CLIENT_SECRET, sizeof(PRINCIPAL_CLIENT_SECRET) - 1);
    client_secret.set_secret(CEPH_SECRET_AES, ptr2);

    bufferptr ptr3(PRINCIPAL_OSD_SECRET, sizeof(PRINCIPAL_OSD_SECRET) - 1);
    osd_secret.set_secret(CEPH_SECRET_AES, ptr2);

    bufferptr ptr4(AUTH_SESSION_KEY, sizeof(AUTH_SESSION_KEY) - 1);
    auth_session_key.set_secret(CEPH_SECRET_AES, ptr3);
  }

/* FIXME: temporary stabs */
  int get_client_secret(CryptoKey& secret) {
     secret = client_secret;
     return 0;
  }

  int get_osd_secret(CryptoKey& secret) {
     secret = osd_secret;
     return 0;
  }

  int get_service_secret(CryptoKey& secret) {
    secret = service_secret;
    return 0;
  }

  int get_session_key(CryptoKey& key) {
    key = auth_session_key; 
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
      req.decode(indata);
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

  switch (request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      EntityName name; /* FIXME should take it from the request */
      entity_addr_t addr;
      PrincipalTicket ticket;
      CryptoKey principal_secret;
      CryptoKey session_key;
      CryptoKey service_secret;

      ticket.expires = g_clock.now();

      auth_server.get_client_secret(principal_secret);
      auth_server.get_session_key(session_key);
      auth_server.get_service_secret(service_secret);

      build_cephx_response_header(request_type, 0, result_bl);
      if (!build_get_tgt_reply(ticket, principal_secret, session_key, service_secret, result_bl)) {
        ret = -EIO;
      }
#if 0
      char buf[1024];
      const char *s = result_bl.c_str();
      int pos = 0;
      for (int i=0; i<result_bl.length(); i++) {
        pos += snprintf(&buf[pos], 1024-pos, "%0.2x ", (int)(unsigned char)s[i]);
        if (i && !(i%8))
          pos += snprintf(&buf[pos], 1024-pos, " ");
        if (i && !(i%16))
          pos += snprintf(&buf[pos], 1024-pos, "\n");
      }
      dout(0) << "result_buf=" << buf << dendl;
#endif
    }
    break;
  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY principal type=" << (cephx_header.request_type & CEPHX_PRINCIPAL_TYPE_MASK) << dendl;
    {
      EntityName name; /* FIXME should take it from the request */
      entity_addr_t addr;
      CryptoKey session_key;
      CryptoKey service_secret;
      uint32_t keys;

      auth_server.get_session_key(session_key);
      auth_server.get_service_secret(service_secret);

      if (!verify_get_session_keys_request(service_secret,
                                     session_key, keys, indata)) {
        ret = -EPERM;
      }

      ServiceTicket service_ticket;
      CryptoKey osd_secret;
      auth_server.get_osd_secret(osd_secret);

      auth_server.get_osd_secret(osd_secret);
      build_cephx_response_header(request_type, ret, result_bl);
      build_ticket_reply(service_ticket, session_key, osd_secret, result_bl);
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
    req.decode(iter);
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


