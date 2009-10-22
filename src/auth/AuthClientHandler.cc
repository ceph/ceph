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
#include "KeyRing.h"

#include "messages/MAuth.h"
#include "messages/MAuthorize.h"
#include "messages/MAuthReply.h"


AuthClientProtocolHandler::AuthClientProtocolHandler(AuthClientHandler *client) : 
                        msg(NULL), got_response(false), lock("AuthClientProtocolHandler")
{
  dout(0) << "AuthClientProtocolHandler::AuthClientProtocolHandler" << dendl;
  this->client = client;
  id = client->_add_proto_handler(this);
}

AuthClientProtocolHandler::~AuthClientProtocolHandler()
{
}

int AuthClientProtocolHandler::build_request()
{
  msg = _get_new_msg();
  if (!msg)
    return -ENOMEM;

  int ret = _build_request(); 

  return ret;
}

int AuthClientProtocolHandler::do_async_request()
{
  got_response = false;
  client->client->send_auth_message(msg);

  return 0;
}

int AuthClientProtocolHandler::handle_response(int ret, bufferlist::iterator& iter)
{
  if (!client) {
    derr(0) << "AuthClientProtocolHandler::handle_response() but client is NULL" << dendl;
    return -EINVAL;
  }

  Mutex::Locker l(lock);

  status = _handle_response(ret, iter);

  return status;
}

int AuthClientAuthenticateHandler::generate_authenticate_request(bufferlist& bl)
{
  dout(0) << "request_state=" << request_state << " response_state=" << response_state << dendl;
  if (request_state != response_state) {
    dout(0) << "can't generate request while waiting for response" << dendl;
    return -EINVAL;
  }

  switch (request_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvRequest1 req;
      req.name = client->name;
      set<__u32> supported;
      supported.insert(CEPH_AUTH_CEPHX);
      ::encode(supported, bl);
      ::encode(req, bl);
    }
    break;
  case 1:
    /* authenticate */
    {
      /* FIXME: init req fields */
      CephXEnvRequest2 req;
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
      req.piggyback = 1;
      ::encode(req, bl);

      /* we first need to get the principle/auth session key */
      CephXRequestHeader header;
      header.request_type = CEPHX_GET_AUTH_SESSION_KEY;
      ::encode(header, bl);
      build_authenticate_request(client->name, bl);
      request_state = 2;
      return 0;
    }
    break;

  case 2:
    /* get service tickets */
    {
      dout(0) << "want=" << hex << want << " have=" << have << dec << dendl;
      if (want == have) {
	response_state = 2;
	return 0;
      }

      AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
      if (!ticket_handler.build_authorizer(authorizer))
	return -EINVAL;

      CephXRequestHeader header;
      header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;
      ::encode(header, bl);
      
      bl.claim_append(authorizer.bl);
      build_service_ticket_request(want, bl);
    }
    break;

  default:
    assert(0);
  }
  request_state++;
  return 0;
}

int AuthClientAuthenticateHandler::_handle_response(int ret, bufferlist::iterator& indata)
{
  if (ret != 0 && ret != -EAGAIN) {
    response_state = request_state;
    return ret;
  }

  dout(0) << "AuthClientHandler::handle_response()" << dendl;
  switch (response_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvResponse1 response;

      response_state++;
      ::decode(response, indata);
      server_challenge = response.server_challenge;
    }
    break;
  case 1:
    /* authenticate */
    {
      struct CephXResponseHeader header;
      ::decode(header, indata);

      dout(0) << "request_type=" << hex << header.request_type << dec << dendl;
      dout(0) << "handle_cephx_response()" << dendl;

      response_state = 2;
      dout(0) << "CEPHX_GET_AUTH_SESSION_KEY" << dendl;
      
      CryptoKey secret;
      g_keyring.get_master(secret);
	
      if (!client->tickets.verify_service_ticket_reply(secret, indata)) {
	dout(0) << "could not verify service_ticket reply" << dendl;
	return -EPERM;
      }
      dout(0) << "want=" << want << " have=" << have << dendl;
      if (want != have)
        ret = -EAGAIN;
    }
    break;

  case 2:
    {
      struct CephXResponseHeader header;
      ::decode(header, indata);
      response_state = 3;
      dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY" << dendl;

      AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
  
      if (!client->tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
      ret = 0;
    }
    break;

  default:
    assert(0);
  }
  return ret;
}

int AuthClientAuthenticateHandler::_build_request()
{
  MAuth *m = (MAuth *)msg;
  bufferlist& bl = m->get_auth_payload();

  int ret = generate_authenticate_request(bl);

  return ret;
}

Message *AuthClientAuthenticateHandler::_get_new_msg()
{
  MAuth *m = new MAuth;
  m->trans_id = id;
  return m;
}

bufferlist& AuthClientAuthenticateHandler::_get_msg_bl(Message *m)
{
  return ((MAuth *)m)->get_auth_payload();
}

Message *AuthClientAuthorizeHandler::_get_new_msg()
{
  MAuthorize *m = new MAuthorize;
  m->trans_id = id;
  return m;
}

bufferlist& AuthClientAuthorizeHandler::_get_msg_bl(Message *m)
{
  return ((MAuthorize *)m)->get_auth_payload();
}

int AuthClientAuthorizeHandler::_build_request()
{
  CephXRequestHeader header;
  if (!client->tickets.has_key(service_id)) {
    dout(0) << "can't authorize: missing service key" << dendl;
    return -EPERM;
  }

  header.request_type = CEPHX_OPEN_SESSION;

  MAuthorize *m = (MAuthorize *)msg;
  bufferlist& bl = m->get_auth_payload();

  ::encode(header, bl);
  utime_t now;

  if (!client->tickets.build_authorizer(service_id, authorizer))
    return -EINVAL;

  bl.claim_append(authorizer.bl);

  return 0;
}

int AuthClientAuthorizeHandler::_handle_response(int ret, bufferlist::iterator& iter)
{
  struct CephXResponseHeader header;
  ::decode(header, iter);

  dout(0) << "AuthClientAuthorizeHandler::_handle_response() ret=" << ret << dendl;

  if (ret) {
    return ret;
  }

  switch (header.request_type & CEPHX_REQUEST_TYPE_MASK) {
  case CEPHX_OPEN_SESSION:
    {
      ret = authorizer.verify_reply(iter);
      break;
    }
    break;
  default:
    dout(0) << "header.request_type = " << hex << header.request_type << dec << dendl;
    ret = -EINVAL;
    break;
  }

  return ret;
}

AuthClientProtocolHandler *AuthClientHandler::_get_proto_handler(uint32_t id)
{
  map<uint32_t, AuthClientProtocolHandler *>::iterator iter = handlers_map.find(id);
  if (iter == handlers_map.end())
    return NULL;

  return iter->second;
}

uint32_t AuthClientHandler::_add_proto_handler(AuthClientProtocolHandler *handler)
{
  uint32_t id = max_proto_handlers++;
  handlers_map[id] = handler;
  return id;
}

int AuthClientHandler::handle_response(int trans_id, Message *response)
{
  MAuthReply* m = (MAuthReply *)response;
  int ret = m->result;

  lock.Lock();
  AuthClientProtocolHandler *handler = _get_proto_handler(trans_id);
  lock.Unlock();
  dout(0) << "AuthClientHandler::handle_response(): got response " << *response << " trans_id=" << trans_id << " handler=" << handler << dendl;
  if (!handler)
    return -EINVAL;

  bufferlist::iterator iter = m->result_bl.begin();
  return handler->handle_response(ret, iter);
}

int AuthClientHandler::send_session_request(AuthClient *client, AuthClientProtocolHandler *handler)
{
  Mutex::Locker l(lock);
  this->client = client;
  dout(10) << "send_session_request" << dendl;

  int err = handler->build_request();
  dout(0) << "handler.build_request returned " << err << dendl;
  if (err < 0)
    return err;

  err = handler->do_async_request();
  dout(0) << "handler.do_async_request returned " << err << dendl;

  return err;
}


void AuthClientHandler::tick()
{
  Mutex::Locker l(lock);

  // do i need to renew any tickets?
  // ...

}

bool AuthClientHandler::build_authorizer(uint32_t service_id, AuthAuthorizer& authorizer)
{
  dout(0) << "going to build authorizer for peer_id=" << service_id << " service_id=" << service_id << dendl;

  if (!tickets.build_authorizer(service_id, authorizer))
    return false;

  dout(0) << "authorizer built successfully" << dendl;
  return true;
}

