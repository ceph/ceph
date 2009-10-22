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
                        msg(NULL), got_response(false), got_timeout(false),
                        timeout_event(NULL), lock("AuthClientProtocolHandler")
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
  bufferlist& bl = _get_msg_bl(msg);

  CephXPremable pre;
  dout(0) << "pre=" << id << dendl;
  pre.trans_id = id;
  ::encode(pre, bl);

  int ret = _build_request(); 

  return ret;
}

int AuthClientProtocolHandler::do_request(double timeout)
{
  got_response = false;
  client->client->send_auth_message(msg);

  // schedule timeout?
  assert(timeout_event == 0);
  timeout_event = new C_OpTimeout(this, timeout);
  client->timer.add_event_after(timeout, timeout_event);

  cond.Wait(lock);

  dout(0) << "got_response=" << got_response << " got_timeout=" << got_timeout << dendl;

  // finish.
  if (timeout_event) {
    client->timer.cancel_event(timeout_event);
    timeout_event = NULL;
  }

  return status;
}

int AuthClientProtocolHandler::do_async_request(double timeout)
{
  got_response = false;
  client->client->send_auth_message(msg);

#if 0
  // schedule timeout?
  assert(timeout_event == 0);
  timeout_event = new C_OpTimeout(this, timeout);
  client->timer.add_event_after(timeout, timeout_event);


  dout(0) << "got_response=" << got_response << " got_timeout=" << got_timeout << dendl;

  // finish.
  if (timeout_event) {
    client->timer.cancel_event(timeout_event);
    timeout_event = NULL;
  }
#endif

  return 0;
}

void AuthClientProtocolHandler::_request_timeout(double timeout)
{
  dout(10) << "_request_timeout" << dendl;
  timeout_event = NULL;
  if (!got_response) {
    got_timeout = 1;
    cond.Signal();
  }
  status = -ETIMEDOUT;
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

  switch(request_state) {
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
      request_state++;
      return generate_cephx_authenticate_request(bl);
    }
    break;
  default:
    return generate_cephx_authenticate_request(bl);
  }
  request_state++;
  return 0;
}

int AuthClientAuthenticateHandler::_handle_response(int ret, bufferlist::iterator& iter)
{

  if (ret != 0 && ret != -EAGAIN) {
    response_state = request_state;
    cephx_response_state = cephx_request_state;
    return ret;
  }

  dout(0) << "AuthClientHandler::handle_response()" << dendl;
  switch(response_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvResponse1 response;

      response_state++;
      ::decode(response, iter);
      server_challenge = response.server_challenge;
    }
    break;
  case 1:
    /* authenticate */
    {
      response_state++;
      return handle_cephx_response(iter);
    }
    break;
  default:
    return handle_cephx_response(iter);
  }
  return -EAGAIN;
}

int AuthClientAuthenticateHandler::generate_cephx_authenticate_request(bufferlist& bl)
{
  CephXRequestHeader header;
  AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);

  if (!ticket_handler.has_key()) {
    dout(0) << "auth ticket: doesn't have key" << dendl;
    /* we first need to get the principle/auth session key */

    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;

    ::encode(header, bl);

    build_authenticate_request(client->name, client->addr, bl);
    cephx_request_state = 1;
    return 0;
  }

  dout(0) << "want=" << hex << want << " have=" << have << dec << dendl;

  cephx_request_state = 2;

  if (want == have) {
    cephx_response_state = 2;
    return 0;
  }

  header.request_type = CEPHX_GET_PRINCIPAL_SESSION_KEY;

  ::encode(header, bl);

  if (!ticket_handler.build_authorizer(authorizer))
    return -EINVAL;

  bl.claim_append(authorizer.bl);

  build_service_ticket_request(want, bl);
  
  return 0;
}

int AuthClientAuthenticateHandler::handle_cephx_response(bufferlist::iterator& indata)
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

    {
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

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    cephx_response_state = 2;
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY" << dendl;
    {
      AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPH_ENTITY_TYPE_AUTH);
  
      if (!client->tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
    }
    ret = 0;
    break;

  default:
    dout(0) << "header.request_type = " << hex << header.request_type << dec << dendl;
    ret = -EINVAL;
    break;
  }

  return ret;
}

bool AuthClientAuthenticateHandler::request_pending() {
  dout(0) << "request_pending(): request_state=" << cephx_request_state << " cephx_response_state=" << cephx_response_state << dendl;
  return (request_state != response_state) || (cephx_request_state != cephx_response_state);
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

  return m;
}

bufferlist& AuthClientAuthenticateHandler::_get_msg_bl(Message *m)
{
  return ((MAuth *)m)->get_auth_payload();
}

Message *AuthClientAuthorizeHandler::_get_new_msg()
{
  MAuthorize *m = new MAuthorize;

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

int AuthClientHandler::handle_response(Message *response)
{
  bufferlist bl;
  int ret;

  MAuthReply* m = (MAuthReply *)response;
  bl = m->result_bl;
  ret = m->result;

  CephXPremable pre;
  bufferlist::iterator iter = bl.begin();
  ::decode(pre, iter);

  lock.Lock();
  AuthClientProtocolHandler *handler = _get_proto_handler(pre.trans_id);
  lock.Unlock();
  dout(0) << "AuthClientHandler::handle_response(): got response " << *response << " trans_id=" << pre.trans_id << " handler=" << handler << dendl;
  if (!handler)
    return -EINVAL;

  return handler->handle_response(ret, iter);
}

int AuthClientHandler::start_session(AuthClient *client, double timeout)
{
  Mutex::Locker l(lock);
  this->client = client;
  dout(10) << "start_session" << dendl;

  AuthClientAuthenticateHandler handler(this, want, have);

  int err;

  do {
    err = handler.build_request();
    dout(0) << "handler.build_request returned " << err << dendl;
    if (err < 0)
      return err;

    err = handler.do_request(timeout);
    dout(0) << "handler.do_request returned " << err << dendl;
    if (err < 0 && err != -EAGAIN)
      return err;

  } while (err == -EAGAIN);

  return err;
}

int AuthClientHandler::send_session_request(AuthClient *client, AuthClientProtocolHandler *handler, double timeout)
{
  Mutex::Locker l(lock);
  this->client = client;
  dout(10) << "start_session" << dendl;

  int err = handler->build_request();
  dout(0) << "handler.build_request returned " << err << dendl;
  if (err < 0)
    return err;

  err = handler->do_async_request(timeout);
  dout(0) << "handler.do_async_request returned " << err << dendl;

  return err;
}

int AuthClientHandler::authorize(uint32_t service_id, double timeout)
{
  Mutex::Locker l(lock);
  AuthClientAuthorizeHandler handler(this, service_id);

  int ret = handler.build_request();
  if (ret < 0)
    return ret;

  ret = handler.do_request(timeout);

  dout(0) << "authorize returned " << ret << dendl;

  return ret;
}

void AuthClientHandler::tick()
{
  Mutex::Locker l(lock);

  // do i need to renew any tickets?
  // ...

}

int AuthClientHandler::build_authorizer(uint32_t service_id, AuthAuthorizer& authorizer)
{
  dout(0) << "going to build authorizer for peer_id=" << service_id << " service_id=" << service_id << dendl;

  if (!tickets.build_authorizer(service_id, authorizer))
    return -EINVAL;

  dout(0) << "authorizer built successfully" << dendl;

  return 0;
}

