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

#include "messages/MAuth.h"
#include "messages/MAuthReply.h"

int AuthClientAuthenticateHandler::generate_authenticate_request(bufferlist& bl)
{
  dout(0) << "status=" << status << dendl;
  if (status < 0) {
    return status;
  }
  if (request_state != response_state) {
    dout(0) << "can't generate request while waiting for response" << dendl;
    return -EINVAL;
  }

  switch(request_state) {
  case 0:
    /* initialize  */
    { 
      CephXEnvRequest1 req;
      req.init();

      ::encode(req, bl);
    }
    break;
  case 1:
   /* authenticate */
    {
      /* FIXME: init req fields */
      CephXEnvRequest2 req;
      memset(&req.client_challenge, 0x88, sizeof(req.client_challenge));
      req.key = req.client_challenge ^ server_challenge;
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
  AuthClientProtocolHandler *handler = _get_proto_handler(pre.trans_id);
  if (!handler)
    return -EINVAL;

  return handler->handle_response(ret, iter);
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
  AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  if (!ticket_handler.has_key()) {
    dout(0) << "auth ticket: doesn't have key" << dendl;
    /* we first need to get the principle/auth session key */

    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;

   ::encode(header, bl);
    CryptoKey key;
    AuthBlob blob;
    build_service_ticket_request(client->name, client->addr, CEPHX_PRINCIPAL_AUTH,
                               false, key, blob, bl);
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
  build_service_ticket_request(client->name, client->addr, want,
                             true, ticket_handler.session_key, ticket_handler.ticket, bl);
  
  return 0;
}

int AuthClientAuthorizeHandler::_build_request()
{
  CephXRequestHeader header;
  if (!(client->have & service_id)) {
    dout(0) << "can't authorize: missing service key" << dendl;
    return -EPERM;
  }

  header.request_type = CEPHX_OPEN_SESSION;

  bufferlist& bl = msg->get_auth_payload();

  ::encode(header, bl);
  utime_t now;
#if 0
  if (!client->tickets.build_authorizer(service_id, bl, ctx))
    return -EINVAL;
#endif
  return 0;
}

int AuthClientAuthorizeHandler::_handle_response(int ret, bufferlist::iterator& iter)
{
  /* FIXME: implement */

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

#define PRINCIPAL_SECRET "123456789ABCDEF0"
    {
      bufferptr p(PRINCIPAL_SECRET, sizeof(PRINCIPAL_SECRET) - 1);
      client->secret.set_secret(CEPH_SECRET_AES, p);
      // AuthTicketHandler& ticket_handler = tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  
      if (!client->tickets.verify_service_ticket_reply(client->secret, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }

      if (want)
        ret = -EAGAIN;
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    cephx_response_state = 2;
    dout(0) << "CEPHX_GET_PRINCIPAL_SESSION_KEY" << dendl;
    {
      AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  
      if (!client->tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
    }
    ret = 0;
    break;

  case CEPHX_OPEN_SESSION:
    {
      AuthTicketHandler& ticket_handler = client->tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
      AuthAuthorizeReply reply;
      if (!ticket_handler.decode_reply_authorizer(indata, reply)) {
        ret = -EINVAL;
        break;
      }
      AuthContext *ctx = client->context_map.get(reply.trans_id);
      if (!ctx) {
        ret = -EINVAL;
        break;
      }
      bool result = ticket_handler.verify_reply_authorizer(*ctx, reply);
      if (result)
        ctx->status = 0;
      else
        ctx->status = -EPERM;

       ctx->cond->Signal();
       ret = 0;
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

bool AuthClientAuthenticateHandler::request_pending() {
  dout(0) << "request_pending(): request_state=" << cephx_request_state << " cephx_response_state=" << cephx_response_state << dendl;
  return (request_state != response_state) || (cephx_request_state != cephx_response_state);
}


// -----------

int AuthClientHandler::start_session(AuthClient *client, double timeout)
{
  Mutex::Locker l(lock);
  this->client = client;
  dout(10) << "start_session" << dendl;

  AuthClientAuthenticateHandler handler(this, want, have);

  int err;

  do {
    err = handler.build_request();
    if (err < 0)
      return err;

    err = handler.do_request(timeout);
    dout(0) << "handler.do_request returned " << err << dendl;
    if (err < 0)
      return err;

  } while (err == -EAGAIN);

  return err;
}

AuthClientProtocolHandler::AuthClientProtocolHandler(AuthClientHandler *client) : 
                        msg(NULL), got_response(false), got_timeout(false),
                        timeout_event(NULL)
{
  this->client = client;
  reset();
  id = client->_add_proto_handler(this);
}

AuthClientProtocolHandler::~AuthClientProtocolHandler()
{
  if (msg)
    delete msg;
}

int AuthClientProtocolHandler::build_request()
{
  msg = new MAuth;
  if (!msg)
    return -ENOMEM;

  bufferlist& bl = msg->get_auth_payload();
  CephXPremable pre;
  pre.trans_id = id;
  ::encode(pre, bl);

  int ret = _build_request(); 

  return ret;
}

int AuthClientHandler::authorize(uint32_t service_id, double timeout)
{
  AuthClientAuthorizeHandler handler(this, service_id);

  int ret = handler.build_request();
  if (ret < 0)
    return ret;

  ret = handler.do_request(timeout);

  return ret;
}

void AuthClientHandler::tick()
{
  Mutex::Locker l(lock);

  // do i need to renew any tickets?
  // ...

}

int AuthClientAuthenticateHandler::_build_request()
{
  msg = new MAuth;
  if (!msg)
    return -ENOMEM;

  bufferlist& bl = msg->get_auth_payload();

  int ret = generate_authenticate_request(bl);
  if (ret < 0) {
    delete msg;
  }

  return ret;
}

#if 0
int AuthClientAuthenticateHandler::_do_request()
{
  Message *msg = build_authenticate_request();
  if (!msg)
    return -EIO;

  int ret = _do_request_generic(timeout, msg);

  return ret;
}
#endif

int AuthClientProtocolHandler::do_request(double timeout)
{
  got_response = false;
  client->client->send_message(msg);

  // schedule timeout?
  assert(timeout_event == 0);
  timeout_event = new C_OpTimeout(this, timeout);
  client->timer.add_event_after(timeout, timeout_event);

  dout(0) << "got_response=" << got_response << " got_timeout=" << got_timeout << dendl;

  cond.Wait(client->lock);

  // finish.
  client->timer.cancel_event(timeout_event);
  timeout_event = NULL;

  return 0;
}

void AuthClientProtocolHandler::_request_timeout(double timeout)
{
  Mutex::Locker l(client->lock);
  dout(10) << "_request_timeout" << dendl;
  timeout_event = 0;
  if (!got_response) {
    got_timeout = 1;
    cond.Signal();
  }
  status = -ETIMEDOUT;
}

int AuthClientProtocolHandler::handle_response(int ret, bufferlist::iterator& iter)
{
  Mutex::Locker l(client->lock);

  got_response = true;

  status = _handle_response(ret, iter);
  cond.Signal();

  return status;
}

AuthContext& AuthContextMap::create()
{
  Mutex::Locker l(lock);
  AuthContext& ctx = m[max_id];
  ctx.id = max_id;
  ctx.cond = NULL;
  ++max_id;
  
  return ctx;
}

void AuthContextMap::remove(int id)
{
  Mutex::Locker l(lock);
  std::map<int, AuthContext>::iterator iter = m.find(id);
  if (iter != m.end()) {
    m.erase(iter);
  }
}

AuthContext *AuthContextMap::get(int id)
{
  Mutex::Locker l(lock);
  std::map<int, AuthContext>::iterator iter = m.find(id);
  if (iter != m.end())
    return &iter->second;

  return NULL;
}


