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

int AuthClientHandler::generate_authenticate_request(bufferlist& bl)
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


int AuthClientHandler::handle_response(Message *response)
{
  bufferlist bl;
  int ret;

#if 0
      char buf[4096];
      const char *s = bl.c_str();
      int pos = 0;
      for (unsigned int i=0; i<bl.length() && pos<sizeof(buf) - 8; i++) {
        pos += snprintf(&buf[pos], sizeof(buf)-pos, "%.2x ", (int)(unsigned char)s[i]);
        if (i && !(i%8))
          pos += snprintf(&buf[pos], sizeof(buf)-pos, " ");
        if (i && !(i%16))
          pos += snprintf(&buf[pos], sizeof(buf)-pos, "\n");
      }
      dout(0) << "result_buf=" << buf << dendl;
#endif

  MAuthReply* m = (MAuthReply *)response;
  bl = m->result_bl;
  ret = m->result;

  got_authenticate_response = true;

  bufferlist::iterator iter = bl.begin();

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

int AuthClientHandler::generate_cephx_authenticate_request(bufferlist& bl)
{
  CephXRequestHeader header;
  AuthTicketHandler& ticket_handler = tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  if (!ticket_handler.has_key()) {
    dout(0) << "auth ticket: doesn't have key" << dendl;
    /* we first need to get the principle/auth session key */

    header.request_type = CEPHX_GET_AUTH_SESSION_KEY;

   ::encode(header, bl);
    CryptoKey key;
    AuthBlob blob;
    build_service_ticket_request(name, addr, CEPHX_PRINCIPAL_AUTH,
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
  build_service_ticket_request(name, addr, want,
                             true, ticket_handler.session_key, ticket_handler.ticket, bl);
  
  return 0;
}

int AuthClientHandler::generate_cephx_authorize_request(uint32_t service_id, bufferlist& bl, AuthorizeContext& ctx)
{
  CephXRequestHeader header;
  if (!(have & service_id)) {
    dout(0) << "can't authorize: missing service key" << dendl;
    return -EPERM;
  }

  header.request_type = CEPHX_OPEN_SESSION;

  ::encode(header, bl);
  //AuthorizeContext& ctx = context_map.create();
  utime_t now;
  if (!tickets.build_authorizer(service_id, bl, ctx))
    return -EINVAL;
  
  return 0;
}

int AuthClientHandler::handle_cephx_response(bufferlist::iterator& indata)
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
      secret.set_secret(CEPH_SECRET_AES, p);
      // AuthTicketHandler& ticket_handler = tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  
      if (!tickets.verify_service_ticket_reply(secret, indata)) {
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
      AuthTicketHandler& ticket_handler = tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
  
      if (!tickets.verify_service_ticket_reply(ticket_handler.session_key, indata)) {
        dout(0) << "could not verify service_ticket reply" << dendl;
        return -EPERM;
      }
    }
    ret = 0;
    break;

  case CEPHX_OPEN_SESSION:
    {
      AuthTicketHandler& ticket_handler = tickets.get_handler(CEPHX_PRINCIPAL_AUTH);
      utime_t then; /* FIXME */
      ticket_handler.verify_reply_authorizer(then, indata);
    }
    break;
  default:
    dout(0) << "header.request_type = " << hex << header.request_type << dec << dendl;
    ret = -EINVAL;
    break;
  }

  return ret;
}

bool AuthClientHandler::request_pending() {
  dout(0) << "request_pending(): cephx_request_state=" << cephx_request_state << " cephx_response_state=" << cephx_response_state << dendl;
  return (request_state != response_state) || (cephx_request_state != cephx_response_state);
}


// -----------

int AuthClientHandler::start_session(AuthClient *client, double timeout)
{
  Mutex::Locker l(lock);
  this->client = client;
  dout(10) << "start_session" << dendl;
  _reset();

  do {
    status = 0;
    int err = _do_authenticate_request(timeout);
    dout(0) << "_do_authenticate_request returned " << err << dendl;
    if (err < 0)
      return err;

  } while (status == -EAGAIN);

  return status;
}

int AuthClientHandler::authorize(uint32_t service_id)
{
  MAuth *msg = new MAuth;
  if (!msg)
    return NULL;
  bufferlist& bl = msg->get_auth_payload();

  AuthorizeContext& ctx = context_map.create();

  int err = generate_cephx_authorize_request(service_id, bl, ctx);
  if (err < 0) {
    context_map.remove(ctx.id);
  }

  client->send_message(msg);

  return 0;  
}

void AuthClientHandler::tick()
{
  Mutex::Locker l(lock);

  // do i need to renew any tickets?
  // ...

}

Message *AuthClientHandler::build_authenticate_request()
{
  MAuth *msg = new MAuth;
  if (!msg)
    return NULL;
  bufferlist& bl = msg->get_auth_payload();

  if (generate_authenticate_request(bl) < 0) {
    delete msg;
    return NULL;
  }

  return msg;
}

int AuthClientHandler::_do_authenticate_request(double timeout)
{
  Message *msg = build_authenticate_request();
  if (!msg)
    return -EIO;

  Cond request_cond;
  cur_request_cond = &request_cond;
  got_authenticate_response = false;

  int ret = _do_request_generic(timeout, msg, request_cond);

  cur_request_cond = NULL;

  return ret;
}

int AuthClientHandler::_do_request_generic(double timeout, Message *msg, Cond& request_cond)
{
  client->send_message(msg);

  // schedule timeout?
  assert(timeout_event == 0);
  timeout_event = new C_OpTimeout(this, timeout);
  timer.add_event_after(timeout, timeout_event);

  dout(0) << "got_authenticate_response=" << got_authenticate_response << " got_timeout=" << got_authenticate_timeout << dendl;

  request_cond.Wait(lock);

  // finish.
  timer.cancel_event(timeout_event);
  timeout_event = NULL;

  return 0;
}

void AuthClientHandler::_authenticate_request_timeout(double timeout)
{
  Mutex::Locker l(lock);
  dout(10) << "_op_timeout" << dendl;
  timeout_event = 0;
  if (!got_authenticate_response) {
    got_authenticate_timeout = 1;
    assert(cur_request_cond);
    cur_request_cond->Signal();
  }
  status = -ETIMEDOUT;
}

void AuthClientHandler::handle_auth_reply(MAuthReply *m)
{
  Mutex::Locker l(lock);

  status = handle_response(m);
  cur_request_cond->Signal();
}

/*
class AuthorizeMap {
  map<int, AuthorizeContext> map;

  Mutex lock;
  int max_id;

public:
*/
AuthorizeContext& AuthorizeContextMap::create()
{
  Mutex::Locker l(lock);
  AuthorizeContext& ctx = m[max_id];
  ctx.id = max_id;
  ++max_id;
  
  return ctx;
}

void AuthorizeContextMap::remove(int id)
{
  std::map<int, AuthorizeContext>::iterator iter = m.find(id);
  if (iter != m.end()) {
    m.erase(iter);
  }
}

AuthorizeContext *AuthorizeContextMap::get(int id)
{
  Mutex::Locker l(lock);
  std::map<int, AuthorizeContext>::iterator iter = m.find(id);
  if (iter != m.end())
    return &iter->second;

  return NULL;
}


