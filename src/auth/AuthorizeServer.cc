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

#include <string>

using namespace std;

#include "config.h"

#include "AuthorizeServer.h"
#include "Auth.h"
#include "msg/SimpleMessenger.h"
#include "messages/MAuthorize.h"
#include "messages/MAuthReply.h"
#include "mon/Session.h"

AuthorizeServer::~AuthorizeServer()
{
}

bool AuthorizeServer::init()
{
  messenger->add_dispatcher_tail(this);
  return true;
}

bool AuthorizeServer::ms_dispatch(Message *m)
{
  lock.Lock();
  bool ret = _dispatch(m);
  lock.Unlock();
  return ret;
}

bool AuthorizeServer::_dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_AUTHORIZE:
      handle_request((class MAuthorize*)m);
    break;
  default:
    return false;
  }
  return true;
}

void AuthorizeServer::handle_request(MAuthorize *m)
{
  dout(0) << "AuthorizeServer::handle_request() blob_size=" << m->get_auth_payload().length() << dendl;
  int ret = 0;

  Session *s = (Session *)m->get_connection()->get_priv();
  s->put();

  bufferlist response_bl;
  bufferlist::iterator indata = m->auth_payload.begin();

  CephXPremable pre;
  ::decode(pre, indata);
  dout(0) << "CephXPremable id=" << pre.trans_id << dendl;
  ::encode(pre, response_bl);

  // handle the request
  try {
    ret = do_authorize(indata, response_bl);
  } catch (buffer::error *err) {
    ret = -EINVAL;
    dout(0) << "caught error when trying to handle authorize request, probably malformed request" << dendl;
  }
  MAuthReply *reply = new MAuthReply(&response_bl, ret);
  messenger->send_message(reply, m->get_orig_source_inst());
}

int AuthorizeServer::do_authorize(bufferlist::iterator& indata, bufferlist& result_bl)
{
  struct CephXRequestHeader cephx_header;

  ::decode(cephx_header, indata);

  uint16_t request_type = cephx_header.request_type & CEPHX_REQUEST_TYPE_MASK;
  int ret;

  dout(0) << "request_type=" << request_type << dendl;

  switch (request_type) {
  case CEPHX_OPEN_SESSION:
    {
       dout(0) << "CEPHX_OPEN_SESSION " << cephx_header.request_type << dendl;

      ret = 0;
      bufferlist tmp_bl;
      AuthServiceTicketInfo auth_ticket_info;
      if (!::verify_authorizer(*keys, indata, auth_ticket_info, tmp_bl)) {
        dout(0) << "could not verify authorizer" << dendl;
        ret = -EPERM;
      }
      result_bl.claim_append(tmp_bl);
    }
    break;
  default:
    ret = -EINVAL;
    break;
  }
  build_cephx_response_header(request_type, ret, result_bl);

  return ret;
}

int AuthorizeServer::verify_authorizer(int peer_type, bufferlist::iterator& indata, bufferlist& result_bl)
{
  int ret = 0;
  AuthServiceTicketInfo auth_ticket_info;
  try {
    if (!::verify_authorizer(*keys, indata, auth_ticket_info, result_bl)) {
      dout(0) << "could not verify authorizer" << dendl;
      ret = -EPERM;
    }
  } catch (buffer::error *err) {
    ret = -EINVAL;
  }

  return ret;
}

void AuthorizeServer::build_cephx_response_header(int request_type, int status, bufferlist& bl)
{
  struct CephXResponseHeader header;
  header.request_type = request_type;
  header.status = status;
  ::encode(header, bl);
}


