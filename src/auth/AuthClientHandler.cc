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

#include "AuthClientHandler.h"
#include "KeyRing.h"

#include "messages/MAuth.h"
#include "messages/MAuthReply.h"

#include "cephx/CephxClientHandler.h"

AuthClientProtocolHandler *AuthClientHandler::get_protocol_handler(int proto)
{
  switch (proto) {
  case CEPH_AUTH_CEPHX:
    return new CephxClientHandler(this);
  default:
    return NULL;
  }
}



int AuthClientHandler::handle_response(MAuthReply *m)
{
  int ret = m->result;

  if (!proto) {
    // set up protocol
    proto = get_protocol_handler(m->protocol);
    if (!proto) {
      dout(0) << " server selected " << m->protocol << ", but we don't support it?" << dendl;
      return -EINVAL;
    }
  }

  dout(0) << "AuthClientHandler::handle_response(): got response " << *m << " handler=" << proto << dendl;

  bufferlist::iterator iter = m->result_bl.begin();
  ret = proto->handle_response(ret, iter);
  if (ret == -EAGAIN) {
    ret = send_request();
    if (ret)
      return ret;
    return -EAGAIN;
  }
  return ret;
}

int AuthClientHandler::send_request()
{
  Mutex::Locker l(lock);
  dout(10) << "send_session_request" << dendl;

  if (!proto) {
    set<__u32> supported;
    supported.insert(CEPH_AUTH_CEPHX);
    dout(0) << " sending supported protocol list " << supported << dendl;
    bufferlist bl;
    ::encode(supported, bl);
    MAuth *m = new MAuth;
    m->protocol = 0;
    m->auth_payload = bl;
    client->send_auth_message(m);
    return 0;
  }

  MAuth *m = new MAuth;
  m->protocol = proto->get_protocol();
  int err = proto->build_request(m->auth_payload);
  dout(0) << "handler.build_request returned " << err << dendl;
  if (err < 0)
    return err;
  client->send_auth_message(m);
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

