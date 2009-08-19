// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MCLIENTAUTHREPLY_H
#define __MCLIENTAUTHREPLY_H

#include "msg/Message.h"

struct MClientAuthReply : public Message {
  __s32 result;
  cstring result_msg;
  bufferlist result_bl;

  MClientAuthReply(bufferlist *bl = NULL, int r = 0, const char *msg = 0) :
    Message(CEPH_MSG_CLIENT_AUTH_REPLY),
    result(r),
    result_msg(msg) {
    if (bl) {
      bufferlist::iterator iter = bl->begin();
      iter.copy(bl->length(), result_bl);
    }
  }

  const char *get_type_name() { return "client_auth_reply"; }
  void print(ostream& o) {
    o << "client_auth_reply(" << result;
    if (result_msg.length()) o << " " << result_msg;
    o << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(result, p);
    ::decode(result_bl, p);
    ::decode(result_msg, p);
  }
  void encode_payload() {
    ::encode(result, payload);
    ::encode(result_bl, payload);
    ::encode(result_msg, payload);

    dout(0) << "MClientAuthReply size=" << payload.length() << dendl;
  }
};

#endif
