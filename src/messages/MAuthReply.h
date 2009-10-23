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

#ifndef __MAUTHREPLY_H
#define __MAUTHREPLY_H

#include "msg/Message.h"

struct MAuthReply : public Message {
  __u32 protocol;
  __s32 result;
  cstring result_msg;
  bufferlist result_bl;

  MAuthReply() : Message(CEPH_MSG_AUTH_REPLY), protocol(0), result(0) {}
  MAuthReply(__u32 p, bufferlist *bl = NULL, int r = 0, const char *msg = 0) :
    Message(CEPH_MSG_AUTH_REPLY),
    protocol(p), result(r),
    result_msg(msg) {
    if (bl)
      result_bl = *bl;
  }

  const char *get_type_name() { return "auth_reply"; }
  void print(ostream& o) {
    char buf[80];
    o << "auth_reply(proto " << protocol << " " << result << " " << strerror_r(-result, buf, sizeof(buf));
    if (result_msg.length())
      o << ": " << result_msg;
    o << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(protocol, p);
    ::decode(result, p);
    ::decode(result_bl, p);
    ::decode(result_msg, p);
  }
  void encode_payload() {
    ::encode(protocol, payload);
    ::encode(result, payload);
    ::encode(result_bl, payload);
    ::encode(result_msg, payload);
  }
};

#endif
