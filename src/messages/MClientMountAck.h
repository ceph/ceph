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

#ifndef __MCLIENTMOUNTACK_H
#define __MCLIENTMOUNTACK_H

#include "msg/Message.h"

struct MClientMountAck : public Message {
  __s32 result;
  cstring result_msg;
  bufferlist monmap_bl;
  bufferlist signed_ticket;

  MClientMountAck(int r = 0, const char *msg = 0) :
    Message(CEPH_MSG_CLIENT_MOUNT_ACK),
    result(r),
    result_msg(msg) { }

  const char *get_type_name() { return "client_mount_ack"; }
  void print(ostream& o) {
    o << "client_mount_ack(" << result;
    if (result_msg.length()) o << " " << result_msg;
    if (monmap_bl.length()) o << " + monmap";
    if (signed_ticket.length()) o << " + ticket";
    o << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(result, p);
    ::decode(result_msg, p);
    ::decode(monmap_bl, p);
    ::decode(signed_ticket, p);
  }
  void encode_payload() {
    ::encode(result, payload);
    ::encode(result_msg, payload);
    ::encode(monmap_bl, payload);
    ::encode(signed_ticket, payload);
  }
};

#endif
