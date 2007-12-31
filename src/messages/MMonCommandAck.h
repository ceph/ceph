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

#ifndef __MMONCOMMANDACK_H
#define __MMONCOMMANDACK_H

#include "msg/Message.h"

class MMonCommandAck : public Message {
 public:
  int r;
  string rs;
  
  MMonCommandAck() : Message(MSG_MON_COMMAND_ACK) {}
  MMonCommandAck(int _r, string s) : Message(MSG_MON_COMMAND_ACK),
				     r(_r), rs(s) { }
  
  const char *get_type_name() { return "mon_command"; }
  void print(ostream& o) {
    o << "mon_command_ack(" << r << " " << rs << ")";
  }
  
  void encode_payload() {
    payload.append((char*)&r, sizeof(r));
    ::_encode(rs, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(r), (char*)&r);
    off += sizeof(r);
    ::_decode(rs, payload, off);
  }
};

#endif
