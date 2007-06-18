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

#ifndef __MCLIENTSESSION_H
#define __MCLIENTSESSION_H

#include "msg/Message.h"

class MClientSession : public Message {
public:
  const static int OP_OPEN      = 1;
  const static int OP_OPEN_ACK  = 2;
  const static int OP_CLOSE     = 3;
  const static int OP_CLOSE_ACK = 4;
  static const char *get_opname(int o) {
    switch (o) {
    case OP_OPEN: return "open";
    case OP_OPEN_ACK: return "open_ack";
    case OP_CLOSE: return "close";
    case OP_CLOSE_ACK: return "close_ack";
    default: assert(0);
    }
  }

  __int32_t op;

  MClientSession() : Message(MSG_CLIENT_SESSION) { }
  MClientSession(int o) : Message(MSG_CLIENT_SESSION),
			  op(o) { }

  char *get_type_name() { return "client_session"; }
  void print(ostream& out) {
    out << "client_session " << get_opname(op);
  }

  void decode_payload() { 
    int off = 0;
    ::_decode(op, payload, off);
  }
  void encode_payload() { 
    ::_encode(op, payload);
  }
};

#endif
