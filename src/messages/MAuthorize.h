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

#ifndef __MAUTHORIZE_H
#define __MAUTHORIZE_H

#include "msg/Message.h"

struct MAuthorize : public Message {
  bufferlist auth_payload;

  MAuthorize() : Message(CEPH_MSG_AUTHORIZE) { }

  const char *get_type_name() { return "authorize"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(auth_payload, p);
  }
  void encode_payload() {
    ::encode(auth_payload, payload);
  }
  bufferlist& get_auth_payload() { return auth_payload; }
};

#endif
