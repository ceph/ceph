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

#ifndef __MMONSUBSCRIBE_H
#define __MMONSUBSCRIBE_H

#include "msg/Message.h"

struct MMonSubscribe : public Message {
  map<nstring, version_t> what;
  
  MMonSubscribe() : Message(CEPH_MSG_MON_SUBSCRIBE) {}
  
  const char *get_type_name() { return "mon_subscribe"; }
  void print(ostream& o) {
    o << "mon_subscribe(" << what << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(what, p);
  }
  void encode_payload() {
    ::encode(what, payload);
  }
};

#endif
