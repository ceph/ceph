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

#ifndef __MLOGACK_H
#define __MLOGACK_H

#include "include/ClassEntry.h"

class MClassAck : public Message {
public:
  ceph_fsid_t fsid;
  version_t last;
  
  MClassAck() : Message(MSG_LOGACK) {}
  MClassAck(ceph_fsid_t& f, version_t l) : Message(MSG_LOGACK), fsid(f), last(l) {}

  const char *get_type_name() { return "class_ack"; }
  void print(ostream& out) {
    out << "class(last " << last << ")";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(last, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(last, p);
  }
};

#endif
