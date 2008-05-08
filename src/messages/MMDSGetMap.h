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

#ifndef __MMDSGETMAP_H
#define __MMDSGETMAP_H

#include "msg/Message.h"

#include "include/types.h"
#include "include/encodable.h"

class MMDSGetMap : public Message {
 public:
  ceph_fsid fsid;
  epoch_t want;

  MMDSGetMap() {}
  MMDSGetMap(ceph_fsid &f, epoch_t w=0) : 
    Message(CEPH_MSG_MDS_GETMAP), 
    fsid(f),
    want(w) { }

  const char *get_type_name() { return "mds_getmap"; }
  void print(ostream& out) {
    out << "mds_getmap(want " << want << ")";
  }
  
  void encode_payload() {
    ::_encode_simple(fsid, payload);
    ::_encode_simple(want, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(fsid, p);
    ::_decode_simple(want, p);
  }
};

#endif
