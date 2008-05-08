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

class MMDSGetMap : public Message {
 public:
  ceph_fsid fsid;
  epoch_t have;

  MMDSGetMap() {}
  MMDSGetMap(ceph_fsid &f, epoch_t h=0) : 
    Message(CEPH_MSG_MDS_GETMAP), 
    fsid(f),
    have(h) { }

  const char *get_type_name() { return "mds_getmap"; }
  
  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(have, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(have, p);
  }
};

#endif
