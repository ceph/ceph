// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __MMDSOffloadComplete_H
#define __MMDSOffloadComplete_H

#include "msg/Message.h"
#include "include/types.h"

#include <map>
using std::map;

class MMDSOffloadComplete : public Message {
 public:
  map<int, double> targets_used;

  MMDSOffloadComplete() : Message(MSG_MDS_OFFLOAD_COMPLETE) {}

  MMDSOffloadComplete(map<int, double>& mds_targets) :
    Message(MSG_MDS_OFFLOAD_COMPLETE),
    targets_used(mds_targets) {}

  const char* get_type_name() { return "mds_offload_complete"; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(targets_used, p);
  }

  void encode_payload() {
    ::encode(targets_used, payload);
  }
};

#endif
