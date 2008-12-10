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

#ifndef __MMONOBSERVE_H
#define __MMONOBSERVE_H

#include "msg/Message.h"

#include <vector>
using std::vector;

class MMonObserve : public Message {
 public:
  ceph_fsid fsid;
  uint32_t monitor_id;
  version_t ver;

  MMonObserve() : Message(MSG_MON_OBSERVE) {}
  MMonObserve(ceph_fsid &f, int mon_id, version_t v) : 
    Message(MSG_MON_OBSERVE),
    fsid(f), monitor_id(mon_id), ver(v) { }
  
  const char *get_type_name() { return "mon_observe"; }
  void print(ostream& o) {
    o << "observe(" << monitor_id << " v" << ver << ")";
  }
  
  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(monitor_id, payload);
    ::encode(ver, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(monitor_id, p);
    ::decode(ver, p);
  }
};

#endif
