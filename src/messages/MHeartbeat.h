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


#ifndef CEPH_MHEARTBEAT_H
#define CEPH_MHEARTBEAT_H

#include "include/types.h"
#include "msg/Message.h"

class MHeartbeat : public Message {
  mds_load_t load;
  __s32        beat;
  map<mds_rank_t, float> import_map;

 public:
  mds_load_t& get_load() { return load; }
  int get_beat() { return beat; }

  map<mds_rank_t, float>& get_import_map() {
    return import_map;
  }

  MHeartbeat()
    : Message(MSG_MDS_HEARTBEAT), load(utime_t()) { }
  MHeartbeat(mds_load_t& load, int beat)
    : Message(MSG_MDS_HEARTBEAT),
      load(load) {
    this->beat = beat;
  }
private:
  ~MHeartbeat() {}

public:
  const char *get_type_name() const { return "HB"; }

  void encode_payload(uint64_t features) {
    ::encode(load, payload);
    ::encode(beat, payload);
    ::encode(import_map, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    utime_t now(ceph_clock_now(NULL));
    ::decode(load, now, p);
    ::decode(beat, p);
    ::decode(import_map, p);
  }

};
REGISTER_MESSAGE(MHeartbeat, MSG_MDS_HEARTBEAT);
#endif
