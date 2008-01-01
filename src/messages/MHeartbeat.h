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


#ifndef __MHEARTBEAT_H
#define __MHEARTBEAT_H

#include "include/types.h"
#include "msg/Message.h"

class MHeartbeat : public Message {
  mds_load_t load;
  int        beat;
  map<int, float> import_map;

 public:
  mds_load_t& get_load() { return load; }
  int get_beat() { return beat; }

  map<int, float>& get_import_map() {
    return import_map;
  }

  MHeartbeat() {}
  MHeartbeat(mds_load_t& load, int beat) :
    Message(MSG_MDS_HEARTBEAT) {
    this->load = load;
    this->beat = beat;
  }

  const char *get_type_name() { return "HB"; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off,sizeof(load), (char*)&load);
    off += sizeof(load);
    payload.copy(off, sizeof(beat), (char*)&beat);
    off += sizeof(beat);
    ::_decode(import_map, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&load, sizeof(load));
    payload.append((char*)&beat, sizeof(beat));
    ::_encode(import_map, payload);
  }

};

#endif
