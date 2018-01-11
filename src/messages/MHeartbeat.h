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
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;
  mds_load_t load;
  __s32        beat = 0;
  __s32        last_epoch_under = 0;
  map<mds_rank_t, float> import_map;

 public:
  mds_load_t& get_load() { return load; }
  int get_beat() { return beat; }
  int get_last_epoch_under() { return last_epoch_under; }

  map<mds_rank_t, float>& get_import_map() {
    return import_map;
  }

  MHeartbeat()
    : Message(MSG_MDS_HEARTBEAT, HEAD_VERSION, COMPAT_VERSION), load(utime_t()) { }
  MHeartbeat(mds_load_t& load, int beat, int last_epoch_under)
    : Message(MSG_MDS_HEARTBEAT, HEAD_VERSION, COMPAT_VERSION),
      load(load) {
    this->beat = beat;
    this->last_epoch_under = last_epoch_under;
  }
private:
  ~MHeartbeat() override {}

public:
  const char *get_type_name() const override { return "HB"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(load, payload);
    encode(beat, payload);
    encode(import_map, payload);
    encode(last_epoch_under, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    utime_t now(ceph_clock_now());
    decode(load, now, p);
    decode(beat, p);
    decode(import_map, p);
    if (header.version >= 2) {
      decode(last_epoch_under, p);
    } else {
      last_epoch_under = 0;
    }
  }

};

#endif
