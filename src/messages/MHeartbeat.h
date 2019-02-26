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
#include "common/DecayCounter.h"

class MHeartbeat : public MessageInstance<MHeartbeat> {
public:
  friend factory;
private:
  mds_load_t load;
  __s32        beat = 0;
  map<mds_rank_t, float> import_map;

 public:
  const mds_load_t& get_load() const { return load; }
  int get_beat() const { return beat; }

  const map<mds_rank_t, float>& get_import_map() const { return import_map; }
  map<mds_rank_t, float>& get_import_map() { return import_map; }

protected:
  MHeartbeat() : MessageInstance(MSG_MDS_HEARTBEAT), load(DecayRate()) {}
  MHeartbeat(mds_load_t& load, int beat)
    : MessageInstance(MSG_MDS_HEARTBEAT),
      load(load) {
    this->beat = beat;
  }
  ~MHeartbeat() override {}

public:
  std::string_view get_type_name() const override { return "HB"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(load, payload);
    encode(beat, payload);
    encode(import_map, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(load, p);
    decode(beat, p);
    decode(import_map, p);
  }

};

#endif
