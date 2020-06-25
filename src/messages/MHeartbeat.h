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
#include "common/DecayCounter.h"
#include "messages/MMDSOp.h"

class MHeartbeat : public MMDSOp {
private:
  mds_load_t load;
  __s32 beat = 0;
  std::map<mds_rank_t, float> import_map;

 public:
  const mds_load_t& get_load() const { return load; }
  int get_beat() const { return beat; }

  const std::map<mds_rank_t, float>& get_import_map() const { return import_map; }
  std::map<mds_rank_t, float>& get_import_map() { return import_map; }

protected:
  MHeartbeat() : MMDSOp(MSG_MDS_HEARTBEAT), load(DecayRate()) {}
  MHeartbeat(mds_load_t& load, int beat)
    : MMDSOp(MSG_MDS_HEARTBEAT),
      load(load),
      beat(beat)
  {}
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
    using ceph::decode;
    auto p = payload.cbegin();
    decode(load, p);
    decode(beat, p);
    decode(import_map, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
