// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * PGCreate2 - instruct an OSD to create some pgs
 */

class MOSDPGCreate2 : public MessageInstance<MOSDPGCreate2> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  epoch_t epoch = 0;
  map<spg_t,pair<epoch_t,utime_t>> pgs;

  MOSDPGCreate2()
    : MessageInstance(MSG_OSD_PG_CREATE2, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGCreate2(epoch_t e)
    : MessageInstance(MSG_OSD_PG_CREATE2, HEAD_VERSION, COMPAT_VERSION),
      epoch(e) { }
private:
  ~MOSDPGCreate2() override {}

public:
  std::string_view get_type_name() const override {
    return "pg_create2";
  }
  void print(ostream& out) const override {
    out << "pg_create2(e" << epoch << " " << pgs << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pgs, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    decode(epoch, p);
    decode(pgs, p);
  }
};
