// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * PGCreate2 - instruct an OSD to create some pgs
 */

class MOSDPGCreate2 : public Message {
public:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  epoch_t epoch = 0;
  std::map<spg_t,std::pair<epoch_t,utime_t>> pgs;
  std::map<spg_t,std::pair<pg_history_t,PastIntervals>> pg_extra;

  MOSDPGCreate2()
    : Message{MSG_OSD_PG_CREATE2, HEAD_VERSION, COMPAT_VERSION} {}
  MOSDPGCreate2(epoch_t e)
    : Message{MSG_OSD_PG_CREATE2, HEAD_VERSION, COMPAT_VERSION},
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
    encode(pg_extra, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    decode(epoch, p);
    decode(pgs, p);
    if (header.version >= 2) {
      decode(pg_extra, p);
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
