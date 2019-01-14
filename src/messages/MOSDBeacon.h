// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "PaxosServiceMessage.h"

class MOSDBeacon : public MessageInstance<MOSDBeacon, PaxosServiceMessage> {
public:
  friend factory;

  std::vector<pg_t> pgs;
  epoch_t min_last_epoch_clean = 0;

  MOSDBeacon()
    : MessageInstance(MSG_OSD_BEACON, 0)
  {}
  MOSDBeacon(epoch_t e, epoch_t min_lec)
    : MessageInstance(MSG_OSD_BEACON, e),
      min_last_epoch_clean(min_lec)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgs, payload);
    encode(min_last_epoch_clean, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(pgs, p);
    decode(min_last_epoch_clean, p);
  }
  std::string_view get_type_name() const override { return "osd_beacon"; }
  void print(ostream &out) const {
    out << get_type_name()
        << "(pgs " << pgs
        << " lec " << min_last_epoch_clean
        << " v" << version << ")";
  }
};
