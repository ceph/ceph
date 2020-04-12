// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "PaxosServiceMessage.h"
#include "osd/osd_types.h"
#include "include/types.h"

class MMonGetPurgedSnapsReply : public PaxosServiceMessage {
public:
  epoch_t start, last;
  std::map<epoch_t,mempool::osdmap::map<int64_t,snap_interval_set_t>> purged_snaps;

  MMonGetPurgedSnapsReply(epoch_t s=0, epoch_t l=0)
    : PaxosServiceMessage{MSG_MON_GET_PURGED_SNAPS_REPLY, 0},
      start(s),
      last(l) {}
private:
  ~MMonGetPurgedSnapsReply() override {}

public:
  std::string_view get_type_name() const override {
    return "mon_get_purged_snaps_reply";
  }
  void print(std::ostream& out) const override {
    out << "mon_get_purged_snaps_reply([" << start << "," << last << "])";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(start, payload);
    encode(last, payload);
    encode(purged_snaps, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(start, p);
    decode(last, p);
    decode(purged_snaps, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
