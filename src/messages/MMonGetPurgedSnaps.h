// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "PaxosServiceMessage.h"
#include "include/types.h"

class MMonGetPurgedSnaps : public PaxosServiceMessage {
public:
  epoch_t start, last;

  MMonGetPurgedSnaps(epoch_t s=0, epoch_t l=0)
    : PaxosServiceMessage{MSG_MON_GET_PURGED_SNAPS, 0},
      start(s),
      last(l) {}
private:
  ~MMonGetPurgedSnaps() override {}

public:
  std::string_view get_type_name() const override {
    return "mon_get_purged_snaps";
  }
  void print(std::ostream& out) const override {
    out << "mon_get_purged_snaps([" << start << "," << last << "])";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(start, payload);
    encode(last, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(start, p);
    decode(last, p);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
