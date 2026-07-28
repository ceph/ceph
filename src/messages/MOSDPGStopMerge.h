// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "osd/osd_types.h"
#include "messages/PaxosServiceMessage.h"

/// OSD -> mon: permanently stop PG merge shrink for a Crimson pool.
class MOSDPGStopMerge : public PaxosServiceMessage {
public:
  static constexpr uint8_t REASON_CROSS_SHARD = 1;

  int64_t pool = -1;
  pg_t pgid;
  uint8_t reason = REASON_CROSS_SHARD;

  MOSDPGStopMerge()
    : PaxosServiceMessage{MSG_OSD_PG_STOP_MERGE, 0}
  {}
  MOSDPGStopMerge(int64_t pool, pg_t pgid, uint8_t reason, epoch_t epoch)
    : PaxosServiceMessage{MSG_OSD_PG_STOP_MERGE, epoch},
      pool(pool),
      pgid(pgid),
      reason(reason)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pool, payload);
    encode(pgid, payload);
    encode(reason, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(pool, p);
    decode(pgid, p);
    decode(reason, p);
  }
  std::string_view get_type_name() const override { return "osd_pg_stop_merge"; }
  void print(std::ostream &out) const {
    out << get_type_name()
        << "(pool " << pool
        << " pg " << pgid
        << " reason " << (unsigned)reason
        << " v" << version << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
