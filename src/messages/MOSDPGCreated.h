// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_types.h"
#include "messages/PaxosServiceMessage.h"

class MOSDPGCreated : public MessageInstance<MOSDPGCreated, PaxosServiceMessage> {
public:
  friend factory;

  pg_t pgid;
  MOSDPGCreated()
    : MessageInstance(MSG_OSD_PG_CREATED, 0)
  {}
  MOSDPGCreated(pg_t pgid)
    : MessageInstance(MSG_OSD_PG_CREATED, 0),
      pgid(pgid)
  {}
  std::string_view get_type_name() const override { return "pg_created"; }
  void print(ostream& out) const override {
    out << "osd_pg_created(" << pgid << ")";
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgid, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(pgid, p);
  }
};
