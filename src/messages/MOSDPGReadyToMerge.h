// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class MOSDPGReadyToMerge
  : public MessageInstance<MOSDPGReadyToMerge, PaxosServiceMessage> {
public:
  pg_t pgid;

  MOSDPGReadyToMerge()
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, 0)
  {}
  MOSDPGReadyToMerge(pg_t p, epoch_t e)
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, e),
      pgid(p)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgid, payload);
  }
  void decode_payload() override {
    bufferlist::const_iterator p = payload.begin();
    paxos_decode(p);
    decode(pgid, p);
  }
  const char *get_type_name() const override { return "osd_pg_ready_to_merge"; }
  void print(ostream &out) const {
    out << get_type_name()
        << "(" << pgid
        << " v" << version << ")";
  }
};
