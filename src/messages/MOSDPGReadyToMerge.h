// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class MOSDPGReadyToMerge
  : public MessageInstance<MOSDPGReadyToMerge, PaxosServiceMessage> {
public:
  pg_t pgid;
  epoch_t last_epoch_started = 0;
  epoch_t last_epoch_clean = 0;
  bool ready = true;

  MOSDPGReadyToMerge()
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, 0)
  {}
  MOSDPGReadyToMerge(pg_t p, epoch_t les, epoch_t lec, bool r, epoch_t v)
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, v),
      pgid(p),
      last_epoch_started(les),
      last_epoch_clean(lec),
      ready(r)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgid, payload);
    encode(last_epoch_started, payload);
    encode(last_epoch_clean, payload);
    encode(ready, payload);
  }
  void decode_payload() override {
    bufferlist::const_iterator p = payload.begin();
    paxos_decode(p);
    decode(pgid, p);
    decode(last_epoch_started, p);
    decode(last_epoch_clean, p);
    decode(ready, p);
  }
  std::string_view get_type_name() const override { return "osd_pg_ready_to_merge"; }
  void print(ostream &out) const {
    out << get_type_name()
        << "(" << pgid
	<< " les/c " << last_epoch_started << "/" << last_epoch_clean
	<< (ready ? " ready" : " NOT READY")
        << " v" << version << ")";
  }
};
