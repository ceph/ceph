// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class MOSDPGReadyToMerge
  : public MessageInstance<MOSDPGReadyToMerge, PaxosServiceMessage> {
public:
  pg_t pgid;
  eversion_t source_version, target_version;
  epoch_t last_epoch_started = 0;
  epoch_t last_epoch_clean = 0;
  bool ready = true;

  MOSDPGReadyToMerge()
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, 0)
  {}
  MOSDPGReadyToMerge(pg_t p, eversion_t sv, eversion_t tv,
		     epoch_t les, epoch_t lec, bool r, epoch_t v)
    : MessageInstance(MSG_OSD_PG_READY_TO_MERGE, v),
      pgid(p),
      source_version(sv),
      target_version(tv),
      last_epoch_started(les),
      last_epoch_clean(lec),
      ready(r)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgid, payload);
    encode(source_version, payload);
    encode(target_version, payload);
    encode(last_epoch_started, payload);
    encode(last_epoch_clean, payload);
    encode(ready, payload);
  }
  void decode_payload() override {
    bufferlist::const_iterator p = payload.begin();
    paxos_decode(p);
    decode(pgid, p);
    decode(source_version, p);
    decode(target_version, p);
    decode(last_epoch_started, p);
    decode(last_epoch_clean, p);
    decode(ready, p);
  }
  std::string_view get_type_name() const override { return "osd_pg_ready_to_merge"; }
  void print(ostream &out) const {
    out << get_type_name()
        << "(" << pgid
	<< " sv " << source_version
	<< " tv " << target_version
	<< " les/c " << last_epoch_started << "/" << last_epoch_clean
	<< (ready ? " ready" : " NOT READY")
        << " v" << version << ")";
  }
};
