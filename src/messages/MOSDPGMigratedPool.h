// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "messages/PaxosServiceMessage.h"

class MOSDPGMigratedPool : public PaxosServiceMessage {
public:
  int64_t migration_target;
  pg_t pgid;

  MOSDPGMigratedPool(epoch_t e, int64_t migration_target, pg_t p)
    : PaxosServiceMessage{MSG_OSD_PG_MIGRATED_POOL, e},
      migration_target(migration_target),
      pgid(p)
  {}
  MOSDPGMigratedPool()
    : MOSDPGMigratedPool(0, 0, pg_t())
  {}
private:
  ~MOSDPGMigratedPool() final {}

public:
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(migration_target, payload);
    encode(pgid, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(migration_target, p);
    decode(pgid, p);
  }
  std::string_view get_type_name() const override { return "osd_pg_migrate_pool"; }
  void print(std::ostream &out) const {
    out << get_type_name()
	<< "( migration_target " << migration_target << " pg " << pgid << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
