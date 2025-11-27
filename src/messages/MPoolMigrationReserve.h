// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "messages/MOSDPeeringOp.h"
#include "osd/PGPeeringEvent.h"

class MPoolMigrationReserve : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
public:
  spg_t pgid;
  epoch_t query_epoch;
  enum {
    REQUEST = 0,         // primary->replica: please reserve a slot
    GRANT = 1,           // replica->primary: ok, i reserved it
    REJECT_TOOFULL = 2,  // replica->primary: too full, sorry, try again later
    RELEASE = 3,         // primary->replcia: release the slot i reserved before
    REVOKE_TOOFULL = 4,  // replica->primary: too full, stop backfilling
    REVOKE = 5,          // replica->primary: i'm taking back the slot i gave you
  };
  uint32_t type;
  uint32_t priority;
  int64_t source_num_bytes;
  int64_t source_num_objects;

  spg_t get_spg() const {
    return pgid;
  }
  epoch_t get_map_epoch() const {
    return query_epoch;
  }
  epoch_t get_min_epoch() const {
    return query_epoch;
  }

  PGPeeringEvent *get_event() override {
    switch (type) {
    case REQUEST:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationRequest(priority, source_num_bytes, source_num_objects));
    case GRANT:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationReserved());
    case REJECT_TOOFULL:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationRejectedTooFull());
    case RELEASE:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationReservationCanceled());
    case REVOKE_TOOFULL:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationRevokedTooFull());
    case REVOKE:
      return new PGPeeringEvent(
	query_epoch,
	query_epoch,
	RemotePoolMigrationRevoked());
    default:
      ceph_abort();
    }
  }

  MPoolMigrationReserve()
    : MOSDPeeringOp{MSG_OSD_POOLMIGRATION_RESERVE, HEAD_VERSION, COMPAT_VERSION},
      query_epoch(0), type(-1), priority(-1), source_num_bytes(0),
      source_num_objects(0) {}
  MPoolMigrationReserve(int type,
			spg_t pgid,
			epoch_t query_epoch,
			unsigned prio = -1,
			int64_t source_num_bytes = 0,
			int64_t source_num_objects = 0)
    : MOSDPeeringOp{MSG_OSD_POOLMIGRATION_RESERVE, HEAD_VERSION, COMPAT_VERSION},
      pgid(pgid), query_epoch(query_epoch),
      type(type), priority(prio), source_num_bytes(source_num_bytes),
      source_num_objects(source_num_objects) {}

  std::string_view get_type_name() const override {
    return "MPoolMigrationReserve";
  }

  void inner_print(std::ostream& out) const override {
    switch (type) {
    case REQUEST:
      out << "REQUEST";
      break;
    case GRANT:
      out << "GRANT";
      break;
    case REJECT_TOOFULL:
      out << "REJECT_TOOFULL";
      break;
    case RELEASE:
      out << "RELEASE";
      break;
    case REVOKE_TOOFULL:
      out << "REVOKE_TOOFULL";
      break;
    case REVOKE:
      out << "REVOKE";
      break;
    }
    if (type == REQUEST) out << " prio: " << priority;
    return;
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    decode(pgid.pgid, p);
    decode(query_epoch, p);
    decode(type, p);
    decode(priority, p);
    decode(pgid.shard, p);
    decode(source_num_bytes, p);
    decode(source_num_objects, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(pgid.pgid, payload);
    encode(query_epoch, payload);
    encode(type, payload);
    encode(priority, payload);
    encode(pgid.shard, payload);
    encode(source_num_bytes, payload);
    encode(source_num_objects, payload);
  }
};
