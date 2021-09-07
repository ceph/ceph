// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOSDPGFORCEOBJECTMISSINGREPLY_H
#define CEPH_MOSDPGFORCEOBJECTMISSINGREPLY_H

#include "MOSDFastDispatchOp.h"

class MOSDPGForceObjectMissingReply final : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  epoch_t map_epoch = 0, min_epoch = 0;
  spg_t pgid;
  shard_id_t from;
  hobject_t oid;
  eversion_t oid_version;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGForceObjectMissingReply()
    : MOSDFastDispatchOp{MSG_OSD_PG_FORCE_OBJECT_MISSING_REPLY, HEAD_VERSION,
			 COMPAT_VERSION} {
  }
  MOSDPGForceObjectMissingReply(spg_t pgid, epoch_t epoch, epoch_t min_epoch,
                                const shard_id_t &from, const hobject_t &oid,
                                eversion_t version)
    : MOSDFastDispatchOp{MSG_OSD_PG_FORCE_OBJECT_MISSING_REPLY, HEAD_VERSION,
			 COMPAT_VERSION},
      map_epoch(epoch), min_epoch(min_epoch), pgid(pgid), from(from), oid(oid),
      oid_version(version) {
  }

private:
  ~MOSDPGForceObjectMissingReply() final {}

public:
  std::string_view get_type_name() const override {
    return "PGForceObjectMissingReply";
  }
  void print(std::ostream& out) const override {
    out << "pg_force_object_missing_reply(" << pgid << " epoch " << map_epoch
        << "/" << min_epoch << " from " << from << " oid " << oid << " version "
        << oid_version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(map_epoch, payload);
    encode(min_epoch, payload);
    encode(pgid, payload);
    encode(from, payload);
    encode(oid, payload);
    encode(oid_version, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(map_epoch, p);
    decode(min_epoch, p);
    decode(pgid, p);
    decode(from, p);
    decode(oid, p);
    decode(oid_version, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
