// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "messages/MOSDPeeringOp.h"
#include "osd/PGPeeringEvent.h"

class MOSDPGQuery2 : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t spgid;
  pg_query_t query;

  spg_t get_spg() const override {
    return spgid;
  }
  epoch_t get_map_epoch() const override {
    return query.epoch_sent;
  }
  epoch_t get_min_epoch() const override {
    return query.epoch_sent;
  }

  PGPeeringEvent *get_event() override {
    return new PGPeeringEvent(
      query.epoch_sent,
      query.epoch_sent,
      MQuery(
	spgid,
	pg_shard_t(get_source().num(), query.from),
	query,
	query.epoch_sent),
      false);
  }

  MOSDPGQuery2() : MOSDPeeringOp{MSG_OSD_PG_QUERY2,
				  HEAD_VERSION, COMPAT_VERSION} {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGQuery2(
    spg_t s,
    pg_query_t q)
    : MOSDPeeringOp{MSG_OSD_PG_QUERY2, HEAD_VERSION, COMPAT_VERSION},
      spgid(s),
      query(q) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

private:
  ~MOSDPGQuery2() override {}

public:
  std::string_view get_type_name() const override {
    return "pg_query2";
  }
  void inner_print(std::ostream& out) const override {
    out << spgid << " " << query;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(spgid, payload);
    encode(query, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(spgid, p);
    decode(query, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
