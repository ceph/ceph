// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "messages/MOSDPeeringOp.h"
#include "osd/PGPeeringEvent.h"

class MOSDPGInfo2 : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t spgid;
  epoch_t epoch_sent;
  epoch_t min_epoch;
  pg_info_t info;
  std::optional<pg_lease_t> lease;
  std::optional<pg_lease_ack_t> lease_ack;

  spg_t get_spg() const override {
    return spgid;
  }
  epoch_t get_map_epoch() const override {
    return epoch_sent;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }

  PGPeeringEvent *get_event() override {
    return new PGPeeringEvent(
      epoch_sent,
      min_epoch,
      MInfoRec(
	pg_shard_t(get_source().num(), info.pgid.shard),
	info,
	epoch_sent,
	lease,
	lease_ack));
  }

  MOSDPGInfo2() : MOSDPeeringOp{MSG_OSD_PG_INFO2,
				  HEAD_VERSION, COMPAT_VERSION} {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGInfo2(
    spg_t s,
    pg_info_t q,
    epoch_t sent,
    epoch_t min,
    std::optional<pg_lease_t> l,
    std::optional<pg_lease_ack_t> la)
    : MOSDPeeringOp{MSG_OSD_PG_INFO2, HEAD_VERSION, COMPAT_VERSION},
      spgid(s),
      epoch_sent(sent),
      min_epoch(min),
      info(q),
      lease(l),
      lease_ack(la) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

private:
  ~MOSDPGInfo2() override {}

public:
  std::string_view get_type_name() const override {
    return "pg_info2";
  }
  void inner_print(std::ostream& out) const override {
    out << spgid << " " << info;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(spgid, payload);
    encode(epoch_sent, payload);
    encode(min_epoch, payload);
    encode(info, payload);
    encode(lease, payload);
    encode(lease_ack, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(spgid, p);
    decode(epoch_sent, p);
    decode(min_epoch, p);
    decode(info, p);
    decode(lease, p);
    decode(lease_ack, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
