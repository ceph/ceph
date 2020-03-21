// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGLeaseAck : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  epoch_t epoch = 0;
  spg_t spgid;
  pg_lease_ack_t lease_ack;

public:
  spg_t get_spg() const {
    return spgid;
  }
  epoch_t get_map_epoch() const {
    return epoch;
  }
  epoch_t get_min_epoch() const {
    return epoch;
  }
  PGPeeringEvent *get_event() override {
    return new PGPeeringEvent(
      epoch,
      epoch,
      MLeaseAck(epoch, get_source().num(), lease_ack));
  }

  MOSDPGLeaseAck() : MOSDPeeringOp{MSG_OSD_PG_LEASE_ACK,
				   HEAD_VERSION, COMPAT_VERSION} {}
  MOSDPGLeaseAck(version_t mv, spg_t p, pg_lease_ack_t lease_ack) :
    MOSDPeeringOp{MSG_OSD_PG_LEASE_ACK,
		  HEAD_VERSION, COMPAT_VERSION},
    epoch(mv),
    spgid(p),
    lease_ack(lease_ack) { }
private:
  ~MOSDPGLeaseAck() override {}

public:
  std::string_view get_type_name() const override { return "pg_lease_ack"; }
  void inner_print(std::ostream& out) const override {
    out << lease_ack;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(spgid, payload);
    encode(lease_ack, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(spgid, p);
    decode(lease_ack, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
