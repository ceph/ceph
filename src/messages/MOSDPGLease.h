// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGLease : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  epoch_t epoch = 0;
  spg_t spgid;
  pg_lease_t lease;

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
      MLease(epoch, get_source().num(), lease));
  }

  MOSDPGLease() : MOSDPeeringOp{MSG_OSD_PG_LEASE,
				HEAD_VERSION, COMPAT_VERSION} {}
  MOSDPGLease(version_t mv, spg_t p, pg_lease_t lease) :
    MOSDPeeringOp{MSG_OSD_PG_LEASE,
		  HEAD_VERSION, COMPAT_VERSION},
    epoch(mv),
    spgid(p),
    lease(lease) { }
private:
  ~MOSDPGLease() override {}

public:
  std::string_view get_type_name() const override { return "pg_lease"; }
  void inner_print(ostream& out) const override {
    out << lease;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(spgid, payload);
    encode(lease, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(spgid, p);
    decode(lease, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
