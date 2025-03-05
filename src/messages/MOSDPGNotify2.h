// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "messages/MOSDPeeringOp.h"
#include "osd/PGPeeringEvent.h"

class MOSDPGNotify2 final : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t spgid;
  pg_notify_t notify;

  spg_t get_spg() const override {
    return spgid;
  }
  epoch_t get_map_epoch() const override {
    return notify.epoch_sent;
  }
  epoch_t get_min_epoch() const override {
    return notify.query_epoch;
  }

  PGPeeringEvent *get_event() override {
    return new PGPeeringEvent(
      notify.epoch_sent,
      notify.query_epoch,
      MNotifyRec(
	spgid,
	pg_shard_t(get_source().num(), notify.from),
	notify,
#ifdef WITH_SEASTAR
	features
#else
	get_connection()->get_features()
#endif
      ),
      true,
      new PGCreateInfo(
	spgid,
	notify.epoch_sent,
	notify.info.history,
	notify.past_intervals,
	false));
  }

  MOSDPGNotify2() : MOSDPeeringOp{MSG_OSD_PG_NOTIFY2,
				  HEAD_VERSION, COMPAT_VERSION} {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGNotify2(
    spg_t s,
    pg_notify_t n)
    : MOSDPeeringOp{MSG_OSD_PG_NOTIFY2, HEAD_VERSION, COMPAT_VERSION},
      spgid(s),
      notify(n) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

private:
  ~MOSDPGNotify2() final {}

public:
  std::string_view get_type_name() const override {
    return "pg_notify2";
  }
  void inner_print(std::ostream& out) const override {
    out << spgid << " " << notify;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(spgid, payload);
    encode(notify, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(spgid, p);
    decode(notify, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
