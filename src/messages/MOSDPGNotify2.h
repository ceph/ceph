// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "messages/MOSDPeeringOp.h"
#include "osd/PGPeeringEvent.h"

class MOSDPGNotify2 final : public MOSDPeeringOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t spgid;
  pg_notify_t notify;
  std::optional<backfill_osd_space_usage_t> osd_space_usage;

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
#ifdef WITH_CRIMSON
	features
#else
	get_connection()->get_features()
#endif
	,
	osd_space_usage
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
    pg_notify_t n,
    std::optional<backfill_osd_space_usage_t> osd_space_usage = {})
    : MOSDPeeringOp{MSG_OSD_PG_NOTIFY2, HEAD_VERSION, COMPAT_VERSION},
      spgid(s),
      notify(n),
      osd_space_usage(osd_space_usage) {
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
    encode(osd_space_usage, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(spgid, p);
    decode(notify, p);
    if (header.version >= 2) {
      decode(osd_space_usage, p);
    } else {
      osd_space_usage = std::nullopt;
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
