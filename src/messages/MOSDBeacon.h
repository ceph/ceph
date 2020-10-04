// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "PaxosServiceMessage.h"

class MOSDBeacon : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;
public:
  std::vector<pg_t> pgs;
  epoch_t min_last_epoch_clean = 0;
  utime_t last_purged_snaps_scrub;
  int osd_beacon_report_interval = 0;

  MOSDBeacon()
    : PaxosServiceMessage{MSG_OSD_BEACON, 0,
			  HEAD_VERSION, COMPAT_VERSION}
  {}
  MOSDBeacon(epoch_t e, epoch_t min_lec, utime_t ls, int interval)
    : PaxosServiceMessage{MSG_OSD_BEACON, e,
			  HEAD_VERSION, COMPAT_VERSION},
      min_last_epoch_clean(min_lec),
      last_purged_snaps_scrub(ls),
      osd_beacon_report_interval(interval)
  {}
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(pgs, payload);
    encode(min_last_epoch_clean, payload);
    encode(last_purged_snaps_scrub, payload);
    encode(osd_beacon_report_interval, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    using ceph::decode;
    paxos_decode(p);
    decode(pgs, p);
    decode(min_last_epoch_clean, p);
    if (header.version >= 2) {
      decode(last_purged_snaps_scrub, p);
    }
    if (header.version >= 3) {
      decode(osd_beacon_report_interval, p);
    } else {
      osd_beacon_report_interval = 0;
    }
  }
  std::string_view get_type_name() const override { return "osd_beacon"; }
  void print(std::ostream &out) const {
    out << get_type_name()
        << "(pgs " << pgs
        << " lec " << min_last_epoch_clean
        << " last_purged_snaps_scrub " << last_purged_snaps_scrub
        << " osd_beacon_report_interval " << osd_beacon_report_interval
        << " v" << version << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
