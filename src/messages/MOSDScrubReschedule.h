// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

/*
 * Cause an OSD to reschedule a scrub of some or all pg(s).
 * If 'immediate_start' is false - this rescheduling will involve updating
 * the relevant 'last scrub' timestamp to 'now' - i.e. creating
 * a 'clean slate', especially regading 'not scrubbed in time'
 * PGs.
 * An 'immediate_start' set to 'true' means adjusting the timestamp so
 * that the window for scheduling the next scrub starts immediately.
 */

class MOSDScrubReschedule final : public Message {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  uuid_d fsid;
  epoch_t epoch; // RRR ?
  std::vector<spg_t> scrub_pgs;
  bool immediate_start{true};
  bool deep{false};

  MOSDScrubReschedule() : Message{MSG_OSD_SCRUB2, HEAD_VERSION, COMPAT_VERSION} {}
  MOSDScrubReschedule(const uuid_d& f, epoch_t e, std::vector<spg_t>& pgs, bool df, bool dp) :
    Message{MSG_OSD_SCRUB2, HEAD_VERSION, COMPAT_VERSION},
    fsid(f), epoch(e), scrub_pgs(pgs), immediate_start(df), deep(dp) {}
private:
  ~MOSDScrubReschedule() final {}

public:
  std::string_view get_type_name() const override { return "scrub2"; }
  void print(std::ostream& out) const override {
    out << "scrub-reschedule(" << scrub_pgs;
    if (immediate_start)
      out << " immediate_start";
    if (deep)
      out << " deep";
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(epoch, payload);
    encode(scrub_pgs, payload);
    encode(immediate_start, payload);
    encode(deep, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(epoch, p);
    decode(scrub_pgs, p);
    decode(immediate_start, p);
    decode(deep, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
