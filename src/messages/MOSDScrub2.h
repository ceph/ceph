// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/Message.h"

/*
 * instruct an OSD to scrub some or all pg(s)
 */

class MOSDScrub2 : public MessageInstance<MOSDScrub2> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  uuid_d fsid;
  epoch_t epoch;
  vector<spg_t> scrub_pgs;
  bool repair = false;
  bool deep = false;

  MOSDScrub2() : MessageInstance(MSG_OSD_SCRUB2, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDScrub2(const uuid_d& f, epoch_t e, vector<spg_t>& pgs, bool r, bool d) :
    MessageInstance(MSG_OSD_SCRUB2, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), epoch(e), scrub_pgs(pgs), repair(r), deep(d) {}
private:
  ~MOSDScrub2() override {}

public:
  std::string_view get_type_name() const override { return "scrub2"; }
  void print(ostream& out) const override {
    out << "scrub2(" << scrub_pgs;
    if (repair)
      out << " repair";
    if (deep)
      out << " deep";
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(epoch, payload);
    encode(scrub_pgs, payload);
    encode(repair, payload);
    encode(deep, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(epoch, p);
    decode(scrub_pgs, p);
    decode(repair, p);
    decode(deep, p);
  }
};
