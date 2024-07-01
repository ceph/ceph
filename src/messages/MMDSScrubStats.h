// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MMDSSCRUBSTATS_H
#define CEPH_MMDSSCRUBSTATS_H

#include "messages/MMDSOp.h"

class MMDSScrubStats : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  std::string_view get_type_name() const override { return "mds_scrub_stats"; }
  void print(std::ostream& o) const override {
    o << "mds_scrub_stats(e" << epoch;
    if (update_scrubbing)
      o << " [" << scrubbing_tags << "]";
    if (aborting)
      o << " aborting";
    o << ")";
  }

  unsigned get_epoch() const { return epoch; }
  const auto& get_scrubbing_tags() const { return scrubbing_tags; }
  bool is_aborting() const { return aborting; }
  bool is_finished(const std::string& tag) const {
    return update_scrubbing && !scrubbing_tags.count(tag);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(scrubbing_tags, payload);
    encode(update_scrubbing, payload);
    encode(aborting, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(scrubbing_tags, p);
    decode(update_scrubbing, p);
    decode(aborting, p);
  }

protected:
  MMDSScrubStats(unsigned e=0) :
    MMDSOp(MSG_MDS_SCRUB_STATS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e) {}
  MMDSScrubStats(unsigned e, std::set<std::string>&& tags, bool abrt=false) :
    MMDSOp(MSG_MDS_SCRUB_STATS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e), scrubbing_tags(std::move(tags)), update_scrubbing(true), aborting(abrt) {}
  MMDSScrubStats(unsigned e, const std::set<std::string>& tags, bool abrt=false) :
    MMDSOp(MSG_MDS_SCRUB_STATS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e), scrubbing_tags(tags), update_scrubbing(true), aborting(abrt) {}
  ~MMDSScrubStats() override {}

private:
  uint32_t epoch;
  std::set<std::string> scrubbing_tags;
  bool update_scrubbing = false;
  bool aborting = false;

  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
