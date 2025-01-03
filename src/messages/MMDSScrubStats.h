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
  static constexpr int HEAD_VERSION = 2;
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
  const std::unordered_map<std::string, std::unordered_map<int, std::vector<_inodeno_t>>>& get_uninline_failed_meta_info() const {
    return uninline_failed_meta_info;
  }
  const std::unordered_map<_inodeno_t, std::string>& get_paths() const {
    return paths;
  }
  const std::unordered_map<std::string, std::vector<uint64_t>>& get_counters() const {
    return counters;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(scrubbing_tags, payload);
    encode(update_scrubbing, payload);
    encode(aborting, payload);
    encode_uninline_failed_info();
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(scrubbing_tags, p);
    decode(update_scrubbing, p);
    decode(aborting, p);
    if (header.version >= 2) {
      decode_uninline_failed_info(p);
    }
  }

  void encode_uninline_failed_info() {
    using ceph::encode;
    int count = (int)uninline_failed_meta_info.size();
    encode(count, payload);
    for (const auto& [tag, meta_info_map] : uninline_failed_meta_info) {
      encode(tag, payload);
      count = (int)meta_info_map.size();
      encode(count, payload);
      for (const auto& [error_code, ino_vec] : meta_info_map) {
	encode(error_code, payload);
	encode(ino_vec, payload);
      }
    }
    count = (int)paths.size();
    encode(count, payload);
    for (auto& [ino, path] : paths) {
      encode(ino, payload);
      encode(path, payload);
    }
    count = (int)counters.size();
    encode(count, payload);
    for (auto& [tag, v] : counters) {
      encode(tag, payload);
      uint64_t started = v[0];
      uint64_t passed = v[1];
      uint64_t failed = v[2];
      uint64_t skipped = v[3];

      encode(started, payload);
      encode(passed, payload);
      encode(failed, payload);
      encode(skipped, payload);
    }
  }
  void decode_uninline_failed_info(ceph::bufferlist::const_iterator& p) {
    using ceph::decode;
    int tag_count = 0;
    decode(tag_count, p);
    while (tag_count--) {
      std::string tag;
      decode(tag, p);
      int count = 0;
      decode(count, p);
      std::unordered_map<int, std::vector<_inodeno_t>> uninline_failed_info;
      while (count--) {
	int error_code;
	std::vector<_inodeno_t> ino_vec;
	decode(error_code, p);
	decode(ino_vec, p);
	uninline_failed_info[error_code] = std::move(ino_vec);
      }
      uninline_failed_meta_info[tag] = std::move(uninline_failed_info);
    }
    int count = 0;
    decode(count, p);
    while (count--) {
      _inodeno_t ino;
      std::string path;
      decode(ino, p);
      decode(path, p);
      paths[ino] = path;
    }
    count = 0;
    decode(count, p);
    while (count--) {
      std::string tag;
      decode(tag, p);
      uint64_t started = 0;
      uint64_t passed = 0;
      uint64_t failed = 0;
      uint64_t skipped = 0;

      decode(started, p);
      decode(passed, p);
      decode(failed, p);
      decode(skipped, p);
      std::vector<uint64_t> c{started, passed, failed, skipped};
      counters[tag] = c;
    }
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
  MMDSScrubStats(unsigned e, const std::set<std::string>& tags,
    std::unordered_map<std::string, std::unordered_map<int, std::vector<_inodeno_t>>>&& ufmi,
    std::unordered_map<_inodeno_t, std::string>&& paths_,
    std::unordered_map<std::string, std::vector<uint64_t>>&& counters_,
    bool abrt = false) :
    MMDSOp(MSG_MDS_SCRUB_STATS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e), scrubbing_tags(tags), update_scrubbing(true), aborting(abrt),
    uninline_failed_meta_info(std::move(ufmi)), paths(std::move(paths_)),
    counters(std::move(counters_)) {}
  ~MMDSScrubStats() override {}

private:
  uint32_t epoch;
  std::set<std::string> scrubbing_tags;
  bool update_scrubbing = false;
  bool aborting = false;
  // <tag, <error_code, [ino1, ino2, ...]>>
  std::unordered_map<std::string, std::unordered_map<int, std::vector<_inodeno_t>>> uninline_failed_meta_info;
  std::unordered_map<_inodeno_t, std::string> paths;
  std::unordered_map<std::string, std::vector<uint64_t>> counters;

  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
