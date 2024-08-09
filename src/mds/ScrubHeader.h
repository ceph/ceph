// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

#include <memory>
#include <string>
#include <string_view>

#include "include/ceph_assert.h"

namespace ceph {
class Formatter;
};

class CInode;
class MDCache;

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 */
class ScrubHeader {
public:
  ScrubHeader(std::string_view tag_, bool is_tag_internal_, bool force_,
              bool recursive_, bool repair_, bool scrub_mdsdir_ = false)
    : tag(tag_), is_tag_internal(is_tag_internal_), force(force_),
      recursive(recursive_), repair(repair_), scrub_mdsdir(scrub_mdsdir_) {}

  // Set after construction because it won't be known until we've
  // started resolving path and locking
  void set_origin(inodeno_t ino) { origin = ino; }

  bool get_recursive() const { return recursive; }
  bool get_repair() const { return repair; }
  bool get_force() const { return force; }
  bool get_scrub_mdsdir() const { return scrub_mdsdir; }
  bool is_internal_tag() const { return is_tag_internal; }
  inodeno_t get_origin() const { return origin; }
  const std::string& get_tag() const { return tag; }

  bool get_repaired() const { return repaired; }
  void set_repaired() { repaired = true; }

  void set_epoch_last_forwarded(unsigned epoch) { epoch_last_forwarded = epoch; }
  unsigned get_epoch_last_forwarded() const { return epoch_last_forwarded; }

  void inc_num_pending() { ++num_pending; }
  void dec_num_pending() {
    ceph_assert(num_pending > 0);
    --num_pending;
  }
  unsigned get_num_pending() const { return num_pending; }

  void record_uninline_status(_inodeno_t ino, int e, std::string_view path) {
    if (uninline_failed_info.find(e) == uninline_failed_info.end()) {
      uninline_failed_info[e] = std::vector<_inodeno_t>();
    }
    auto& v = uninline_failed_info.at(e);
    v.push_back(ino);
    paths[ino] = path;
  }

  std::unordered_map<int, std::vector<_inodeno_t>>& get_uninline_failed_info() {
    return uninline_failed_info;
  }

  std::unordered_map<_inodeno_t, std::string>& get_paths() {
    return paths;
  }

  void record_uninline_started() {
    uninline_started++;
  }
  void record_uninline_passed() {
    uninline_passed++;
  }
  void record_uninline_failed() {
    uninline_failed++;
  }
  void record_uninline_skipped() {
    uninline_skipped++;
  }
  uint64_t get_uninline_started() const {
    return uninline_started;
  }
  uint64_t get_uninline_passed() const {
    return uninline_passed;
  }
  uint64_t get_uninline_failed() const {
    return uninline_failed;
  }
  uint64_t get_uninline_skipped() const {
    return uninline_skipped;
  }

protected:
  const std::string tag;
  bool is_tag_internal;
  const bool force;
  const bool recursive;
  const bool repair;
  const bool scrub_mdsdir;
  inodeno_t origin;

  bool repaired = false;  // May be set during scrub if repairs happened
  unsigned epoch_last_forwarded = 0;
  unsigned num_pending = 0;
  // errno -> [ino1, ino2, ino3, ...]
  std::unordered_map<int, std::vector<_inodeno_t>> uninline_failed_info;
  std::unordered_map<_inodeno_t, std::string> paths;

  // scrub counters
  uint64_t uninline_started = 0;
  uint64_t uninline_passed = 0;
  uint64_t uninline_failed = 0;
  uint64_t uninline_skipped = 0;
};

typedef std::shared_ptr<ScrubHeader> ScrubHeaderRef;
typedef std::shared_ptr<const ScrubHeader> ScrubHeaderRefConst;

#endif // SCRUB_HEADER_H_
