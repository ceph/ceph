// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "osd/osd_types_core.h"

/*
 * summarize pg contents for purposes of a scrub
 *
 * If members are added to ScrubMap, make sure to modify swap().
 */
struct ScrubMap {
  struct object {
    std::map<std::string, ceph::buffer::list, std::less<>> attrs;
    uint64_t size;
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1;
    bool read_error:1;
    bool stat_error:1;
    bool ec_hash_mismatch:1;
    bool ec_size_mismatch:1;
    bool large_omap_object_found:1;
    uint64_t large_omap_object_key_count = 0;
    uint64_t large_omap_object_value_size = 0;
    uint64_t object_omap_bytes = 0;
    uint64_t object_omap_keys = 0;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0),
      negative(false), digest_present(false), omap_digest_present(false),
      read_error(false), stat_error(false), ec_hash_mismatch(false),
      ec_size_mismatch(false), large_omap_object_found(false) {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static std::list<object> generate_test_instances();
  };
  WRITE_CLASS_ENCODER(object)

  std::map<hobject_t,object> objects;
  eversion_t valid_through;
  eversion_t incr_since;
  bool has_large_omap_object_errors{false};
  bool has_omap_keys{false};

  void merge_incr(const ScrubMap &l);
  void clear_from(const hobject_t& start) {
    objects.erase(objects.lower_bound(start), objects.end());
  }
  void insert(const ScrubMap &r) {
    objects.insert(r.objects.begin(), r.objects.end());
  }
  void swap(ScrubMap &r) {
    using std::swap;
    swap(objects, r.objects);
    swap(valid_through, r.valid_through);
    swap(incr_since, r.incr_since);
    swap(has_large_omap_object_errors, r.has_large_omap_object_errors);
    swap(has_omap_keys, r.has_omap_keys);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl, int64_t pool=-1);
  void dump(ceph::Formatter *f) const;
  static std::list<ScrubMap> generate_test_instances();
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)

struct ScrubMapBuilder {
  bool deep = false;
  std::vector<hobject_t> ls;
  bool metadata_done = false;
  size_t pos = 0;
  int64_t data_pos = 0;
  std::string omap_pos;
  int ret = 0;
  ceph::buffer::hash data_hash;  ///< accumulating hash value
  uint32_t omap_hash;
  uint64_t omap_keys = 0;
  uint64_t omap_bytes = 0;

  bool empty() {
    return ls.empty();
  }
  bool done() {
    return pos >= ls.size();
  }
  void reset() {
    *this = ScrubMapBuilder();
  }

  bool data_done() {
    return data_pos < 0;
  }

  void next_object() {
    ++pos;
    metadata_done = false;
    data_pos = 0;
    omap_pos.clear();
    omap_keys = 0;
    omap_bytes = 0;
  }

  std::string fmt_print() const;

  friend std::ostream& operator<<(std::ostream& out, const ScrubMapBuilder& pos);
};
