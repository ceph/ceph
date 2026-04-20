// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "mon/FeatureMap.h"

#include <cstdint>
#include <iomanip>
#include <sstream>

#include "include/ceph_features.h" // for CEPH_FEATURE_*
#include "include/encoding_map.h"
#include "include/msgr.h" //for CEPH_ENTITY_TYPE_*
#include "common/ceph_strings.h" // for ceph_release_from_features()
#include "common/Formatter.h"

void FeatureMap::add(uint32_t type, uint64_t features) {
  if (type == CEPH_ENTITY_TYPE_MON) {
    return;
  }
  m[type][features]++;
}

void FeatureMap::add_mon(uint64_t features) {
  m[CEPH_ENTITY_TYPE_MON][features]++;
}

void FeatureMap::rm(uint32_t type, uint64_t features) {
  if (type == CEPH_ENTITY_TYPE_MON) {
    return;
  }
  auto p = m.find(type);
  ceph_assert(p != m.end());
  auto q = p->second.find(features);
  ceph_assert(q != p->second.end());
  if (--q->second == 0) {
    p->second.erase(q);
    if (p->second.empty()) {
      m.erase(p);
    }
  }
}

FeatureMap& FeatureMap::operator+=(const FeatureMap& o) {
  for (auto& p : o.m) {
    auto &v = m[p.first];
    for (auto& q : p.second) {
      v[q.first] += q.second;
    }
  }
  return *this;
}

void FeatureMap::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(m, bl);
  ENCODE_FINISH(bl);
}

void FeatureMap::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(m, p);
  DECODE_FINISH(p);
}

void FeatureMap::dump(ceph::Formatter *f) const {
  for (auto& p : m) {
    f->open_array_section(ceph_entity_type_name(p.first));
    for (auto& q : p.second) {
      f->open_object_section("group");
      std::stringstream ss;
      ss << "0x" << std::hex << q.first << std::dec;
      f->dump_string("features", ss.str());
      f->dump_string("release", ceph_release_name(
                       ceph_release_from_features(q.first)));
      f->dump_unsigned("num", q.second);
      f->close_section();
    }
    f->close_section();
  }
}

std::list<FeatureMap> FeatureMap::generate_test_instances() {
  std::list<FeatureMap> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().add(CEPH_ENTITY_TYPE_OSD, CEPH_FEATURE_UID);
  ls.back().add(CEPH_ENTITY_TYPE_OSD, CEPH_FEATURE_NOSRCADDR);
  ls.back().add(CEPH_ENTITY_TYPE_OSD, CEPH_FEATURE_PGID64);
  ls.back().add(CEPH_ENTITY_TYPE_OSD, CEPH_FEATURE_INCSUBOSDMAP);
  return ls;
}
