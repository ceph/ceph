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

#ifndef CEPH_MON_FEATURE_MAP_H
#define CEPH_MON_FEATURE_MAP_H

#include <cstdint>
#include <list>
#include <map>

#include "include/encoding.h"

namespace ceph { class Formatter; }

// map of entity_type -> features -> count
struct FeatureMap {
  std::map<uint32_t,std::map<uint64_t,uint64_t>> m;

  void add(uint32_t type, uint64_t features);
  void add_mon(uint64_t features);
  void rm(uint32_t type, uint64_t features);

  FeatureMap& operator+=(const FeatureMap& o);

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void dump(ceph::Formatter *f) const;

  static std::list<FeatureMap> generate_test_instances();
};
WRITE_CLASS_ENCODER(FeatureMap)

#endif
