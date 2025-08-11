// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/CompatSet.h"

#include <iostream>

#include "common/Formatter.h"
#include "include/types.h"

void CompatSet::FeatureSet::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  /* See below, mask always has the lowest bit set in memory, but
   * unset in the encoding */
  encode(mask & (~(uint64_t)1), bl);
  encode(names, bl);
}

void CompatSet::FeatureSet::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  decode(mask, bl);
  decode(names, bl);
  /**
   * Previously, there was a bug where insert did
   * mask |= f.id rather than mask |= (1 << f.id).
   * In FeatureSets from those version, mask always
   * has the lowest bit set.  Since then, masks always
   * have the lowest bit unset.
   *
   * When we encounter such a FeatureSet, we have to
   * reconstruct the mask from the names map.
   */
  if (mask & 1) {
    mask = 1;
    std::map<uint64_t, std::string> temp_names;
    temp_names.swap(names);
    for (auto i = temp_names.begin(); i != temp_names.end(); ++i) {
      insert(Feature(i->first, i->second));
    }
  } else {
    mask |= 1;
  }
}

void CompatSet::FeatureSet::dump(ceph::Formatter *f) const {
  for (auto p = names.cbegin(); p != names.cend(); ++p) {
    char s[18];
    snprintf(s, sizeof(s), "feature_%llu", (unsigned long long)p->first);
    f->dump_string(s, p->second);
  }
}

std::ostream& CompatSet::printlite(std::ostream& o) const {
  o << "{c=[" << std::hex << compat.mask << "]";
  o << ",r=[" << std::hex << ro_compat.mask << "]";
  o << ",i=[" << std::hex << incompat.mask << "]}";
  o << std::dec;
  return o;
}

void CompatSet::dump(ceph::Formatter *f) const {
  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();
  f->open_object_section("ro_compat");
  ro_compat.dump(f);
  f->close_section();
  f->open_object_section("incompat");
  incompat.dump(f);
  f->close_section();
}

void CompatSet::generate_test_instances(std::list<CompatSet*>& o) {
  o.push_back(new CompatSet);
  o.push_back(new CompatSet);
  o.back()->compat.insert(Feature(1, "one"));
  o.back()->compat.insert(Feature(2, "two"));
  o.back()->ro_compat.insert(Feature(4, "four"));
  o.back()->incompat.insert(Feature(3, "three"));
}

std::ostream& operator<<(std::ostream& out, const CompatSet::Feature& f)
{
  return out << "F(" << f.id << ", \"" << f.name << "\")";
}

std::ostream& operator<<(std::ostream& out, const CompatSet::FeatureSet& fs)
{
  return out << fs.names;
}

std::ostream& operator<<(std::ostream& out, const CompatSet& compat)
{
  return out << "compat=" << compat.compat
	     << ",rocompat=" << compat.ro_compat
	     << ",incompat=" << compat.incompat;
}
