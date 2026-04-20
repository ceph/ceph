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

#include "mon/mon_feature_t.h"

#include <ostream>

#include "common/bit_str.h" // for print_bit_str()
#include "common/ceph_releases.h"
#include "common/Formatter.h"

void mon_feature_t::print(std::ostream& out) const {
  out << "[";
  print_bit_str(features, out, ceph::features::mon::get_feature_name);
  out << "]";
}

void mon_feature_t::print_with_value(std::ostream& out) const {
  out << "[";
  print_bit_str(features, out, ceph::features::mon::get_feature_name, true);
  out << "]";
}

void mon_feature_t::dump(ceph::Formatter *f, const char *sec_name) const {
  f->open_array_section((sec_name ? sec_name : "features"));
  dump_bit_str(features, f, ceph::features::mon::get_feature_name);
  f->close_section();
}

void mon_feature_t::dump_with_value(ceph::Formatter *f, const char *sec_name) const {
  f->open_array_section((sec_name ? sec_name : "features"));
  dump_bit_str(features, f, ceph::features::mon::get_feature_name, true);
  f->close_section();
}

void mon_feature_t::encode(ceph::buffer::list& bl) const {
  ENCODE_START(HEAD_VERSION, COMPAT_VERSION, bl);
  encode(features, bl);
  ENCODE_FINISH(bl);
}

void mon_feature_t::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(COMPAT_VERSION, p);
  decode(features, p);
  DECODE_FINISH(p);
}

std::list<mon_feature_t> mon_feature_t::generate_test_instances() {
  std::list<mon_feature_t> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().features = 1;
  ls.emplace_back();
  ls.back().features = 2;
  return ls;
}

ceph_release_t infer_ceph_release_from_mon_features(mon_feature_t f)
{
  if (f.contains_all(ceph::features::mon::FEATURE_UMBRELLA)) {
    return ceph_release_t::umbrella;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_TENTACLE)) {
    return ceph_release_t::tentacle;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_SQUID)) {
    return ceph_release_t::squid;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_REEF)) {
    return ceph_release_t::reef;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_QUINCY)) {
    return ceph_release_t::quincy;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_PACIFIC)) {
    return ceph_release_t::pacific;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_OCTOPUS)) {
    return ceph_release_t::octopus;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
    return ceph_release_t::nautilus;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_MIMIC)) {
    return ceph_release_t::mimic;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_LUMINOUS)) {
    return ceph_release_t::luminous;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_KRAKEN)) {
    return ceph_release_t::kraken;
  }
  return ceph_release_t::unknown;
}

const char *ceph::features::mon::get_feature_name(uint64_t b) {
  mon_feature_t f(b);

  if (f == FEATURE_KRAKEN) {
    return "kraken";
  } else if (f == FEATURE_LUMINOUS) {
    return "luminous";
  } else if (f == FEATURE_MIMIC) {
    return "mimic";
  } else if (f == FEATURE_OSDMAP_PRUNE) {
    return "osdmap-prune";
  } else if (f == FEATURE_NAUTILUS) {
    return "nautilus";
  } else if (f == FEATURE_PINGING) {
    return "elector-pinging";
  } else if (f == FEATURE_OCTOPUS) {
    return "octopus";
  } else if (f == FEATURE_PACIFIC) {
    return "pacific";
  } else if (f == FEATURE_QUINCY) {
    return "quincy";
  } else if (f == FEATURE_REEF) {
    return "reef";
  } else if (f == FEATURE_SQUID) {
    return "squid";
  } else if (f == FEATURE_TENTACLE) {
    return "tentacle";
  } else if (f == FEATURE_UMBRELLA) {
    return "umbrella";
  // Release-independent features
  } else if (f == FEATURE_NVMEOF_BEACON_DIFF) {
    return "nvmeof_beacon_diff";
  } else if (f == FEATURE_RESERVED) {
    return "reserved";
  }
  return "unknown";
}

mon_feature_t ceph::features::mon::get_feature_by_name(const std::string &n) {

  if (n == "kraken") {
    return FEATURE_KRAKEN;
  } else if (n == "luminous") {
    return FEATURE_LUMINOUS;
  } else if (n == "mimic") {
    return FEATURE_MIMIC;
  } else if (n == "osdmap-prune") {
    return FEATURE_OSDMAP_PRUNE;
  } else if (n == "nautilus") {
    return FEATURE_NAUTILUS;
  } else if (n == "feature-pinging") {
    return FEATURE_PINGING;
  } else if (n == "octopus") {
    return FEATURE_OCTOPUS;
  } else if (n == "pacific") {
    return FEATURE_PACIFIC;
  } else if (n == "quincy") {
    return FEATURE_QUINCY;
  } else if (n == "reef") {
    return FEATURE_REEF;
  } else if (n == "squid") {
    return FEATURE_SQUID;
  } else if (n == "tentacle") {
    return FEATURE_TENTACLE;
  } else if (n == "umbrella") {
    return FEATURE_UMBRELLA;
  // Release-independent features
  } else if (n == "nvmeof_beacon_diff") {
    return FEATURE_NVMEOF_BEACON_DIFF;
  } else if (n == "reserved") {
    return FEATURE_RESERVED;
  }
  return FEATURE_NONE;
}

std::ostream& operator<<(std::ostream& out, const mon_feature_t& f) {
  out << "mon_feature_t(";
  f.print(out);
  out << ")";
  return out;
}
