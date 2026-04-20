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

#include "mon/ScrubResult.h"

#include <ostream>

#include "include/container_ios.h"
#include "include/encoding_map.h"
#include "include/encoding_string.h"
#include "common/Formatter.h"

void ScrubResult::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(prefix_crc, bl);
  encode(prefix_keys, bl);
  ENCODE_FINISH(bl);
}

void ScrubResult::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(prefix_crc, p);
  decode(prefix_keys, p);
  DECODE_FINISH(p);
}

void ScrubResult::dump(ceph::Formatter *f) const {
  f->open_object_section("crc");
  for (auto p = prefix_crc.begin(); p != prefix_crc.end(); ++p)
    f->dump_unsigned(p->first.c_str(), p->second);
  f->close_section();
  f->open_object_section("keys");
  for (auto p = prefix_keys.begin(); p != prefix_keys.end(); ++p)
    f->dump_unsigned(p->first.c_str(), p->second);
  f->close_section();
}

std::list<ScrubResult> ScrubResult::generate_test_instances() {
  std::list<ScrubResult> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().prefix_crc["foo"] = 123;
  ls.back().prefix_keys["bar"] = 456;
  return ls;
}

std::ostream& operator<<(std::ostream& out, const ScrubResult& r) {
  return out << "ScrubResult(keys " << r.prefix_keys << " crc " << r.prefix_crc << ")";
}
