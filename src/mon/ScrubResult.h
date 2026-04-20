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

#ifndef CEPH_MON_SCRUB_RESULT_H
#define CEPH_MON_SCRUB_RESULT_H

#include <cstdint>
#include <iosfwd>
#include <list>
#include <map>
#include <string>

#include "include/encoding.h"

namespace ceph { class Formatter; }

struct ScrubResult {
  std::map<std::string,uint32_t> prefix_crc;  ///< prefix -> crc
  std::map<std::string,uint64_t> prefix_keys; ///< prefix -> key count

  bool operator!=(const ScrubResult& other) {
    return prefix_crc != other.prefix_crc || prefix_keys != other.prefix_keys;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static std::list<ScrubResult> generate_test_instances();
};
WRITE_CLASS_ENCODER(ScrubResult)

std::ostream& operator<<(std::ostream& out, const ScrubResult& r);

#endif
