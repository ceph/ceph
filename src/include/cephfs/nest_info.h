// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */
#ifndef CEPH_CEPHFS_NEST_INFO_H
#define CEPH_CEPHFS_NEST_INFO_H

#include <cstdint>
#include <cstring> // for memcmp()
#include <iosfwd>

#include "scatter_info.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/utime.h"

namespace ceph { class Formatter; }
class JSONObj;

struct nest_info_t : public scatter_info_t {
  int64_t rsize() const { return rfiles + rsubdirs; }

  void zero() {
    *this = nest_info_t();
  }

  void sub(const nest_info_t &other) {
    add(other, -1);
  }
  void add(const nest_info_t &other, int fac=1) {
    if (other.rctime > rctime)
      rctime = other.rctime;
    rbytes += fac*other.rbytes;
    rfiles += fac*other.rfiles;
    rsubdirs += fac*other.rsubdirs;
    rsnaps += fac*other.rsnaps;
  }

  // *this += cur - acc;
  void add_delta(const nest_info_t &cur, const nest_info_t &acc) {
    if (cur.rctime > rctime)
      rctime = cur.rctime;
    rbytes += cur.rbytes - acc.rbytes;
    rfiles += cur.rfiles - acc.rfiles;
    rsubdirs += cur.rsubdirs - acc.rsubdirs;
    rsnaps += cur.rsnaps - acc.rsnaps;
  }

  bool same_sums(const nest_info_t &o) const {
    return rctime <= o.rctime &&
        rbytes == o.rbytes &&
        rfiles == o.rfiles &&
        rsubdirs == o.rsubdirs &&
        rsnaps == o.rsnaps;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static std::list<nest_info_t> generate_test_instances();

  // this frag + children
  utime_t rctime;
  int64_t rbytes = 0;
  int64_t rfiles = 0;
  int64_t rsubdirs = 0;
  int64_t rsnaps = 0;
};
WRITE_CLASS_ENCODER(nest_info_t)

inline bool operator==(const nest_info_t &l, const nest_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}
inline bool operator!=(const nest_info_t &l, const nest_info_t &r) {
  return !(l == r);
}

std::ostream& operator<<(std::ostream &out, const nest_info_t &n);

#endif
