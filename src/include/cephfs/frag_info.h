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
#ifndef CEPH_CEPHFS_FRAG_INFO_H
#define CEPH_CEPHFS_FRAG_INFO_H

#include <cstdint>
#include <cstring> // for memcmp()
#include <iosfwd>

#include "scatter_info.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/utime.h"

namespace ceph { class Formatter; }
class JSONObj;

struct frag_info_t : public scatter_info_t {
  int64_t size() const { return nfiles + nsubdirs; }

  void zero() {
    *this = frag_info_t();
  }

  // *this += cur - acc;
  void add_delta(const frag_info_t &cur, const frag_info_t &acc, bool *touched_mtime=0, bool *touched_chattr=0) {
    if (cur.mtime > mtime) {
      mtime = cur.mtime;
      if (touched_mtime)
	*touched_mtime = true;
    }
    if (cur.change_attr > change_attr) {
      change_attr = cur.change_attr;
      if (touched_chattr)
	*touched_chattr = true;
    }
    nfiles += cur.nfiles - acc.nfiles;
    nsubdirs += cur.nsubdirs - acc.nsubdirs;
  }

  void add(const frag_info_t& other) {
    if (other.mtime > mtime)
      mtime = other.mtime;
    if (other.change_attr > change_attr)
      change_attr = other.change_attr;
    nfiles += other.nfiles;
    nsubdirs += other.nsubdirs;
  }

  bool same_sums(const frag_info_t &o) const {
    return mtime <= o.mtime &&
	nfiles == o.nfiles &&
	nsubdirs == o.nsubdirs;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
  static std::list<frag_info_t>  generate_test_instances();

  // this frag
  utime_t mtime;
  uint64_t change_attr = 0;
  int64_t nfiles = 0;        // files
  int64_t nsubdirs = 0;      // subdirs
};
WRITE_CLASS_ENCODER(frag_info_t)

inline bool operator==(const frag_info_t &l, const frag_info_t &r) {
  return memcmp(&l, &r, sizeof(l)) == 0;
}
inline bool operator!=(const frag_info_t &l, const frag_info_t &r) {
  return !(l == r);
}

std::ostream& operator<<(std::ostream &out, const frag_info_t &f);

#endif
