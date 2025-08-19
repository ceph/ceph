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
#ifndef CEPH_CEPHFS_VINODENO_H
#define CEPH_CEPHFS_VINODENO_H

#include "include/encoding.h"
#include "include/fs_types.h" // for inodeno_t
#include "include/object.h" // for snapid_t

struct vinodeno_t {
  vinodeno_t() {}
  vinodeno_t(inodeno_t i, snapid_t s) : ino(i), snapid(s) {}

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static std::list<vinodeno_t> generate_test_instances() {
    std::list<vinodeno_t> ls;
    ls.emplace_back();
    ls.push_back(vinodeno_t(1, 2));
    return ls;
  }
  inodeno_t ino;
  snapid_t snapid;
};
WRITE_CLASS_ENCODER(vinodeno_t)

inline bool operator==(const vinodeno_t &l, const vinodeno_t &r) {
  return l.ino == r.ino && l.snapid == r.snapid;
}
inline bool operator!=(const vinodeno_t &l, const vinodeno_t &r) {
  return !(l == r);
}
inline bool operator<(const vinodeno_t &l, const vinodeno_t &r) {
  return
    l.ino < r.ino ||
    (l.ino == r.ino && l.snapid < r.snapid);
}

#endif
