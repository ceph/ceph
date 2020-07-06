// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_ANCHOR_H
#define CEPH_ANCHOR_H

#include <string>

#include "include/types.h"
#include "mdstypes.h"
#include "include/buffer.h"

/*
 * Anchor represents primary linkage of an inode. When adding inode to an
 * anchor table, MDS ensures that the table also contains inode's ancestor
 * inodes. MDS can get inode's path by looking up anchor table recursively.
 */
class Anchor {
public:
  Anchor() {}
  Anchor(inodeno_t i, inodeno_t di, std::string_view str, __u8 tp) :
    ino(i), dirino(di), d_name(str), d_type(tp) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<Anchor*>& ls);
  bool operator==(const Anchor &r) const {
    return ino == r.ino && dirino == r.dirino &&
	   d_name == r.d_name && d_type == r.d_type &&
	   frags == r.frags;
  }

  inodeno_t ino;	// anchored ino
  inodeno_t dirino;
  std::string d_name;
  __u8 d_type = 0;
  std::set<frag_t> frags;

  int omap_idx = -1;	// stored in which omap object
};
WRITE_CLASS_ENCODER(Anchor)

class RecoveredAnchor : public Anchor {
public:
  RecoveredAnchor() {}

  mds_rank_t auth = MDS_RANK_NONE; // auth hint
};

class OpenedAnchor : public Anchor {
public:
  OpenedAnchor(inodeno_t i, inodeno_t di, std::string_view str, __u8 tp, int nr) :
      Anchor(i, di, str, tp),
      nref(nr)
  {}

  mutable int nref = 0; // how many children
};

ostream& operator<<(ostream& out, const Anchor &a);
#endif
