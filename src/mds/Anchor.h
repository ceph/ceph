// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_ANCHOR_H
#define CEPH_ANCHOR_H

#include <string>
using std::string;

#include "include/types.h"
#include "mdstypes.h"
#include "include/buffer.h"


// identifies a anchor table mutation

namespace ceph {
  class Formatter;
}

// anchor type

class Anchor {
public:
  inodeno_t ino;      // anchored ino
  inodeno_t dirino;
  __u32     dn_hash;
  int       nref;     // reference count
  version_t updated;

  Anchor() : dn_hash(0), nref(0), updated(0) {}
  Anchor(inodeno_t i, inodeno_t di, __u32 hash, int nr, version_t u) :
    ino(i), dirino(di), dn_hash(hash), nref(nr), updated(u) { }
  
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<Anchor*>& ls);
};
WRITE_CLASS_ENCODER(Anchor)

ostream& operator<<(ostream& out, const Anchor &a);

#endif
