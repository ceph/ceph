// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_KSTORE_TYPES_H
#define CEPH_OSD_KSTORE_TYPES_H

#include <ostream>
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
#include "common/hobject.h"

namespace ceph {
  class Formatter;
}
/// collection metadata
struct kstore_cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit kstore_cnode_t(int b=0) : bits(b) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<kstore_cnode_t*>& o);
};
WRITE_CLASS_ENCODER(kstore_cnode_t)

/// onode: per-object metadata
struct kstore_onode_t {
  uint64_t nid;                        ///< numeric id (locally unique)
  uint64_t size;                       ///< object size
  map<string, bufferptr> attrs;        ///< attrs
  uint64_t omap_head;                  ///< id for omap root node
  uint32_t stripe_size;                ///< stripe size

  uint32_t expected_object_size;
  uint32_t expected_write_size;

  kstore_onode_t()
    : nid(0),
      size(0),
      omap_head(0),
      stripe_size(0),
      expected_object_size(0),
      expected_write_size(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<kstore_onode_t*>& o);
};
WRITE_CLASS_ENCODER(kstore_onode_t)

#endif
