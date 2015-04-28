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

#ifndef CEPH_OSD_NEWSTORE_TYPES_H
#define CEPH_OSD_NEWSTORE_TYPES_H

#include <ostream>
#include "include/types.h"

namespace ceph {
  class Formatter;
}

/// collection metadata
struct cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  cnode_t(int b=0) : bits(b) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<cnode_t*>& o);
};
WRITE_CLASS_ENCODER(cnode_t)

/// unique id for a local file
struct fid_t {
  uint32_t fset, fno;
  string handle;
  fid_t() : fset(0), fno(0) { }
  fid_t(uint32_t s, uint32_t n) : fset(s), fno(n) { }

  void encode(bufferlist& bl) const {
    ::encode(fset, bl);
    ::encode(fno, bl);
    ::encode(handle, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(fset, p);
    ::decode(fno, p);
    ::decode(handle, p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<fid_t*>& o);
};
WRITE_CLASS_ENCODER(fid_t)

static inline ostream& operator<<(ostream& out, const fid_t& fid) {
  out << fid.fset << "/" << fid.fno;
  if (fid.handle.length())
    out << "~";
  return out;
}

static inline bool operator==(const fid_t& a, const fid_t& b) {
  return a.fset == b.fset && a.fno == b.fno && a.handle == b.handle;
}
static inline bool operator!=(const fid_t& a, const fid_t& b) {
  return !(a == b);
}

/// fragment: a byte extent backed by a file
struct fragment_t {
  uint32_t offset;   ///< offset in file to first byte of this fragment
  uint32_t length;   ///< length of fragment/extent
  fid_t fid;         ///< file backing this fragment

  fragment_t() : offset(0), length(0) {}
  fragment_t(uint32_t o, uint32_t l) : offset(o), length(l) {}
  fragment_t(uint32_t o, uint32_t l, fid_t f) : offset(o), length(l), fid(f) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<fragment_t*>& o);
};
WRITE_CLASS_ENCODER(fragment_t)

ostream& operator<<(ostream& out, const fragment_t& o);

struct overlay_t {
  uint64_t key;          ///< key (offset of start of original k/v pair)
  uint32_t value_offset; ///< offset in associated value for this extent
  uint32_t length;

  overlay_t() : key(0), value_offset(0), length(0) {}
  overlay_t(uint64_t k, uint32_t vo, uint32_t l)
    : key(k), value_offset(vo), length(l) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<overlay_t*>& o);

};
WRITE_CLASS_ENCODER(overlay_t)

ostream& operator<<(ostream& out, const overlay_t& o);

/// onode: per-object metadata
struct onode_t {
  uint64_t nid;                        ///< numeric id (locally unique)
  uint64_t size;                       ///< object size
  map<string, bufferptr> attrs;        ///< attrs
  map<uint64_t, fragment_t> data_map;  ///< data (offset to fragment mapping)
  map<uint64_t,overlay_t> overlay_map; ///< overlay data (stored in db)
  set<uint64_t> shared_overlays;       ///< overlay keys that are shared
  uint32_t last_overlay_key;           ///< key for next overlay
  uint64_t omap_head;                  ///< id for omap root node

  uint32_t expected_object_size;
  uint32_t expected_write_size;

  onode_t()
    : nid(0),
      size(0),
      last_overlay_key(0),
      omap_head(0),
      expected_object_size(0),
      expected_write_size(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<onode_t*>& o);
};
WRITE_CLASS_ENCODER(onode_t)


/// writeahead-logged op
struct wal_op_t {
  typedef enum {
    OP_WRITE = 1,
    OP_TRUNCATE = 3,
    OP_ZERO = 4,
    OP_REMOVE = 5,
  } type_t;
  __u8 op;
  fid_t fid;
  uint64_t offset, length;
  bufferlist data;
  uint64_t nid;
  vector<overlay_t> overlays;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<wal_op_t*>& o);
};
WRITE_CLASS_ENCODER(wal_op_t)


/// writeahead-logged transaction
struct wal_transaction_t {
  uint64_t seq;
  list<wal_op_t> ops;
  vector<string> shared_overlay_keys;

  int64_t _bytes;  ///< cached byte count

  wal_transaction_t() : _bytes(-1) {}

  uint64_t get_bytes() {
    if (_bytes < 0) {
      _bytes = 0;
      for (list<wal_op_t>::iterator p = ops.begin(); p != ops.end(); ++p) {
	_bytes += p->length;
      }
    }
    return _bytes;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<wal_transaction_t*>& o);
};
WRITE_CLASS_ENCODER(wal_transaction_t)

#endif
