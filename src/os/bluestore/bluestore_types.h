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

#ifndef CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H
#define CEPH_OSD_BLUESTORE_BLUESTORE_TYPES_H

#include <ostream>
#include "include/types.h"
#include "include/interval_set.h"
#include "include/utime.h"
#include "common/hobject.h"

namespace ceph {
  class Formatter;
}

/// label for block device
struct bluestore_bdev_label_t {
  uuid_d osd_uuid;     ///< osd uuid
  uint64_t size;       ///< device size
  utime_t btime;       ///< birth time
  string description;  ///< device description

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_bdev_label_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_bdev_label_t)

ostream& operator<<(ostream& out, const bluestore_bdev_label_t& l);

/// collection metadata
struct bluestore_cnode_t {
  uint32_t bits;   ///< how many bits of coll pgid are significant

  explicit bluestore_cnode_t(int b=0) : bits(b) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_cnode_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_cnode_t)

/// extent: a byte extent back by the block device
struct bluestore_extent_t {
  enum {
    FLAG_UNWRITTEN = 1,   ///< extent is unwritten (and defined to be zero)
    FLAG_SHARED = 2,      ///< extent is shared by another object, and refcounted
    FLAG_COW_HEAD = 4,    ///< extent has pending wal OP_COPY for head
    FLAG_COW_TAIL = 8,    ///< extent has pending wal OP_COPY for tail
  };
  static string get_flags_string(unsigned flags);

  uint64_t offset;
  uint32_t length;
  uint32_t flags;  /// or reserved

  bluestore_extent_t(uint64_t o=0, uint32_t l=0, uint32_t f=0)
    : offset(o), length(l), flags(f) {}

  uint64_t end() const {
    return offset + length;
  }

  bool has_flag(unsigned f) const {
    return flags & f;
  }
  void set_flag(unsigned f) {
    flags |= f;
  }
  void clear_flag(unsigned f) {
    flags &= ~f;
  }

  void encode(bufferlist& bl) const {
    ::encode(offset, bl);
    ::encode(length, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(offset, p);
    ::decode(length, p);
    ::decode(flags, p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_extent_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_extent_t)

ostream& operator<<(ostream& out, const bluestore_extent_t& bp);

/// extent_map: a map of reference counted extents
struct bluestore_extent_ref_map_t {
  struct record_t {
    uint32_t length;
    uint32_t refs;
    record_t(uint32_t l=0, uint32_t r=0) : length(l), refs(r) {}
    void encode(bufferlist& bl) const {
      ::encode(length, bl);
      ::encode(refs, bl);
    }
    void decode(bufferlist::iterator& p) {
      ::decode(length, p);
      ::decode(refs, p);
    }
  };
  WRITE_CLASS_ENCODER(record_t)

  map<uint64_t,record_t> ref_map;

  void _check() const;
  void _maybe_merge_left(map<uint64_t,record_t>::iterator& p);

  void clear() {
    ref_map.clear();
  }
  bool empty() const {
    return ref_map.empty();
  }

  void add(uint64_t offset, uint32_t len, unsigned ref=2);
  void get(uint64_t offset, uint32_t len);
  void put(uint64_t offset, uint32_t len, vector<bluestore_extent_t> *release);

  bool contains(uint64_t offset, uint32_t len) const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_extent_ref_map_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_extent_ref_map_t::record_t)
WRITE_CLASS_ENCODER(bluestore_extent_ref_map_t)

ostream& operator<<(ostream& out, const bluestore_extent_ref_map_t& rm);
static inline bool operator==(const bluestore_extent_ref_map_t::record_t& l,
			      const bluestore_extent_ref_map_t::record_t& r) {
  return l.length == r.length && l.refs == r.refs;
}
static inline bool operator==(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return l.ref_map == r.ref_map;
}
static inline bool operator!=(const bluestore_extent_ref_map_t& l,
			      const bluestore_extent_ref_map_t& r) {
  return !(l == r);
}

/// overlay: a byte extent backed by kv pair, logically overlaying other content
struct bluestore_overlay_t {
  uint64_t key;          ///< key (nid+key identify the kv pair in the kvdb)
  uint32_t value_offset; ///< offset in associated value for this extent
  uint32_t length;

  bluestore_overlay_t() : key(0), value_offset(0), length(0) {}
  bluestore_overlay_t(uint64_t k, uint32_t vo, uint32_t l)
    : key(k), value_offset(vo), length(l) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_overlay_t*>& o);

};
WRITE_CLASS_ENCODER(bluestore_overlay_t)

ostream& operator<<(ostream& out, const bluestore_overlay_t& o);

/// onode: per-object metadata
struct bluestore_onode_t {
  uint64_t nid;                        ///< numeric id (locally unique)
  uint64_t size;                       ///< object size
  map<string, bufferptr> attrs;        ///< attrs
  map<uint64_t, bluestore_extent_t> block_map;   ///< block data
  map<uint64_t,bluestore_overlay_t> overlay_map; ///< overlay data (stored in db)
  map<uint64_t,uint16_t> overlay_refs; ///< overlay keys ref counts (if >1)
  uint32_t last_overlay_key;           ///< key for next overlay
  uint64_t omap_head;                  ///< id for omap root node

  uint32_t expected_object_size;
  uint32_t expected_write_size;

  bluestore_onode_t()
    : nid(0),
      size(0),
      last_overlay_key(0),
      omap_head(0),
      expected_object_size(0),
      expected_write_size(0) {}

  map<uint64_t,bluestore_extent_t>::iterator find_extent(uint64_t offset) {
    map<uint64_t,bluestore_extent_t>::iterator fp = block_map.lower_bound(offset);
    if (fp != block_map.begin()) {
      --fp;
      if (fp->first + fp->second.length <= offset) {
	++fp;
      }
    }
    if (fp != block_map.end() && fp->first > offset)
      return block_map.end();  // extent is past offset
    return fp;
  }

  map<uint64_t,bluestore_extent_t>::iterator seek_extent(uint64_t offset) {
    map<uint64_t,bluestore_extent_t>::iterator fp = block_map.lower_bound(offset);
    if (fp != block_map.begin()) {
      --fp;
      if (fp->first + fp->second.length <= offset) {
	++fp;
      }
    }
    return fp;
  }

  bool put_overlay_ref(uint64_t key) {
    map<uint64_t,uint16_t>::iterator q = overlay_refs.find(key);
    if (q == overlay_refs.end())
      return true;
    assert(q->second >= 2);
    if (--q->second == 1) {
      overlay_refs.erase(q);
    }
    return false;
  }
  void get_overlay_ref(uint64_t key) {
    map<uint64_t,uint16_t>::iterator q = overlay_refs.find(key);
    if (q == overlay_refs.end())
      overlay_refs[key] = 2;
    else
      ++q->second;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_onode_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_onode_t)


/// writeahead-logged op
struct bluestore_wal_op_t {
  typedef enum {
    OP_WRITE = 1,
    OP_COPY = 2,
    OP_ZERO = 4,
  } type_t;
  __u8 op = 0;
  bluestore_extent_t extent;
  bluestore_extent_t src_extent;
  uint64_t src_rmw_head, src_rmw_tail;
  bufferlist data;
  uint64_t nid;
  vector<bluestore_overlay_t> overlays;
  vector<uint64_t> removed_overlays;

  bluestore_wal_op_t() : src_rmw_head(0), src_rmw_tail(0), nid(0) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_op_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_wal_op_t)


/// writeahead-logged transaction
struct bluestore_wal_transaction_t {
  uint64_t seq = 0;
  list<bluestore_wal_op_t> ops;
  interval_set<uint64_t> released;  ///< allocations to release after wal

  int64_t _bytes;  ///< cached byte count

  bluestore_wal_transaction_t() : seq(0), _bytes(-1) {}

#if 0
  no users for htis
  uint64_t get_bytes() {
    if (_bytes < 0) {
      _bytes = 0;
      for (list<bluestore_wal_op_t>::iterator p = ops.begin();
	   p != ops.end();
	   ++p) {
	_bytes += p->extent.length;
      }
    }
    return _bytes;
  }
#endif

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluestore_wal_transaction_t*>& o);
};
WRITE_CLASS_ENCODER(bluestore_wal_transaction_t)

#endif
