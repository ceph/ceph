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

#ifndef __CEPH_OS_HOBJECT_H
#define __CEPH_OS_HOBJECT_H

#include <string.h>
#include "include/types.h"
#include "include/object.h"
#include "include/cmp.h"

#include "json_spirit/json_spirit_value.h"
#include "include/assert.h"   // spirit clobbers it!

typedef uint64_t filestore_hobject_key_t;

namespace ceph {
  class Formatter;
}

struct hobject_t {
  object_t oid;
  snapid_t snap;
  uint32_t hash;
private:
  bool max;
  static const int64_t POOL_IS_TEMP = -1;
public:
  int64_t pool;
  string nspace;

private:
  string key;

public:
  const string &get_key() const {
    return key;
  }

  string to_str() const;

  static bool match_hash(uint32_t to_check, uint32_t bits, uint32_t match) {
    return (match & ~((~0)<<bits)) == (to_check & ~((~0)<<bits));
  }
  bool match(uint32_t bits, uint32_t match) const {
    return match_hash(hash, bits, match);
  }

  static hobject_t make_temp(const string &name) {
    hobject_t ret(object_t(name), "", CEPH_NOSNAP, 0, POOL_IS_TEMP, "");
    return ret;
  }
  bool is_temp() const {
    return pool == POOL_IS_TEMP;
  }
  
  hobject_t() : snap(0), hash(0), max(false), pool(-1) {}

  hobject_t(object_t oid, const string& key, snapid_t snap, uint64_t hash,
	    int64_t pool, string nspace) :
    oid(oid), snap(snap), hash(hash), max(false),
    pool(pool), nspace(nspace),
    key(oid.name == key ? string() : key) {}

  hobject_t(const sobject_t &soid, const string &key, uint32_t hash,
	    int64_t pool, string nspace) :
    oid(soid.oid), snap(soid.snap), hash(hash), max(false),
    pool(pool), nspace(nspace),
    key(soid.oid.name == key ? string() : key) {}

  /// @return min hobject_t ret s.t. ret.hash == this->hash
  hobject_t get_boundary() const {
    if (is_max())
      return *this;
    hobject_t ret;
    ret.hash = hash;
    return ret;
  }

  /// @return head version of this hobject_t
  hobject_t get_head() const {
    hobject_t ret(*this);
    ret.snap = CEPH_NOSNAP;
    return ret;
  }

  /// @return snapdir version of this hobject_t
  hobject_t get_snapdir() const {
    hobject_t ret(*this);
    ret.snap = CEPH_SNAPDIR;
    return ret;
  }

  /// @return true if object is snapdir
  bool is_snapdir() const {
    return snap == CEPH_SNAPDIR;
  }

  /// @return true if object is head
  bool is_head() const {
    return snap == CEPH_NOSNAP;
  }

  /// @return true if object is neither head nor snapdir
  bool is_snap() const {
    return (snap != CEPH_NOSNAP) && (snap != CEPH_SNAPDIR);
  }

  /// @return true iff the object should have a snapset in it's attrs
  bool has_snapset() const {
    return !is_snap();
  }

  /* Do not use when a particular hash function is needed */
  explicit hobject_t(const sobject_t &o) :
    oid(o.oid), snap(o.snap), max(false), pool(-1) {
    hash = CEPH_HASH_NAMESPACE::hash<sobject_t>()(o);
  }

  // maximum sorted value.
  static hobject_t get_max() {
    hobject_t h;
    h.max = true;
    return h;
  }
  bool is_max() const {
    return max;
  }
  bool is_min() const {
    // this needs to match how it's constructed
    return snap == 0 &&
	   hash == 0 &&
	   !max &&
	   pool == -1;
  }

  static uint32_t _reverse_nibbles(uint32_t retval) {
    // reverse nibbles
    retval = ((retval & 0x0f0f0f0f) << 4) | ((retval & 0xf0f0f0f0) >> 4);
    retval = ((retval & 0x00ff00ff) << 8) | ((retval & 0xff00ff00) >> 8);
    retval = ((retval & 0x0000ffff) << 16) | ((retval & 0xffff0000) >> 16);
    return retval;
  }

  /**
   * Returns set S of strings such that for any object
   * h where h.match(bits, mask), there is some string
   * s \in S such that s is a prefix of h.to_str().
   * Furthermore, for any s \in S, s is a prefix of
   * h.str() implies that h.match(bits, mask).
   */
  static set<string> get_prefixes(
    uint32_t bits,
    uint32_t mask,
    int64_t pool);

  filestore_hobject_key_t get_filestore_key_u32() const {
    assert(!max);
    return _reverse_nibbles(hash);
  }
  filestore_hobject_key_t get_filestore_key() const {
    if (max)
      return 0x100000000ull;
    else
      return get_filestore_key_u32();
  }

  const string& get_effective_key() const {
    if (key.length())
      return key;
    return oid.name;
  }

  void swap(hobject_t &o) {
    hobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  const string &get_namespace() const {
    return nspace;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(json_spirit::Value& v);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<hobject_t*>& o);
  friend bool operator<(const hobject_t&, const hobject_t&);
  friend bool operator>(const hobject_t&, const hobject_t&);
  friend bool operator<=(const hobject_t&, const hobject_t&);
  friend bool operator>=(const hobject_t&, const hobject_t&);
  friend bool operator==(const hobject_t&, const hobject_t&);
  friend bool operator!=(const hobject_t&, const hobject_t&);
  friend struct ghobject_t;
};
WRITE_CLASS_ENCODER(hobject_t)

CEPH_HASH_NAMESPACE_START
  template<> struct hash<hobject_t> {
    size_t operator()(const hobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.oid) ^ I(r.snap);
    }
  };
CEPH_HASH_NAMESPACE_END

ostream& operator<<(ostream& out, const hobject_t& o);

WRITE_EQ_OPERATORS_7(hobject_t, oid, get_key(), snap, hash, max, pool, nspace)
// sort hobject_t's by <max, get_filestore_key(hash), key, oid, snapid>
WRITE_CMP_OPERATORS_7(hobject_t,
		      max,
		      get_filestore_key(),
		      nspace,
		      pool,
		      get_effective_key(),
		      oid,
		      snap)

typedef version_t gen_t;
typedef uint8_t shard_t;

#ifndef UINT8_MAX
#define UINT8_MAX (255)
#endif
#ifndef UINT64_MAX
#define UINT64_MAX (18446744073709551615ULL)
#endif

struct ghobject_t {
  hobject_t hobj;
  gen_t generation;
  shard_t shard_id;

public:
  static const shard_t NO_SHARD = UINT8_MAX;
  static shard_t no_shard() { return NO_SHARD; }
  static const gen_t NO_GEN = UINT64_MAX;

  ghobject_t() : generation(NO_GEN), shard_id(NO_SHARD) {}

  ghobject_t(const hobject_t &obj) : hobj(obj), generation(NO_GEN), shard_id(NO_SHARD) {}

  ghobject_t(const hobject_t &obj, gen_t gen, shard_t shard) : hobj(obj), generation(gen), shard_id(shard) {}

  bool match(uint32_t bits, uint32_t match) const {
    return hobj.match_hash(hobj.hash, bits, match);
  }
  /// @return min ghobject_t ret s.t. ret.hash == this->hash
  ghobject_t get_boundary() const {
    if (hobj.is_max())
      return *this;
    ghobject_t ret;
    ret.hobj.hash = hobj.hash;
    return ret;
  }
  filestore_hobject_key_t get_filestore_key_u32() const {
    return hobj.get_filestore_key_u32();
  }
  filestore_hobject_key_t get_filestore_key() const {
    return hobj.get_filestore_key();
  }

  bool is_degenerate() const {
    return generation == NO_GEN && shard_id == NO_SHARD;
  }

  bool is_no_gen() const {
    return generation == NO_GEN;
  }

  bool is_no_shard() const {
    return shard_id == NO_SHARD;
  }

  // maximum sorted value.
  static ghobject_t get_max() {
    ghobject_t h(hobject_t::get_max());
    return h;
  }
  bool is_max() const {
    return hobj.is_max();
  }

  void swap(ghobject_t &o) {
    ghobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(json_spirit::Value& v);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ghobject_t*>& o);
  friend bool operator<(const ghobject_t&, const ghobject_t&);
  friend bool operator>(const ghobject_t&, const ghobject_t&);
  friend bool operator<=(const ghobject_t&, const ghobject_t&);
  friend bool operator>=(const ghobject_t&, const ghobject_t&);
  friend bool operator==(const ghobject_t&, const ghobject_t&);
  friend bool operator!=(const ghobject_t&, const ghobject_t&);
};
WRITE_CLASS_ENCODER(ghobject_t)

CEPH_HASH_NAMESPACE_START
  template<> struct hash<ghobject_t> {
    size_t operator()(const ghobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.hobj.oid) ^ I(r.hobj.snap);
    }
  };
CEPH_HASH_NAMESPACE_END

ostream& operator<<(ostream& out, const ghobject_t& o);

WRITE_EQ_OPERATORS_3(ghobject_t, hobj, shard_id, generation)
// sort ghobject_t's by <hobj, shard_id, generation> 
// 
// Two objects which differ by generation are more related than
// two objects of the same generation which differ by shard.
// 
WRITE_CMP_OPERATORS_3(ghobject_t,
		      hobj,
		      shard_id,
		      generation)
#endif
