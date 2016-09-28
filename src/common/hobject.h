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

namespace ceph {
  class Formatter;
}

#ifndef UINT64_MAX
#define UINT64_MAX (18446744073709551615ULL)
#endif
#ifndef INT64_MIN
#define INT64_MIN ((int64_t)0x8000000000000000ll)
#endif

struct hobject_t {
  object_t oid;
  snapid_t snap;
private:
  uint32_t hash;
  bool max;
  uint32_t nibblewise_key_cache;
  uint32_t hash_reverse_bits;
  static const int64_t POOL_META = -1;
  static const int64_t POOL_TEMP_START = -2; // and then negative
  friend class spg_t;  // for POOL_TEMP_START
public:
  int64_t pool;
  string nspace;

private:
  string key;

  class hobject_t_max {};

public:
  const string &get_key() const {
    return key;
  }

  void set_key(const std::string &key_) {
    if (key_ == oid.name)
      key.clear();
    else
      key = key_;
  }

  string to_str() const;
  
  uint32_t get_hash() const { 
    return hash;
  }
  void set_hash(uint32_t value) { 
    hash = value;
    build_hash_cache();
  }

  static bool match_hash(uint32_t to_check, uint32_t bits, uint32_t match) {
    return (match & ~((~0)<<bits)) == (to_check & ~((~0)<<bits));
  }
  bool match(uint32_t bits, uint32_t match) const {
    return match_hash(hash, bits, match);
  }

  bool is_temp() const {
    return pool <= POOL_TEMP_START && pool != INT64_MIN;
  }
  bool is_meta() const {
    return pool == POOL_META;
  }

  hobject_t() : snap(0), hash(0), max(false), pool(INT64_MIN) {
    build_hash_cache();
  }

  hobject_t(const hobject_t &rhs) = default;
  hobject_t(hobject_t &&rhs) = default;
  hobject_t(hobject_t_max &&singleon) : hobject_t() {
    max = true;
  }
  hobject_t &operator=(const hobject_t &rhs) = default;
  hobject_t &operator=(hobject_t &&rhs) = default;
  hobject_t &operator=(hobject_t_max &&singleton) {
    *this = hobject_t();
    max = true;
    return *this;
  }

  // maximum sorted value.
  static hobject_t_max get_max() {
    return hobject_t_max();
  }

  hobject_t(object_t oid, const string& key, snapid_t snap, uint32_t hash,
	    int64_t pool, string nspace)
    : oid(oid), snap(snap), hash(hash), max(false),
      pool(pool), nspace(nspace),
      key(oid.name == key ? string() : key) {
    build_hash_cache();
  }

  hobject_t(const sobject_t &soid, const string &key, uint32_t hash,
	    int64_t pool, string nspace)
    : oid(soid.oid), snap(soid.snap), hash(hash), max(false),
      pool(pool), nspace(nspace),
      key(soid.oid.name == key ? string() : key) {
    build_hash_cache();
  }

  /// @return min hobject_t ret s.t. ret.hash == this->hash
  hobject_t get_boundary() const {
    if (is_max())
      return *this;
    hobject_t ret;
    ret.set_hash(hash);
    ret.pool = pool;
    return ret;
  }

  hobject_t get_object_boundary() const {
    if (is_max())
      return *this;
    hobject_t ret = *this;
    ret.snap = 0;
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

  /// @return true if object is neither head nor snapdir nor max
  bool is_snap() const {
    return !is_max() && !is_head() && !is_snapdir();
  }

  /// @return true iff the object should have a snapset in it's attrs
  bool has_snapset() const {
    return is_head() || is_snapdir();
  }

  /* Do not use when a particular hash function is needed */
  explicit hobject_t(const sobject_t &o) :
    oid(o.oid), snap(o.snap), max(false), pool(POOL_META) {
    set_hash(std::hash<sobject_t>()(o));
  }

  bool is_max() const {
    return max;
  }
  bool is_min() const {
    // this needs to match how it's constructed
    return snap == 0 &&
	   hash == 0 &&
	   !max &&
	   pool == INT64_MIN;
  }

  static uint32_t _reverse_bits(uint32_t v) {
    if (v == 0)
      return v;
    // reverse bits
    // swap odd and even bits
    v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
    // swap consecutive pairs
    v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
    // swap nibbles ...
    v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
    // swap bytes
    v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
    // swap 2-byte long pairs
    v = ( v >> 16             ) | ( v               << 16);
    return v;
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
   * s \f$\in\f$ S such that s is a prefix of h.to_str().
   * Furthermore, for any s $f\in\f$ S, s is a prefix of
   * h.str() implies that h.match(bits, mask).
   */
  static set<string> get_prefixes(
    uint32_t bits,
    uint32_t mask,
    int64_t pool);

  // filestore nibble-based key
  uint32_t get_nibblewise_key_u32() const {
    assert(!max);
    return nibblewise_key_cache;
  }
  uint64_t get_nibblewise_key() const {
    return max ? 0x100000000ull : nibblewise_key_cache;
  }

  // newer bit-reversed key
  uint32_t get_bitwise_key_u32() const {
    assert(!max);
    return hash_reverse_bits;
  }
  uint64_t get_bitwise_key() const {
    return max ? 0x100000000ull : hash_reverse_bits;
  }

  void build_hash_cache() {
    nibblewise_key_cache = _reverse_nibbles(hash);
    hash_reverse_bits = _reverse_bits(hash);
  }
  void set_nibblewise_key_u32(uint32_t value) {
    hash = _reverse_nibbles(value);
    build_hash_cache();
  }
  void set_bitwise_key_u32(uint32_t value) {
    hash = _reverse_bits(value);
    build_hash_cache();
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

  bool parse(const string& s);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(json_spirit::Value& v);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<hobject_t*>& o);
  friend int cmp_nibblewise(const hobject_t& l, const hobject_t& r);
  friend int cmp_bitwise(const hobject_t& l, const hobject_t& r);
  friend bool operator==(const hobject_t&, const hobject_t&);
  friend bool operator!=(const hobject_t&, const hobject_t&);
  friend struct ghobject_t;

  struct NibblewiseComparator {
    bool operator()(const hobject_t& l, const hobject_t& r) const {
      return cmp_nibblewise(l, r) < 0;
    }
  };

  struct BitwiseComparator {
    bool operator()(const hobject_t& l, const hobject_t& r) const {
      return cmp_bitwise(l, r) < 0;
    }
  };

  struct Comparator {
    bool bitwise;
    explicit Comparator(bool b) : bitwise(b) {}
    bool operator()(const hobject_t& l, const hobject_t& r) const {
      if (bitwise)
	return cmp_bitwise(l, r) < 0;
      else
	return cmp_nibblewise(l, r) < 0;
    }
  };
  struct ComparatorWithDefault {
    bool bitwise;
    explicit ComparatorWithDefault(bool b=true) : bitwise(b) {}
    bool operator()(const hobject_t& l, const hobject_t& r) const {
      if (bitwise)
	return cmp_bitwise(l, r) < 0;
      else
	return cmp_nibblewise(l, r) < 0;
    }
  };
};
WRITE_CLASS_ENCODER(hobject_t)

namespace std {
  template<> struct hash<hobject_t> {
    size_t operator()(const hobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.oid) ^ I(r.snap);
    }
  };
} // namespace std

ostream& operator<<(ostream& out, const hobject_t& o);

WRITE_EQ_OPERATORS_7(hobject_t, hash, oid, get_key(), snap, pool, max, nspace)

template <typename T>
struct always_false {
  using value = std::false_type;
};

template <typename T>
inline bool operator==(const hobject_t &lhs, const T&) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return lhs.is_max();
}
template <typename T>
inline bool operator==(const T&, const hobject_t &rhs) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return rhs.is_max();
}
template <typename T>
inline bool operator!=(const hobject_t &lhs, const T&) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return !lhs.is_max();
}
template <typename T>
inline bool operator!=(const T&, const hobject_t &rhs) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return !rhs.is_max();
}

extern int cmp_nibblewise(const hobject_t& l, const hobject_t& r);
extern int cmp_bitwise(const hobject_t& l, const hobject_t& r);
static inline int cmp(const hobject_t& l, const hobject_t& r, bool sort_bitwise) {
  if (sort_bitwise)
    return cmp_bitwise(l, r);
  else
    return cmp_nibblewise(l, r);
}
template <typename T>
static inline int cmp(const hobject_t &l, const T&, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return l.is_max() ? 0 : -1;
}
template <typename T>
static inline int cmp(const T&, const hobject_t&r, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return r.is_max() ? 0 : 1;
}
template <typename T>
static inline int cmp_nibblewise(const hobject_t &l, const T&, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return l.is_max() ? 0 : -1;
}
template <typename T>
static inline int cmp_nibblewise(const T&, const hobject_t&r, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return r.is_max() ? 0 : 1;
}
template <typename T>
static inline int cmp_bitwise(const hobject_t &l, const T&, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return l.is_max() ? 0 : -1;
}
template <typename T>
static inline int cmp_bitwise(const T&, const hobject_t&r, bool sort_bitwise) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return r.is_max() ? 0 : 1;
}



// these are convenient
static inline hobject_t MAX_HOBJ(const hobject_t& l, const hobject_t& r, bool bitwise) {
  if (cmp(l, r, bitwise) >= 0)
    return l;
  else
    return r;
}

static inline hobject_t MIN_HOBJ(const hobject_t& l, const hobject_t& r, bool bitwise) {
  if (cmp(l, r, bitwise) <= 0)
    return l;
  else
    return r;
}

typedef version_t gen_t;

struct ghobject_t {
  hobject_t hobj;
  gen_t generation;
  shard_id_t shard_id;
  bool max;

public:
  static const gen_t NO_GEN = UINT64_MAX;

  ghobject_t()
    : generation(NO_GEN),
      shard_id(shard_id_t::NO_SHARD),
      max(false) {}

  explicit ghobject_t(const hobject_t &obj)
    : hobj(obj),
      generation(NO_GEN),
      shard_id(shard_id_t::NO_SHARD),
      max(false) {}

  ghobject_t(const hobject_t &obj, gen_t gen, shard_id_t shard)
    : hobj(obj),
      generation(gen),
      shard_id(shard),
      max(false) {}

  static ghobject_t make_pgmeta(int64_t pool, uint32_t hash, shard_id_t shard) {
    hobject_t h(object_t(), string(), CEPH_NOSNAP, hash, pool, string());
    return ghobject_t(h, NO_GEN, shard);
  }
  bool is_pgmeta() const {
    // make sure we are distinct from hobject_t(), which has pool INT64_MIN
    return hobj.pool >= 0 && hobj.oid.name.empty();
  }

  bool match(uint32_t bits, uint32_t match) const {
    return hobj.match_hash(hobj.hash, bits, match);
  }
  /// @return min ghobject_t ret s.t. ret.hash == this->hash
  ghobject_t get_boundary() const {
    if (hobj.is_max())
      return *this;
    ghobject_t ret;
    ret.hobj.set_hash(hobj.hash);
    ret.shard_id = shard_id;
    ret.hobj.pool = hobj.pool;
    return ret;
  }
  uint32_t get_nibblewise_key_u32() const {
    return hobj.get_nibblewise_key_u32();
  }
  uint32_t get_nibblewise_key() const {
    return hobj.get_nibblewise_key();
  }

  bool is_degenerate() const {
    return generation == NO_GEN && shard_id == shard_id_t::NO_SHARD;
  }

  bool is_no_gen() const {
    return generation == NO_GEN;
  }

  bool is_no_shard() const {
    return shard_id == shard_id_t::NO_SHARD;
  }

  void set_shard(shard_id_t s) {
    shard_id = s;
  }

  bool parse(const string& s);

  // maximum sorted value.
  static ghobject_t get_max() {
    ghobject_t h;
    h.max = true;
    h.hobj = hobject_t::get_max();  // so that is_max() => hobj.is_max()
    return h;
  }
  bool is_max() const {
    return max;
  }
  bool is_min() const {
    return *this == ghobject_t();
  }

  void swap(ghobject_t &o) {
    ghobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(json_spirit::Value& v);
  size_t encoded_size() const;
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ghobject_t*>& o);
  friend int cmp_nibblewise(const ghobject_t& l, const ghobject_t& r);
  friend int cmp_bitwise(const ghobject_t& l, const ghobject_t& r);
  friend bool operator==(const ghobject_t&, const ghobject_t&);
  friend bool operator!=(const ghobject_t&, const ghobject_t&);

  struct NibblewiseComparator {
    bool operator()(const ghobject_t& l, const ghobject_t& r) const {
      return cmp_nibblewise(l, r) < 0;
    }
  };

  struct BitwiseComparator {
    bool operator()(const ghobject_t& l, const ghobject_t& r) const {
      return cmp_bitwise(l, r) < 0;
    }
  };

  struct Comparator {
    bool bitwise;
    explicit Comparator(bool b) : bitwise(b) {}
    bool operator()(const ghobject_t& l, const ghobject_t& r) const {
         if (bitwise)
	return cmp_bitwise(l, r) < 0;
      else
	return cmp_nibblewise(l, r) < 0;
    }
  };
};
WRITE_CLASS_ENCODER(ghobject_t)

namespace std {
  template<> struct hash<ghobject_t> {
    size_t operator()(const ghobject_t &r) const {
      static hash<object_t> H;
      static rjhash<uint64_t> I;
      return H(r.hobj.oid) ^ I(r.hobj.snap);
    }
  };
} // namespace std

ostream& operator<<(ostream& out, const ghobject_t& o);

WRITE_EQ_OPERATORS_4(ghobject_t, max, shard_id, hobj, generation)

extern int cmp_nibblewise(const ghobject_t& l, const ghobject_t& r);
extern int cmp_bitwise(const ghobject_t& l, const ghobject_t& r);
static inline int cmp(const ghobject_t& l, const ghobject_t& r,
		      bool sort_bitwise) {
  if (sort_bitwise)
    return cmp_bitwise(l, r);
  else
    return cmp_nibblewise(l, r);
}

// these are convenient
static inline ghobject_t MAX_GHOBJ(const ghobject_t& l, const ghobject_t& r,
				   bool bitwise) {
  if (cmp(l, r, bitwise) >= 0)
    return l;
  else
    return r;
}

static inline ghobject_t MIN_GHOBJ(const ghobject_t& l, const ghobject_t& r,
				   bool bitwise) {
  if (cmp(l, r, bitwise) <= 0)
    return l;
  else
    return r;
}

#endif
