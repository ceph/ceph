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

#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "include/types.h"

#include "json_spirit/json_spirit_value.h"
#include "include/ceph_assert.h"   // spirit clobbers it!

#include "reverse.h"

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
public:
  static const int64_t POOL_META = -1;
  static const int64_t POOL_TEMP_START = -2; // and then negative

  static bool is_temp_pool(int64_t pool) {
    return pool <= POOL_TEMP_START;
  }
  static int64_t get_temp_pool(int64_t pool) {
    return POOL_TEMP_START - pool;
  }
  static bool is_meta_pool(int64_t pool) {
    return pool == POOL_META;
  }

public:
  object_t oid;
  snapid_t snap;
private:
  uint32_t hash;
  bool max;
  uint32_t nibblewise_key_cache;
  uint32_t hash_reverse_bits;
public:
  int64_t pool;
  std::string nspace;

private:
  std::string key;

  class hobject_t_max {};

public:
  const std::string& get_key() const {
    return key;
  }

  void set_key(const std::string& key_) {
    if (key_ == oid.name)
      key.clear();
    else
      key = key_;
  }

  std::string to_str() const;
  
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
    return is_temp_pool(pool) && pool != INT64_MIN;
  }
  bool is_meta() const {
    return is_meta_pool(pool);
  }
  int64_t get_logical_pool() const {
    if (is_temp_pool(pool))
      return get_temp_pool(pool);  // it's reversible
    else
      return pool;
  }

  hobject_t() : snap(0), hash(0), max(false), pool(INT64_MIN) {
    build_hash_cache();
  }

  hobject_t(const hobject_t &rhs) = default;
  hobject_t(hobject_t &&rhs) = default;
  hobject_t(hobject_t_max &&singleton) : hobject_t() {
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

  hobject_t(const object_t& oid, const std::string& key, snapid_t snap,
            uint32_t hash, int64_t pool, const std::string& nspace)
    : oid(oid), snap(snap), hash(hash), max(false),
      pool(pool), nspace(nspace),
      key(oid.name == key ? std::string() : key) {
    build_hash_cache();
  }

  hobject_t(const sobject_t &soid, const std::string &key, uint32_t hash,
	    int64_t pool, const std::string& nspace)
    : oid(soid.oid), snap(soid.snap), hash(hash), max(false),
      pool(pool), nspace(nspace),
      key(soid.oid.name == key ? std::string() : key) {
    build_hash_cache();
  }

  // used by Crimson
  hobject_t(const std::string &key, snapid_t snap, uint32_t reversed_hash,
            int64_t pool, const std::string& nspace)
    : oid(key), snap(snap), max(false), pool(pool), nspace(nspace) {
    set_bitwise_key_u32(reversed_hash);
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

  /// @return min hobject_t ret s.t. ret.get_head() == get_head()
  hobject_t get_object_boundary() const {
    if (is_max())
      return *this;
    hobject_t ret = *this;
    ret.snap = 0;
    return ret;
  }

  /// @return max hobject_t ret s.t. ret.get_head() == get_head()
  hobject_t get_max_object_boundary() const {
    if (is_max())
      return *this;
    // CEPH_SNAPDIR happens to sort above HEAD and MAX_SNAP and is no longer used
    // for actual objects
    return get_snapdir();
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
    ceph_assert(!max || (*this == hobject_t(hobject_t::get_max())));
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
    return reverse_bits(v);
  }
  static uint32_t _reverse_nibbles(uint32_t retval) {
    return reverse_nibbles(retval);
  }

  /**
   * Returns set S of strings such that for any object
   * h where h.match(bits, mask), there is some string
   * s \f$\in\f$ S such that s is a prefix of h.to_str().
   * Furthermore, for any s \f$\in\f$ S, s is a prefix of
   * h.str() implies that h.match(bits, mask).
   */
  static std::set<std::string> get_prefixes(
    uint32_t bits,
    uint32_t mask,
    int64_t pool);

  // filestore nibble-based key
  uint32_t get_nibblewise_key_u32() const {
    ceph_assert(!max);
    return nibblewise_key_cache;
  }
  uint64_t get_nibblewise_key() const {
    return max ? 0x100000000ull : nibblewise_key_cache;
  }

  // newer bit-reversed key
  uint32_t get_bitwise_key_u32() const {
    ceph_assert(!max);
    return hash_reverse_bits;
  }
  uint64_t get_bitwise_key() const {
    return max ? 0x100000000ull : hash_reverse_bits;
  }

  // please remember to update set_bitwise_key_u32() also
  // once you change build_hash_cache()
  void build_hash_cache() {
    nibblewise_key_cache = _reverse_nibbles(hash);
    hash_reverse_bits = _reverse_bits(hash);
  }
  void set_bitwise_key_u32(uint32_t value) {
    hash = _reverse_bits(value);
    // below is identical to build_hash_cache() and shall be
    // updated correspondingly if you change build_hash_cache() 
    nibblewise_key_cache = _reverse_nibbles(hash);
    hash_reverse_bits = value;
  }

  const std::string& get_effective_key() const {
    if (key.length())
      return key;
    return oid.name;
  }

  hobject_t make_temp_hobject(const std::string& name) const {
    return hobject_t(object_t(name), "", CEPH_NOSNAP,
		     hash,
		     get_temp_pool(pool),
		     "");
  }

  void swap(hobject_t &o) {
    hobject_t temp(o);
    o = (*this);
    (*this) = temp;
  }

  const std::string &get_namespace() const {
    return nspace;
  }

  bool parse(const std::string& s);

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::bufferlist::const_iterator& bl);
  void decode(json_spirit::Value& v);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<hobject_t*>& o);
  friend int cmp(const hobject_t& l, const hobject_t& r);
  auto operator<=>(const hobject_t &rhs) const noexcept {
    auto cmp = max <=> rhs.max;
    if (cmp != 0) return cmp;
    cmp = pool <=> rhs.pool;
    if (cmp != 0) return cmp;
    cmp = get_bitwise_key() <=> rhs.get_bitwise_key();
    if (cmp != 0) return cmp;
    cmp = nspace <=> rhs.nspace;
    if (cmp != 0) return cmp;
    if (!(get_key().empty() && rhs.get_key().empty())) {
      cmp = get_effective_key() <=> rhs.get_effective_key();
      if (cmp != 0) return cmp;
    }
    cmp = oid <=> rhs.oid;
    if (cmp != 0) return cmp;
    return snap <=> rhs.snap;
  }
  bool operator==(const hobject_t& rhs) const noexcept {
    return operator<=>(rhs) == 0;
  }
  friend struct ghobject_t;
};
WRITE_CLASS_ENCODER(hobject_t)

namespace std {
template<> struct hash<hobject_t> {
  size_t operator()(const hobject_t &r) const {
    static rjhash<uint64_t> RJ;
    return RJ(r.get_hash() ^ r.snap);
  }
};
} // namespace std

std::ostream& operator<<(std::ostream& out, const hobject_t& o);

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

extern int cmp(const hobject_t& l, const hobject_t& r);
template <typename T>
static inline int cmp(const hobject_t &l, const T&) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return l.is_max() ? 0 : -1;
}
template <typename T>
static inline int cmp(const T&, const hobject_t&r) {
  static_assert(always_false<T>::value::value, "Do not compare to get_max()");
  return r.is_max() ? 0 : 1;
}



typedef version_t gen_t;

struct ghobject_t {
  static const gen_t NO_GEN = UINT64_MAX;

  bool max = false;
  shard_id_t shard_id = shard_id_t::NO_SHARD;
  hobject_t hobj;
  gen_t generation = NO_GEN;

  ghobject_t() = default;

  explicit ghobject_t(const hobject_t &obj)
    : hobj(obj) {}

  ghobject_t(const hobject_t &obj, gen_t gen, shard_id_t shard)
    : shard_id(shard),
      hobj(obj),
      generation(gen) {}

  // used by Crimson
  ghobject_t(shard_id_t shard, int64_t pool, uint32_t reversed_hash,
             const std::string& nspace, const std::string& oid,
             snapid_t snap, gen_t gen)
    : shard_id(shard),
      hobj(oid, snap, reversed_hash, pool, nspace),
      generation(gen) {}

  static ghobject_t make_pgmeta(int64_t pool, uint32_t hash, shard_id_t shard) {
    hobject_t h(object_t(), std::string(), CEPH_NOSNAP, hash, pool, std::string());
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

  bool parse(const std::string& s);

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

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void decode(json_spirit::Value& v);
  size_t encoded_size() const;
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ghobject_t*>& o);
  friend int cmp(const ghobject_t& l, const ghobject_t& r);
  auto operator<=>(const ghobject_t&) const = default;
  bool operator==(const ghobject_t&) const = default;
};
WRITE_CLASS_ENCODER(ghobject_t)

namespace std {
  template<> struct hash<ghobject_t> {
    size_t operator()(const ghobject_t &r) const {
      static rjhash<uint64_t> RJ;
      static hash<hobject_t> HO;
      size_t hash = HO(r.hobj);
      hash = RJ(hash ^ r.generation);
      hash = hash ^ r.shard_id.id;
      return hash;
    }
  };
} // namespace std

std::ostream& operator<<(std::ostream& out, const ghobject_t& o);

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<ghobject_t> : fmt::ostream_formatter {};
#endif

extern int cmp(const ghobject_t& l, const ghobject_t& r);


#endif
