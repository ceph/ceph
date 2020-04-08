// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <limits>
#include <optional>
#include <ostream>

#include "crimson/os/seastore/onode_manager/staged-fltree/fwd.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree_types.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;
class key_view_t;
class key_hobj_t;
enum class KeyT { VIEW, HOBJ };
template <KeyT> struct _key_type;
template<> struct _key_type<KeyT::VIEW> { using type = key_view_t; };
template<> struct _key_type<KeyT::HOBJ> { using type = key_hobj_t; };
template <KeyT type>
using full_key_t = typename _key_type<type>::type;

// TODO: consider alignments
struct shard_pool_t {
  bool operator==(const shard_pool_t& x) const {
    return (shard == x.shard && pool == x.pool);
  }
  bool operator!=(const shard_pool_t& x) const { return !(*this == x); }

  template <KeyT KT>
  static shard_pool_t from_key(const full_key_t<KT>& key);

  shard_t shard;
  pool_t pool;
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const shard_pool_t& sp) {
  return os << (unsigned)sp.shard << "," << sp.pool;
}

struct crush_t {
  bool operator==(const crush_t& x) const { return crush == x.crush; }
  bool operator!=(const crush_t& x) const { return !(*this == x); }

  template <KeyT KT>
  static crush_t from_key(const full_key_t<KT>& key);

  crush_hash_t crush;
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const crush_t& c) {
  return os << c.crush;
}

struct shard_pool_crush_t {
  bool operator==(const shard_pool_crush_t& x) const {
    return (shard_pool == x.shard_pool && crush == x.crush);
  }
  bool operator!=(const shard_pool_crush_t& x) const { return !(*this == x); }

  template <KeyT KT>
  static shard_pool_crush_t from_key(const full_key_t<KT>& key);

  shard_pool_t shard_pool;
  crush_t crush;
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const shard_pool_crush_t& spc) {
  return os << spc.shard_pool << "," << spc.crush;
}

struct snap_gen_t {
  bool operator==(const snap_gen_t& x) const {
    return (snap == x.snap && gen == x.gen);
  }
  bool operator!=(const snap_gen_t& x) const { return !(*this == x); }

  template <KeyT KT>
  static snap_gen_t from_key(const full_key_t<KT>& key);

  snap_t snap;
  gen_t gen;
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const snap_gen_t& sg) {
  return os << sg.snap << "," << sg.gen;
}

struct string_key_view_t {
  enum class Type {MIN, STR, MAX};
  // presumably the maximum string length is 2KiB
  using string_size_t = uint16_t;
  string_key_view_t(const char* p_end) {
    p_length = p_end - sizeof(string_size_t);
    std::memcpy(&length, p_length, sizeof(string_size_t));
    if (length && length != std::numeric_limits<string_size_t>::max()) {
      auto _p_key = p_length - length;
      p_key = static_cast<const char*>(_p_key);
    } else {
      p_key = nullptr;
    }
  }
  Type type() const {
    if (length == 0u) {
      return Type::MIN;
    } else if (length == std::numeric_limits<string_size_t>::max()) {
      return Type::MAX;
    } else {
      return Type::STR;
    }
  }
  const char* p_start() const {
    if (p_key) {
      return p_key;
    } else {
      return p_length;
    }
  }
  const char* p_next_end() const {
    if (p_key) {
      return p_start();
    } else {
      return p_length + sizeof(string_size_t);
    }
  }
  size_t size() const { return length + sizeof(string_size_t); }
  bool operator==(const string_key_view_t& x) const {
    if (type() == x.type() && type() != Type::STR)
      return true;
    if (type() != x.type())
      return false;
    if (length != x.length)
      return false;
    return (memcmp(p_key, x.p_key, length) == 0);
  }
  bool operator!=(const string_key_view_t& x) const { return !(*this == x); }

  static void append_str(
      NodeExtentMutable&, const char* data, size_t len, char*& p_append);

  static void append_str(const char* data, size_t len, char*& p_append) {
    p_append -= sizeof(string_size_t);
    assert(len < std::numeric_limits<string_size_t>::max());
    string_size_t _len = len;
    std::memcpy(p_append, &_len, sizeof(string_size_t));
    p_append -= len;
    std::memcpy(p_append, data, len);
  }

  static void append_str(NodeExtentMutable& mut,
                         const std::string& str,
                         char*& p_append) {
    append_str(mut, str.data(), str.length(), p_append);
  }

  static void append_str(NodeExtentMutable& mut,
                         const string_key_view_t& view,
                         char*& p_append) {
    assert(view.type() == Type::STR);
    append_str(mut, view.p_key, view.length, p_append);
  }

  static void append_str(const std::string& str, char*& p_append) {
    append_str(str.data(), str.length(), p_append);
  }

  static void append_dedup(
      NodeExtentMutable&, const Type& dedup_type, char*& p_append);

  static void append_dedup(const Type& dedup_type, char*& p_append) {
    p_append -= sizeof(string_size_t);
    string_size_t len;
    if (dedup_type == Type::MIN) {
      len = 0u;
    } else if (dedup_type == Type::MAX) {
      len = std::numeric_limits<string_size_t>::max();
    } else {
      assert(false);
    }
    std::memcpy(p_append, &len, sizeof(string_size_t));
  }

  const char* p_key;
  const char* p_length;
  // TODO: remove if p_length is aligned
  string_size_t length;

  friend std::ostream& operator<<(std::ostream&, const string_key_view_t&);
};
inline MatchKindCMP compare_to(const string_key_view_t& l, const string_key_view_t& r) {
  using Type = string_key_view_t::Type;
  auto l_type = l.type();
  auto r_type = r.type();
  if (l_type == Type::STR && r_type == Type::STR) {
    return toMatchKindCMP(l.p_key, l.length, r.p_key, r.length);
  } else if (l_type == r_type) {
    return MatchKindCMP::EQ;
  } else if (l_type == Type::MIN || r_type == Type::MAX) {
    return MatchKindCMP::NE;
  } else { // l_type == Type::MAX || r_type == Type::MIN
    return MatchKindCMP::PO;
  }
}
inline MatchKindCMP compare_to(const std::string& key, const string_key_view_t& target) {
  assert(key.length());
  if (target.type() == string_key_view_t::Type::MIN) {
    return MatchKindCMP::PO;
  } else if (target.type() == string_key_view_t::Type::MAX) {
    return MatchKindCMP::NE;
  } else {
    return toMatchKindCMP(key, target.p_key, target.length);
  }
}
inline MatchKindCMP compare_to(const string_key_view_t& key, const std::string& target) {
  return reverse(compare_to(target, key));
}
inline MatchKindCMP compare_to(const std::string& key, const std::string& target) {
  return toMatchKindCMP(key, target);
}

inline std::ostream& operator<<(std::ostream& os, const string_key_view_t& view) {
  auto type = view.type();
  if (type == string_key_view_t::Type::MIN) {
    return os << "MIN";
  } else if (type == string_key_view_t::Type::MAX) {
    return os << "MAX";
  } else {
    if (view.length <= 12) {
      os << "\"" << std::string(view.p_key, 0, view.length) << "\"";
    } else {
      os << "\"" << std::string(view.p_key, 0, 4) << ".."
         << std::string(view.p_key + view.length - 2, 0, 2)
         << "/" << view.length << "B\"";
    }
    return os;
  }
}

struct ns_oid_view_t {
  using string_size_t = string_key_view_t::string_size_t;
  using Type = string_key_view_t::Type;

  ns_oid_view_t(const char* p_end) : nspace(p_end), oid(nspace.p_next_end()) {}
  Type type() const { return oid.type(); }
  const char* p_start() const { return oid.p_start(); }
  size_t size() const {
    if (type() == Type::STR) {
      return nspace.size() + oid.size();
    } else {
      return sizeof(string_size_t);
    }
  }
  bool operator==(const ns_oid_view_t& x) const {
    return (nspace == x.nspace && oid == x.oid);
  }
  bool operator!=(const ns_oid_view_t& x) const { return !(*this == x); }

  template <KeyT KT>
  static node_offset_t estimate_size(const full_key_t<KT>& key);

  template <KeyT KT>
  static void append(NodeExtentMutable&,
                     const full_key_t<KT>& key,
                     char*& p_append);

  static void append(NodeExtentMutable& mut,
                     const ns_oid_view_t& view,
                     char*& p_append) {
    if (view.type() == Type::STR) {
      string_key_view_t::append_str(mut, view.nspace, p_append);
      string_key_view_t::append_str(mut, view.oid, p_append);
    } else {
      string_key_view_t::append_dedup(mut, view.type(), p_append);
    }
  }

  template <KeyT KT>
  static void append(const full_key_t<KT>& key, char*& p_append);

  string_key_view_t nspace;
  string_key_view_t oid;
};
inline std::ostream& operator<<(std::ostream& os, const ns_oid_view_t& ns_oid) {
  return os << ns_oid.nspace << "," << ns_oid.oid;
}

class key_hobj_t {
 public:
  explicit key_hobj_t(const onode_key_t& key) : key{key} {}

  /*
   * common interface as full_key_t
   */
  shard_t shard() const {
    return key.shard;
  }
  pool_t pool() const {
    return key.pool;
  }
  crush_hash_t crush() const {
    return key.crush;
  }
  const std::string& nspace() const {
    return key.nspace;
  }
  const std::string& oid() const {
    return key.oid;
  }
  ns_oid_view_t::Type dedup_type() const {
    return _dedup_type;
  }
  snap_t snap() const {
    return key.snap;
  }
  gen_t gen() const {
    return key.gen;
  }

  bool operator==(const full_key_t<KeyT::VIEW>& o) const;
  bool operator==(const full_key_t<KeyT::HOBJ>& o) const;
  bool operator!=(const full_key_t<KeyT::VIEW>& o) const {
    return !operator==(o);
  }
  bool operator!=(const full_key_t<KeyT::HOBJ>& o) const {
    return !operator==(o);
  }

 private:
  ns_oid_view_t::Type _dedup_type = ns_oid_view_t::Type::STR;
  onode_key_t key;
};
inline std::ostream& operator<<(std::ostream& os, const key_hobj_t& key) {
  return os << "key_hobj("
            << (unsigned)key.shard() << ","
            << key.pool() << "," << key.crush() << "; \""
            << key.nspace() << "\",\"" << key.oid() << "\"; "
            << key.snap() << "," << key.gen() << ")";
}

class key_view_t {
 public:
  /*
   * common interface as full_key_t
   */
  shard_t shard() const {
    return shard_pool_packed().shard;
  }
  pool_t pool() const {
    return shard_pool_packed().pool;
  }
  crush_hash_t crush() const {
    return crush_packed().crush;
  }
  const string_key_view_t& nspace() const {
    return ns_oid_view().nspace;
  }
  const string_key_view_t& oid() const {
    return ns_oid_view().oid;
  }
  ns_oid_view_t::Type dedup_type() const {
    return ns_oid_view().type();
  }
  snap_t snap() const {
    return snap_gen_packed().snap;
  }
  gen_t gen() const {
    return snap_gen_packed().gen;
  }

  bool operator==(const full_key_t<KeyT::VIEW>& o) const;
  bool operator==(const full_key_t<KeyT::HOBJ>& o) const;
  bool operator!=(const full_key_t<KeyT::VIEW>& o) const {
    return !operator==(o);
  }
  bool operator!=(const full_key_t<KeyT::HOBJ>& o) const {
    return !operator==(o);
  }

  /*
   * key_view_t specific interfaces
   */

  bool has_shard_pool() const {
    return p_shard_pool != nullptr;
  }
  bool has_crush() const {
    return p_crush != nullptr;
  }
  bool has_ns_oid() const {
    return p_ns_oid.has_value();
  }
  bool has_snap_gen() const {
    return p_snap_gen != nullptr;
  }

  const shard_pool_t& shard_pool_packed() const {
    assert(has_shard_pool());
    return *p_shard_pool;
  }
  const crush_t& crush_packed() const {
    assert(has_crush());
    return *p_crush;
  }
  const ns_oid_view_t& ns_oid_view() const {
    assert(has_ns_oid());
    return *p_ns_oid;
  }
  const snap_gen_t& snap_gen_packed() const {
    assert(has_snap_gen());
    return *p_snap_gen;
  }

  void set(const crush_t& key) {
    assert(!has_crush());
    p_crush = &key;
  }
  void set(const shard_pool_crush_t& key) {
    set(key.crush);
    assert(!has_shard_pool());
    p_shard_pool = &key.shard_pool;
  }
  void set(const ns_oid_view_t& key) {
    assert(!has_ns_oid());
    p_ns_oid = key;
  }
  void set(const snap_gen_t& key) {
    assert(!has_snap_gen());
    p_snap_gen = &key;
  }

 private:
  const shard_pool_t* p_shard_pool = nullptr;
  const crush_t* p_crush = nullptr;
  std::optional<ns_oid_view_t> p_ns_oid;
  const snap_gen_t* p_snap_gen = nullptr;
};

template <KeyT TypeL, KeyT TypeR>
bool compare_full_key(const full_key_t<TypeL>& l, const full_key_t<TypeR>& r) {
  if (l.shard() != r.shard())
    return false;
  if (l.pool() != r.pool())
    return false;
  if (l.crush() != r.crush())
    return false;
  if (compare_to(l.nspace(), r.nspace()) != MatchKindCMP::EQ)
    return false;
  if (compare_to(l.oid(), r.oid()) != MatchKindCMP::EQ)
    return false;
  if (l.snap() != r.snap())
    return false;
  if (l.gen() != r.gen())
    return false;
  return true;
}

inline bool key_hobj_t::operator==(const full_key_t<KeyT::VIEW>& o) const {
  return compare_full_key<KeyT::HOBJ, KeyT::VIEW>(*this, o);
}
inline bool key_hobj_t::operator==(const full_key_t<KeyT::HOBJ>& o) const {
  return compare_full_key<KeyT::HOBJ, KeyT::HOBJ>(*this, o);
}
inline bool key_view_t::operator==(const full_key_t<KeyT::VIEW>& o) const {
  return compare_full_key<KeyT::VIEW, KeyT::VIEW>(*this, o);
}
inline bool key_view_t::operator==(const full_key_t<KeyT::HOBJ>& o) const {
  return compare_full_key<KeyT::VIEW, KeyT::HOBJ>(*this, o);
}

inline std::ostream& operator<<(std::ostream& os, const key_view_t& key) {
  os << "key_view(";
  if (key.has_shard_pool()) {
    os << (unsigned)key.shard() << "," << key.pool() << ",";
  } else {
    os << "X,X,";
  }
  if (key.has_crush()) {
    os << key.crush() << "; ";
  } else {
    os << "X; ";
  }
  if (key.has_ns_oid()) {
    os << key.nspace() << "," << key.oid() << "; ";
  } else {
    os << "X,X; ";
  }
  if (key.has_snap_gen()) {
    os << key.snap() << "," << key.gen() << ")";
  } else {
    os << "X,X)";
  }
  return os;
}

template <KeyT Type>
MatchKindCMP compare_to(const full_key_t<Type>& key, const shard_pool_t& target) {
  auto ret = toMatchKindCMP(key.shard(), target.shard);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(key.pool(), target.pool);
}

template <KeyT Type>
MatchKindCMP compare_to(const full_key_t<Type>& key, const crush_t& target) {
  return toMatchKindCMP(key.crush(), target.crush);
}

template <KeyT Type>
MatchKindCMP compare_to(const full_key_t<Type>& key, const shard_pool_crush_t& target) {
  auto ret = compare_to<Type>(key, target.shard_pool);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to<Type>(key, target.crush);
}

template <KeyT Type>
MatchKindCMP compare_to(const full_key_t<Type>& key, const ns_oid_view_t& target) {
  auto ret = compare_to(key.nspace(), target.nspace);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to(key.oid(), target.oid);
}

template <KeyT Type>
MatchKindCMP compare_to(const full_key_t<Type>& key, const snap_gen_t& target) {
  auto ret = toMatchKindCMP(key.snap(), target.snap);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(key.gen(), target.gen);
}

template <KeyT KT>
shard_pool_t shard_pool_t::from_key(const full_key_t<KT>& key) {
  if constexpr (KT == KeyT::VIEW) {
    return key.shard_pool_packed();
  } else {
    return {key.shard(), key.pool()};
  }
}

template <KeyT KT>
crush_t crush_t::from_key(const full_key_t<KT>& key) {
  if constexpr (KT == KeyT::VIEW) {
    return key.crush_packed();
  } else {
    return {key.crush()};
  }
}

template <KeyT KT>
shard_pool_crush_t shard_pool_crush_t::from_key(const full_key_t<KT>& key) {
  return {shard_pool_t::from_key<KT>(key), crush_t::from_key<KT>(key)};
}

template <KeyT KT>
snap_gen_t snap_gen_t::from_key(const full_key_t<KT>& key) {
  if constexpr (KT == KeyT::VIEW) {
    return key.snap_gen_packed();
  } else {
    return {key.snap(), key.gen()};
  }
}

template <KeyT KT>
node_offset_t ns_oid_view_t::estimate_size(const full_key_t<KT>& key) {
  if constexpr (KT == KeyT::VIEW) {
    return key.ns_oid_view().size();
  } else {
    if (key.dedup_type() != Type::STR) {
      // size after deduplication
      return sizeof(string_size_t);
    } else {
      return 2 * sizeof(string_size_t) + key.nspace().size() + key.oid().size();
    }
  }
}

template <KeyT KT>
void ns_oid_view_t::append(
    NodeExtentMutable& mut, const full_key_t<KT>& key, char*& p_append) {
  if (key.dedup_type() == Type::STR) {
    string_key_view_t::append_str(mut, key.nspace(), p_append);
    string_key_view_t::append_str(mut, key.oid(), p_append);
  } else {
    string_key_view_t::append_dedup(mut, key.dedup_type(), p_append);
  }
}

template <KeyT KT>
void ns_oid_view_t::append(const full_key_t<KT>& key, char*& p_append) {
  if (key.dedup_type() == Type::STR) {
    string_key_view_t::append_str(key.nspace(), p_append);
    string_key_view_t::append_str(key.oid(), p_append);
  } else {
    string_key_view_t::append_dedup(key.dedup_type(), p_append);
  }
}

}
