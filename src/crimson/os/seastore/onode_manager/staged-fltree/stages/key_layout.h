// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <limits>
#include <optional>
#include <ostream>

#include "common/hobject.h"
#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fwd.h"

namespace crimson::os::seastore::onode {

using shard_t = int8_t;
using pool_t = int64_t;
using crush_hash_t = uint32_t;
using snap_t = uint64_t;
using gen_t = uint64_t;
static_assert(sizeof(shard_t) == sizeof(ghobject_t().shard_id.id));
static_assert(sizeof(pool_t) == sizeof(ghobject_t().hobj.pool));
static_assert(sizeof(crush_hash_t) == sizeof(ghobject_t().hobj.get_hash()));
static_assert(sizeof(snap_t) == sizeof(ghobject_t().hobj.snap.val));
static_assert(sizeof(gen_t) == sizeof(ghobject_t().generation));

constexpr auto MAX_SHARD = std::numeric_limits<shard_t>::max();
constexpr auto MAX_POOL = std::numeric_limits<pool_t>::max();
constexpr auto MAX_CRUSH = std::numeric_limits<crush_hash_t>::max();
constexpr auto MAX_SNAP = std::numeric_limits<snap_t>::max();
constexpr auto MAX_GEN = std::numeric_limits<gen_t>::max();

class NodeExtentMutable;
class key_view_t;
class key_hobj_t;
enum class KeyT { VIEW, HOBJ };
template <KeyT> struct _full_key_type;
template<> struct _full_key_type<KeyT::VIEW> { using type = key_view_t; };
template<> struct _full_key_type<KeyT::HOBJ> { using type = key_hobj_t; };
template <KeyT type>
using full_key_t = typename _full_key_type<type>::type;

struct node_offset_packed_t {
  node_offset_t value;
} __attribute__((packed));

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
  return os << (int)sp.shard << "," << sp.pool;
}
inline MatchKindCMP compare_to(const shard_pool_t& l, const shard_pool_t& r) {
  auto ret = toMatchKindCMP(l.shard, r.shard);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(l.pool, r.pool);
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
inline MatchKindCMP compare_to(const crush_t& l, const crush_t& r) {
  return toMatchKindCMP(l.crush, r.crush);
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
inline MatchKindCMP compare_to(
    const shard_pool_crush_t& l, const shard_pool_crush_t& r) {
  auto ret = compare_to(l.shard_pool, r.shard_pool);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to(l.crush, r.crush);
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
inline MatchKindCMP compare_to(const snap_gen_t& l, const snap_gen_t& r) {
  auto ret = toMatchKindCMP(l.snap, r.snap);
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(l.gen, r.gen);
}

/**
 * string_key_view_t
 *
 * The layout to store char array as an oid or an ns string which may be
 * compressed.
 *
 * (TODO) If compressed, the physical block only stores an unsigned int of
 * string_size_t, with value MARKER_MIN denoting Type::MIN, and value
 * MARKER_MAX denoting Type::MAX.
 *
 * If not compressed (Type::STR), the physical block stores the char array and
 * a valid string_size_t value.
 */
struct string_key_view_t {
  enum class Type {MIN, STR, MAX};
  static constexpr auto MARKER_MAX = std::numeric_limits<string_size_t>::max();
  static constexpr auto MARKER_MIN = std::numeric_limits<string_size_t>::max() - 1;
  static constexpr auto VALID_UPPER_BOUND = std::numeric_limits<string_size_t>::max() - 2;
  static bool is_valid_size(size_t size) {
    return size <= VALID_UPPER_BOUND;
  }

  string_key_view_t(const char* p_end) {
    p_length = p_end - sizeof(string_size_t);
    std::memcpy(&length, p_length, sizeof(string_size_t));
    if (is_valid_size(length)) {
      auto _p_key = p_length - length;
      p_key = static_cast<const char*>(_p_key);
    } else {
      assert(length == MARKER_MAX || length == MARKER_MIN);
      p_key = nullptr;
    }
  }
  Type type() const {
    if (length == MARKER_MIN) {
      return Type::MIN;
    } else if (length == MARKER_MAX) {
      return Type::MAX;
    } else {
      assert(is_valid_size(length));
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
  node_offset_t size() const {
    size_t ret = length + sizeof(string_size_t);
    assert(ret < MAX_NODE_SIZE);
    return ret;
  }
  node_offset_t size_logical() const {
    assert(type() == Type::STR);
    assert(is_valid_size(length));
    return length;
  }
  node_offset_t size_overhead() const {
    assert(type() == Type::STR);
    return sizeof(string_size_t);
  }

  std::string_view to_string_view() const {
    assert(type() == Type::STR);
    assert(is_valid_size(length));
    return {p_key, length};
  }
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

  void reset_to(const char* origin_base,
                const char* new_base,
                extent_len_t node_size) {
    reset_ptr(p_key, origin_base, new_base, node_size);
    reset_ptr(p_length, origin_base, new_base, node_size);
#ifndef NDEBUG
    string_size_t current_length;
    std::memcpy(&current_length, p_length, sizeof(string_size_t));
    assert(length == current_length);
#endif
  }

  static void append_str(
      NodeExtentMutable&, std::string_view, char*& p_append);

  static void test_append_str(std::string_view str, char*& p_append) {
    assert(is_valid_size(str.length()));
    p_append -= sizeof(string_size_t);
    string_size_t len = str.length();
    std::memcpy(p_append, &len, sizeof(string_size_t));
    p_append -= len;
    std::memcpy(p_append, str.data(), len);
  }

  static void append_dedup(
      NodeExtentMutable&, const Type& dedup_type, char*& p_append);

  static void test_append_dedup(const Type& dedup_type, char*& p_append) {
    p_append -= sizeof(string_size_t);
    string_size_t len;
    if (dedup_type == Type::MIN) {
      len = MARKER_MIN;
    } else if (dedup_type == Type::MAX) {
      len = MARKER_MAX;
    } else {
      ceph_abort("impossible path");
    }
    std::memcpy(p_append, &len, sizeof(string_size_t));
  }

  const char* p_key;
  const char* p_length;
  // TODO: remove if p_length is aligned
  string_size_t length;
};

/**
 * string_view_masked_t
 *
 * A common class to hide the underlying string implementation regardless of a
 * string_key_view_t (maybe compressed), a string/string_view, or a compressed
 * string. And leverage this consistant class to do compare, print, convert and
 * append operations.
 */
class string_view_masked_t {
 public:
  using Type = string_key_view_t::Type;
  explicit string_view_masked_t(const string_key_view_t& index)
      : type{index.type()} {
    if (type == Type::STR) {
      view = index.to_string_view();
    }
  }
  explicit string_view_masked_t(std::string_view str)
      : type{Type::STR}, view{str} {
    assert(string_key_view_t::is_valid_size(view.size()));
  }

  Type get_type() const { return type; }
  std::string_view to_string_view() const {
    assert(get_type() == Type::STR);
    return view;
  }
  string_size_t size() const {
    assert(get_type() == Type::STR);
    assert(string_key_view_t::is_valid_size(view.size()));
    return view.size();
  }
  bool operator==(const string_view_masked_t& x) const {
    if (get_type() == x.get_type() && get_type() != Type::STR)
      return true;
    if (get_type() != x.get_type())
      return false;
    if (size() != x.size())
      return false;
    return (memcmp(view.data(), x.view.data(), size()) == 0);
  }
  bool operator!=(const string_view_masked_t& x) const { return !(*this == x); }
  void encode(ceph::bufferlist& bl) const {
    if (get_type() == Type::MIN) {
      ceph::encode(string_key_view_t::MARKER_MIN, bl);
    } else if (get_type() == Type::MAX) {
      ceph::encode(string_key_view_t::MARKER_MAX, bl);
    } else {
      ceph::encode(size(), bl);
      ceph::encode_nohead(view, bl);
    }
  }
  static auto min() { return string_view_masked_t{Type::MIN}; }
  static auto max() { return string_view_masked_t{Type::MAX}; }
  static string_view_masked_t decode(
      std::string& str_storage, ceph::bufferlist::const_iterator& delta) {
    string_size_t size;
    ceph::decode(size, delta);
    if (size == string_key_view_t::MARKER_MIN) {
      return min();
    } else if (size == string_key_view_t::MARKER_MAX) {
      return max();
    } else {
      ceph::decode_nohead(size, str_storage, delta);
      return string_view_masked_t(str_storage);
    }
  }

 private:
  explicit string_view_masked_t(Type type)
      : type{type} {}

  Type type;
  std::string_view view;
};
inline MatchKindCMP compare_to(const string_view_masked_t& l, const string_view_masked_t& r) {
  using Type = string_view_masked_t::Type;
  auto l_type = l.get_type();
  auto r_type = r.get_type();
  if (l_type == Type::STR && r_type == Type::STR) {
    assert(string_key_view_t::is_valid_size(l.size()));
    assert(string_key_view_t::is_valid_size(r.size()));
    return toMatchKindCMP(l.to_string_view(), r.to_string_view());
  } else if (l_type == r_type) {
    return MatchKindCMP::EQ;
  } else if (l_type == Type::MIN || r_type == Type::MAX) {
    return MatchKindCMP::LT;
  } else { // l_type == Type::MAX || r_type == Type::MIN
    return MatchKindCMP::GT;
  }
}
inline MatchKindCMP compare_to(std::string_view l, const string_view_masked_t& r) {
  using Type = string_view_masked_t::Type;
  assert(string_key_view_t::is_valid_size(l.size()));
  auto r_type = r.get_type();
  if (r_type == Type::MIN) {
    return MatchKindCMP::GT;
  } else if (r_type == Type::MAX) {
    return MatchKindCMP::LT;
  } else { // r_type == Type::STR
    assert(string_key_view_t::is_valid_size(r.size()));
    return toMatchKindCMP(l, r.to_string_view());
  }
}
inline MatchKindCMP compare_to(const string_view_masked_t& l, std::string_view r) {
  return reverse(compare_to(r, l));
}
inline std::ostream& operator<<(std::ostream& os, const string_view_masked_t& masked) {
  using Type = string_view_masked_t::Type;
  auto type = masked.get_type();
  if (type == Type::MIN) {
    return os << "MIN";
  } else if (type == Type::MAX) {
    return os << "MAX";
  } else { // type == Type::STR
    auto view = masked.to_string_view();
    if (view.length() <= 12) {
      os << "\"" << view << "\"";
    } else {
      os << "\"" << std::string_view(view.data(), 4) << ".."
         << std::string_view(view.data() + view.length() - 2, 2)
         << "/" << view.length() << "B\"";
    }
    return os;
  }
}

struct ns_oid_view_t {
  using Type = string_key_view_t::Type;

  ns_oid_view_t(const char* p_end) : nspace(p_end), oid(nspace.p_next_end()) {}
  Type type() const { return oid.type(); }
  const char* p_start() const { return oid.p_start(); }
  node_offset_t size() const {
    if (type() == Type::STR) {
      size_t ret = nspace.size() + oid.size();
      assert(ret < MAX_NODE_SIZE);
      return ret;
    } else {
      return sizeof(string_size_t);
    }
  }
  node_offset_t size_logical() const {
    assert(type() == Type::STR);
    return nspace.size_logical() + oid.size_logical();
  }
  node_offset_t size_overhead() const {
    assert(type() == Type::STR);
    return nspace.size_overhead() + oid.size_overhead();
  }
  bool operator==(const ns_oid_view_t& x) const {
    return (string_view_masked_t{nspace} == string_view_masked_t{x.nspace} &&
            string_view_masked_t{oid} == string_view_masked_t{x.oid});
  }
  bool operator!=(const ns_oid_view_t& x) const { return !(*this == x); }

  void reset_to(const char* origin_base,
                const char* new_base,
                extent_len_t node_size) {
    nspace.reset_to(origin_base, new_base, node_size);
    oid.reset_to(origin_base, new_base, node_size);
  }

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
      string_key_view_t::append_str(mut, view.nspace.to_string_view(), p_append);
      string_key_view_t::append_str(mut, view.oid.to_string_view(), p_append);
    } else {
      string_key_view_t::append_dedup(mut, view.type(), p_append);
    }
  }

  template <KeyT KT>
  static void test_append(const full_key_t<KT>& key, char*& p_append);

  string_key_view_t nspace;
  string_key_view_t oid;
};
inline std::ostream& operator<<(std::ostream& os, const ns_oid_view_t& ns_oid) {
  return os << string_view_masked_t{ns_oid.nspace} << ","
            << string_view_masked_t{ns_oid.oid};
}
inline MatchKindCMP compare_to(const ns_oid_view_t& l, const ns_oid_view_t& r) {
  auto ret = compare_to(string_view_masked_t{l.nspace},
                        string_view_masked_t{r.nspace});
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to(string_view_masked_t{l.oid},
                    string_view_masked_t{r.oid});
}

inline const ghobject_t _MIN_OID() {
  assert(ghobject_t().is_min());
  // don't extern _MIN_OID
  return ghobject_t();
}

/*
 * Unfortunally the ghobject_t representitive as tree key doesn't have max
 * field, so we define our own _MAX_OID and translate it from/to
 * ghobject_t::get_max() if necessary.
 */
inline const ghobject_t _MAX_OID() {
  return ghobject_t(shard_id_t(MAX_SHARD), MAX_POOL, MAX_CRUSH,
                    "MAX", "MAX", MAX_SNAP, MAX_GEN);
}

// the valid key stored in tree should be in the range of (_MIN_OID, _MAX_OID)
template <KeyT KT>
bool is_valid_key(const full_key_t<KT>& key);

/**
 * key_hobj_t
 *
 * A specialized implementation of a full_key_t storing a ghobject_t passed
 * from user.
 */
class key_hobj_t {
 public:
  explicit key_hobj_t(const ghobject_t& _ghobj) {
    if (_ghobj.is_max()) {
      ghobj = _MAX_OID();
    } else {
      // including when _ghobj.is_min()
      ghobj = _ghobj;
    }
    // I can be in the range of [_MIN_OID, _MAX_OID]
    assert(ghobj >= _MIN_OID());
    assert(ghobj <= _MAX_OID());
  }
  /*
   * common interfaces as a full_key_t
   */
  shard_t shard() const {
    return ghobj.shard_id;
  }
  pool_t pool() const {
    return ghobj.hobj.pool;
  }
  crush_hash_t crush() const {
    return ghobj.hobj.get_hash();
  }
  std::string_view nspace() const {
    // TODO(cross-node string dedup)
    return ghobj.hobj.nspace;
  }
  string_view_masked_t nspace_masked() const {
    // TODO(cross-node string dedup)
    return string_view_masked_t{nspace()};
  }
  std::string_view oid() const {
    // TODO(cross-node string dedup)
    return ghobj.hobj.oid.name;
  }
  string_view_masked_t oid_masked() const {
    // TODO(cross-node string dedup)
    return string_view_masked_t{oid()};
  }
  ns_oid_view_t::Type dedup_type() const {
    return _dedup_type;
  }
  snap_t snap() const {
    return ghobj.hobj.snap;
  }
  gen_t gen() const {
    return ghobj.generation;
  }

  MatchKindCMP compare_to(const full_key_t<KeyT::VIEW>&) const;
  MatchKindCMP compare_to(const full_key_t<KeyT::HOBJ>&) const;

  std::ostream& dump(std::ostream& os) const {
    os << "key_hobj(" << (int)shard() << ","
       << pool() << "," << crush() << "; "
       << string_view_masked_t{nspace()} << ","
       << string_view_masked_t{oid()} << "; "
       << snap() << "," << gen() << ")";
    return os;
  }

  bool is_valid() const {
    return is_valid_key<KeyT::HOBJ>(*this);
  }

  static key_hobj_t decode(ceph::bufferlist::const_iterator& delta) {
    shard_t shard;
    ceph::decode(shard, delta);
    pool_t pool;
    ceph::decode(pool, delta);
    crush_hash_t crush;
    ceph::decode(crush, delta);
    std::string nspace;
    [[maybe_unused]] auto nspace_masked = string_view_masked_t::decode(nspace, delta);
    // TODO(cross-node string dedup)
    assert(nspace_masked.get_type() == string_view_masked_t::Type::STR);
    std::string oid;
    [[maybe_unused]] auto oid_masked = string_view_masked_t::decode(oid, delta);
    // TODO(cross-node string dedup)
    assert(oid_masked.get_type() == string_view_masked_t::Type::STR);
    snap_t snap;
    ceph::decode(snap, delta);
    gen_t gen;
    ceph::decode(gen, delta);
    return key_hobj_t(ghobject_t(
        shard_id_t(shard), pool, crush, nspace, oid, snap, gen));
  }

 private:
  ns_oid_view_t::Type _dedup_type = ns_oid_view_t::Type::STR;
  ghobject_t ghobj;
};
inline std::ostream& operator<<(std::ostream& os, const key_hobj_t& key) {
  return key.dump(os);
}

/**
 * key_view_t
 *
 * A specialized implementation of a full_key_t pointing to the locations
 * storing the full key in a tree node.
 */
class key_view_t {
 public:
  /**
   * common interfaces as a full_key_t
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
  std::string_view nspace() const {
    // TODO(cross-node string dedup)
    return ns_oid_view().nspace.to_string_view();
  }
  string_view_masked_t nspace_masked() const {
    // TODO(cross-node string dedup)
    return string_view_masked_t{ns_oid_view().nspace};
  }
  std::string_view oid() const {
    // TODO(cross-node string dedup)
    return ns_oid_view().oid.to_string_view();
  }
  string_view_masked_t oid_masked() const {
    // TODO(cross-node string dedup)
    return string_view_masked_t{ns_oid_view().oid};
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

  MatchKindCMP compare_to(const full_key_t<KeyT::VIEW>&) const;
  MatchKindCMP compare_to(const full_key_t<KeyT::HOBJ>&) const;

  /**
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

  size_t size_logical() const {
    return sizeof(shard_t) + sizeof(pool_t) + sizeof(crush_hash_t) +
           sizeof(snap_t) + sizeof(gen_t) + ns_oid_view().size_logical();
  }

  ghobject_t to_ghobj() const {
    assert(is_valid_key<KeyT::VIEW>(*this));
    return ghobject_t(
        shard_id_t(shard()), pool(), crush(),
        std::string(nspace()), std::string(oid()), snap(), gen());
  }

  void replace(const crush_t& key) { p_crush = &key; }
  void set(const crush_t& key) {
    assert(!has_crush());
    replace(key);
  }
  void replace(const shard_pool_crush_t& key) { p_shard_pool = &key.shard_pool; }
  void set(const shard_pool_crush_t& key) {
    set(key.crush);
    assert(!has_shard_pool());
    replace(key);
  }
  void replace(const ns_oid_view_t& key) { p_ns_oid = key; }
  void set(const ns_oid_view_t& key) {
    assert(!has_ns_oid());
    replace(key);
  }
  void replace(const snap_gen_t& key) { p_snap_gen = &key; }
  void set(const snap_gen_t& key) {
    assert(!has_snap_gen());
    replace(key);
  }

  void reset_to(const char* origin_base,
                const char* new_base,
                extent_len_t node_size) {
    if (p_shard_pool != nullptr) {
      reset_ptr(p_shard_pool, origin_base, new_base, node_size);
    }
    if (p_crush != nullptr) {
      reset_ptr(p_crush, origin_base, new_base, node_size);
    }
    if (p_ns_oid.has_value()) {
      p_ns_oid->reset_to(origin_base, new_base, node_size);
    }
    if (p_snap_gen != nullptr) {
      reset_ptr(p_snap_gen, origin_base, new_base, node_size);
    }
  }

  std::ostream& dump(std::ostream& os) const {
    os << "key_view(";
    if (has_shard_pool()) {
      os << (int)shard() << "," << pool() << ",";
    } else {
      os << "X,X,";
    }
    if (has_crush()) {
      os << crush() << "; ";
    } else {
      os << "X; ";
    }
    if (has_ns_oid()) {
      os << ns_oid_view() << "; ";
    } else {
      os << "X,X; ";
    }
    if (has_snap_gen()) {
      os << snap() << "," << gen() << ")";
    } else {
      os << "X,X)";
    }
    return os;
  }

 private:
  const shard_pool_t* p_shard_pool = nullptr;
  const crush_t* p_crush = nullptr;
  std::optional<ns_oid_view_t> p_ns_oid;
  const snap_gen_t* p_snap_gen = nullptr;
};

template <KeyT KT>
void encode_key(const full_key_t<KT>& key, ceph::bufferlist& bl) {
  ceph::encode(key.shard(), bl);
  ceph::encode(key.pool(), bl);
  ceph::encode(key.crush(), bl);
  key.nspace_masked().encode(bl);
  key.oid_masked().encode(bl);
  ceph::encode(key.snap(), bl);
  ceph::encode(key.gen(), bl);
}

template <KeyT TypeL, KeyT TypeR>
MatchKindCMP compare_full_key(
    const full_key_t<TypeL>& l, const full_key_t<TypeR>& r) {
  auto ret = toMatchKindCMP(l.shard(), r.shard());
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(l.pool(), r.pool());
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(l.crush(), r.crush());
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(l.nspace(), r.nspace());
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(l.oid(), r.oid());
  if (ret != MatchKindCMP::EQ)
    return ret;
  ret = toMatchKindCMP(l.snap(), r.snap());
  if (ret != MatchKindCMP::EQ)
    return ret;
  return toMatchKindCMP(l.gen(), r.gen());
}

inline MatchKindCMP key_hobj_t::compare_to(
    const full_key_t<KeyT::VIEW>& o) const {
  return compare_full_key<KeyT::HOBJ, KeyT::VIEW>(*this, o);
}
inline MatchKindCMP key_hobj_t::compare_to(
    const full_key_t<KeyT::HOBJ>& o) const {
  return compare_full_key<KeyT::HOBJ, KeyT::HOBJ>(*this, o);
}
inline MatchKindCMP key_view_t::compare_to(
    const full_key_t<KeyT::VIEW>& o) const {
  return compare_full_key<KeyT::VIEW, KeyT::VIEW>(*this, o);
}
inline MatchKindCMP key_view_t::compare_to(
    const full_key_t<KeyT::HOBJ>& o) const {
  return compare_full_key<KeyT::VIEW, KeyT::HOBJ>(*this, o);
}

template <KeyT KT>
bool is_valid_key(const full_key_t<KT>& key) {
  return key.compare_to(key_hobj_t(ghobject_t())) == MatchKindCMP::GT &&
         key.compare_to(key_hobj_t(ghobject_t::get_max())) == MatchKindCMP::LT;
}

inline std::ostream& operator<<(std::ostream& os, const key_view_t& key) {
  return key.dump(os);
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
  auto ret = compare_to(key.nspace(), string_view_masked_t{target.nspace});
  if (ret != MatchKindCMP::EQ)
    return ret;
  return compare_to(key.oid(), string_view_masked_t{target.oid});
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
void ns_oid_view_t::test_append(const full_key_t<KT>& key, char*& p_append) {
  if (key.dedup_type() == Type::STR) {
    string_key_view_t::test_append_str(key.nspace(), p_append);
    string_key_view_t::test_append_str(key.oid(), p_append);
  } else {
    string_key_view_t::test_append_dedup(key.dedup_type(), p_append);
  }
}

}
