// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/common/log.h"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {
class Cache;

struct op_context_t {
  Cache &cache;
  Transaction &trans;
};

template <typename bound_t>
struct fixed_kv_node_meta_t {
  bound_t begin = min_max_t<bound_t>::min;
  bound_t end = min_max_t<bound_t>::min;
  depth_t depth = 0;

  bool is_parent_of(const fixed_kv_node_meta_t &other) const {
    return (depth == other.depth + 1) &&
      (begin <= other.begin) &&
      (end > other.begin);
  }

  bool is_in_range(const bound_t key) const {
    return begin <= key && end > key;
  }

  std::pair<fixed_kv_node_meta_t, fixed_kv_node_meta_t> split_into(bound_t pivot) const {
    return std::make_pair(
      fixed_kv_node_meta_t{begin, pivot, depth},
      fixed_kv_node_meta_t{pivot, end, depth});
  }

  static fixed_kv_node_meta_t merge_from(
    const fixed_kv_node_meta_t &lhs, const fixed_kv_node_meta_t &rhs) {
    ceph_assert(lhs.depth == rhs.depth);
    return fixed_kv_node_meta_t{lhs.begin, rhs.end, lhs.depth};
  }

  static std::pair<fixed_kv_node_meta_t, fixed_kv_node_meta_t>
  rebalance(const fixed_kv_node_meta_t &lhs, const fixed_kv_node_meta_t &rhs, bound_t pivot) {
    ceph_assert(lhs.depth == rhs.depth);
    return std::make_pair(
      fixed_kv_node_meta_t{lhs.begin, pivot, lhs.depth},
      fixed_kv_node_meta_t{pivot, rhs.end, lhs.depth});
  }

  bool is_root() const {
    return begin == min_max_t<bound_t>::min && end == min_max_t<bound_t>::max;
  }
};

template <typename bound_t>
inline std::ostream &operator<<(
  std::ostream &lhs,
  const fixed_kv_node_meta_t<bound_t> &rhs)
{
  return lhs << "btree_node_meta_t("
	     << "begin=" << rhs.begin
	     << ", end=" << rhs.end
	     << ", depth=" << rhs.depth
	     << ")";
}

/**
 * fixed_kv_node_meta_le_t
 *
 * On disk layout for fixed_kv_node_meta_t
 */
template <typename bound_le_t>
struct __attribute__((packed)) fixed_kv_node_meta_le_t {
  bound_le_t begin = bound_le_t(0);
  bound_le_t end = bound_le_t(0);
  depth_le_t depth = init_depth_le(0);

  fixed_kv_node_meta_le_t() = default;
  fixed_kv_node_meta_le_t(
    const fixed_kv_node_meta_le_t<bound_le_t> &) = default;
  explicit fixed_kv_node_meta_le_t(
    const fixed_kv_node_meta_t<typename bound_le_t::orig_type> &val)
    : begin(val.begin),
      end(val.end),
      depth(init_depth_le(val.depth)) {}

  operator fixed_kv_node_meta_t<typename bound_le_t::orig_type>() const {
    return fixed_kv_node_meta_t<typename bound_le_t::orig_type>{
	    begin, end, depth };
  }
};

namespace lba {

/**
 * lba_map_val_t
 *
 * struct representing a single lba mapping
 */
struct lba_map_val_t {
  extent_len_t len = 0;  ///< length of mapping
  pladdr_t pladdr;         ///< direct addr of mapping or
			   //	laddr of a direct lba mapping(see btree_lba_manager.h)
  extent_ref_count_t refcount = 0; ///< refcount
  checksum_t checksum = 0; ///< checksum of original block written at paddr (TODO)

  lba_map_val_t() = default;
  lba_map_val_t(
    extent_len_t len,
    pladdr_t pladdr,
    extent_ref_count_t refcount,
    checksum_t checksum)
    : len(len), pladdr(pladdr), refcount(refcount), checksum(checksum) {}
  bool operator==(const lba_map_val_t&) const = default;
};

std::ostream& operator<<(std::ostream& out, const lba_map_val_t&);

/**
 * lba_map_val_le_t
 *
 * On disk layout for lba_map_val_t.
 */
struct __attribute__((packed)) lba_map_val_le_t {
  extent_len_le_t len = init_extent_len_le(0);
  pladdr_le_t pladdr;
  extent_ref_count_le_t refcount{0};
  checksum_le_t checksum{0};

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(init_extent_len_le(val.len)),
      pladdr(pladdr_le_t(val.pladdr)),
      refcount(val.refcount),
      checksum(val.checksum) {}

  operator lba_map_val_t() const {
    return lba_map_val_t{ len, pladdr, refcount, checksum };
  }
};

} // namespace lba

namespace backref {

struct backref_map_val_t {
  extent_len_t len = 0;	///< length of extents
  laddr_t laddr = L_ADDR_MIN; ///< logical address of extents
  extent_types_t type = extent_types_t::NONE;

  backref_map_val_t() = default;
  backref_map_val_t(
    extent_len_t len,
    laddr_t laddr,
    extent_types_t type)
    : len(len), laddr(laddr), type(type) {}

  bool operator==(const backref_map_val_t& rhs) const noexcept {
    return len == rhs.len && laddr == rhs.laddr;
  }
};

std::ostream& operator<<(std::ostream &out, const backref_map_val_t& val);

struct __attribute__((packed)) backref_map_val_le_t {
  extent_len_le_t len = init_extent_len_le(0);
  laddr_le_t laddr = laddr_le_t(L_ADDR_MIN);
  extent_types_le_t type = 0;

  backref_map_val_le_t() = default;
  backref_map_val_le_t(const backref_map_val_le_t &) = default;
  explicit backref_map_val_le_t(const backref_map_val_t &val)
    : len(init_extent_len_le(val.len)),
      laddr(val.laddr),
      type(extent_types_le_t(val.type)) {}

  operator backref_map_val_t() const {
    return backref_map_val_t{len, laddr, (extent_types_t)type};
  }
};

} // namespace backerf

/**
 * BtreeCursor
 *
 * BtreeCursor is the type-erased wrapper for FixedKVBtree::iterator, stores
 * a key-value mapping's location and the snapshot of its data at construction
 * time.
 */
template <typename key_t, typename val_t>
struct BtreeCursor {
  BtreeCursor(
    op_context_t &ctx,
    CachedExtentRef parent,
    uint64_t modifications,
    key_t key,
    std::optional<val_t> val,
    btreenode_pos_t pos)
      : ctx(ctx),
	parent(std::move(parent)),
	modifications(modifications),
	key(key),
	val(std::move(val)),
	pos(pos)
  {
    if constexpr (std::is_same_v<key_t, laddr_t>) {
      static_assert(std::is_same_v<val_t, lba::lba_map_val_t>,
        "the value type of laddr_t for BtreeCursor should be lba_map_val_t");
    } else {
      static_assert(std::is_same_v<key_t, paddr_t>,
        "the key type of BtreeCursor should be either laddr_t or paddr_t");
      static_assert(std::is_same_v<val_t, backref::backref_map_val_t>,
        "the value type should be either lba_map_val_t or backref_map_val_t");
    }
  }

  op_context_t ctx;
  CachedExtentRef parent;
  uint64_t modifications;
  key_t key;
  std::optional<val_t> val;
  btreenode_pos_t pos;

  // NOTE: The overhead of calling is_viewable() might be not negligible in the
  // case of the parent extent is stable and shared by multiple transactions.
  // The best practice is to only hold cursors whose parent is pending in
  // current transaction in the long term.
  bool is_viewable() const;

  bool is_end() const {
    auto max_key = min_max_t<key_t>::max;
    assert((key != max_key) == (bool)val);
    return key == max_key;
  }

  extent_len_t get_length() const {
    assert(!is_end());
    return val->len;
  }
};

struct LBACursor : BtreeCursor<laddr_t, lba::lba_map_val_t> {
  using Base = BtreeCursor<laddr_t, lba::lba_map_val_t>;
  using Base::BtreeCursor;
  bool is_indirect() const {
    assert(!is_end());
    return val->pladdr.is_laddr();
  }
  laddr_t get_laddr() const {
    return key;
  }
  paddr_t get_paddr() const {
    assert(!is_indirect());
    assert(!is_end());
    return val->pladdr.get_paddr();
  }
  laddr_t get_intermediate_key() const {
    assert(is_indirect());
    assert(!is_end());
    return val->pladdr.get_laddr();
  }
  checksum_t get_checksum() const {
    assert(!is_end());
    assert(!is_indirect());
    return val->checksum;
  }
  extent_ref_count_t get_refcount() const {
    assert(!is_end());
    assert(!is_indirect());
    return val->refcount;
  }
  std::unique_ptr<LBACursor> duplicate() const {
    return std::make_unique<LBACursor>(*this);
  }
};
using LBACursorRef = std::unique_ptr<LBACursor>;

struct BackrefCursor : BtreeCursor<paddr_t, backref::backref_map_val_t> {
  using Base = BtreeCursor<paddr_t, backref::backref_map_val_t>;
  using Base::BtreeCursor;
  paddr_t get_paddr() const {
    return key;
  }
  laddr_t get_laddr() const {
    assert(!is_end());
    return val->laddr;
  }
  extent_types_t get_type() const {
    assert(!is_end());
    return val->type;
  }
};
using BackrefCursorRef = std::unique_ptr<BackrefCursor>;

template <typename key_t, typename val_t>
std::ostream &operator<<(
  std::ostream &out, const BtreeCursor<key_t, val_t> &cursor)
{
  if constexpr (std::is_same_v<key_t, laddr_t>) {
    out << "LBACursor(";
  } else {
    out << "BackrefCursor(";
  }
  out << (void*)cursor.parent.get()
      << "@" << cursor.pos
      << "#" << cursor.modifications
      << ",";
  if (cursor.is_end()) {
    return out << "END)";
  }
  return out << "," << cursor.key
	     << "~" << *cursor.val
	     << ")";
}

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LBACursor> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::BackrefCursor> : fmt::ostream_formatter {};
#endif
