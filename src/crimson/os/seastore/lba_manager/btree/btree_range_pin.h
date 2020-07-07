// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

class LBANode;
using LBANodeRef = TCachedExtentRef<LBANode>;

struct lba_node_meta_t {
  laddr_t begin = 0;
  laddr_t end = 0;
  depth_t depth = 0;

  bool is_parent_of(const lba_node_meta_t &other) const {
    return (depth == other.depth + 1) &&
      (begin <= other.begin) &&
      (end >= other.end);
  }

  std::pair<lba_node_meta_t, lba_node_meta_t> split_into(laddr_t pivot) const {
    return std::make_pair(
      lba_node_meta_t{begin, pivot, depth},
      lba_node_meta_t{pivot, end, depth});
  }

  static lba_node_meta_t merge_from(
    const lba_node_meta_t &lhs, const lba_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return lba_node_meta_t{lhs.begin, rhs.end, lhs.depth};
  }

  static std::pair<lba_node_meta_t, lba_node_meta_t>
  rebalance(const lba_node_meta_t &lhs, const lba_node_meta_t &rhs, laddr_t pivot) {
    assert(lhs.depth == rhs.depth);
    return std::make_pair(
      lba_node_meta_t{lhs.begin, pivot, lhs.depth},
      lba_node_meta_t{pivot, rhs.end, lhs.depth});
  }

  bool is_root() const {
    return begin == 0 && end == L_ADDR_MAX;
  }
};

inline std::ostream &operator<<(
  std::ostream &lhs,
  const lba_node_meta_t &rhs)
{
  return lhs << "btree_node_meta_t("
	     << "begin=" << rhs.begin
	     << ", end=" << rhs.end
	     << ", depth=" << rhs.depth
	     << ")";
}

/**
 * btree_range_pin_t
 *
 * Element tracked by btree_pin_set_t below.  Encapsulates the intrusive_set
 * hook, the lba_node_meta_t representing the lba range covered by a node,
 * and extent and ref members intended to hold a reference when the extent
 * should be pinned.
 */
class btree_pin_set_t;
class btree_range_pin_t : public boost::intrusive::set_base_hook<> {
  friend class btree_pin_set_t;
  lba_node_meta_t range;

  btree_pin_set_t *pins = nullptr;

  // We need to be able to remember extent without holding a reference,
  // but we can do it more compactly -- TODO
  CachedExtent *extent = nullptr;
  CachedExtentRef ref;

  using index_t = boost::intrusive::set<btree_range_pin_t>;

  static auto get_tuple(const lba_node_meta_t &meta) {
    return std::make_tuple(-meta.depth, meta.begin);
  }

  void acquire_ref() {
    ref = CachedExtentRef(extent);
  }

  void drop_ref() {
    ref.reset();
  }

public:
  btree_range_pin_t() = default;
  btree_range_pin_t(CachedExtent *extent)
    : extent(extent) {}
  btree_range_pin_t(const btree_range_pin_t &rhs, CachedExtent *extent)
    : range(rhs.range), extent(extent) {}

  bool has_ref() const {
    return !!ref;
  }

  bool is_root() const {
    return range.is_root();
  }

  void set_range(const lba_node_meta_t &nrange) {
    range = nrange;
  }
  void set_extent(CachedExtent *nextent) {
    assert(!extent);
    extent = nextent;
  }

  friend bool operator<(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) < get_tuple(rhs.range);
  }
  friend bool operator>(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) > get_tuple(rhs.range);
  }
  friend bool operator==(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    return get_tuple(lhs.range) == rhs.get_tuple(rhs.range);
  }

  struct meta_cmp_t {
    bool operator()(
      const btree_range_pin_t &lhs, const lba_node_meta_t &rhs) const {
      return get_tuple(lhs.range) < get_tuple(rhs);
    }
    bool operator()(
      const lba_node_meta_t &lhs, const btree_range_pin_t &rhs) const {
      return get_tuple(lhs) < get_tuple(rhs.range);
    }
  };

  friend std::ostream &operator<<(
    std::ostream &lhs,
    const btree_range_pin_t &rhs) {
    return lhs << "btree_range_pin_t("
	       << "begin=" << rhs.range.begin
	       << ", end=" << rhs.range.end
	       << ", depth=" << rhs.range.depth
	       << ", extent=" << rhs.extent
	       << ")";
  }

  friend class BtreeLBAPin;
  ~btree_range_pin_t();
};

/**
 * btree_pin_set_t
 *
 * Ensures that for every cached node, all parent LBANodes required
 * to map it are present in cache.  Relocating these nodes can
 * therefore be done without further reads or cache space.
 *
 * Contains a btree_range_pin_t for every clean or dirty LBANode
 * or LogicalCachedExtent instance in cache at any point in time.
 * For any LBANode, the contained btree_range_pin_t will hold
 * a reference to that node pinning it in cache as long as that
 * node has children in the set.  This invariant can be violated
 * only by calling retire_extent and is repaired by calling
 * check_parent synchronously after adding any new extents.
 */
class btree_pin_set_t {
  friend class btree_range_pin_t;
  using pins_t = btree_range_pin_t::index_t;
  pins_t pins;

  pins_t::iterator get_iter(btree_range_pin_t &pin) {
    return pins_t::s_iterator_to(pin);
  }

  /// Removes pin from set optionally checking whether parent has other children
  void remove_pin(btree_range_pin_t &pin, bool check_parent);

  /// Returns parent pin if exists
  btree_range_pin_t *maybe_get_parent(const lba_node_meta_t &pin);

  /// Returns earliest child pin if exist
  const btree_range_pin_t *maybe_get_first_child(const lba_node_meta_t &pin) const;

  /// Releases pin if it has no children
  void release_if_no_children(btree_range_pin_t &pin);

public:
  /// Adds pin to set, assumes set is consistent
  void add_pin(btree_range_pin_t &pin);

  /**
   * retire/check_parent
   *
   * See BtreeLBAManager::complete_transaction.
   * retire removes the specified pin from the set, but does not
   * check parents.  After any new extents are added to the set,
   * the caller is required to call check_parent to restore the
   * invariant.
   */
  void retire(btree_range_pin_t &pin);
  void check_parent(btree_range_pin_t &pin);

  ~btree_pin_set_t() {
    assert(pins.empty());
  }
};

class BtreeLBAPin : public LBAPin {
  friend class BtreeLBAManager;
  paddr_t paddr;
  btree_range_pin_t pin;

public:
  BtreeLBAPin() = default;

  BtreeLBAPin(
    paddr_t paddr,
    lba_node_meta_t &&meta)
    : paddr(paddr) {
    pin.set_range(std::move(meta));
  }

  void link_extent(LogicalCachedExtent *ref) final {
    pin.set_extent(ref);
  }

  extent_len_t get_length() const final {
    assert(pin.range.end > pin.range.begin);
    return pin.range.end - pin.range.begin;
  }

  paddr_t get_paddr() const final {
    return paddr;
  }

  laddr_t get_laddr() const final {
    return pin.range.begin;
  }

  LBAPinRef duplicate() const final {
    auto ret = std::unique_ptr<BtreeLBAPin>(new BtreeLBAPin);
    ret->pin.set_range(pin.range);
    ret->paddr = paddr;
    return ret;
  }
};

}
