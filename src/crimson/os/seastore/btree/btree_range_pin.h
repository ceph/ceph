// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/common/log.h"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

constexpr uint16_t MAX_FIXEDKVBTREE_DEPTH = 8;

template <typename T>
struct min_max_t {};

template <>
struct min_max_t<laddr_t> {
  static constexpr laddr_t max = L_ADDR_MAX;
  static constexpr laddr_t min = L_ADDR_MIN;
};

template <>
struct min_max_t<paddr_t> {
  static constexpr paddr_t max = P_ADDR_MAX;
  static constexpr paddr_t min = P_ADDR_MIN;
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
struct fixed_kv_node_meta_le_t {
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


/**
 * btree_range_pin_t
 *
 * Element tracked by btree_pin_set_t below.  Encapsulates the intrusive_set
 * hook, the fixed_kv_node_meta_t representing the key range covered by a node,
 * and extent and ref members intended to hold a reference when the extent
 * should be pinned.
 */
template <typename T>
class btree_pin_set_t;

template <typename node_key_t>
class FixedKVNode;

template <typename node_bound_t>
class btree_range_pin_t : public boost::intrusive::set_base_hook<> {
  friend class btree_pin_set_t<node_bound_t>;
  friend class FixedKVNode<node_bound_t>;
  fixed_kv_node_meta_t<node_bound_t> range;

  btree_pin_set_t<node_bound_t> *pins = nullptr;

  // We need to be able to remember extent without holding a reference,
  // but we can do it more compactly -- TODO
  CachedExtent *extent = nullptr;
  CachedExtentRef ref;

  using index_t = boost::intrusive::set<btree_range_pin_t>;

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

  void set_range(const fixed_kv_node_meta_t<node_bound_t> &nrange) {
    range = nrange;
  }
  void set_extent(CachedExtent *nextent) {
    ceph_assert(!extent);
    extent = nextent;
  }

  CachedExtent &get_extent() {
    assert(extent);
    return *extent;
  }

  bool has_ref() {
    return !!ref;
  }

  void take_pin(btree_range_pin_t &other)
  {
    ceph_assert(other.extent);
    if (other.pins) {
      other.pins->replace_pin(*this, other);
      pins = other.pins;
      other.pins = nullptr;

      if (other.has_ref()) {
	other.drop_ref();
	acquire_ref();
      }
    }
  }

  friend bool operator<(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    assert(lhs.range.depth == rhs.range.depth);
    return lhs.range.begin < rhs.range.begin;
  }
  friend bool operator>(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    assert(lhs.range.depth == rhs.range.depth);
    return lhs.range.begin > rhs.range.begin;
  }
  friend bool operator==(
    const btree_range_pin_t &lhs, const btree_range_pin_t &rhs) {
    assert(lhs.range.depth == rhs.range.depth);
    return lhs.range.begin == rhs.range.begin;
  }

  struct meta_cmp_t {
    bool operator()(
      const btree_range_pin_t &lhs, const fixed_kv_node_meta_t<node_bound_t> &rhs) const {
      assert(lhs.range.depth == rhs.depth);
      return lhs.range.begin < rhs.begin;
    }
    bool operator()(
      const fixed_kv_node_meta_t<node_bound_t> &lhs, const btree_range_pin_t &rhs) const {
      assert(lhs.depth == rhs.range.depth);
      return lhs.begin < rhs.range.begin;
    }
  };

  friend std::ostream &operator<<(
    std::ostream &lhs,
    const btree_range_pin_t<node_bound_t> &rhs) {
    return lhs << "btree_range_pin_t("
	       << "begin=" << rhs.range.begin
	       << ", end=" << rhs.range.end
	       << ", depth=" << rhs.range.depth
	       << ", extent=" << rhs.extent
	       << ")";
  }

  template <typename, typename>
  friend class BtreeNodePin;
  ~btree_range_pin_t()
  {
    ceph_assert(!pins == !is_linked());
    ceph_assert(!ref);
    if (pins) {
      crimson::get_logger(ceph_subsys_seastore_lba
	).debug("{}: removing {}", __func__, *this);
      pins->remove_pin(*this, true);
    }
    extent = nullptr;
  }

};

/**
 * btree_pin_set_t
 *
 * Ensures that for every cached node, all parent btree nodes required
 * to map it are present in cache.  Relocating these nodes can
 * therefore be done without further reads or cache space.
 *
 * Contains a btree_range_pin_t for every clean or dirty btree node
 * or LogicalCachedExtent instance in cache at any point in time.
 * For any btree node, the contained btree_range_pin_t will hold
 * a reference to that node pinning it in cache as long as that
 * node has children in the set.  This invariant can be violated
 * only by calling retire_extent and is repaired by calling
 * check_parent synchronously after adding any new extents.
 */
template <typename node_bound_t>
class btree_pin_set_t {
  friend class btree_range_pin_t<node_bound_t>;
  using pins_by_depth_t = std::array<
    typename btree_range_pin_t<node_bound_t>::index_t,
    MAX_FIXEDKVBTREE_DEPTH>;
  pins_by_depth_t pins_by_depth;

  /// Removes pin from set optionally checking whether parent has other children
  void remove_pin(btree_range_pin_t<node_bound_t> &pin, bool do_check_parent)
  {
    crimson::get_logger(ceph_subsys_seastore_lba).debug("{}: {}", __func__, pin);
    ceph_assert(pin.is_linked());
    ceph_assert(pin.pins);
    ceph_assert(!pin.ref);

    auto &layer = pins_by_depth[pin.range.depth];
    layer.erase(layer.s_iterator_to(pin));
    pin.pins = nullptr;

    if (do_check_parent) {
      check_parent(pin);
    }
  }

  void replace_pin(
    btree_range_pin_t<node_bound_t> &to,
    btree_range_pin_t<node_bound_t> &from)
  {
    assert(to.range.depth == from.range.depth);
    pins_by_depth[from.range.depth].replace_node(
      btree_range_pin_t<node_bound_t>::index_t::s_iterator_to(from), to);
  }

  /// Returns parent pin if exists
  btree_range_pin_t<node_bound_t> *maybe_get_parent(
    const fixed_kv_node_meta_t<node_bound_t> &meta)
  {
    auto cmeta = meta;
    cmeta.depth++;
    auto &layer = pins_by_depth[cmeta.depth];
    auto iter = layer.upper_bound(
      cmeta,
      typename btree_range_pin_t<node_bound_t>::meta_cmp_t());
    if (iter == layer.begin()) {
      return nullptr;
    } else {
      --iter;
      if (iter->range.is_parent_of(meta)) {
	return &*iter;
      } else {
	return nullptr;
      }
    }
  }

  /// Returns earliest child pin if exist
  const btree_range_pin_t<node_bound_t>
  *maybe_get_first_child(const fixed_kv_node_meta_t<node_bound_t> &meta) const
  {
    if (meta.depth == 0) {
      return nullptr;
    }

    auto cmeta = meta;
    cmeta.depth--;

    auto &layer = pins_by_depth[cmeta.depth];
    auto iter = layer.lower_bound(
      cmeta,
      typename btree_range_pin_t<node_bound_t>::meta_cmp_t());
    if (iter == layer.end()) {
      return nullptr;
    } else if (meta.is_parent_of(iter->range)) {
      return &*iter;
    } else {
      return nullptr;
    }
  }

  /// Releases pin if it has no children
  void release_if_no_children(btree_range_pin_t<node_bound_t> &pin)
  {
    ceph_assert(pin.is_linked());
    if (maybe_get_first_child(pin.range) == nullptr) {
      pin.drop_ref();
    }
  }

public:
  btree_pin_set_t() {}
  /// Adds pin to set, assumes set is consistent
  void add_pin(btree_range_pin_t<node_bound_t> &pin)
  {
    ceph_assert(!pin.is_linked());
    ceph_assert(!pin.pins);
    ceph_assert(!pin.ref);

    auto &layer = pins_by_depth[pin.range.depth];
    auto [prev, inserted] = layer.insert(pin);
    if (!inserted) {
      crimson::get_logger(ceph_subsys_seastore_lba).error(
	"{}: unable to add {} ({}), found {} ({})",
	__func__,
	pin,
	*(pin.extent),
	*prev,
	*(prev->extent));
      ceph_assert(0 == "impossible");
      return;
    }
    pin.pins = this;
    if (!pin.is_root()) {
      auto *parent = maybe_get_parent(pin.range);
      ceph_assert(parent);
      if (!parent->has_ref()) {
	crimson::get_logger(ceph_subsys_seastore_lba
	  ).debug("{}: acquiring parent {}", __func__,
	    static_cast<void*>(parent));
	parent->acquire_ref();
      } else {
	crimson::get_logger(ceph_subsys_seastore_lba).debug(
	  "{}: parent has ref {}", __func__,
	  static_cast<void*>(parent));
      }
    }
    if (maybe_get_first_child(pin.range) != nullptr) {
      crimson::get_logger(ceph_subsys_seastore_lba).debug(
	"{}: acquiring self {}", __func__, pin);
      pin.acquire_ref();
    }
  }


  /**
   * retire/check_parent
   *
   * See BtreeLBAManager::complete_transaction.
   * retire removes the specified pin from the set, but does not
   * check parents.  After any new extents are added to the set,
   * the caller is required to call check_parent to restore the
   * invariant.
   */
  void retire(btree_range_pin_t<node_bound_t> &pin)
  {
    pin.drop_ref();
    remove_pin(pin, false);
  }

  void check_parent(btree_range_pin_t<node_bound_t> &pin)
  {
    auto parent = maybe_get_parent(pin.range);
    if (parent) {
      crimson::get_logger(ceph_subsys_seastore_lba
	).debug("{}: releasing parent {}", __func__, *parent);
      release_if_no_children(*parent);
    }
  }

  template <typename F>
  void scan(F &&f) {
    for (auto &layer : pins_by_depth) {
      for (auto &i : layer) {
	std::invoke(f, i);
      }
    }
  }

  ~btree_pin_set_t() {
    for (auto &layer : pins_by_depth) {
      ceph_assert(layer.empty());
    }
  }
};

template <typename key_t, typename val_t>
class BtreeNodePin : public PhysicalNodePin<key_t, val_t> {

  /**
   * parent
   *
   * populated until link_extent is called to ensure cache residence
   * until add_pin is called.
   */
  CachedExtentRef parent;

  val_t value;
  extent_len_t len;
  btree_range_pin_t<key_t> pin;
  uint16_t pos = std::numeric_limits<uint16_t>::max();

public:
  using val_type = val_t;
  BtreeNodePin() = default;

  BtreeNodePin(
    CachedExtentRef parent,
    uint16_t pos,
    val_t &value,
    extent_len_t len,
    fixed_kv_node_meta_t<key_t> &&meta)
    : parent(parent), value(value), len(len), pos(pos) {
    pin.set_range(std::move(meta));
  }

  CachedExtentRef get_parent() const final {
    return parent;
  }

  btree_range_pin_t<key_t>& get_range_pin() {
    return pin;
  }

  CachedExtentRef get_parent() {
    return parent;
  }

  void set_parent(CachedExtentRef pin) {
    parent = pin;
  }

  void link_extent(LogicalCachedExtent *ref) final;

  extent_len_t get_length() const final {
    ceph_assert(pin.range.end > pin.range.begin);
    return len;
  }

  extent_types_t get_type() const override {
    ceph_abort("should never happen");
    return extent_types_t::ROOT;
  }

  val_t get_val() const final {
    return value;
  }

  key_t get_key() const final {
    return pin.range.begin;
  }

  PhysicalNodePinRef<key_t, val_t> duplicate() const final {
    auto ret = std::unique_ptr<BtreeNodePin<key_t, val_t>>(
      new BtreeNodePin<key_t, val_t>);
    ret->pin.set_range(pin.range);
    ret->value = value;
    ret->parent = parent;
    ret->len = len;
    return ret;
  }

  void take_pin(PhysicalNodePin<key_t, val_t> &opin) final {
    pin.take_pin(static_cast<BtreeNodePin<key_t, val_t>&>(opin).pin);
  }

  bool has_been_invalidated() const final {
    return parent->has_been_invalidated();
  }
};

}

#if FMT_VERSION >= 90000
template <typename node_bound_t> struct fmt::formatter<crimson::os::seastore::btree_range_pin_t<node_bound_t>> : fmt::ostream_formatter {};
#endif
