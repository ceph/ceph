// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/linked_tree_node.h"
#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"

namespace crimson::os::seastore {

class LogicalChildNode : public LogicalCachedExtent,
			 public ChildNode<lba::LBALeafNode,
					  LogicalChildNode,
					  laddr_t> {
  using lba_child_node_t = ChildNode<
    lba::LBALeafNode, LogicalChildNode, laddr_t>;
public:
  template <typename... T>
  LogicalChildNode(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  virtual ~LogicalChildNode() {
    if (this->is_stable()) {
      lba_child_node_t::destroy();
    }
  }

  bool is_btree_root() const {
    return false;
  }

  laddr_t get_begin() const {
    return get_laddr();
  }

  laddr_t get_end() const {
    return (get_laddr() + get_length()).checked_to_laddr();
  }

  void replace_placeholder(LogicalChildNode &prior) {
    assert(prior.get_type() == extent_types_t::REMAPPED_PLACEHOLDER);
    _take_parent_tracker(*this, prior.parent_tracker);
  }
protected:
  void on_replace_prior() final {
    assert(is_seen_by_users());
    lba_child_node_t::on_replace_prior();
    do_on_replace_prior();
  }
  virtual void do_on_replace_prior() {}
};
using LogicalChildNodeRef = TCachedExtentRef<LogicalChildNode>;

/**
 * RemappedExtentPlaceholder
 *
 * This placeholder represents a slice remapped from a logical cached extent
 * that is stable but not resident in memory. These extents are transaction-local,
 * so they should not be added to the cache. It is used by the backref manager
 * to update the extent type of remapped backref entries correctly. See
 * BtreeBackrefManager::merge_cached_backrefs() for more details.
 */
struct RemappedExtentPlaceholder : LogicalChildNode {
  explicit RemappedExtentPlaceholder(extent_len_t length)
      : LogicalChildNode(length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    ceph_assert(0 == "Should never happen for a placeholder");
    return CachedExtentRef();
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "Should never happen for a placeholder");
    return ceph::bufferlist();
  }

  void apply_delta(const ceph::bufferlist &) final {
    ceph_assert(0 == "Should never happen for a placeholder");
  }

  static constexpr extent_types_t TYPE = extent_types_t::REMAPPED_PLACEHOLDER;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void unlink_parent() {
    destroy();
  }
};
using RemappedExtentPlaceholderRef =
    TCachedExtentRef<RemappedExtentPlaceholder>;

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LogicalChildNode> : fmt::ostream_formatter {};
#endif
