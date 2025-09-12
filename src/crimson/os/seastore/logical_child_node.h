// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/linked_tree_node.h"
#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"

namespace crimson::os::seastore {

/**
 * RetiredExtentPlaceholder
 *
 * Cache::retire_extent_addr(Transaction&, paddr_t, extent_len_t) can retire an
 * extent not currently in cache. In that case, in order to detect transaction
 * invalidation, we need to add a placeholder to the cache to create the
 * mapping back to the transaction. And whenever there is a transaction tries
 * to read the placeholder extent out, Cache is responsible to replace the
 * placeholder by the real one. Anyway, No placeholder extents should escape
 * the Cache interface boundary.
 */
class RetiredExtentPlaceholder
  : public CachedExtent,
    public ChildNode<lba::LBALeafNode, RetiredExtentPlaceholder, laddr_t> {

  using lba_child_node_t =
    ChildNode<lba::LBALeafNode, RetiredExtentPlaceholder, laddr_t>;
public:
  RetiredExtentPlaceholder(extent_len_t length)
    : CachedExtent(CachedExtent::retired_placeholder_construct_t{}, length) {}

  ~RetiredExtentPlaceholder() {
    if (this->is_stable()) {
      lba_child_node_t::destroy();
    }
  }

  void on_invalidated(Transaction&) final {
    this->lba_child_node_t::on_invalidated();
  }

  CachedExtentRef duplicate_for_write(Transaction&) final {
    ceph_abort_msg("Should never happen for a placeholder");
    return CachedExtentRef();
  }

  ceph::bufferlist get_delta() final {
    ceph_abort_msg("Should never happen for a placeholder");
    return ceph::bufferlist();
  }

  static constexpr extent_types_t TYPE = extent_types_t::RETIRED_PLACEHOLDER;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &bl) final {
    ceph_abort_msg("Should never happen for a placeholder");
  }

  void on_rewrite(Transaction &, CachedExtent&, extent_len_t) final {}

  std::ostream &print_detail(std::ostream &out) const final {
    return out << ", RetiredExtentPlaceholder";
  }

  void on_delta_write(paddr_t record_block_offset) final {
    ceph_abort_msg("Should never happen for a placeholder");
  }

  void set_laddr(laddr_t laddr) {
    this->laddr = laddr;
  }

  bool is_btree_root() const {
    return false;
  }

  laddr_t get_begin() const {
    assert(laddr != L_ADDR_NULL);
    return laddr;
  }

  laddr_t get_end() const {
    assert(laddr != L_ADDR_NULL);
    return (laddr + get_length()).checked_to_laddr();
  }

private:
  laddr_t laddr = L_ADDR_NULL;
};

class LogicalChildNode : public LogicalCachedExtent,
			 public ChildNode<lba::LBALeafNode,
					  LogicalChildNode,
					  laddr_t> {
  using lba_child_node_t = ChildNode<
    lba::LBALeafNode, LogicalChildNode, laddr_t>;
public:
  template <typename... T>
  LogicalChildNode(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  virtual void lcn_on_invalidated(Transaction &t) {}

  void on_invalidated(Transaction &t) final {
    this->lba_child_node_t::on_invalidated();
    lcn_on_invalidated(t);
  }

  virtual ~LogicalChildNode() {
    if (this->is_stable() && !is_shadow_extent()) {
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
protected:
  void on_replace_prior() final {
    assert(is_seen_by_users());
    lba_child_node_t::on_replace_prior();
    do_on_replace_prior();
  }
  virtual void do_on_replace_prior() {}
};
using LogicalChildNodeRef = TCachedExtentRef<LogicalChildNode>;
} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LogicalChildNode> : fmt::ostream_formatter {};
#endif
