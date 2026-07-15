// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

  virtual void lcn_on_invalidated(Transaction &t) {}

  void on_invalidated(Transaction &t) final {
    this->lba_child_node_t::on_invalidated();
    lcn_on_invalidated(t);
  }

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
protected:
  void on_replace_prior(Transaction &t) final {
    assert(is_seen_by_users());
    if (!is_rewrite_transaction(t.get_src())) {
      lba_child_node_t::on_replace_prior();
    }
    do_on_replace_prior(t);
  }
  virtual void do_on_replace_prior(Transaction &t) {}
  void on_data_commit() final {
    ceph_abort("impossible");
  }
};
using LogicalChildNodeRef = TCachedExtentRef<LogicalChildNode>;
} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LogicalChildNode> : fmt::ostream_formatter {};
#endif
