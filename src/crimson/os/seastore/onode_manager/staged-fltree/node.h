// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <ostream>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "crimson/common/type_helpers.h"

#include "node_extent_mutable.h"
#include "stages/key_layout.h"
#include "stages/stage_types.h"
#include "super.h"
#include "tree_types.h"

namespace crimson::os::seastore::onode {

/**
 * in-memory subtree management:
 *
 * resource management (bottom-up):
 * USER          --> Ref<tree_cursor_t>
 * tree_cursor_t --> Ref<LeafNode>
 * Node (child)  --> Ref<InternalNode> (see parent_info_t)
 * Node (root)   --> Super::URef
 * Super         --> Btree
 *
 * tracked lookup (top-down):
 * Btree         --> Super*
 * Super         --> Node* (root)
 * InternalNode  --> Node* (children)
 * LeafNode      --> tree_cursor_t*
 */

class LeafNode;
class InternalNode;

using layout_version_t = uint32_t;
class tree_cursor_t final
  : public boost::intrusive_ref_counter<
           tree_cursor_t, boost::thread_unsafe_counter> {
 public:
  // public to Btree
  ~tree_cursor_t();
  tree_cursor_t(const tree_cursor_t&) = delete;
  tree_cursor_t(tree_cursor_t&&) = delete;
  tree_cursor_t& operator=(const tree_cursor_t&) = delete;
  tree_cursor_t& operator=(tree_cursor_t&&) = delete;

  bool is_end() const { return position.is_end(); }
  const key_view_t& get_key_view() const;
  const onode_t* get_p_value() const;

 private:
  tree_cursor_t(Ref<LeafNode>, const search_position_t&);
  tree_cursor_t(Ref<LeafNode>, const search_position_t&,
                const key_view_t& key, const onode_t*, layout_version_t);
  // lookup reaches the end, contain leaf node for further insert
  tree_cursor_t(Ref<LeafNode>);
  const search_position_t& get_position() const { return position; }
  Ref<LeafNode> get_leaf_node() { return leaf_node; }
  void update_track(Ref<LeafNode>, const search_position_t&);
  void update_kv(const key_view_t&, const onode_t*, layout_version_t) const;
  void ensure_kv() const;

 private:
  Ref<LeafNode> leaf_node;
  search_position_t position;
  // cached information
  mutable std::optional<key_view_t> key_view;
  mutable const onode_t* p_value;
  mutable layout_version_t node_version;

  friend class LeafNode;
  friend class Node; // get_position(), get_leaf_node()
};

class Node
  : public boost::intrusive_ref_counter<
           Node, boost::thread_unsafe_counter> {
 public:
  // public to Btree
  using node_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  template <class... ValuesT>
  using node_future = node_ertr::future<ValuesT...>;
  struct search_result_t {
    bool is_end() const { return p_cursor->is_end(); }
    Ref<tree_cursor_t> p_cursor;
    MatchKindBS match;
  };

  virtual ~Node();
  Node(const Node&) = delete;
  Node(Node&&) = delete;
  Node& operator=(const Node&) = delete;
  Node& operator=(Node&&) = delete;

  level_t level() const;
  virtual node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) = 0;
  virtual node_future<Ref<tree_cursor_t>> lookup_largest(context_t) = 0;
  node_future<search_result_t> lower_bound(context_t c, const key_hobj_t& key);
  node_future<std::pair<Ref<tree_cursor_t>, bool>> insert(
      context_t, const key_hobj_t&, const onode_t&);
  std::ostream& dump(std::ostream&) const;
  std::ostream& dump_brief(std::ostream&) const;

  void test_make_destructable(context_t, NodeExtentMutable&, Super::URef&&);
  virtual node_future<> test_clone_root(context_t, RootNodeTracker&) const = 0;

  static node_future<> mkfs(context_t, RootNodeTracker&);
  static node_future<Ref<Node>> load_root(context_t, RootNodeTracker&);

 protected:
  virtual node_future<> test_clone_non_root(context_t, Ref<InternalNode>) const {
    assert(false && "impossible path");
  }
  virtual node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) = 0;

 protected:
  Node(NodeImplURef&&);
  bool is_root() const {
    assert((super && !_parent_info.has_value()) ||
           (!super && _parent_info.has_value()));
    return !_parent_info.has_value();
  }

  // as root
  void make_root(context_t c, Super::URef&& _super);
  void make_root_new(context_t c, Super::URef&& _super) {
    assert(_super->get_root_laddr() == L_ADDR_NULL);
    make_root(c, std::move(_super));
  }
  void make_root_from(context_t c, Super::URef&& _super, laddr_t from_addr) {
    assert(_super->get_root_laddr() == from_addr);
    make_root(c, std::move(_super));
  }
  void as_root(Super::URef&& _super);
  node_future<> upgrade_root(context_t);

  // as child/non-root
  template <bool VALIDATE = true>
  void as_child(const search_position_t&, Ref<InternalNode>);
  struct parent_info_t {
    search_position_t position;
    Ref<InternalNode> ptr;
  };
  const parent_info_t& parent_info() const { return *_parent_info; }
  node_future<> insert_parent(context_t, Ref<Node> right_node);

 private:
  // as root
  Super::URef super;
  // as child/non-root
  std::optional<parent_info_t> _parent_info;

 private:
  static node_future<Ref<Node>> load(context_t, laddr_t, bool expect_is_level_tail);

  NodeImplURef impl;
  friend class InternalNode;
};
inline std::ostream& operator<<(std::ostream& os, const Node& node) {
  return node.dump_brief(os);
}

class InternalNode final : public Node {
 public:
  // public to Node
  InternalNode(InternalNodeImpl*, NodeImplURef&&);
  ~InternalNode() override { assert(tracked_child_nodes.empty()); }
  InternalNode(const InternalNode&) = delete;
  InternalNode(InternalNode&&) = delete;
  InternalNode& operator=(const InternalNode&) = delete;
  InternalNode& operator=(InternalNode&&) = delete;

  node_future<> apply_child_split(
      context_t, const search_position_t&, Ref<Node> left, Ref<Node> right);
  template <bool VALIDATE>
  void do_track_child(Node& child) {
    if constexpr (VALIDATE) {
      validate_child(child);
    }
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos) == tracked_child_nodes.end());
    tracked_child_nodes[child_pos] = &child;
  }
  void do_untrack_child(const Node& child) {
    auto& child_pos = child.parent_info().position;
    assert(tracked_child_nodes.find(child_pos)->second == &child);
    auto removed = tracked_child_nodes.erase(child_pos);
    assert(removed);
  }

  static node_future<Ref<InternalNode>> allocate_root(
      context_t, level_t, laddr_t, Super::URef&&);

 protected:
  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override;
  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override;
  node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) override;

  node_future<> test_clone_root(context_t, RootNodeTracker&) const override;

 private:
  // XXX: extract a common tracker for InternalNode to track Node,
  // and LeafNode to track tree_cursor_t.
  node_future<Ref<Node>> get_or_track_child(context_t, const search_position_t&, laddr_t);
  void track_insert(
      const search_position_t&, match_stage_t, Ref<Node>, Ref<Node> nxt_child = nullptr);
  void replace_track(const search_position_t&, Ref<Node> new_child, Ref<Node> old_child);
  void track_split(const search_position_t&, Ref<InternalNode>);
  void validate_tracked_children() const {
#ifndef NDEBUG
    for (auto& kv : tracked_child_nodes) {
      assert(kv.first == kv.second->parent_info().position);
      validate_child(*kv.second);
    }
#endif
  }
  void validate_child(const Node& child) const;

  struct fresh_node_t {
    Ref<InternalNode> node;
    NodeExtentMutable mut;
    std::pair<Ref<Node>, NodeExtentMutable> make_pair() {
      return std::make_pair(Ref<Node>(node), mut);
    }
  };
  static node_future<fresh_node_t> allocate(context_t, field_type_t, bool, level_t);

 private:
  // XXX: leverage intrusive data structure to control memory overhead
  // track the current living child nodes by position
  std::map<search_position_t, Node*> tracked_child_nodes;
  InternalNodeImpl* impl;
};

class LeafNode final : public Node {
 public:
  // public to tree_cursor_t
  ~LeafNode() override { assert(tracked_cursors.empty()); }
  LeafNode(const LeafNode&) = delete;
  LeafNode(LeafNode&&) = delete;
  LeafNode& operator=(const LeafNode&) = delete;
  LeafNode& operator=(LeafNode&&) = delete;

  bool is_level_tail() const;
  layout_version_t get_layout_version() const { return layout_version; }
  std::tuple<key_view_t, const onode_t*, layout_version_t> get_kv(
      const search_position_t&) const;
  void do_track_cursor(tree_cursor_t& cursor) {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos) == tracked_cursors.end());
    tracked_cursors[cursor_pos] = &cursor;
  }
  void do_untrack_cursor(tree_cursor_t& cursor) {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos)->second == &cursor);
    auto removed = tracked_cursors.erase(cursor_pos);
    assert(removed);
  }

 protected:
  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override;
  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override;
  node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) override;

  node_future<> test_clone_root(context_t, RootNodeTracker&) const override;

 private:
  LeafNode(LeafNodeImpl*, NodeImplURef&&);
  node_future<Ref<tree_cursor_t>> insert_value(
      context_t, const key_hobj_t&, const onode_t&,
      const search_position_t&, const MatchHistory&);
  static node_future<Ref<LeafNode>> allocate_root(context_t, RootNodeTracker&);
  friend class Node;

 private:
  // XXX: extract a common tracker for InternalNode to track Node,
  // and LeafNode to track tree_cursor_t.
  Ref<tree_cursor_t> get_or_track_cursor(
      const search_position_t&, const key_view_t&, const onode_t*);
  Ref<tree_cursor_t> track_insert(
      const search_position_t&, match_stage_t, const onode_t*);
  void track_split(const search_position_t&, Ref<LeafNode>);
  void validate_tracked_cursors() const {
#ifndef NDEBUG
    for (auto& kv : tracked_cursors) {
      assert(kv.first == kv.second->get_position());
      validate_cursor(*kv.second);
    }
#endif
  }
  void validate_cursor(tree_cursor_t& cursor) const;
  // invalidate p_value pointers in tree_cursor_t
  void on_layout_change() { ++layout_version; }

  struct fresh_node_t {
    Ref<LeafNode> node;
    NodeExtentMutable mut;
    std::pair<Ref<Node>, NodeExtentMutable> make_pair() {
      return std::make_pair(Ref<Node>(node), mut);
    }
  };
  static node_future<fresh_node_t> allocate(context_t, field_type_t, bool);

 private:
  // XXX: leverage intrusive data structure to control memory overhead
  // track the current living cursors by position
  std::map<search_position_t, tree_cursor_t*> tracked_cursors;
  LeafNodeImpl* impl;
  layout_version_t layout_version = 0;
};

}
