// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
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
#include "value.h"

/**
 * Tree example (2 levels):
 *
 * Root node keys:               [  3  7  ]
 *           values:             [p1 p2 p3]
 *                                /  |   \
 *                         -------   |    -------
 *                         |         |          |
 *                         V         V          V
 * Leaf node keys:   [ 1  2  3] [ 4  5  7] [ 9 11 12]
 *           values: [v1 v2 v3] [v4 v5 v6] [v7 v8 v9]
 *
 * Tree structure properties:
 * - As illustrated above, the parent key is strictly equal to its left child's
 *   largest key;
 * - If a tree is indexing multiple seastore transactions, each transaction
 *   will be mapped to a Super which points to a distinct root node. So the
 *   transactions are isolated at tree level. However, tree nodes from
 *   different transactions can reference the same seastore CachedExtent before
 *   modification;
 * - The resources of the transactional tree are tracked by tree_cursor_ts held
 *   by users. As long as any cursor is alive, the according tree hierarchy is
 *   alive and keeps tracked. See the reversed resource management sections
 *   below;
 */

namespace crimson::os::seastore::onode {

class LeafNode;
class InternalNode;

using layout_version_t = uint32_t;
struct node_version_t {
  layout_version_t layout;
  bool is_duplicate;

  bool operator==(const node_version_t& rhs) const {
    return (layout == rhs.layout && is_duplicate == rhs.is_duplicate);
  }
  bool operator!=(const node_version_t& rhs) const {
    return !(*this == rhs);
  }
};

/**
 * tree_cursor_t
 *
 * A cursor points to a position (LeafNode and search_position_t) of the tree
 * where it can find the according key and value pair. The position is updated
 * by LeafNode insert/split/delete/merge internally and is kept valid. It also
 * caches the key-value information for a specific node layout version.
 *
 * Exposes public interfaces for Btree::Cursor.
 */
class tree_cursor_t final
  : public boost::intrusive_ref_counter<
           tree_cursor_t, boost::thread_unsafe_counter> {
 public:
  using ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange,
    crimson::ct_error::eagain>;
  template <class ValueT=void>
  using future = ertr::future<ValueT>;

  ~tree_cursor_t();
  tree_cursor_t(const tree_cursor_t&) = delete;
  tree_cursor_t(tree_cursor_t&&) = delete;
  tree_cursor_t& operator=(const tree_cursor_t&) = delete;
  tree_cursor_t& operator=(tree_cursor_t&&) = delete;

  // public to Btree

  /**
   * is_end
   *
   * Represents one-past-the-last of all the sorted key-value
   * pairs in the tree. An end cursor won't contain valid key-value
   * information.
   */
  bool is_end() const { return position.is_end(); }

  /// Returns the key view in tree if it is not an end cursor.
  const key_view_t& get_key_view(value_magic_t magic) const {
    assert(!is_end());
    return cache.get_key_view(magic, position);
  }

  /// Returns the next tree_cursor_t in tree, can be end if there's no next.
  future<Ref<tree_cursor_t>> get_next(context_t);

  /// Check that this is next to prv
  void assert_next_to(const tree_cursor_t&, value_magic_t) const;

  MatchKindCMP compare_to(const tree_cursor_t&, value_magic_t) const;

  // public to Value

  /// Get the latest value_header_t pointer for read.
  const value_header_t* read_value_header(value_magic_t magic) const {
    assert(!is_end());
    return cache.get_p_value_header(magic, position);
  }

  /// Prepare the node extent to be mutable and recorded.
  std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  prepare_mutate_value_payload(context_t c) {
    assert(!is_end());
    return cache.prepare_mutate_value_payload(c, position);
  }

  /// Extends the size of value payload.
  future<> extend_value(context_t, value_size_t);

  /// Trim and shrink the value payload.
  future<> trim_value(context_t, value_size_t);

 private:
  tree_cursor_t(Ref<LeafNode>, const search_position_t&);
  tree_cursor_t(Ref<LeafNode>, const search_position_t&,
                const key_view_t&, const value_header_t*);
  // lookup reaches the end, contain leaf node for further insert
  tree_cursor_t(Ref<LeafNode>);

  const search_position_t& get_position() const { return position; }
  Ref<LeafNode> get_leaf_node() const { return ref_leaf_node; }
  template <bool VALIDATE>
  void update_track(Ref<LeafNode>, const search_position_t&);
  void update_cache_same_node(const key_view_t&,
                              const value_header_t*) const;

  static Ref<tree_cursor_t> create(Ref<LeafNode> node, const search_position_t& pos) {
    return new tree_cursor_t(node, pos);
  }

  static Ref<tree_cursor_t> create(Ref<LeafNode> node, const search_position_t& pos,
                                   const key_view_t& key, const value_header_t* p_header) {
    return new tree_cursor_t(node, pos, key, p_header);
  }

  static Ref<tree_cursor_t> create_end(Ref<LeafNode> node) {
    return new tree_cursor_t(node);
  }

  /**
   * Reversed resource management (tree_cursor_t)
   *
   * tree_cursor_t holds a reference to the LeafNode, so the LeafNode will be
   * alive as long as any of it's cursors is still referenced by user.
   */
  Ref<LeafNode> ref_leaf_node;
  search_position_t position;

  /** Cache
   *
   * Cached memory pointers or views which may be outdated due to
   * extent copy-on-write or asynchronous leaf node updates.
   */
  class Cache {
   public:
    Cache(Ref<LeafNode>&);
    void validate_is_latest(const search_position_t&) const;
    void invalidate() { needs_update_all = true; }
    void update_all(const node_version_t&, const key_view_t&, const value_header_t*);
    const key_view_t& get_key_view(
        value_magic_t magic, const search_position_t& pos) {
      make_latest(magic, pos);
      return *key_view;
    }
    const value_header_t* get_p_value_header(
        value_magic_t magic, const search_position_t& pos) {
      make_latest(magic, pos);
      return p_value_header;
    }
    std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
    prepare_mutate_value_payload(context_t, const search_position_t&);

   private:
    void maybe_duplicate(const node_version_t&);
    void make_latest(value_magic_t, const search_position_t&);

    // metadata about how cache is valid
    Ref<LeafNode>& ref_leaf_node;
    bool needs_update_all = true;
    node_version_t version;

    // cached key value info
    const char* p_node_base = nullptr;
    std::optional<key_view_t> key_view;
    const value_header_t* p_value_header = nullptr;

    // cached data-structures to update value payload
    std::optional<NodeExtentMutable> value_payload_mut;
    ValueDeltaRecorder* p_value_recorder = nullptr;
  };
  mutable Cache cache;

  friend class LeafNode;
  friend class Node; // get_position(), get_leaf_node()
};

/**
 * Node
 *
 * An abstracted class for both InternalNode and LeafNode.
 *
 * Exposes public interfaces for Btree.
 */
class Node
  : public boost::intrusive_ref_counter<
           Node, boost::thread_unsafe_counter> {
 public:
  // public to Btree
  using node_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange,
    crimson::ct_error::eagain>;
  template <class ValueT=void>
  using node_future = node_ertr::future<ValueT>;

  struct search_result_t {
    bool is_end() const { return p_cursor->is_end(); }
    Ref<tree_cursor_t> p_cursor;
    match_stat_t mstat;

    MatchKindBS match() const {
      assert(mstat >= MSTAT_MIN && mstat <= MSTAT_MAX);
      return (mstat == MSTAT_EQ ? MatchKindBS::EQ : MatchKindBS::NE);
    }
  };

  virtual ~Node();
  Node(const Node&) = delete;
  Node(Node&&) = delete;
  Node& operator=(const Node&) = delete;
  Node& operator=(Node&&) = delete;

  /**
   * level
   *
   * A positive value denotes the level (or height) of this node in tree.
   * 0 means LeafNode, positive means InternalNode.
   */
  level_t level() const;

  /**
   * lookup_smallest
   *
   * Returns a cursor pointing to the smallest key in the sub-tree formed by
   * this node.
   *
   * Returns an end cursor if it is an empty root node.
   */
  virtual node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) = 0;

  /**
   * lookup_largest
   *
   * Returns a cursor pointing to the largest key in the sub-tree formed by
   * this node.
   *
   * Returns an end cursor if it is an empty root node.
   */
  virtual node_future<Ref<tree_cursor_t>> lookup_largest(context_t) = 0;

  /**
   * lower_bound
   *
   * Returns a cursor pointing to the first element in the range [first, last)
   * of the sub-tree which does not compare less than the input key. The
   * result also denotes whether the pointed key is equal to the input key.
   *
   * Returns an end cursor with MatchKindBS::NE if:
   * - It is an empty root node;
   * - Or the input key is larger than all the keys in the sub-tree;
   */
  node_future<search_result_t> lower_bound(context_t c, const key_hobj_t& key);

  /**
   * insert
   *
   * Try to insert a key-value pair into the sub-tree formed by this node.
   *
   * Returns a boolean denoting whether the insertion is successful:
   * - If true, the returned cursor points to the inserted element in tree;
   * - If false, the returned cursor points to the conflicting element in tree;
   */
  node_future<std::pair<Ref<tree_cursor_t>, bool>> insert(
      context_t, const key_hobj_t&, value_config_t);

  /// Recursively collects the statistics of the sub-tree formed by this node
  node_future<tree_stats_t> get_tree_stats(context_t);

  /// Returns an ostream containing a dump of all the elements in the node.
  std::ostream& dump(std::ostream&) const;

  /// Returns an ostream containing an one-line summary of this node.
  std::ostream& dump_brief(std::ostream&) const;

  /// Initializes the tree by allocating an empty root node.
  static node_future<> mkfs(context_t, RootNodeTracker&);

  /// Loads the tree root. The tree must be initialized.
  static node_future<Ref<Node>> load_root(context_t, RootNodeTracker&);

  // Only for unit test purposes.
  void test_make_destructable(context_t, NodeExtentMutable&, Super::URef&&);
  virtual node_future<> test_clone_root(context_t, RootNodeTracker&) const = 0;

 protected:
  virtual node_future<> test_clone_non_root(context_t, Ref<InternalNode>) const {
    ceph_abort("impossible path");
  }
  virtual node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) = 0;
  virtual node_future<> do_get_tree_stats(context_t, tree_stats_t&) = 0;

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
  node_future<> insert_parent(context_t, const key_view_t&, Ref<Node> right_node);
  node_future<Ref<tree_cursor_t>> get_next_cursor_from_parent(context_t);

 private:
  /**
   * Reversed resource management (Node)
   *
   * Root Node holds a reference to its parent Super class, so its parent
   * will be alive as long as this root node is alive.
   *
   * None-root Node holds a reference to its parent Node, so its parent will
   * be alive as long as any of it's children is alive.
   */
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

/**
 * InternalNode
 *
 * A concrete implementation of Node class that represents an internal tree
 * node. Its level is always positive and its values are logical block
 * addresses to its child nodes. An internal node cannot be empty.
 */
class InternalNode final : public Node {
 public:
  // public to Node
  InternalNode(InternalNodeImpl*, NodeImplURef&&);
  ~InternalNode() override { assert(tracked_child_nodes.empty()); }
  InternalNode(const InternalNode&) = delete;
  InternalNode(InternalNode&&) = delete;
  InternalNode& operator=(const InternalNode&) = delete;
  InternalNode& operator=(InternalNode&&) = delete;

  node_future<Ref<tree_cursor_t>> get_next_cursor(context_t, const search_position_t&);

  node_future<> apply_child_split(
      context_t, const search_position_t&, const key_view_t& left_key,
      Ref<Node> left, Ref<Node> right);

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
    [[maybe_unused]] auto removed = tracked_child_nodes.erase(child_pos);
    assert(removed);
  }

  static node_future<Ref<InternalNode>> allocate_root(
      context_t, level_t, laddr_t, Super::URef&&);

 protected:
  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override;
  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override;
  node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) override;
  node_future<> do_get_tree_stats(context_t, tree_stats_t&) override;

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
  /**
   * Reversed resource management (InternalNode)
   *
   * InteralNode keeps track of its child nodes which are still alive in
   * memory, and their positions will be updated throughout
   * insert/split/delete/merge operations of this node.
   */
  // XXX: leverage intrusive data structure to control memory overhead
  std::map<search_position_t, Node*> tracked_child_nodes;
  InternalNodeImpl* impl;
};

/**
 * LeafNode
 *
 * A concrete implementation of Node class that represents a leaf tree node.
 * Its level is always 0. A leaf node can only be empty if it is root.
 */
class LeafNode final : public Node {
 public:
  // public to tree_cursor_t
  ~LeafNode() override { assert(tracked_cursors.empty()); }
  LeafNode(const LeafNode&) = delete;
  LeafNode(LeafNode&&) = delete;
  LeafNode& operator=(const LeafNode&) = delete;
  LeafNode& operator=(LeafNode&&) = delete;

  bool is_level_tail() const;
  node_version_t get_version() const;
  const char* read() const;
  std::tuple<key_view_t, const value_header_t*> get_kv(const search_position_t&) const;
  node_future<Ref<tree_cursor_t>> get_next_cursor(context_t, const search_position_t&);

  template <bool VALIDATE>
  void do_track_cursor(tree_cursor_t& cursor) {
    if constexpr (VALIDATE) {
      validate_cursor(cursor);
    }
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.count(cursor_pos) == 0);
    tracked_cursors.emplace(cursor_pos, &cursor);
  }
  void do_untrack_cursor(const tree_cursor_t& cursor) {
    validate_cursor(cursor);
    auto& cursor_pos = cursor.get_position();
    assert(tracked_cursors.find(cursor_pos)->second == &cursor);
    [[maybe_unused]] auto removed = tracked_cursors.erase(cursor_pos);
    assert(removed);
  }

  node_future<> extend_value(context_t, const search_position_t&, value_size_t);
  node_future<> trim_value(context_t, const search_position_t&, value_size_t);

  std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  prepare_mutate_value_payload(context_t);

 protected:
  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override;
  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override;
  node_future<search_result_t> lower_bound_tracked(
      context_t, const key_hobj_t&, MatchHistory&) override;
  node_future<> do_get_tree_stats(context_t, tree_stats_t&) override;

  node_future<> test_clone_root(context_t, RootNodeTracker&) const override;

 private:
  LeafNode(LeafNodeImpl*, NodeImplURef&&);
  node_future<Ref<tree_cursor_t>> insert_value(
      context_t, const key_hobj_t&, value_config_t,
      const search_position_t&, const MatchHistory&,
      match_stat_t mstat);
  static node_future<Ref<LeafNode>> allocate_root(context_t, RootNodeTracker&);
  friend class Node;

 private:
  // XXX: extract a common tracker for InternalNode to track Node,
  // and LeafNode to track tree_cursor_t.
  Ref<tree_cursor_t> get_or_track_cursor(
      const search_position_t&, const key_view_t&, const value_header_t*);
  Ref<tree_cursor_t> track_insert(
      const search_position_t&, match_stage_t, const value_header_t*);
  void track_split(const search_position_t&, Ref<LeafNode>);
  void validate_tracked_cursors() const {
#ifndef NDEBUG
    for (auto& kv : tracked_cursors) {
      assert(kv.first == kv.second->get_position());
      validate_cursor(*kv.second);
    }
#endif
  }
  void validate_cursor(const tree_cursor_t& cursor) const;
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
  /**
   * Reversed resource management (LeafNode)
   *
   * LeafNode keeps track of the referencing cursors which are still alive in
   * memory, and their positions will be updated throughout
   * insert/split/delete/merge operations of this node.
   */
  // XXX: leverage intrusive data structure to control memory overhead
  std::map<search_position_t, tree_cursor_t*> tracked_cursors;
  LeafNodeImpl* impl;
  layout_version_t layout_version = 0;
};

}
