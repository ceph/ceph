// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node.h"

#include <cassert>
#include <exception>
#include <sstream>

#include "common/likely.h"
#include "crimson/common/log.h"
#include "node_extent_manager.h"
#include "node_impl.h"
#include "stages/node_stage_layout.h"

namespace {

seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_filestore);
}

}

namespace crimson::os::seastore::onode {

using node_ertr = Node::node_ertr;
template <class ValueT=void>
using node_future = Node::node_future<ValueT>;

/*
 * tree_cursor_t
 */

tree_cursor_t::tree_cursor_t(Ref<LeafNode> node, const search_position_t& pos)
      : ref_leaf_node{node}, position{pos}, cache{ref_leaf_node}
{
  assert(is_tracked());
  ref_leaf_node->do_track_cursor<true>(*this);
}

tree_cursor_t::tree_cursor_t(
    Ref<LeafNode> node, const search_position_t& pos,
    const key_view_t& key_view, const value_header_t* p_value_header)
      : ref_leaf_node{node}, position{pos}, cache{ref_leaf_node}
{
  assert(is_tracked());
  update_cache_same_node(key_view, p_value_header);
  ref_leaf_node->do_track_cursor<true>(*this);
}

tree_cursor_t::tree_cursor_t(Ref<LeafNode> node)
      : ref_leaf_node{node}, position{search_position_t::end()}, cache{ref_leaf_node}
{
  assert(is_end());
  assert(ref_leaf_node->is_level_tail());
}

tree_cursor_t::~tree_cursor_t()
{
  if (is_tracked()) {
    ref_leaf_node->do_untrack_cursor(*this);
  }
}

tree_cursor_t::future<Ref<tree_cursor_t>>
tree_cursor_t::get_next(context_t c)
{
  assert(is_tracked());
  return ref_leaf_node->get_next_cursor(c, position);
}

void tree_cursor_t::assert_next_to(
    const tree_cursor_t& prv, value_magic_t magic) const
{
#ifndef NDEBUG
  assert(!prv.is_end());
  if (is_end()) {
    assert(ref_leaf_node == prv.ref_leaf_node);
    assert(ref_leaf_node->is_level_tail());
  } else if (is_tracked()) {
    auto key = get_key_view(magic);
    auto prv_key = prv.get_key_view(magic);
    assert(key.compare_to(prv_key) == MatchKindCMP::GT);
    if (ref_leaf_node == prv.ref_leaf_node) {
      position.assert_next_to(prv.position);
    } else {
      assert(!prv.ref_leaf_node->is_level_tail());
      assert(position == search_position_t::begin());
    }
  } else {
    assert(is_invalid());
    ceph_abort("impossible");
  }
#endif
}

template <bool FORCE_MERGE>
tree_cursor_t::future<Ref<tree_cursor_t>>
tree_cursor_t::erase(context_t c, bool get_next)
{
  assert(is_tracked());
  return ref_leaf_node->erase<FORCE_MERGE>(c, position, get_next);
}
template tree_cursor_t::future<Ref<tree_cursor_t>>
tree_cursor_t::erase<true>(context_t, bool);
template tree_cursor_t::future<Ref<tree_cursor_t>>
tree_cursor_t::erase<false>(context_t, bool);

MatchKindCMP tree_cursor_t::compare_to(
    const tree_cursor_t& o, value_magic_t magic) const
{
  if (!is_tracked() && !o.is_tracked()) {
    return MatchKindCMP::EQ;
  } else if (!is_tracked()) {
    return MatchKindCMP::GT;
  } else if (!o.is_tracked()) {
    return MatchKindCMP::LT;
  }

  assert(is_tracked() && o.is_tracked());
  // all tracked cursors are singletons
  if (this == &o) {
    return MatchKindCMP::EQ;
  }

  MatchKindCMP ret;
  if (ref_leaf_node == o.ref_leaf_node) {
    ret = position.compare_to(o.position);
  } else {
    auto key = get_key_view(magic);
    auto o_key = o.get_key_view(magic);
    ret = key.compare_to(o_key);
  }
  assert(ret != MatchKindCMP::EQ);
  return ret;
}

tree_cursor_t::future<>
tree_cursor_t::extend_value(context_t c, value_size_t extend_size)
{
  assert(is_tracked());
  return ref_leaf_node->extend_value(c, position, extend_size);
}

tree_cursor_t::future<>
tree_cursor_t::trim_value(context_t c, value_size_t trim_size)
{
  assert(is_tracked());
  return ref_leaf_node->trim_value(c, position, trim_size);
}

template <bool VALIDATE>
void tree_cursor_t::update_track(
    Ref<LeafNode> node, const search_position_t& pos)
{
  // I must be already untracked
  assert(is_tracked());
  assert(!ref_leaf_node->check_is_tracking(*this));
  // track the new node and new pos
  assert(!pos.is_end());
  ref_leaf_node = node;
  position = pos;
  // we lazy update the key/value information until user asked
  cache.invalidate();
  ref_leaf_node->do_track_cursor<VALIDATE>(*this);
}
template void tree_cursor_t::update_track<true>(Ref<LeafNode>, const search_position_t&);
template void tree_cursor_t::update_track<false>(Ref<LeafNode>, const search_position_t&);

void tree_cursor_t::update_cache_same_node(const key_view_t& key_view,
                                           const value_header_t* p_value_header) const
{
  assert(is_tracked());
  cache.update_all(ref_leaf_node->get_version(), key_view, p_value_header);
  cache.validate_is_latest(position);
}

void tree_cursor_t::invalidate()
{
  assert(is_tracked());
  ref_leaf_node.reset();
  assert(is_invalid());
  // I must be removed from LeafNode
}

/*
 * tree_cursor_t::Cache
 */

tree_cursor_t::Cache::Cache(Ref<LeafNode>& ref_leaf_node)
  : ref_leaf_node{ref_leaf_node} {}

void tree_cursor_t::Cache::update_all(const node_version_t& current_version,
                                      const key_view_t& _key_view,
                                      const value_header_t* _p_value_header)
{
  assert(_p_value_header);

  needs_update_all = false;
  version = current_version;

  p_node_base = ref_leaf_node->read();
  key_view = _key_view;
  p_value_header = _p_value_header;
  assert((const char*)p_value_header > p_node_base);
  assert((const char*)p_value_header - p_node_base < NODE_BLOCK_SIZE);

  value_payload_mut.reset();
  p_value_recorder = nullptr;
}

void tree_cursor_t::Cache::maybe_duplicate(const node_version_t& current_version)
{
  assert(!needs_update_all);
  assert(version.layout == current_version.layout);
  if (version.state == current_version.state) {
    // cache is already latest.
  } else if (version.state < current_version.state) {
    // the extent has been copied but the layout has not been changed.
    assert(p_node_base != nullptr);
    assert(key_view.has_value());
    assert(p_value_header != nullptr);

    auto current_p_node_base = ref_leaf_node->read();
    assert(current_p_node_base != p_node_base);

    version.state = current_version.state;
    reset_ptr(p_value_header, p_node_base, current_p_node_base);
    key_view->reset_to(p_node_base, current_p_node_base);
    value_payload_mut.reset();
    p_value_recorder = nullptr;

    p_node_base = current_p_node_base;
  } else {
    // It is impossible to change state backwards, see node_types.h.
    ceph_abort("impossible");
  }
}

void tree_cursor_t::Cache::make_latest(
    value_magic_t magic, const search_position_t& pos)
{
  auto current_version = ref_leaf_node->get_version();
  if (needs_update_all || version.layout != current_version.layout) {
    auto [_key_view, _p_value_header] = ref_leaf_node->get_kv(pos);
    update_all(current_version, _key_view, _p_value_header);
  } else {
    maybe_duplicate(current_version);
  }
  assert(p_value_header->magic == magic);
  validate_is_latest(pos);
}

void tree_cursor_t::Cache::validate_is_latest(const search_position_t& pos) const
{
#ifndef NDEBUG
  assert(!needs_update_all);
  assert(version == ref_leaf_node->get_version());

  auto [_key_view, _p_value_header] = ref_leaf_node->get_kv(pos);
  assert(p_node_base == ref_leaf_node->read());
  assert(key_view->compare_to(_key_view) == MatchKindCMP::EQ);
  assert(p_value_header == _p_value_header);
#endif
}

std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
tree_cursor_t::Cache::prepare_mutate_value_payload(
    context_t c, const search_position_t& pos)
{
  make_latest(c.vb.get_header_magic(), pos);
  if (!value_payload_mut.has_value()) {
    assert(!p_value_recorder);
    auto value_mutable = ref_leaf_node->prepare_mutate_value_payload(c);
    auto current_version = ref_leaf_node->get_version();
    maybe_duplicate(current_version);
    value_payload_mut = p_value_header->get_payload_mutable(value_mutable.first);
    p_value_recorder = value_mutable.second;
    validate_is_latest(pos);
  }
  return {*value_payload_mut, p_value_recorder};
}

/*
 * Node
 */

Node::Node(NodeImplURef&& impl) : impl{std::move(impl)} {}

Node::~Node()
{
  // XXX: tolerate failure between allocate() and as_child()
  if (!super && !_parent_info.has_value()) {
    // To erase a node:
    // 1. I'm not tracking any children or cursors
    // 2. unlink parent/super --ptr-> me
    // 3. unlink me --ref-> parent/super
    // 4. extent is retired
    assert(!impl->is_extent_valid());

    // TODO: maybe its possible when eagain happens internally, we should
    // revisit to make sure tree operations can be aborted normally,
    // without resource leak or hitting unexpected asserts.
  } else {
    assert(impl->is_extent_valid());
    if (is_root()) {
      super->do_untrack_root(*this);
    } else {
      _parent_info->ptr->do_untrack_child(*this);
    }
  }
}

level_t Node::level() const
{
  return impl->level();
}

node_future<Node::search_result_t> Node::lower_bound(
    context_t c, const key_hobj_t& key)
{
  return seastar::do_with(
    MatchHistory(), [this, c, &key](auto& history) {
      return lower_bound_tracked(c, key, history);
    }
  );
}

node_future<std::pair<Ref<tree_cursor_t>, bool>> Node::insert(
    context_t c, const key_hobj_t& key, value_config_t vconf)
{
  return seastar::do_with(
    MatchHistory(), [this, c, &key, vconf](auto& history) {
      return lower_bound_tracked(c, key, history
      ).safe_then([c, &key, vconf, &history](auto result) {
        if (result.match() == MatchKindBS::EQ) {
          return node_ertr::make_ready_future<std::pair<Ref<tree_cursor_t>, bool>>(
              std::make_pair(result.p_cursor, false));
        } else {
          auto leaf_node = result.p_cursor->get_leaf_node();
          return leaf_node->insert_value(
              c, key, vconf, result.p_cursor->get_position(), history, result.mstat
          ).safe_then([](auto p_cursor) {
            return node_ertr::make_ready_future<std::pair<Ref<tree_cursor_t>, bool>>(
                std::make_pair(p_cursor, true));
          });
        }
      });
    }
  );
}

node_future<std::size_t> Node::erase(
    context_t c, const key_hobj_t& key)
{
  return lower_bound(c, key).safe_then([c] (auto result) {
    if (result.match() != MatchKindBS::EQ) {
      return node_ertr::make_ready_future<std::size_t>(0);
    }
    auto ref_cursor = result.p_cursor;
    return ref_cursor->erase(c, false
    ).safe_then([ref_cursor] (auto next_cursor) {
      assert(ref_cursor->is_invalid());
      assert(!next_cursor);
      return std::size_t(1);
    });
  });
}

node_future<tree_stats_t> Node::get_tree_stats(context_t c)
{
  return seastar::do_with(
    tree_stats_t(), [this, c](auto& stats) {
      return do_get_tree_stats(c, stats).safe_then([&stats] {
        return stats;
      });
    }
  );
}

std::ostream& Node::dump(std::ostream& os) const
{
  return impl->dump(os);
}

std::ostream& Node::dump_brief(std::ostream& os) const
{
  return impl->dump_brief(os);
}

const std::string& Node::get_name() const
{
  return impl->get_name();
}

void Node::test_make_destructable(
    context_t c, NodeExtentMutable& mut, Super::URef&& _super)
{
  impl->test_set_tail(mut);
  make_root(c, std::move(_super));
}

node_future<> Node::mkfs(context_t c, RootNodeTracker& root_tracker)
{
  return LeafNode::allocate_root(c, root_tracker
  ).safe_then([](auto ret) {
    logger().info("OTree::Node::Mkfs: allocated root {}",
                  ret->get_name());
  });
}

node_future<Ref<Node>> Node::load_root(context_t c, RootNodeTracker& root_tracker)
{
  return c.nm.get_super(c.t, root_tracker
  ).safe_then([c, &root_tracker](auto&& _super) {
    auto root_addr = _super->get_root_laddr();
    assert(root_addr != L_ADDR_NULL);
    return Node::load(c, root_addr, true
    ).safe_then([c, _super = std::move(_super),
                 &root_tracker](auto root) mutable {
      logger().trace("OTree::Node::LoadRoot: loaded {}",
                     root->get_name());
      assert(root->impl->field_type() == field_type_t::N0);
      root->as_root(std::move(_super));
      std::ignore = c; // as only used in an assert
      std::ignore = root_tracker;
      assert(root == root_tracker.get_root(c.t));
      return node_ertr::make_ready_future<Ref<Node>>(root);
    });
  });
}

void Node::make_root(context_t c, Super::URef&& _super)
{
  _super->write_root_laddr(c, impl->laddr());
  as_root(std::move(_super));
}

void Node::as_root(Super::URef&& _super)
{
  assert(!super && !_parent_info);
  assert(_super->get_root_laddr() == impl->laddr());
  assert(impl->is_level_tail());
  super = std::move(_super);
  super->do_track_root(*this);
}

node_future<> Node::upgrade_root(context_t c)
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  super->do_untrack_root(*this);
  return InternalNode::allocate_root(c, impl->level(), impl->laddr(), std::move(super)
  ).safe_then([this](auto new_root) {
    as_child(search_position_t::end(), new_root);
    logger().info("OTree::Node::UpgradeRoot: upgraded from {} to {}",
                  get_name(), new_root->get_name());
  });
}

template <bool VALIDATE>
void Node::as_child(const search_position_t& pos, Ref<InternalNode> parent_node)
{
  // I must be already untracked.
  assert(!super);
#ifndef NDEBUG
  if (_parent_info.has_value()) {
    assert(!_parent_info->ptr->check_is_tracking(*this));
  }
#endif
  _parent_info = parent_info_t{pos, parent_node};
  parent_info().ptr->do_track_child<VALIDATE>(*this);
}
template void Node::as_child<true>(const search_position_t&, Ref<InternalNode>);
template void Node::as_child<false>(const search_position_t&, Ref<InternalNode>);

node_future<> Node::apply_split_to_parent(
    context_t c,
    Ref<Node>&& this_ref,
    Ref<Node>&& split_right,
    bool update_right_index)
{
  assert(!is_root());
  assert(this == this_ref.get());
  // TODO(cross-node string dedup)
  return parent_info().ptr->apply_child_split(
      c, std::move(this_ref), std::move(split_right), update_right_index);
}

node_future<Ref<tree_cursor_t>>
Node::get_next_cursor_from_parent(context_t c)
{
  assert(!impl->is_level_tail());
  assert(!is_root());
  return parent_info().ptr->get_next_cursor(c, parent_info().position);
}

template <bool FORCE_MERGE>
node_future<>
Node::try_merge_adjacent(
    context_t c, bool update_parent_index, Ref<Node>&& this_ref)
{
  assert(this == this_ref.get());
  impl->validate_non_empty();
  assert(!is_root());
  if constexpr (!FORCE_MERGE) {
    if (!impl->is_size_underflow()) {
      if (update_parent_index) {
        return fix_parent_index(c, std::move(this_ref), false);
      } else {
        parent_info().ptr->validate_child_tracked(*this);
        return node_ertr::now();
      }
    }
  }

  return parent_info().ptr->get_child_peers(c, parent_info().position
  ).safe_then([c, this_ref = std::move(this_ref), this,
               update_parent_index] (auto lr_nodes) mutable -> node_future<> {
    auto& [lnode, rnode] = lr_nodes;
    Ref<Node> left_for_merge;
    Ref<Node> right_for_merge;
    Ref<Node>* p_this_ref;
    bool is_left;
    if (!lnode && !rnode) {
      // XXX: this is possible before node rebalance is implemented,
      // when its parent cannot merge with its peers and has only one child
      // (this node).
      p_this_ref = &this_ref;
    } else if (!lnode) {
      left_for_merge = std::move(this_ref);
      p_this_ref = &left_for_merge;
      right_for_merge = std::move(rnode);
      is_left = true;
    } else if (!rnode) {
      left_for_merge = std::move(lnode);
      right_for_merge = std::move(this_ref);
      p_this_ref = &right_for_merge;
      is_left = false;
    } else { // lnode && rnode
      if (lnode->impl->free_size() > rnode->impl->free_size()) {
        left_for_merge = std::move(lnode);
        right_for_merge = std::move(this_ref);
        p_this_ref = &right_for_merge;
        is_left = false;
      } else { // lnode free size <= rnode free size
        left_for_merge = std::move(this_ref);
        p_this_ref = &left_for_merge;
        right_for_merge = std::move(rnode);
        is_left = true;
      }
    }

    if (left_for_merge) {
      assert(right_for_merge);
      auto [merge_stage, merge_size] = left_for_merge->impl->evaluate_merge(
          *right_for_merge->impl);
      if (merge_size <= left_for_merge->impl->total_size()) {
        // proceed merge
        bool update_index_after_merge;
        if (is_left) {
          update_index_after_merge = false;
        } else {
          update_index_after_merge = update_parent_index;
        }
        logger().info("OTree::Node::MergeAdjacent: merge {} and {} "
                      "at merge_stage={}, merge_size={}B, update_index={}, is_left={} ...",
                      left_for_merge->get_name(), right_for_merge->get_name(),
                      merge_stage, merge_size, update_index_after_merge, is_left);
        // we currently cannot generate delta depends on another extent content,
        // so use rebuild_extent() as a workaround to rebuild the node from a
        // fresh extent, thus no need to generate delta.
        auto left_addr = left_for_merge->impl->laddr();
        return left_for_merge->rebuild_extent(c
        ).safe_then([c, update_index_after_merge,
                     left_addr,
                     merge_stage = merge_stage,
                     merge_size = merge_size,
                     left_for_merge = std::move(left_for_merge),
                     right_for_merge = std::move(right_for_merge)] (auto left_mut) mutable {
          if (left_for_merge->impl->node_type() == node_type_t::LEAF) {
            auto& left = *static_cast<LeafNode*>(left_for_merge.get());
            left.on_layout_change();
          }
          search_position_t left_last_pos = left_for_merge->impl->merge(
              left_mut, *right_for_merge->impl, merge_stage, merge_size);
          left_for_merge->track_merge(right_for_merge, merge_stage, left_last_pos);
          return left_for_merge->parent_info().ptr->apply_children_merge(
              c, std::move(left_for_merge), left_addr,
              std::move(right_for_merge), update_index_after_merge);
        });
      } else {
        // size will overflow if merge
      }
    }

    // cannot merge
    if (update_parent_index) {
      return fix_parent_index(c, std::move(*p_this_ref), false);
    } else {
      parent_info().ptr->validate_child_tracked(*this);
      return node_ertr::now();
    }
    // XXX: rebalance
  });
}
template node_future<> Node::try_merge_adjacent<true>(context_t, bool, Ref<Node>&&);
template node_future<> Node::try_merge_adjacent<false>(context_t, bool, Ref<Node>&&);

node_future<> Node::erase_node(context_t c, Ref<Node>&& this_ref)
{
  assert(this_ref.get() == this);
  assert(!is_tracking());
  assert(!is_root());
  assert(this_ref->use_count() == 1);
  return parent_info().ptr->erase_child(c, std::move(this_ref));
}

template <bool FORCE_MERGE>
node_future<> Node::fix_parent_index(
    context_t c, Ref<Node>&& this_ref, bool check_downgrade)
{
  assert(!is_root());
  assert(this == this_ref.get());
  auto& parent = parent_info().ptr;
  // one-way unlink
  parent->do_untrack_child(*this);
  // the rest of parent tracks should be correct
  parent->validate_tracked_children();
  return parent->fix_index<FORCE_MERGE>(c, std::move(this_ref), check_downgrade);
}
template node_future<> Node::fix_parent_index<true>(context_t, Ref<Node>&&, bool);
template node_future<> Node::fix_parent_index<false>(context_t, Ref<Node>&&, bool);

node_future<Ref<Node>> Node::load(
    context_t c, laddr_t addr, bool expect_is_level_tail)
{
  // NOTE:
  // *option1: all types of node have the same length;
  // option2: length is defined by node/field types;
  // option3: length is totally flexible;
  return c.nm.read_extent(c.t, addr, NODE_BLOCK_SIZE
  ).safe_then([expect_is_level_tail](auto extent) {
    auto [node_type, field_type] = extent->get_types();
    if (node_type == node_type_t::LEAF) {
      auto impl = LeafNodeImpl::load(extent, field_type, expect_is_level_tail);
      return Ref<Node>(new LeafNode(impl.get(), std::move(impl)));
    } else if (node_type == node_type_t::INTERNAL) {
      auto impl = InternalNodeImpl::load(extent, field_type, expect_is_level_tail);
      return Ref<Node>(new InternalNode(impl.get(), std::move(impl)));
    } else {
      ceph_abort("impossible path");
    }
  });
}

node_future<NodeExtentMutable> Node::rebuild_extent(context_t c)
{
  logger().debug("OTree::Node::Rebuild: {} ...", get_name());
  assert(!is_root());
  // assume I'm already ref counted by caller

  // note: laddr can be changed after rebuild, but we don't fix the parent
  // mapping as it is part of the merge process.
  return impl->rebuild_extent(c);
}

node_future<> Node::retire(context_t c, Ref<Node>&& this_ref)
{
  logger().debug("OTree::Node::Retire: {} ...", get_name());
  assert(this_ref.get() == this);
  assert(!is_tracking());
  assert(!super);
  // make sure the parent also has untracked this node
  assert(!_parent_info.has_value());
  assert(this_ref->use_count() == 1);

  return impl->retire_extent(c
  ).safe_then([this_ref = std::move(this_ref)]{ /* deallocate node */});
}

void Node::make_tail(context_t c)
{
  assert(!impl->is_level_tail());
  assert(!impl->is_keys_empty());
  logger().debug("OTree::Node::MakeTail: {} ...", get_name());
  impl->prepare_mutate(c);
  auto tail_pos = impl->make_tail();
  if (impl->node_type() == node_type_t::INTERNAL) {
    auto& node = *static_cast<InternalNode*>(this);
    node.track_make_tail(tail_pos);
  }
}

/*
 * InternalNode
 */

InternalNode::InternalNode(InternalNodeImpl* impl, NodeImplURef&& impl_ref)
  : Node(std::move(impl_ref)), impl{impl} {}

node_future<Ref<tree_cursor_t>>
InternalNode::get_next_cursor(context_t c, const search_position_t& pos)
{
  impl->validate_non_empty();
  if (pos.is_end()) {
    assert(impl->is_level_tail());
    return get_next_cursor_from_parent(c);
  }

  search_position_t next_pos = pos;
  const laddr_packed_t* p_child_addr = nullptr;
  impl->get_next_slot(next_pos, nullptr, &p_child_addr);
  if (next_pos.is_end() && !impl->is_level_tail()) {
    return get_next_cursor_from_parent(c);
  } else {
    if (next_pos.is_end()) {
      p_child_addr = impl->get_tail_value();
    }
    assert(p_child_addr);
    return get_or_track_child(c, next_pos, p_child_addr->value
    ).safe_then([c](auto child) {
      return child->lookup_smallest(c);
    });
  }
}

node_future<> InternalNode::apply_child_split(
    context_t c, Ref<Node>&& left_child, Ref<Node>&& right_child,
    bool update_right_index)
{
  auto& left_pos = left_child->parent_info().position;

#ifndef NDEBUG
  assert(left_child->parent_info().ptr.get() == this);
  assert(!left_child->impl->is_level_tail());
  if (left_pos.is_end()) {
    assert(impl->is_level_tail());
    assert(right_child->impl->is_level_tail());
    assert(!update_right_index);
  }
#endif

  impl->prepare_mutate(c);

  logger().debug("OTree::Internal::ApplyChildSplit: apply {}'s child "
                 "{} to split to {}, update_index={} ...",
                 get_name(), left_child->get_name(),
                 right_child->get_name(), update_right_index);

  // update layout from left_pos => left_child_addr to right_child_addr
  auto left_child_addr = left_child->impl->laddr();
  auto right_child_addr = right_child->impl->laddr();
  impl->replace_child_addr(left_pos, right_child_addr, left_child_addr);

  // update track from left_pos => left_child to right_child
  replace_track(right_child, left_child, update_right_index);

  auto left_key = *left_child->impl->get_pivot_index();
  Ref<Node> this_ref = this;
  return insert_or_split(
      c, left_pos, left_key, left_child,
      (update_right_index ? right_child : nullptr)
  ).safe_then([this, c,
               this_ref = std::move(this_ref)] (auto split_right) mutable {
    if (split_right) {
      // even if update_right_index could be true,
      // we haven't fixed the right_child index of this node yet,
      // so my parent index should be correct now.
      return apply_split_to_parent(
          c, std::move(this_ref), std::move(split_right), false);
    } else {
      return node_ertr::now();
    }
  }).safe_then([c, update_right_index,
                right_child = std::move(right_child)] () mutable {
    if (update_right_index) {
      // right_child must be already untracked by insert_or_split()
      return right_child->parent_info().ptr->fix_index(
          c, std::move(right_child), false);
    } else {
      // there is no need to call try_merge_adjacent() because
      // the filled size of the inserted node or the split right node
      // won't be reduced if update_right_index is false.
      return node_ertr::now();
    }
  });
}

node_future<> InternalNode::erase_child(context_t c, Ref<Node>&& child_ref)
{
  // this is a special version of recursive merge
  impl->validate_non_empty();
  assert(child_ref->use_count() == 1);
  validate_child_tracked(*child_ref);

  // fix the child's previous node as the new tail,
  // and trigger prv_child_ref->try_merge_adjacent() at the end
  bool fix_tail = (child_ref->parent_info().position.is_end() &&
                   !impl->is_keys_empty());
  return node_ertr::now().safe_then([c, this, fix_tail] {
    if (fix_tail) {
      search_position_t new_tail_pos;
      const laddr_packed_t* new_tail_p_addr = nullptr;
      impl->get_largest_slot(&new_tail_pos, nullptr, &new_tail_p_addr);
      return get_or_track_child(c, new_tail_pos, new_tail_p_addr->value);
    } else {
      return node_ertr::make_ready_future<Ref<Node>>();
    }
  }).safe_then([c, this, child_ref = std::move(child_ref)]
               (auto&& new_tail_child) mutable {
    auto child_pos = child_ref->parent_info().position;
    if (new_tail_child) {
      logger().info("OTree::Internal::EraseChild: erase {}'s child {} at pos({}), "
                    "and fix new child tail {} at pos({}) ...",
                    get_name(), child_ref->get_name(), child_pos,
                    new_tail_child->get_name(),
                    new_tail_child->parent_info().position);
      assert(!new_tail_child->impl->is_level_tail());
      new_tail_child->make_tail(c);
      assert(new_tail_child->impl->is_level_tail());
      if (new_tail_child->impl->node_type() == node_type_t::LEAF) {
        // no need to proceed merge because the filled size is not changed
        new_tail_child.reset();
      }
    } else {
      logger().info("OTree::Internal::EraseChild: erase {}'s child {} at pos({}) ...",
                    get_name(), child_ref->get_name(), child_pos);
    }

    do_untrack_child(*child_ref);
    Ref<Node> this_ref = this;
    child_ref->_parent_info.reset();
    return child_ref->retire(c, std::move(child_ref)
    ).safe_then([c, this, child_pos, this_ref = std::move(this_ref)] () mutable {
      if ((impl->is_level_tail() && impl->is_keys_empty()) ||
          (!impl->is_level_tail() && impl->is_keys_one())) {
        // there is only one value left
        // fast path without mutating the extent
        logger().debug("OTree::Internal::EraseChild: {} has one value left, erase ...",
                       get_name());
#ifndef NDEBUG
        if (impl->is_level_tail()) {
          assert(child_pos.is_end());
        } else {
          assert(child_pos == search_position_t::begin());
        }
#endif

        if (is_root()) {
          // Note: if merge/split works as expected, we should never encounter the
          // situation when the internal root has <=1 children:
          //
          // A newly created internal root (see Node::upgrade_root()) will have 2
          // children after split is finished.
          //
          // When merge happens, children will try to merge each other, and if the
          // root detects there is only one child left, the root will be
          // down-graded to the only child.
          //
          // In order to preserve the invariant, we need to make sure the new
          // internal root also has at least 2 children.
          ceph_abort("trying to erase the last item from the internal root node");
        }

        // track erase
        assert(tracked_child_nodes.empty());

        // no child should be referencing this node now, this_ref is the last one.
        assert(this_ref->use_count() == 1);
        return Node::erase_node(c, std::move(this_ref));
      }

      impl->prepare_mutate(c);
      auto [erase_stage, next_or_last_pos] = impl->erase(child_pos);
      if (child_pos.is_end()) {
        // next_or_last_pos as last_pos
        track_make_tail(next_or_last_pos);
      } else {
        // next_or_last_pos as next_pos
        track_erase(child_pos, erase_stage);
      }
      validate_tracked_children();

      if (is_root()) {
        return try_downgrade_root(c, std::move(this_ref));
      } else {
        bool update_parent_index;
        if (impl->is_level_tail()) {
          update_parent_index = false;
        } else {
          // next_or_last_pos as next_pos
          next_or_last_pos.is_end() ? update_parent_index = true
                                    : update_parent_index = false;
        }
        return try_merge_adjacent(c, update_parent_index, std::move(this_ref));
      }
    }).safe_then([c, new_tail_child = std::move(new_tail_child)] () mutable {
      // finally, check if the new tail child needs to merge
      if (new_tail_child && !new_tail_child->is_root()) {
        assert(new_tail_child->impl->is_level_tail());
        return new_tail_child->try_merge_adjacent(
            c, false, std::move(new_tail_child));
      } else {
        return node_ertr::now();
      }
    });
  });
}

template <bool FORCE_MERGE>
node_future<> InternalNode::fix_index(
    context_t c, Ref<Node>&& child, bool check_downgrade)
{
  impl->validate_non_empty();

  auto& child_pos = child->parent_info().position;
  // child must already be untracked before calling fix_index()
  assert(tracked_child_nodes.find(child_pos) == tracked_child_nodes.end());
  validate_child_inconsistent(*child);

  impl->prepare_mutate(c);

  key_view_t new_key = *child->impl->get_pivot_index();
  logger().debug("OTree::Internal::FixIndex: fix {}'s index of child {} at pos({}), "
                 "new_key={} ...",
                 get_name(), child->get_name(), child_pos, new_key);

  // erase the incorrect item
  auto [erase_stage, next_pos] = impl->erase(child_pos);
  track_erase(child_pos, erase_stage);
  validate_tracked_children();

  // find out whether there is a need to fix parent index recursively
  bool update_parent_index;
  if (impl->is_level_tail()) {
    update_parent_index = false;
  } else {
    next_pos.is_end() ? update_parent_index = true
                      : update_parent_index = false;
  }

  Ref<Node> this_ref = this;
  return insert_or_split(c, next_pos, new_key, child
  ).safe_then([this, c, update_parent_index, check_downgrade,
               this_ref = std::move(this_ref)] (auto split_right) mutable {
    if (split_right) {
      // after split, the parent index to the split_right will be incorrect
      // if update_parent_index is true.
      return apply_split_to_parent(
          c, std::move(this_ref), std::move(split_right), update_parent_index);
    } else {
      // no split path
      if (is_root()) {
        if (check_downgrade) {
          return try_downgrade_root(c, std::move(this_ref));
        } else {
          // no need to call try_downgrade_root() because the number of keys
          // has not changed, and I must have at least 2 keys.
          assert(!impl->is_keys_empty());
          return node_ertr::now();
        }
      } else {
        // for non-root, maybe need merge adjacent or fix parent,
        // because the filled node size may be reduced.
        return try_merge_adjacent<FORCE_MERGE>(
            c, update_parent_index, std::move(this_ref));
      }
    }
  });
}

template <bool FORCE_MERGE>
node_future<> InternalNode::apply_children_merge(
    context_t c, Ref<Node>&& left_child, laddr_t origin_left_addr,
    Ref<Node>&& right_child, bool update_index)
{
  auto left_pos = left_child->parent_info().position;
  auto left_addr = left_child->impl->laddr();
  auto& right_pos = right_child->parent_info().position;
  auto right_addr = right_child->impl->laddr();
  logger().debug("OTree::Internal::ApplyChildMerge: apply {}'s child {} (was {:#x}) "
                 "at pos({}), to merge with {} at pos({}), update_index={} ...",
                 get_name(), left_child->get_name(), origin_left_addr, left_pos,
                 right_child->get_name(), right_pos, update_index);

#ifndef NDEBUG
  assert(left_child->parent_info().ptr == this);
  assert(!left_pos.is_end());
  const laddr_packed_t* p_value_left;
  impl->get_slot(left_pos, nullptr, &p_value_left);
  assert(p_value_left->value == origin_left_addr);

  assert(right_child->use_count() == 1);
  assert(right_child->parent_info().ptr == this);
  const laddr_packed_t* p_value_right;
  if (right_pos.is_end()) {
    assert(right_child->impl->is_level_tail());
    assert(left_child->impl->is_level_tail());
    assert(impl->is_level_tail());
    assert(!update_index);
    p_value_right = impl->get_tail_value();
  } else {
    assert(!right_child->impl->is_level_tail());
    assert(!left_child->impl->is_level_tail());
    impl->get_slot(right_pos, nullptr, &p_value_right);
  }
  assert(p_value_right->value == right_addr);
#endif

  // XXX: we may jump to try_downgrade_root() without mutating this node.

  // update layout from right_pos => right_addr to left_addr
  impl->prepare_mutate(c);
  impl->replace_child_addr(right_pos, left_addr, right_addr);

  // update track from right_pos => right_child to left_child
  do_untrack_child(*left_child);
  replace_track(left_child, right_child, update_index);

  // erase left_pos from layout
  auto [erase_stage, next_pos] = impl->erase(left_pos);
  track_erase<false>(left_pos, erase_stage);
  assert(next_pos == left_child->parent_info().position);

  // All good to retire the right_child.
  // I'm already ref-counted by left_child.
  return right_child->retire(c, std::move(right_child)
  ).safe_then([c, this, update_index,
               left_child = std::move(left_child)] () mutable {
    if (update_index) {
      // I'm all good but:
      // - my number of keys is reduced by 1
      // - my size may underflow, but try_merge_adjacent() is already part of fix_index()
      return left_child->fix_parent_index<FORCE_MERGE>(c, std::move(left_child), true);
    } else {
      validate_tracked_children();
      Ref<Node> this_ref = this;
      left_child.reset();
      // I'm all good but:
      // - my number of keys is reduced by 1
      // - my size may underflow
      if (is_root()) {
        return try_downgrade_root(c, std::move(this_ref));
      } else {
        return try_merge_adjacent<FORCE_MERGE>(
            c, false, std::move(this_ref));
      }
    }
  });
}
template node_future<> InternalNode::apply_children_merge<true>(
    context_t, Ref<Node>&&, laddr_t, Ref<Node>&&, bool);
template node_future<> InternalNode::apply_children_merge<false>(
    context_t, Ref<Node>&&, laddr_t, Ref<Node>&&, bool);

node_future<std::pair<Ref<Node>, Ref<Node>>> InternalNode::get_child_peers(
    context_t c, const search_position_t& pos)
{
  // assume I'm already ref counted by caller
  search_position_t prev_pos;
  const laddr_packed_t* prev_p_child_addr = nullptr;
  search_position_t next_pos;
  const laddr_packed_t* next_p_child_addr = nullptr;

  if (pos.is_end()) {
    assert(impl->is_level_tail());
    if (!impl->is_keys_empty()) {
      // got previous child only
      impl->get_largest_slot(&prev_pos, nullptr, &prev_p_child_addr);
      assert(prev_pos < pos);
      assert(prev_p_child_addr != nullptr);
    } else {
      // no keys, so no peer children
    }
  } else { // !pos.is_end()
    if (pos != search_position_t::begin()) {
      // got previous child
      prev_pos = pos;
      impl->get_prev_slot(prev_pos, nullptr, &prev_p_child_addr);
      assert(prev_pos < pos);
      assert(prev_p_child_addr != nullptr);
    } else {
      // is already the first child, so no previous child
    }

    next_pos = pos;
    impl->get_next_slot(next_pos, nullptr, &next_p_child_addr);
    if (next_pos.is_end()) {
      if (impl->is_level_tail()) {
        // the next child is the tail
        next_p_child_addr = impl->get_tail_value();
        assert(pos < next_pos);
        assert(next_p_child_addr != nullptr);
      } else {
        // next child doesn't exist
        assert(next_p_child_addr == nullptr);
      }
    } else {
      // got the next child
      assert(pos < next_pos);
      assert(next_p_child_addr != nullptr);
    }
  }

  return node_ertr::now().safe_then([this, c, prev_pos, prev_p_child_addr] {
    if (prev_p_child_addr != nullptr) {
      return get_or_track_child(c, prev_pos, prev_p_child_addr->value);
    } else {
      return node_ertr::make_ready_future<Ref<Node>>();
    }
  }).safe_then([this, c, next_pos, next_p_child_addr] (Ref<Node> lnode) {
    if (next_p_child_addr != nullptr) {
      return get_or_track_child(c, next_pos, next_p_child_addr->value
      ).safe_then([lnode] (Ref<Node> rnode) {
        return node_ertr::make_ready_future<std::pair<Ref<Node>, Ref<Node>>>(
            lnode, rnode);
      });
    } else {
      return node_ertr::make_ready_future<std::pair<Ref<Node>, Ref<Node>>>(
          lnode, nullptr);
    }
  });
}

node_future<Ref<InternalNode>> InternalNode::allocate_root(
    context_t c, level_t old_root_level,
    laddr_t old_root_addr, Super::URef&& super)
{
  return InternalNode::allocate(c, field_type_t::N0, true, old_root_level + 1
  ).safe_then([c, old_root_addr,
               super = std::move(super)](auto fresh_node) mutable {
    auto root = fresh_node.node;
    assert(root->impl->is_keys_empty());
    auto p_value = root->impl->get_tail_value();
    fresh_node.mut.copy_in_absolute(
        const_cast<laddr_packed_t*>(p_value), old_root_addr);
    root->make_root_from(c, std::move(super), old_root_addr);
    return root;
  });
}

node_future<Ref<tree_cursor_t>>
InternalNode::lookup_smallest(context_t c)
{
  impl->validate_non_empty();
  auto position = search_position_t::begin();
  const laddr_packed_t* p_child_addr;
  impl->get_slot(position, nullptr, &p_child_addr);
  return get_or_track_child(c, position, p_child_addr->value
  ).safe_then([c](auto child) {
    return child->lookup_smallest(c);
  });
}

node_future<Ref<tree_cursor_t>>
InternalNode::lookup_largest(context_t c)
{
  // NOTE: unlike LeafNode::lookup_largest(), this only works for the tail
  // internal node to return the tail child address.
  impl->validate_non_empty();
  assert(impl->is_level_tail());
  auto p_child_addr = impl->get_tail_value();
  return get_or_track_child(c, search_position_t::end(), p_child_addr->value
  ).safe_then([c](auto child) {
    return child->lookup_largest(c);
  });
}

node_future<Node::search_result_t>
InternalNode::lower_bound_tracked(
    context_t c, const key_hobj_t& key, MatchHistory& history)
{
  auto result = impl->lower_bound(key, history);
  return get_or_track_child(c, result.position, result.p_value->value
  ).safe_then([c, &key, &history](auto child) {
    // XXX(multi-type): pass result.mstat to child
    return child->lower_bound_tracked(c, key, history);
  });
}

node_future<> InternalNode::do_get_tree_stats(
    context_t c, tree_stats_t& stats)
{
  impl->validate_non_empty();
  auto nstats = impl->get_stats();
  stats.size_persistent_internal += nstats.size_persistent;
  stats.size_filled_internal += nstats.size_filled;
  stats.size_logical_internal += nstats.size_logical;
  stats.size_overhead_internal += nstats.size_overhead;
  stats.size_value_internal += nstats.size_value;
  stats.num_kvs_internal += nstats.num_kvs;
  stats.num_nodes_internal += 1;

  Ref<Node> this_ref = this;
  return seastar::do_with(
    search_position_t(), (const laddr_packed_t*)(nullptr),
    [this, this_ref, c, &stats](auto& pos, auto& p_child_addr) {
      pos = search_position_t::begin();
      impl->get_slot(pos, nullptr, &p_child_addr);
      return crimson::do_until(
          [this, this_ref, c, &stats, &pos, &p_child_addr]() -> node_future<bool> {
        return get_or_track_child(c, pos, p_child_addr->value
        ).safe_then([c, &stats](auto child) {
          return child->do_get_tree_stats(c, stats);
        }).safe_then([this, this_ref, &pos, &p_child_addr] {
          if (pos.is_end()) {
            return node_ertr::make_ready_future<bool>(true);
          } else {
            impl->get_next_slot(pos, nullptr, &p_child_addr);
            if (pos.is_end()) {
              if (impl->is_level_tail()) {
                p_child_addr = impl->get_tail_value();
                return node_ertr::make_ready_future<bool>(false);
              } else {
                return node_ertr::make_ready_future<bool>(true);
              }
            } else {
              return node_ertr::make_ready_future<bool>(false);
            }
          }
        });
      });
    }
  );
}

void InternalNode::track_merge(
    Ref<Node> _right_node, match_stage_t stage, search_position_t& left_last_pos)
{
  assert(level() == _right_node->level());
  assert(impl->node_type() == _right_node->impl->node_type());
  auto& right_node = *static_cast<InternalNode*>(_right_node.get());
  if (right_node.tracked_child_nodes.empty()) {
    return;
  }

  match_stage_t curr_stage = STAGE_BOTTOM;

  // prepare the initial left_last_pos for offset
  while (curr_stage < stage) {
    left_last_pos.index_by_stage(curr_stage) = 0;
    ++curr_stage;
  }
  ++left_last_pos.index_by_stage(curr_stage);

  // fix the tracked child nodes of right_node, stage by stage.
  auto& right_tracked_children = right_node.tracked_child_nodes;
  auto rit = right_tracked_children.begin();
  while (curr_stage <= STAGE_TOP) {
    auto right_pos_until = search_position_t::begin();
    right_pos_until.index_by_stage(curr_stage) = INDEX_UPPER_BOUND;
    auto rend = right_tracked_children.lower_bound(right_pos_until);
    while (rit != rend) {
      auto new_pos = rit->second->parent_info().position;
      assert(new_pos == rit->first);
      assert(rit->second->parent_info().ptr == &right_node);
      new_pos += left_last_pos;
      auto p_child = rit->second;
      rit = right_tracked_children.erase(rit);
      p_child->as_child(new_pos, this);
    }
    left_last_pos.index_by_stage(curr_stage) = 0;
    ++curr_stage;
  }

  // fix the end tracked child node of right_node, if exists.
  if (rit != right_tracked_children.end()) {
    assert(rit->first == search_position_t::end());
    assert(rit->second->parent_info().position == search_position_t::end());
    assert(right_node.impl->is_level_tail());
    assert(impl->is_level_tail());
    auto p_child = rit->second;
    rit = right_tracked_children.erase(rit);
    p_child->as_child(search_position_t::end(), this);
  }
  assert(right_tracked_children.empty());

  validate_tracked_children();
}

node_future<> InternalNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const Node> this_ref = this;
  return InternalNode::allocate(c_other, field_type_t::N0, true, impl->level()
  ).safe_then([this, c_other, &tracker_other](auto fresh_other) {
    impl->test_copy_to(fresh_other.mut);
    auto cloned_root = fresh_other.node;
    return c_other.nm.get_super(c_other.t, tracker_other
    ).safe_then([c_other, cloned_root](auto&& super_other) {
      cloned_root->make_root_new(c_other, std::move(super_other));
      return cloned_root;
    });
  }).safe_then([this_ref, this, c_other](auto cloned_root) {
    // clone tracked children
    // In some unit tests, the children are stubbed out that they
    // don't exist in NodeExtentManager, and are only tracked in memory.
    return crimson::do_for_each(
      tracked_child_nodes.begin(),
      tracked_child_nodes.end(),
      [this_ref, c_other, cloned_root](auto& kv) {
        assert(kv.first == kv.second->parent_info().position);
        return kv.second->test_clone_non_root(c_other, cloned_root);
      }
    );
  });
}

node_future<> InternalNode::try_downgrade_root(
    context_t c, Ref<Node>&& this_ref)
{
  assert(this_ref.get() == this);
  assert(is_root());
  assert(impl->is_level_tail());
  if (!impl->is_keys_empty()) {
    // I have more than 1 values, no need to downgrade
    return node_ertr::now();
  }

  // proceed downgrade root to the only child
  laddr_t child_addr = impl->get_tail_value()->value;
  return get_or_track_child(c, search_position_t::end(), child_addr
  ).safe_then([c, this, this_ref = std::move(this_ref)] (auto child) mutable {
    logger().info("OTree::Internal::DownGradeRoot: downgrade {} to new root {}",
                  get_name(), child->get_name());
    // Invariant, see InternalNode::erase_child()
    // the new internal root should have at least 2 children.
    assert(child->impl->is_level_tail());
    if (child->impl->node_type() == node_type_t::INTERNAL) {
      ceph_assert(!child->impl->is_keys_empty());
    }

    this->super->do_untrack_root(*this);
    assert(tracked_child_nodes.size() == 1);
    do_untrack_child(*child);
    child->_parent_info.reset();
    child->make_root_from(c, std::move(this->super), impl->laddr());
    return retire(c, std::move(this_ref));
  });
}

node_future<Ref<InternalNode>> InternalNode::insert_or_split(
    context_t c,
    const search_position_t& pos,
    const key_view_t& insert_key,
    Ref<Node> insert_child,
    Ref<Node> outdated_child)
{
  // XXX: check the insert_child is unlinked from this node
#ifndef NDEBUG
  auto _insert_key = *insert_child->impl->get_pivot_index();
  assert(insert_key.compare_to(_insert_key) == MatchKindCMP::EQ);
#endif
  auto insert_value = insert_child->impl->laddr();
  auto insert_pos = pos;
  logger().debug("OTree::Internal::InsertSplit: insert {} "
                 "with insert_key={}, insert_child={}, insert_pos({}), "
                 "outdated_child={} ...",
                 get_name(), insert_key, insert_child->get_name(),
                 insert_pos, (outdated_child ? "True" : "False"));
  auto [insert_stage, insert_size] = impl->evaluate_insert(
      insert_key, insert_value, insert_pos);
  auto free_size = impl->free_size();
  if (free_size >= insert_size) {
    // proceed to insert
    [[maybe_unused]] auto p_value = impl->insert(
        insert_key, insert_value, insert_pos, insert_stage, insert_size);
    assert(impl->free_size() == free_size - insert_size);
    assert(insert_pos <= pos);
    assert(p_value->value == insert_value);

    if (outdated_child) {
      track_insert<false>(insert_pos, insert_stage, insert_child);
      // untrack the inaccurate child after updated its position
      // before validate, and before fix_index()
      validate_child_inconsistent(*outdated_child);
      // we will need its parent_info valid for the following fix_index()
      do_untrack_child(*outdated_child);
    } else {
      track_insert(insert_pos, insert_stage, insert_child);
    }

    validate_tracked_children();
    return node_ertr::make_ready_future<Ref<InternalNode>>(nullptr);
  }

  // proceed to split with insert
  // assume I'm already ref-counted by caller
  return (is_root() ? upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return InternalNode::allocate(
        c, impl->field_type(), impl->is_level_tail(), impl->level());
  }).safe_then([this, insert_key, insert_child, insert_pos,
                insert_stage=insert_stage, insert_size=insert_size,
                outdated_child](auto fresh_right) mutable {
    // I'm the left_node and need to split into the right_node
    auto right_node = fresh_right.node;
    logger().info("OTree::Internal::InsertSplit: proceed split {} to fresh {} "
                  "with insert_child={}, outdated_child={} ...",
                  get_name(), right_node->get_name(),
                  insert_child->get_name(),
                  (outdated_child ? outdated_child->get_name() : "N/A"));
    auto insert_value = insert_child->impl->laddr();
    auto [split_pos, is_insert_left, p_value] = impl->split_insert(
        fresh_right.mut, *right_node->impl, insert_key, insert_value,
        insert_pos, insert_stage, insert_size);
    assert(p_value->value == insert_value);
    track_split(split_pos, right_node);

    if (outdated_child) {
      if (is_insert_left) {
        track_insert<false>(insert_pos, insert_stage, insert_child);
      } else {
        right_node->template track_insert<false>(insert_pos, insert_stage, insert_child);
      }
      // untrack the inaccurate child after updated its position
      // before validate, and before fix_index()
      auto& _parent = outdated_child->parent_info().ptr;
      _parent->validate_child_inconsistent(*outdated_child);
      // we will need its parent_info valid for the following fix_index()
      _parent->do_untrack_child(*outdated_child);
    } else {
      if (is_insert_left) {
        track_insert(insert_pos, insert_stage, insert_child);
      } else {
        right_node->track_insert(insert_pos, insert_stage, insert_child);
      }
    }

    validate_tracked_children();
    right_node->validate_tracked_children();
    return right_node;
  });
}

node_future<Ref<Node>> InternalNode::get_or_track_child(
    context_t c, const search_position_t& position, laddr_t child_addr)
{
  bool level_tail = position.is_end();
  Ref<Node> child;
  auto found = tracked_child_nodes.find(position);
  Ref<Node> this_ref = this;
  return (found == tracked_child_nodes.end()
    ? (Node::load(c, child_addr, level_tail
       ).safe_then([this, position] (auto child) {
         child->as_child(position, this);
         logger().trace("OTree::Internal::GetTrackChild: loaded child untracked {} at pos({})",
                        child->get_name(), position);
         return child;
       }))
    : (logger().trace("OTree::Internal::GetTrackChild: loaded child tracked {} at pos({})",
                      found->second->get_name(), position),
       node_ertr::make_ready_future<Ref<Node>>(found->second))
  ).safe_then([this_ref, this, position, child_addr] (auto child) {
    assert(child_addr == child->impl->laddr());
    assert(position == child->parent_info().position);
    std::ignore = position;
    std::ignore = child_addr;
    validate_child_tracked(*child);
    return child;
  });
}

template <bool VALIDATE>
void InternalNode::track_insert(
      const search_position_t& insert_pos, match_stage_t insert_stage,
      Ref<Node> insert_child, Ref<Node> nxt_child)
{
  // update tracks
  auto pos_upper_bound = insert_pos;
  pos_upper_bound.index_by_stage(insert_stage) = INDEX_UPPER_BOUND;
  auto first = tracked_child_nodes.lower_bound(insert_pos);
  auto last = tracked_child_nodes.lower_bound(pos_upper_bound);
  std::vector<Node*> nodes;
  std::for_each(first, last, [&nodes](auto& kv) {
    nodes.push_back(kv.second);
  });
  tracked_child_nodes.erase(first, last);
  for (auto& node : nodes) {
    auto _pos = node->parent_info().position;
    assert(!_pos.is_end());
    ++_pos.index_by_stage(insert_stage);
    node->as_child<VALIDATE>(_pos, this);
  }
  // track insert
  insert_child->as_child(insert_pos, this);

#ifndef NDEBUG
  // validate left_child is before right_child
  if (nxt_child) {
    auto iter = tracked_child_nodes.find(insert_pos);
    ++iter;
    assert(iter->second == nxt_child);
  }
#endif
}
template void InternalNode::track_insert<true>(const search_position_t&, match_stage_t, Ref<Node>, Ref<Node>);
template void InternalNode::track_insert<false>(const search_position_t&, match_stage_t, Ref<Node>, Ref<Node>);

void InternalNode::replace_track(
    Ref<Node> new_child, Ref<Node> old_child, bool is_new_child_outdated)
{
  assert(old_child->parent_info().ptr == this);
  auto& pos = old_child->parent_info().position;
  do_untrack_child(*old_child);
  if (is_new_child_outdated) {
    // we need to keep track of the outdated child through
    // insert and split.
    new_child->as_child<false>(pos, this);
  } else {
    new_child->as_child(pos, this);
  }
  // ok, safe to release ref
  old_child->_parent_info.reset();

#ifndef NDEBUG
  if (is_new_child_outdated) {
    validate_child_inconsistent(*new_child);
  } else {
    validate_child_tracked(*new_child);
  }
#endif
}

void InternalNode::track_split(
    const search_position_t& split_pos, Ref<InternalNode> right_node)
{
  auto iter = tracked_child_nodes.lower_bound(split_pos);
  while (iter != tracked_child_nodes.end()) {
    auto new_pos = iter->first;
    auto p_node = iter->second;
    iter = tracked_child_nodes.erase(iter);
    new_pos -= split_pos;
    p_node->as_child<false>(new_pos, right_node);
  }
}

template <bool VALIDATE>
void InternalNode::track_erase(
    const search_position_t& erase_pos, match_stage_t erase_stage)
{
  auto first = tracked_child_nodes.lower_bound(erase_pos);
  assert(first == tracked_child_nodes.end() ||
         first->first != erase_pos);
  auto pos_upper_bound = erase_pos;
  pos_upper_bound.index_by_stage(erase_stage) = INDEX_UPPER_BOUND;
  auto last = tracked_child_nodes.lower_bound(pos_upper_bound);
  std::vector<Node*> p_nodes;
  std::for_each(first, last, [&p_nodes](auto& kv) {
    p_nodes.push_back(kv.second);
  });
  tracked_child_nodes.erase(first, last);
  for (auto& p_node: p_nodes) {
    auto new_pos = p_node->parent_info().position;
    assert(new_pos.index_by_stage(erase_stage) > 0);
    --new_pos.index_by_stage(erase_stage);
    p_node->as_child<VALIDATE>(new_pos, this);
  }
}
template void InternalNode::track_erase<true>(const search_position_t&, match_stage_t);
template void InternalNode::track_erase<false>(const search_position_t&, match_stage_t);

void InternalNode::track_make_tail(const search_position_t& last_pos)
{
  // assume I'm ref counted by the caller.
  assert(impl->is_level_tail());
  assert(!last_pos.is_end());
  assert(tracked_child_nodes.find(search_position_t::end()) ==
         tracked_child_nodes.end());
  auto last_it = tracked_child_nodes.find(last_pos);
  if (last_it != tracked_child_nodes.end()) {
    assert(std::next(last_it) == tracked_child_nodes.end());
    auto p_last_child = last_it->second;
    tracked_child_nodes.erase(last_it);
    p_last_child->as_child(search_position_t::end(), this);
  } else {
    assert(tracked_child_nodes.lower_bound(last_pos) ==
           tracked_child_nodes.end());
  }
}

void InternalNode::validate_child(const Node& child) const
{
#ifndef NDEBUG
  assert(impl->level() - 1 == child.impl->level());
  assert(this == child.parent_info().ptr);
  auto& child_pos = child.parent_info().position;
  if (child_pos.is_end()) {
    assert(impl->is_level_tail());
    assert(child.impl->is_level_tail());
    assert(impl->get_tail_value()->value == child.impl->laddr());
  } else {
    assert(!child.impl->is_level_tail());
    key_view_t index_key;
    const laddr_packed_t* p_child_addr;
    impl->get_slot(child_pos, &index_key, &p_child_addr);
    assert(index_key.compare_to(*child.impl->get_pivot_index()) == MatchKindCMP::EQ);
    assert(p_child_addr->value == child.impl->laddr());
  }
  // XXX(multi-type)
  assert(impl->field_type() <= child.impl->field_type());
#endif
}

void InternalNode::validate_child_inconsistent(const Node& child) const
{
#ifndef NDEBUG
  assert(impl->level() - 1 == child.impl->level());
  assert(this == child.parent_info().ptr);
  auto& child_pos = child.parent_info().position;
  // the tail value has no key to fix
  assert(!child_pos.is_end());
  assert(!child.impl->is_level_tail());

  key_view_t current_key;
  const laddr_packed_t* p_value;
  impl->get_slot(child_pos, &current_key, &p_value);
  key_view_t new_key = *child.impl->get_pivot_index();
  assert(current_key.compare_to(new_key) != MatchKindCMP::EQ);
  assert(p_value->value == child.impl->laddr());
#endif
}

node_future<InternalNode::fresh_node_t> InternalNode::allocate(
    context_t c, field_type_t field_type, bool is_level_tail, level_t level)
{
  return InternalNodeImpl::allocate(c, field_type, is_level_tail, level
  ).safe_then([](auto&& fresh_impl) {
    auto node = Ref<InternalNode>(new InternalNode(
          fresh_impl.impl.get(), std::move(fresh_impl.impl)));
    return fresh_node_t{node, fresh_impl.mut};
  });
}

/*
 * LeafNode
 */

LeafNode::LeafNode(LeafNodeImpl* impl, NodeImplURef&& impl_ref)
  : Node(std::move(impl_ref)), impl{impl} {}

bool LeafNode::is_level_tail() const
{
  return impl->is_level_tail();
}

node_version_t LeafNode::get_version() const
{
  return {layout_version, impl->get_extent_state()};
}

const char* LeafNode::read() const
{
  return impl->read();
}

std::tuple<key_view_t, const value_header_t*>
LeafNode::get_kv(const search_position_t& pos) const
{
  key_view_t key_view;
  const value_header_t* p_value_header;
  impl->get_slot(pos, &key_view, &p_value_header);
  return {key_view, p_value_header};
}

node_future<Ref<tree_cursor_t>>
LeafNode::get_next_cursor(context_t c, const search_position_t& pos)
{
  impl->validate_non_empty();
  search_position_t next_pos = pos;
  key_view_t index_key;
  const value_header_t* p_value_header = nullptr;
  impl->get_next_slot(next_pos, &index_key, &p_value_header);
  if (next_pos.is_end()) {
    if (unlikely(is_level_tail())) {
      return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
          tree_cursor_t::create_end(this));
    } else {
      return get_next_cursor_from_parent(c);
    }
  } else {
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(next_pos, index_key, p_value_header));
  }
}

template <bool FORCE_MERGE>
node_future<Ref<tree_cursor_t>>
LeafNode::erase(context_t c, const search_position_t& pos, bool get_next)
{
  assert(!pos.is_end());
  assert(!impl->is_keys_empty());
  Ref<Node> this_ref = this;
  logger().debug("OTree::Leaf::Erase: erase {}'s pos({}), get_next={} ...",
                 get_name(), pos, get_next);

  // get the next cursor
  return node_ertr::now().safe_then([c, &pos, get_next, this] {
    if (get_next) {
      return get_next_cursor(c, pos);
    } else {
      return node_ertr::make_ready_future<Ref<tree_cursor_t>>();
    }
  }).safe_then([c, &pos, this_ref = std::move(this_ref),
                this] (Ref<tree_cursor_t> next_cursor) {
    if (next_cursor && next_cursor->is_end()) {
      // reset the node reference from the end cursor
      next_cursor.reset();
    }
    return node_ertr::now(
    ).safe_then([c, &pos, this_ref = std::move(this_ref), this] () mutable {
#ifndef NDEBUG
      assert(!impl->is_keys_empty());
      if (impl->is_keys_one()) {
        assert(pos == search_position_t::begin());
      }
#endif
      if (!is_root() && impl->is_keys_one()) {
        // we need to keep the root as an empty leaf node
        // fast path without mutating the extent
        // track_erase
        logger().debug("OTree::Leaf::Erase: {} has one value left, erase ...",
                       get_name());
        assert(tracked_cursors.size() == 1);
        auto iter = tracked_cursors.begin();
        assert(iter->first == pos);
        iter->second->invalidate();
        tracked_cursors.clear();

        // no cursor should be referencing this node now, this_ref is the last one.
        assert(this_ref->use_count() == 1);
        return Node::erase_node(c, std::move(this_ref));
      }

      on_layout_change();
      impl->prepare_mutate(c);
      auto [erase_stage, next_pos] = impl->erase(pos);
      track_erase(pos, erase_stage);
      validate_tracked_cursors();

      if (is_root()) {
        return node_ertr::now();
      } else {
        bool update_parent_index;
        if (impl->is_level_tail()) {
          update_parent_index = false;
        } else {
          next_pos.is_end() ? update_parent_index = true
                            : update_parent_index = false;
        }
        return try_merge_adjacent<FORCE_MERGE>(
            c, update_parent_index, std::move(this_ref));
      }
    }).safe_then([next_cursor] {
      return next_cursor;
    });
  });
}
template node_future<Ref<tree_cursor_t>>
LeafNode::erase<true>(context_t, const search_position_t&, bool);
template node_future<Ref<tree_cursor_t>>
LeafNode::erase<false>(context_t, const search_position_t&, bool);

node_future<> LeafNode::extend_value(
    context_t c, const search_position_t& pos, value_size_t extend_size)
{
  ceph_abort("not implemented");
  return node_ertr::now();
}

node_future<> LeafNode::trim_value(
    context_t c, const search_position_t& pos, value_size_t trim_size)
{
  ceph_abort("not implemented");
  return node_ertr::now();
}

std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
LeafNode::prepare_mutate_value_payload(context_t c)
{
  return impl->prepare_mutate_value_payload(c);
}

node_future<Ref<tree_cursor_t>>
LeafNode::lookup_smallest(context_t)
{
  if (unlikely(impl->is_keys_empty())) {
    assert(is_root());
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        tree_cursor_t::create_end(this));
  }
  auto pos = search_position_t::begin();
  key_view_t index_key;
  const value_header_t* p_value_header;
  impl->get_slot(pos, &index_key, &p_value_header);
  return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, index_key, p_value_header));
}

node_future<Ref<tree_cursor_t>>
LeafNode::lookup_largest(context_t)
{
  if (unlikely(impl->is_keys_empty())) {
    assert(is_root());
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        tree_cursor_t::create_end(this));
  }
  search_position_t pos;
  key_view_t index_key;
  const value_header_t* p_value_header = nullptr;
  impl->get_largest_slot(&pos, &index_key, &p_value_header);
  return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, index_key, p_value_header));
}

node_future<Node::search_result_t>
LeafNode::lower_bound_tracked(
    context_t c, const key_hobj_t& key, MatchHistory& history)
{
  key_view_t index_key;
  auto result = impl->lower_bound(key, history, &index_key);
  Ref<tree_cursor_t> cursor;
  if (result.position.is_end()) {
    assert(!result.p_value);
    cursor = tree_cursor_t::create_end(this);
  } else {
    cursor = get_or_track_cursor(result.position, index_key, result.p_value);
  }
  search_result_t ret{cursor, result.mstat};
  ret.validate_input_key(key, c.vb.get_header_magic());
  return node_ertr::make_ready_future<search_result_t>(ret);
}

node_future<> LeafNode::do_get_tree_stats(context_t, tree_stats_t& stats)
{
  auto nstats = impl->get_stats();
  stats.size_persistent_leaf += nstats.size_persistent;
  stats.size_filled_leaf += nstats.size_filled;
  stats.size_logical_leaf += nstats.size_logical;
  stats.size_overhead_leaf += nstats.size_overhead;
  stats.size_value_leaf += nstats.size_value;
  stats.num_kvs_leaf += nstats.num_kvs;
  stats.num_nodes_leaf += 1;
  return node_ertr::now();
}

void LeafNode::track_merge(
    Ref<Node> _right_node, match_stage_t stage, search_position_t& left_last_pos)
{
  assert(level() == _right_node->level());
  // assert(impl->node_type() == _right_node->impl->node_type());
  auto& right_node = *static_cast<LeafNode*>(_right_node.get());
  if (right_node.tracked_cursors.empty()) {
    return;
  }

  match_stage_t curr_stage = STAGE_BOTTOM;

  // prepare the initial left_last_pos for offset
  while (curr_stage < stage) {
    left_last_pos.index_by_stage(curr_stage) = 0;
    ++curr_stage;
  }
  ++left_last_pos.index_by_stage(curr_stage);

  // fix the tracked child nodes of right_node, stage by stage.
  auto& right_tracked_cursors = right_node.tracked_cursors;
  auto rit = right_tracked_cursors.begin();
  while (curr_stage <= STAGE_TOP) {
    auto right_pos_until = search_position_t::begin();
    right_pos_until.index_by_stage(curr_stage) = INDEX_UPPER_BOUND;
    auto rend = right_tracked_cursors.lower_bound(right_pos_until);
    while (rit != rend) {
      auto new_pos = rit->second->get_position();
      assert(new_pos == rit->first);
      assert(rit->second->get_leaf_node().get() == &right_node);
      new_pos += left_last_pos;
      auto p_cursor = rit->second;
      rit = right_tracked_cursors.erase(rit);
      p_cursor->update_track<true>(this, new_pos);
    }
    left_last_pos.index_by_stage(curr_stage) = 0;
    ++curr_stage;
  }
  assert(right_tracked_cursors.empty());

  validate_tracked_cursors();
}

node_future<> LeafNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const Node> this_ref = this;
  return LeafNode::allocate(c_other, field_type_t::N0, true
  ).safe_then([this, c_other, &tracker_other](auto fresh_other) {
    impl->test_copy_to(fresh_other.mut);
    auto cloned_root = fresh_other.node;
    return c_other.nm.get_super(c_other.t, tracker_other
    ).safe_then([c_other, cloned_root](auto&& super_other) {
      cloned_root->make_root_new(c_other, std::move(super_other));
    });
  }).safe_then([this_ref]{});
}

node_future<Ref<tree_cursor_t>> LeafNode::insert_value(
    context_t c, const key_hobj_t& key, value_config_t vconf,
    const search_position_t& pos, const MatchHistory& history,
    match_stat_t mstat)
{
#ifndef NDEBUG
  if (pos.is_end()) {
    assert(impl->is_level_tail());
  }
#endif
  logger().debug("OTree::Leaf::InsertValue: insert {} "
                 "with insert_key={}, insert_value={}, insert_pos({}), "
                 "history={}, mstat({}) ...",
                 get_name(), key, vconf, pos, history, mstat);
  search_position_t insert_pos = pos;
  auto [insert_stage, insert_size] = impl->evaluate_insert(
      key, vconf, history, mstat, insert_pos);
  auto free_size = impl->free_size();
  if (free_size >= insert_size) {
    // proceed to insert
    on_layout_change();
    impl->prepare_mutate(c);
    auto p_value_header = impl->insert(key, vconf, insert_pos, insert_stage, insert_size);
    assert(impl->free_size() == free_size - insert_size);
    assert(insert_pos <= pos);
    assert(p_value_header->payload_size == vconf.payload_size);
    auto ret = track_insert(insert_pos, insert_stage, p_value_header);
    validate_tracked_cursors();
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(ret);
  }
  // split and insert
  Ref<Node> this_ref = this;
  return (is_root() ? upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return LeafNode::allocate(c, impl->field_type(), impl->is_level_tail());
  }).safe_then([this_ref = std::move(this_ref), this, c, &key, vconf,
                insert_pos, insert_stage=insert_stage, insert_size=insert_size](auto fresh_right) mutable {
    auto right_node = fresh_right.node;
    logger().info("OTree::Leaf::InsertValue: proceed split {} to fresh {} ...",
                  get_name(), right_node->get_name());
    // no need to bump version for right node, as it is fresh
    on_layout_change();
    impl->prepare_mutate(c);
    auto [split_pos, is_insert_left, p_value_header] = impl->split_insert(
        fresh_right.mut, *right_node->impl, key, vconf,
        insert_pos, insert_stage, insert_size);
    assert(p_value_header->payload_size == vconf.payload_size);
    track_split(split_pos, right_node);
    Ref<tree_cursor_t> ret;
    if (is_insert_left) {
      ret = track_insert(insert_pos, insert_stage, p_value_header);
    } else {
      ret = right_node->track_insert(insert_pos, insert_stage, p_value_header);
    }
    validate_tracked_cursors();
    right_node->validate_tracked_cursors();

    return apply_split_to_parent(
        c, std::move(this_ref), std::move(right_node), false
    ).safe_then([ret] {
      return ret;
    });
    // TODO (optimize)
    // try to acquire space from siblings before split... see btrfs
  });
}

node_future<Ref<LeafNode>> LeafNode::allocate_root(
    context_t c, RootNodeTracker& root_tracker)
{
  return LeafNode::allocate(c, field_type_t::N0, true
  ).safe_then([c, &root_tracker](auto fresh_node) {
    auto root = fresh_node.node;
    return c.nm.get_super(c.t, root_tracker
    ).safe_then([c, root](auto&& super) {
      root->make_root_new(c, std::move(super));
      return root;
    });
  });
}

Ref<tree_cursor_t> LeafNode::get_or_track_cursor(
    const search_position_t& position,
    const key_view_t& key, const value_header_t* p_value_header)
{
  assert(!position.is_end());
  assert(p_value_header);
  Ref<tree_cursor_t> p_cursor;
  auto found = tracked_cursors.find(position);
  if (found == tracked_cursors.end()) {
    p_cursor = tree_cursor_t::create(this, position, key, p_value_header);
  } else {
    p_cursor = found->second;
    assert(p_cursor->get_leaf_node() == this);
    assert(p_cursor->get_position() == position);
    p_cursor->update_cache_same_node(key, p_value_header);
  }
  return p_cursor;
}

void LeafNode::validate_cursor(const tree_cursor_t& cursor) const
{
#ifndef NDEBUG
  assert(this == cursor.get_leaf_node().get());
  assert(cursor.is_tracked());
  auto [key, p_value_header] = get_kv(cursor.get_position());
  auto magic = p_value_header->magic;
  assert(key.compare_to(cursor.get_key_view(magic)) == MatchKindCMP::EQ);
  assert(p_value_header == cursor.read_value_header(magic));
#endif
}

Ref<tree_cursor_t> LeafNode::track_insert(
    const search_position_t& insert_pos, match_stage_t insert_stage,
    const value_header_t* p_value_header)
{
  // update cursor position
  auto pos_upper_bound = insert_pos;
  pos_upper_bound.index_by_stage(insert_stage) = INDEX_UPPER_BOUND;
  auto first = tracked_cursors.lower_bound(insert_pos);
  auto last = tracked_cursors.lower_bound(pos_upper_bound);
  std::vector<tree_cursor_t*> p_cursors;
  std::for_each(first, last, [&p_cursors](auto& kv) {
    p_cursors.push_back(kv.second);
  });
  tracked_cursors.erase(first, last);
  for (auto& p_cursor : p_cursors) {
    search_position_t new_pos = p_cursor->get_position();
    ++new_pos.index_by_stage(insert_stage);
    p_cursor->update_track<true>(this, new_pos);
  }

  // track insert
  // TODO: getting key_view_t from stage::proceed_insert() and
  // stage::append_insert() has not supported yet
  return tree_cursor_t::create(this, insert_pos);
}

void LeafNode::track_split(
    const search_position_t& split_pos, Ref<LeafNode> right_node)
{
  // update cursor ownership and position
  auto iter = tracked_cursors.lower_bound(split_pos);
  while (iter != tracked_cursors.end()) {
    auto new_pos = iter->first;
    auto p_cursor = iter->second;
    iter = tracked_cursors.erase(iter);
    new_pos -= split_pos;
    p_cursor->update_track<false>(right_node, new_pos);
  }
}

void LeafNode::track_erase(
    const search_position_t& erase_pos, match_stage_t erase_stage)
{
  // erase tracking and invalidate the erased cursor
  auto to_erase = tracked_cursors.find(erase_pos);
  assert(to_erase != tracked_cursors.end());
  to_erase->second->invalidate();
  auto first = tracked_cursors.erase(to_erase);

  // update cursor position
  assert(first == tracked_cursors.lower_bound(erase_pos));
  auto pos_upper_bound = erase_pos;
  pos_upper_bound.index_by_stage(erase_stage) = INDEX_UPPER_BOUND;
  auto last = tracked_cursors.lower_bound(pos_upper_bound);
  std::vector<tree_cursor_t*> p_cursors;
  std::for_each(first, last, [&p_cursors](auto& kv) {
    p_cursors.push_back(kv.second);
  });
  tracked_cursors.erase(first, last);
  for (auto& p_cursor : p_cursors) {
    search_position_t new_pos = p_cursor->get_position();
    assert(new_pos.index_by_stage(erase_stage) > 0);
    --new_pos.index_by_stage(erase_stage);
    p_cursor->update_track<true>(this, new_pos);
  }
}

node_future<LeafNode::fresh_node_t> LeafNode::allocate(
    context_t c, field_type_t field_type, bool is_level_tail)
{
  return LeafNodeImpl::allocate(c, field_type, is_level_tail
  ).safe_then([](auto&& fresh_impl) {
    auto node = Ref<LeafNode>(new LeafNode(
          fresh_impl.impl.get(), std::move(fresh_impl.impl)));
    return fresh_node_t{node, fresh_impl.mut};
  });
}

}
