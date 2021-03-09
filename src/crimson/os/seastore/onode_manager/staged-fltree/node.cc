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
  assert(!is_end());
  ref_leaf_node->do_track_cursor<true>(*this);
}

tree_cursor_t::tree_cursor_t(
    Ref<LeafNode> node, const search_position_t& pos,
    const key_view_t& key_view, const value_header_t* p_value_header)
      : ref_leaf_node{node}, position{pos}, cache{ref_leaf_node}
{
  assert(!is_end());
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
  if (!is_end()) {
    ref_leaf_node->do_untrack_cursor(*this);
  }
}

tree_cursor_t::future<Ref<tree_cursor_t>>
tree_cursor_t::get_next(context_t c)
{
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
  } else {
    auto key = get_key_view(magic);
    auto prv_key = prv.get_key_view(magic);
    assert(key.compare_to(prv_key) == MatchKindCMP::GT);
    if (ref_leaf_node == prv.ref_leaf_node) {
      position.assert_next_to(prv.position);
    } else {
      assert(!prv.ref_leaf_node->is_level_tail());
      assert(position == search_position_t::begin());
    }
  }
#endif
}

MatchKindCMP tree_cursor_t::compare_to(
    const tree_cursor_t& o, value_magic_t magic) const
{
  // singleton
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
  return ref_leaf_node->extend_value(c, position, extend_size);
}

tree_cursor_t::future<>
tree_cursor_t::trim_value(context_t c, value_size_t trim_size)
{
  return ref_leaf_node->trim_value(c, position, trim_size);
}

template <bool VALIDATE>
void tree_cursor_t::update_track(
    Ref<LeafNode> node, const search_position_t& pos)
{
  // the cursor must be already untracked
  // track the new node and new pos
  assert(!pos.is_end());
  assert(!is_end());
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
  assert(!is_end());
  cache.update_all(ref_leaf_node->get_version(), key_view, p_value_header);
  cache.validate_is_latest(position);
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
  assert(version.layout == current_version.layout);
  if (!version.is_duplicate && current_version.is_duplicate) {
    assert(p_node_base != nullptr);
    assert(key_view.has_value());
    assert(p_value_header != nullptr);
    assert(!value_payload_mut);
    assert(p_value_recorder == nullptr);

    auto current_p_node_base = ref_leaf_node->read();
    assert(current_p_node_base != p_node_base);

    version.is_duplicate = true;
    reset_ptr(p_value_header, p_node_base, current_p_node_base);
    key_view->reset_to(p_node_base, current_p_node_base);

    p_node_base = current_p_node_base;
  } else {
    // cache must be latest.
    // node cannot change is_duplicate from true to false.
    assert(!(version.is_duplicate && !current_version.is_duplicate));
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
  if (is_root()) {
    super->do_untrack_root(*this);
  } else {
    _parent_info->ptr->do_untrack_child(*this);
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

void Node::test_make_destructable(
    context_t c, NodeExtentMutable& mut, Super::URef&& _super)
{
  impl->test_set_tail(mut);
  make_root(c, std::move(_super));
}

node_future<> Node::mkfs(context_t c, RootNodeTracker& root_tracker)
{
  return LeafNode::allocate_root(c, root_tracker
  ).safe_then([](auto ret) { /* FIXME: discard_result(); */ });
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
  });
}

template <bool VALIDATE>
void Node::as_child(const search_position_t& pos, Ref<InternalNode> parent_node)
{
  assert(!super);
  _parent_info = parent_info_t{pos, parent_node};
  parent_info().ptr->do_track_child<VALIDATE>(*this);
}
template void Node::as_child<true>(const search_position_t&, Ref<InternalNode>);
template void Node::as_child<false>(const search_position_t&, Ref<InternalNode>);

node_future<> Node::insert_parent(
    context_t c, const key_view_t& pivot_index, Ref<Node> right_node)
{
  assert(!is_root());
  // TODO(cross-node string dedup)
  return parent_info().ptr->apply_child_split(
      c, parent_info().position, pivot_index, this, right_node);
}

node_future<Ref<tree_cursor_t>>
Node::get_next_cursor_from_parent(context_t c)
{
  assert(!impl->is_level_tail());
  assert(!is_root());
  return parent_info().ptr->get_next_cursor(c, parent_info().position);
}

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

/*
 * InternalNode
 */

InternalNode::InternalNode(InternalNodeImpl* impl, NodeImplURef&& impl_ref)
  : Node(std::move(impl_ref)), impl{impl} {}

node_future<Ref<tree_cursor_t>>
InternalNode::get_next_cursor(context_t c, const search_position_t& pos)
{
  assert(!impl->is_empty());
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
    context_t c, const search_position_t& pos, const key_view_t& left_key,
    Ref<Node> left_child, Ref<Node> right_child)
{
#ifndef NDEBUG
  if (pos.is_end()) {
    assert(impl->is_level_tail());
    assert(right_child->impl->is_level_tail());
  }
  auto _left_key = *left_child->impl->get_pivot_index();
  assert(left_key.compare_to(_left_key) == MatchKindCMP::EQ);
#endif
  impl->prepare_mutate(c);

  auto left_child_addr = left_child->impl->laddr();
  auto right_child_addr = right_child->impl->laddr();
  auto right_key = right_child->impl->get_pivot_index();
  if (right_key.has_value()) {
    logger().debug("OTree::Internal::Insert: "
                   "pos({}), left_child({}, {:#x}), right_child({}, {:#x}) ...",
                   pos, left_key, left_child_addr, *right_key, right_child_addr);
  } else {
    logger().debug("OTree::Internal::Insert: "
                   "pos({}), left_child({}, {:#x}), right_child(N/A, {:#x}) ...",
                   pos, left_key, left_child_addr, right_child_addr);
  }
  // update pos => left_child to pos => right_child
  impl->replace_child_addr(pos, right_child_addr, left_child_addr);
  replace_track(pos, right_child, left_child);

  search_position_t insert_pos = pos;
  auto [insert_stage, insert_size] = impl->evaluate_insert(
      left_key, left_child_addr, insert_pos);
  auto free_size = impl->free_size();
  if (free_size >= insert_size) {
    // insert
    [[maybe_unused]] auto p_value = impl->insert(
        left_key, left_child_addr, insert_pos, insert_stage, insert_size);
    assert(impl->free_size() == free_size - insert_size);
    assert(insert_pos <= pos);
    assert(p_value->value == left_child_addr);
    track_insert(insert_pos, insert_stage, left_child, right_child);
    validate_tracked_children();
    return node_ertr::now();
  }
  // split and insert
  Ref<InternalNode> this_ref = this;
  return (is_root() ? upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return InternalNode::allocate(
        c, impl->field_type(), impl->is_level_tail(), impl->level());
  }).safe_then([this_ref, this, c, left_key, left_child, right_child,
                insert_pos, insert_stage=insert_stage, insert_size=insert_size](auto fresh_right) mutable {
    auto right_node = fresh_right.node;
    auto left_child_addr = left_child->impl->laddr();
    auto [split_pos, is_insert_left, p_value] = impl->split_insert(
        fresh_right.mut, *right_node->impl, left_key, left_child_addr,
        insert_pos, insert_stage, insert_size);
    assert(p_value->value == left_child_addr);
    track_split(split_pos, right_node);
    if (is_insert_left) {
      track_insert(insert_pos, insert_stage, left_child);
    } else {
      right_node->track_insert(insert_pos, insert_stage, left_child);
    }
    validate_tracked_children();
    right_node->validate_tracked_children();

    // propagate index to parent
    // TODO: get from trim()
    key_view_t pivot_index;
    impl->get_largest_slot(nullptr, &pivot_index, nullptr);
    return insert_parent(c, pivot_index, right_node);
    // TODO (optimize)
    // try to acquire space from siblings before split... see btrfs
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
    assert(root->impl->is_empty());
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
  assert(!impl->is_empty());
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
  assert(!impl->is_empty());
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
  assert(!impl->is_empty());
  auto nstats = impl->get_stats();
  stats.size_persistent_internal += nstats.size_persistent;
  stats.size_filled_internal += nstats.size_filled;
  stats.size_logical_internal += nstats.size_logical;
  stats.size_overhead_internal += nstats.size_overhead;
  stats.size_value_internal += nstats.size_value;
  stats.num_kvs_internal += nstats.num_kvs;
  stats.num_nodes_internal += 1;

  Ref<const InternalNode> this_ref = this;
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

node_future<> InternalNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const InternalNode> this_ref = this;
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

node_future<Ref<Node>> InternalNode::get_or_track_child(
    context_t c, const search_position_t& position, laddr_t child_addr)
{
  bool level_tail = position.is_end();
  Ref<Node> child;
  auto found = tracked_child_nodes.find(position);
  Ref<InternalNode> this_ref = this;
  return (found == tracked_child_nodes.end()
    ? (logger().trace("OTree::Internal: load child untracked at {:#x}, pos({}), level={}",
                      child_addr, position, level() - 1),
       Node::load(c, child_addr, level_tail
       ).safe_then([this, position] (auto child) {
         child->as_child(position, this);
         return child;
       }))
    : (logger().trace("OTree::Internal: load child tracked at {:#x}, pos({}), level={}",
                      child_addr, position, level() - 1),
       node_ertr::make_ready_future<Ref<Node>>(found->second))
  ).safe_then([this_ref, this, position, child_addr] (auto child) {
    assert(child_addr == child->impl->laddr());
    assert(position == child->parent_info().position);
    std::ignore = position;
    std::ignore = child_addr;
    validate_child(*child);
    return child;
  });
}

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
    node->as_child(_pos, this);
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

void InternalNode::replace_track(
    const search_position_t& position, Ref<Node> new_child, Ref<Node> old_child)
{
  assert(tracked_child_nodes[position] == old_child);
  tracked_child_nodes.erase(position);
  new_child->as_child(position, this);
  assert(tracked_child_nodes[position] == new_child);
}

void InternalNode::track_split(
    const search_position_t& split_pos, Ref<InternalNode> right_node)
{
  auto first = tracked_child_nodes.lower_bound(split_pos);
  auto iter = first;
  while (iter != tracked_child_nodes.end()) {
    search_position_t new_pos = iter->first;
    new_pos -= split_pos;
    iter->second->as_child<false>(new_pos, right_node);
    ++iter;
  }
  tracked_child_nodes.erase(first, tracked_child_nodes.end());
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
  return {layout_version, impl->is_duplicate()};
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
  assert(!impl->is_empty());
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
  if (unlikely(impl->is_empty())) {
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
  if (unlikely(impl->is_empty())) {
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
  return node_ertr::make_ready_future<search_result_t>(
      search_result_t{cursor, result.mstat});
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

node_future<> LeafNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const LeafNode> this_ref = this;
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
  logger().debug("OTree::Leaf::Insert: "
                 "pos({}), {}, {}, {}, mstat({}) ...",
                 pos, key, vconf, history, mstat);
  search_position_t insert_pos = pos;
  auto [insert_stage, insert_size] = impl->evaluate_insert(
      key, vconf, history, mstat, insert_pos);
  auto free_size = impl->free_size();
  if (free_size >= insert_size) {
    // insert
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
  Ref<LeafNode> this_ref = this;
  return (is_root() ? upgrade_root(c) : node_ertr::now()
  ).safe_then([this, c] {
    return LeafNode::allocate(c, impl->field_type(), impl->is_level_tail());
  }).safe_then([this_ref, this, c, &key, vconf,
                insert_pos, insert_stage=insert_stage, insert_size=insert_size](auto fresh_right) mutable {
    auto right_node = fresh_right.node;
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

    // propagate insert to parent
    // TODO: get from trim()
    key_view_t pivot_index;
    impl->get_largest_slot(nullptr, &pivot_index, nullptr);
    return insert_parent(c, pivot_index, right_node).safe_then([ret] {
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
  assert(!cursor.is_end());
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
  auto first = tracked_cursors.lower_bound(split_pos);
  auto iter = first;
  while (iter != tracked_cursors.end()) {
    search_position_t new_pos = iter->first;
    new_pos -= split_pos;
    iter->second->update_track<false>(right_node, new_pos);
    ++iter;
  }
  tracked_cursors.erase(first, tracked_cursors.end());
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
