// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "node.h"

#include <cassert>
#include <exception>
#include <sstream>

#include "common/likely.h"
#include "crimson/common/utility.h"
#include "crimson/os/seastore/logging.h"

#include "node_extent_manager.h"
#include "node_impl.h"
#include "stages/node_stage_layout.h"

SET_SUBSYS(seastore_onode);

namespace fmt {
template <typename T>
const void* ptr(const ::boost::intrusive_ptr<T>& p) {
  return p.get();
}
}

namespace crimson::os::seastore::onode {
/*
 * tree_cursor_t
 */

// create from insert
tree_cursor_t::tree_cursor_t(Ref<LeafNode> node, const search_position_t& pos)
      : ref_leaf_node{node}, position{pos}, cache{ref_leaf_node}
{
  assert(is_tracked());
  ref_leaf_node->do_track_cursor<true>(*this);
  // do not account updates for the inserted values
  is_mutated = true;
}

// create from lookup
tree_cursor_t::tree_cursor_t(
    Ref<LeafNode> node, const search_position_t& pos,
    const key_view_t& key_view, const value_header_t* p_value_header)
      : ref_leaf_node{node}, position{pos}, cache{ref_leaf_node}
{
  assert(is_tracked());
  update_cache_same_node(key_view, p_value_header);
  ref_leaf_node->do_track_cursor<true>(*this);
}

// lookup reaches the end, contain leaf node for further insert
tree_cursor_t::tree_cursor_t(Ref<LeafNode> node)
      : ref_leaf_node{node}, position{search_position_t::end()}, cache{ref_leaf_node}
{
  assert(is_end());
  assert(ref_leaf_node->is_level_tail());
}

// create an invalid tree_cursor_t
tree_cursor_t::~tree_cursor_t()
{
  if (is_tracked()) {
    ref_leaf_node->do_untrack_cursor(*this);
  }
}

eagain_ifuture<Ref<tree_cursor_t>>
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
    assert(key > prv_key);
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
eagain_ifuture<Ref<tree_cursor_t>>
tree_cursor_t::erase(context_t c, bool get_next)
{
  assert(is_tracked());
  return ref_leaf_node->erase<FORCE_MERGE>(c, position, get_next);
}
template eagain_ifuture<Ref<tree_cursor_t>>
tree_cursor_t::erase<true>(context_t, bool);
template eagain_ifuture<Ref<tree_cursor_t>>
tree_cursor_t::erase<false>(context_t, bool);

std::strong_ordering tree_cursor_t::compare_to(
    const tree_cursor_t& o, value_magic_t magic) const
{
  if (!is_tracked() && !o.is_tracked()) {
    return std::strong_ordering::equal;
  } else if (!is_tracked()) {
    return std::strong_ordering::greater;
  } else if (!o.is_tracked()) {
    return std::strong_ordering::less;
  }

  assert(is_tracked() && o.is_tracked());
  // all tracked cursors are singletons
  if (this == &o) {
    return std::strong_ordering::equal;
  }

  std::strong_ordering ret = std::strong_ordering::equal;
  if (ref_leaf_node == o.ref_leaf_node) {
    ret = position <=> o.position;
  } else {
    auto key = get_key_view(magic);
    auto o_key = o.get_key_view(magic);
    ret = key <=> o_key;
  }
  assert(ret != 0);
  return ret;
}

eagain_ifuture<>
tree_cursor_t::extend_value(context_t c, value_size_t extend_size)
{
  assert(is_tracked());
  return ref_leaf_node->extend_value(c, position, extend_size);
}

eagain_ifuture<>
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
  assert((const char*)p_value_header - p_node_base <
         (int)ref_leaf_node->get_node_size());

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
    auto node_size = ref_leaf_node->get_node_size();

    version.state = current_version.state;
    reset_ptr(p_value_header, p_node_base,
              current_p_node_base, node_size);
    key_view->reset_to(p_node_base, current_p_node_base, node_size);
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
  assert(key_view ==_key_view);
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
  if (!is_tracked()) {
    // possible scenarios:
    //   a. I'm erased;
    //   b. Eagain happened after the node extent is allocated/loaded
    //      and before the node is initialized correctly;
  } else {
    assert(!impl->is_extent_retired());
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

eagain_ifuture<Node::search_result_t> Node::lower_bound(
    context_t c, const key_hobj_t& key)
{
  return seastar::do_with(
    MatchHistory(), [this, c, &key](auto& history) {
      return lower_bound_tracked(c, key, history);
    }
  );
}

eagain_ifuture<std::pair<Ref<tree_cursor_t>, bool>> Node::insert(
    context_t c,
    const key_hobj_t& key,
    value_config_t vconf,
    Ref<Node>&& this_ref)
{
  return seastar::do_with(
    MatchHistory(), [this, c, &key, vconf,
                     this_ref = std::move(this_ref)] (auto& history) mutable {
      return lower_bound_tracked(c, key, history
      ).si_then([c, &key, vconf, &history,
                   this_ref = std::move(this_ref)] (auto result) mutable {
        // the cursor in the result should already hold the root node upwards
        this_ref.reset();
        if (result.match() == MatchKindBS::EQ) {
          return eagain_iertr::make_ready_future<std::pair<Ref<tree_cursor_t>, bool>>(
              std::make_pair(result.p_cursor, false));
        } else {
          auto leaf_node = result.p_cursor->get_leaf_node();
          return leaf_node->insert_value(
              c, key, vconf, result.p_cursor->get_position(), history, result.mstat
          ).si_then([](auto p_cursor) {
            return seastar::make_ready_future<std::pair<Ref<tree_cursor_t>, bool>>(
                std::make_pair(p_cursor, true));
          });
        }
      });
    }
  );
}

eagain_ifuture<std::size_t> Node::erase(
    context_t c,
    const key_hobj_t& key,
    Ref<Node>&& this_ref)
{
  return lower_bound(c, key
  ).si_then([c, this_ref = std::move(this_ref)] (auto result) mutable {
    // the cursor in the result should already hold the root node upwards
    this_ref.reset();
    if (result.match() != MatchKindBS::EQ) {
      return eagain_iertr::make_ready_future<std::size_t>(0);
    }
    auto ref_cursor = result.p_cursor;
    return ref_cursor->erase(c, false
    ).si_then([ref_cursor] (auto next_cursor) {
      assert(ref_cursor->is_invalid());
      assert(!next_cursor);
      return std::size_t(1);
    });
  });
}

eagain_ifuture<tree_stats_t> Node::get_tree_stats(context_t c)
{
  return seastar::do_with(
    tree_stats_t(), [this, c](auto& stats) {
      return do_get_tree_stats(c, stats).si_then([&stats] {
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

eagain_ifuture<> Node::mkfs(context_t c, RootNodeTracker& root_tracker)
{
  LOG_PREFIX(OTree::Node::mkfs);
  return LeafNode::allocate_root(c, root_tracker
  ).si_then([c, FNAME](auto ret) {
    c.t.get_onode_tree_stats().extents_num_delta++;
    INFOT("allocated root {}", c.t, ret->get_name());
  });
}

eagain_ifuture<Ref<Node>> Node::load_root(context_t c, RootNodeTracker& root_tracker)
{
  LOG_PREFIX(OTree::Node::load_root);
  return c.nm.get_super(c.t, root_tracker
  ).handle_error_interruptible(
    eagain_iertr::pass_further{},
    crimson::ct_error::input_output_error::assert_failure([FNAME, c] {
      ERRORT("EIO during get_super()", c.t);
    })
  ).si_then([c, &root_tracker, FNAME](auto&& _super) {
    assert(_super);
    auto root_addr = _super->get_root_laddr();
    assert(root_addr != L_ADDR_NULL);
    TRACET("loading root_addr={:x} ...", c.t, root_addr);
    return Node::load(c, root_addr, true
    ).si_then([c, _super = std::move(_super),
                 &root_tracker, FNAME](auto root) mutable {
      TRACET("loaded {}", c.t, root->get_name());
      assert(root->impl->field_type() == field_type_t::N0);
      root->as_root(std::move(_super));
      std::ignore = c; // as only used in an assert
      std::ignore = root_tracker;
      assert(root == root_tracker.get_root(c.t));
      return seastar::make_ready_future<Ref<Node>>(root);
    });
  });
}

void Node::make_root(context_t c, Super::URef&& _super)
{
  _super->write_root_laddr(c, impl->laddr());
  as_root(std::move(_super));
  c.t.get_onode_tree_stats().depth = static_cast<uint64_t>(level()) + 1;
}

void Node::as_root(Super::URef&& _super)
{
  assert(!is_tracked());
  assert(_super->get_root_laddr() == impl->laddr());
  assert(impl->is_level_tail());
  super = std::move(_super);
  super->do_track_root(*this);
  assert(is_root());
}

Super::URef Node::deref_super()
{
  assert(is_root());
  assert(super->get_root_laddr() == impl->laddr());
  assert(impl->is_level_tail());
  super->do_untrack_root(*this);
  auto ret = std::move(super);
  assert(!is_tracked());
  return ret;
}

eagain_ifuture<> Node::upgrade_root(context_t c, laddr_t hint)
{
  LOG_PREFIX(OTree::Node::upgrade_root);
  assert(impl->field_type() == field_type_t::N0);
  auto super_to_move = deref_super();
  return InternalNode::allocate_root(
      c, hint, impl->level(), impl->laddr(), std::move(super_to_move)
  ).si_then([this, c, FNAME](auto new_root) {
    as_child(search_position_t::end(), new_root);
    INFOT("upgraded from {} to {}",
          c.t, get_name(), new_root->get_name());
  });
}

template <bool VALIDATE>
void Node::as_child(const search_position_t& pos, Ref<InternalNode> parent_node)
{
  assert(!is_tracked() || !is_root());
#ifndef NDEBUG
  // Although I might have an outdated _parent_info during fixing,
  // I must be already untracked.
  if (_parent_info.has_value()) {
    assert(!_parent_info->ptr->check_is_tracking(*this));
  }
#endif
  _parent_info = parent_info_t{pos, parent_node};
  parent_info().ptr->do_track_child<VALIDATE>(*this);
  assert(!is_root());
}
template void Node::as_child<true>(const search_position_t&, Ref<InternalNode>);
template void Node::as_child<false>(const search_position_t&, Ref<InternalNode>);

Ref<InternalNode> Node::deref_parent()
{
  assert(!is_root());
  auto parent_ref = std::move(parent_info().ptr);
  parent_ref->do_untrack_child(*this);
  _parent_info.reset();
  assert(!is_tracked());
  return parent_ref;
}

eagain_ifuture<> Node::apply_split_to_parent(
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

eagain_ifuture<Ref<tree_cursor_t>>
Node::get_next_cursor_from_parent(context_t c)
{
  assert(!impl->is_level_tail());
  assert(!is_root());
  return parent_info().ptr->get_next_cursor(c, parent_info().position);
}

template <bool FORCE_MERGE>
eagain_ifuture<>
Node::try_merge_adjacent(
    context_t c, bool update_parent_index, Ref<Node>&& this_ref)
{
  LOG_PREFIX(OTree::Node::try_merge_adjacent);
  assert(this == this_ref.get());
  impl->validate_non_empty();
  assert(!is_root());
  if constexpr (!FORCE_MERGE) {
    if (!impl->is_size_underflow() &&
        !impl->has_single_value()) {
      // skip merge
      if (update_parent_index) {
        return fix_parent_index(c, std::move(this_ref), false);
      } else {
        parent_info().ptr->validate_child_tracked(*this);
        return eagain_iertr::now();
      }
    }
  }

  return parent_info().ptr->get_child_peers(c, parent_info().position
  ).si_then([c, this_ref = std::move(this_ref), this, FNAME,
               update_parent_index] (auto lr_nodes) mutable -> eagain_ifuture<> {
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
        DEBUGT("merge {} and {} at merge_stage={}, merge_size={}B, "
               "update_index={}, is_left={} ...",
               c.t, left_for_merge->get_name(), right_for_merge->get_name(),
               merge_stage, merge_size, update_index_after_merge, is_left);
        // we currently cannot generate delta depends on another extent content,
        // so use rebuild_extent() as a workaround to rebuild the node from a
        // fresh extent, thus no need to generate delta.
        auto left_addr = left_for_merge->impl->laddr();
        return left_for_merge->rebuild_extent(c
        ).si_then([c, update_index_after_merge,
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
	  --(c.t.get_onode_tree_stats().extents_num_delta);
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
      return eagain_iertr::now();
    }
    // XXX: rebalance
  });
}
template eagain_ifuture<> Node::try_merge_adjacent<true>(context_t, bool, Ref<Node>&&);
template eagain_ifuture<> Node::try_merge_adjacent<false>(context_t, bool, Ref<Node>&&);

eagain_ifuture<> Node::erase_node(context_t c, Ref<Node>&& this_ref)
{
  // To erase a node:
  // 1. I'm supposed to have already untracked any children or cursors
  // 2. unlink parent/super --ptr-> me
  // 3. unlink me --ref-> parent/super
  // 4. retire extent
  // 5. destruct node
  assert(this_ref.get() == this);
  assert(!is_tracking());
  assert(!is_root());
  assert(this_ref->use_count() == 1);
  return parent_info().ptr->erase_child(c, std::move(this_ref));
}

template <bool FORCE_MERGE>
eagain_ifuture<> Node::fix_parent_index(
    context_t c, Ref<Node>&& this_ref, bool check_downgrade)
{
  assert(!is_root());
  assert(this == this_ref.get());
  return parent_info().ptr->fix_index<FORCE_MERGE>(
      c, std::move(this_ref), check_downgrade);
}
template eagain_ifuture<> Node::fix_parent_index<true>(context_t, Ref<Node>&&, bool);
template eagain_ifuture<> Node::fix_parent_index<false>(context_t, Ref<Node>&&, bool);

eagain_ifuture<Ref<Node>> Node::load(
    context_t c, laddr_t addr, bool expect_is_level_tail)
{
  LOG_PREFIX(OTree::Node::load);
  return c.nm.read_extent(c.t, addr
  ).handle_error_interruptible(
    eagain_iertr::pass_further{},
    crimson::ct_error::input_output_error::assert_failure(
        [FNAME, c, addr, expect_is_level_tail] {
      ERRORT("EIO -- addr={:x}, is_level_tail={}",
             c.t, addr, expect_is_level_tail);
    }),
    crimson::ct_error::invarg::assert_failure(
        [FNAME, c, addr, expect_is_level_tail] {
      ERRORT("EINVAL -- addr={:x}, is_level_tail={}",
             c.t, addr, expect_is_level_tail);
    }),
    crimson::ct_error::enoent::assert_failure(
        [FNAME, c, addr, expect_is_level_tail] {
      ERRORT("ENOENT -- addr={:x}, is_level_tail={}",
             c.t, addr, expect_is_level_tail);
    }),
    crimson::ct_error::erange::assert_failure(
        [FNAME, c, addr, expect_is_level_tail] {
      ERRORT("ERANGE -- addr={:x}, is_level_tail={}",
             c.t, addr, expect_is_level_tail);
    })
  ).si_then([FNAME, c, addr, expect_is_level_tail](auto extent)
	      -> eagain_ifuture<Ref<Node>> {
    assert(extent);
    auto header = extent->get_header();
    auto field_type = header.get_field_type();
    if (!field_type) {
      ERRORT("load addr={:x}, is_level_tail={} error, "
             "got invalid header -- {}",
             c.t, addr, expect_is_level_tail, fmt::ptr(extent));
      ceph_abort("fatal error");
    }
    if (header.get_is_level_tail() != expect_is_level_tail) {
      ERRORT("load addr={:x}, is_level_tail={} error, "
             "is_level_tail mismatch -- {}",
             c.t, addr, expect_is_level_tail, fmt::ptr(extent));
      ceph_abort("fatal error");
    }

    auto node_type = header.get_node_type();
    if (node_type == node_type_t::LEAF) {
      if (extent->get_length() != c.vb.get_leaf_node_size()) {
        ERRORT("load addr={:x}, is_level_tail={} error, "
               "leaf length mismatch -- {}",
               c.t, addr, expect_is_level_tail, fmt::ptr(extent));
        ceph_abort("fatal error");
      }
      auto impl = LeafNodeImpl::load(extent, *field_type);
      auto *derived_ptr = impl.get();
      return eagain_iertr::make_ready_future<Ref<Node>>(
	new LeafNode(derived_ptr, std::move(impl)));
    } else if (node_type == node_type_t::INTERNAL) {
      if (extent->get_length() != c.vb.get_internal_node_size()) {
        ERRORT("load addr={:x}, is_level_tail={} error, "
               "internal length mismatch -- {}",
               c.t, addr, expect_is_level_tail, fmt::ptr(extent));
        ceph_abort("fatal error");
      }
      auto impl = InternalNodeImpl::load(extent, *field_type);
      auto *derived_ptr = impl.get();
      return eagain_iertr::make_ready_future<Ref<Node>>(
	new InternalNode(derived_ptr, std::move(impl)));
    } else {
      ceph_abort("impossible path");
    }
  });
}

eagain_ifuture<NodeExtentMutable> Node::rebuild_extent(context_t c)
{
  LOG_PREFIX(OTree::Node::rebuild_extent);
  DEBUGT("{} ...", c.t, get_name());
  assert(!is_root());
  // assume I'm already ref counted by caller

  // note: laddr can be changed after rebuild, but we don't fix the parent
  // mapping as it is part of the merge process.
  return impl->rebuild_extent(c);
}

eagain_ifuture<> Node::retire(context_t c, Ref<Node>&& this_ref)
{
  LOG_PREFIX(OTree::Node::retire);
  DEBUGT("{} ...", c.t, get_name());
  assert(this_ref.get() == this);
  assert(!is_tracking());
  assert(!is_tracked());
  assert(this_ref->use_count() == 1);

  return impl->retire_extent(c
  ).si_then([this_ref = std::move(this_ref)]{ /* deallocate node */});
}

void Node::make_tail(context_t c)
{
  LOG_PREFIX(OTree::Node::make_tail);
  assert(!impl->is_level_tail());
  assert(!impl->is_keys_empty());
  DEBUGT("{} ...", c.t, get_name());
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

eagain_ifuture<Ref<tree_cursor_t>>
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
    ).si_then([c](auto child) {
      return child->lookup_smallest(c);
    });
  }
}

eagain_ifuture<> InternalNode::apply_child_split(
    context_t c, Ref<Node>&& left_child, Ref<Node>&& right_child,
    bool update_right_index)
{
  LOG_PREFIX(OTree::InternalNode::apply_child_split);
  auto& left_pos = left_child->parent_info().position;

#ifndef NDEBUG
  assert(left_child->parent_info().ptr.get() == this);
  assert(!left_child->impl->is_level_tail());
  if (left_pos.is_end()) {
    assert(impl->is_level_tail());
    assert(right_child->impl->is_level_tail());
    assert(!update_right_index);
  }

  // right_child has not assigned parent yet
  assert(!right_child->is_tracked());
#endif

  impl->prepare_mutate(c);

  DEBUGT("apply {}'s child {} to split to {}, update_index={} ...",
         c.t, get_name(), left_child->get_name(),
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
  ).si_then([this, c,
             this_ref = std::move(this_ref)] (auto split_right) mutable {
    if (split_right) {
      // even if update_right_index could be true,
      // we haven't fixed the right_child index of this node yet,
      // so my parent index should be correct now.
      return apply_split_to_parent(
          c, std::move(this_ref), std::move(split_right), false);
    } else {
      return eagain_iertr::now();
    }
  }).si_then([c, update_right_index,
                right_child = std::move(right_child)] () mutable {
    if (update_right_index) {
      // XXX: might not need to call validate_tracked_children() in fix_index()
      return right_child->fix_parent_index(c, std::move(right_child), false);
    } else {
      // there is no need to call try_merge_adjacent() because
      // the filled size of the inserted node or the split right node
      // won't be reduced if update_right_index is false.
      return eagain_iertr::now();
    }
  });
}

eagain_ifuture<> InternalNode::erase_child(context_t c, Ref<Node>&& child_ref)
{
  LOG_PREFIX(OTree::InternalNode::erase_child);
  // this is a special version of recursive merge
  impl->validate_non_empty();
  assert(child_ref->use_count() == 1);
  validate_child_tracked(*child_ref);

  // fix the child's previous node as the new tail,
  // and trigger prv_child_ref->try_merge_adjacent() at the end
  bool fix_tail = (child_ref->parent_info().position.is_end() &&
                   !impl->is_keys_empty());
  return eagain_iertr::now().si_then([c, this, fix_tail] {
    if (fix_tail) {
      search_position_t new_tail_pos;
      const laddr_packed_t* new_tail_p_addr = nullptr;
      impl->get_largest_slot(&new_tail_pos, nullptr, &new_tail_p_addr);
      return get_or_track_child(c, new_tail_pos, new_tail_p_addr->value);
    } else {
      return eagain_iertr::make_ready_future<Ref<Node>>();
    }
  }).si_then([c, this, child_ref = std::move(child_ref), FNAME]
               (auto&& new_tail_child) mutable {
    auto child_pos = child_ref->parent_info().position;
    if (new_tail_child) {
      DEBUGT("erase {}'s child {} at pos({}), "
             "and fix new child tail {} at pos({}) ...",
             c.t, get_name(), child_ref->get_name(), child_pos,
             new_tail_child->get_name(), new_tail_child->parent_info().position);
      assert(!new_tail_child->impl->is_level_tail());
      new_tail_child->make_tail(c);
      assert(new_tail_child->impl->is_level_tail());
      if (new_tail_child->impl->node_type() == node_type_t::LEAF) {
        // no need to proceed merge because the filled size is not changed
        new_tail_child.reset();
      }
    } else {
      DEBUGT("erase {}'s child {} at pos({}) ...",
             c.t, get_name(), child_ref->get_name(), child_pos);
    }

    Ref<Node> this_ref = child_ref->deref_parent();
    assert(this_ref == this);
    return child_ref->retire(c, std::move(child_ref)
    ).si_then([c, this, child_pos, FNAME,
                 this_ref = std::move(this_ref)] () mutable {
      if (impl->has_single_value()) {
        // fast path without mutating the extent
        DEBUGT("{} has one value left, erase ...", c.t, get_name());
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
    }).si_then([c, new_tail_child = std::move(new_tail_child)] () mutable {
      // finally, check if the new tail child needs to merge
      if (new_tail_child && !new_tail_child->is_root()) {
        assert(new_tail_child->impl->is_level_tail());
        return new_tail_child->try_merge_adjacent(
            c, false, std::move(new_tail_child));
      } else {
        return eagain_iertr::now();
      }
    });
  });
}

template <bool FORCE_MERGE>
eagain_ifuture<> InternalNode::fix_index(
    context_t c, Ref<Node>&& child, bool check_downgrade)
{
  LOG_PREFIX(OTree::InternalNode::fix_index);
  impl->validate_non_empty();

  validate_child_inconsistent(*child);
  auto& child_pos = child->parent_info().position;
  Ref<Node> this_ref = child->deref_parent();
  assert(this_ref == this);
  validate_tracked_children();

  impl->prepare_mutate(c);

  key_view_t new_key = *child->impl->get_pivot_index();
  DEBUGT("fix {}'s index of child {} at pos({}), new_key={} ...",
         c.t, get_name(), child->get_name(), child_pos, new_key);

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

  return insert_or_split(c, next_pos, new_key, child
  ).si_then([this, c, update_parent_index, check_downgrade,
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
          return eagain_iertr::now();
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
eagain_ifuture<> InternalNode::apply_children_merge(
    context_t c, Ref<Node>&& left_child, laddr_t origin_left_addr,
    Ref<Node>&& right_child, bool update_index)
{
  LOG_PREFIX(OTree::InternalNode::apply_children_merge);
  auto left_pos = left_child->parent_info().position;
  auto left_addr = left_child->impl->laddr();
  auto& right_pos = right_child->parent_info().position;
  auto right_addr = right_child->impl->laddr();
  DEBUGT("apply {}'s child {} (was {:#x}) at pos({}), "
         "to merge with {} at pos({}), update_index={} ...",
         c.t, get_name(), left_child->get_name(), origin_left_addr, left_pos,
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
  left_child->deref_parent();
  replace_track(left_child, right_child, update_index);

  // erase left_pos from layout
  auto [erase_stage, next_pos] = impl->erase(left_pos);
  track_erase<false>(left_pos, erase_stage);
  assert(next_pos == left_child->parent_info().position);

  // All good to retire the right_child.
  // I'm already ref-counted by left_child.
  return right_child->retire(c, std::move(right_child)
  ).si_then([c, this, update_index,
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
template eagain_ifuture<> InternalNode::apply_children_merge<true>(
    context_t, Ref<Node>&&, laddr_t, Ref<Node>&&, bool);
template eagain_ifuture<> InternalNode::apply_children_merge<false>(
    context_t, Ref<Node>&&, laddr_t, Ref<Node>&&, bool);

eagain_ifuture<std::pair<Ref<Node>, Ref<Node>>> InternalNode::get_child_peers(
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

  return eagain_iertr::now().si_then([this, c, prev_pos, prev_p_child_addr] {
    if (prev_p_child_addr != nullptr) {
      return get_or_track_child(c, prev_pos, prev_p_child_addr->value);
    } else {
      return eagain_iertr::make_ready_future<Ref<Node>>();
    }
  }).si_then([this, c, next_pos, next_p_child_addr] (Ref<Node> lnode) {
    if (next_p_child_addr != nullptr) {
      return get_or_track_child(c, next_pos, next_p_child_addr->value
      ).si_then([lnode] (Ref<Node> rnode) {
        return seastar::make_ready_future<std::pair<Ref<Node>, Ref<Node>>>(
            lnode, rnode);
      });
    } else {
      return eagain_iertr::make_ready_future<std::pair<Ref<Node>, Ref<Node>>>(
          lnode, nullptr);
    }
  });
}

eagain_ifuture<Ref<InternalNode>> InternalNode::allocate_root(
    context_t c, laddr_t hint, level_t old_root_level,
    laddr_t old_root_addr, Super::URef&& super)
{
  // support tree height up to 256
  ceph_assert(old_root_level < MAX_LEVEL);
  return InternalNode::allocate(c, hint, field_type_t::N0, true, old_root_level + 1
  ).si_then([c, old_root_addr,
               super = std::move(super)](auto fresh_node) mutable {
    auto root = fresh_node.node;
    assert(root->impl->is_keys_empty());
    auto p_value = root->impl->get_tail_value();
    fresh_node.mut.copy_in_absolute(
        const_cast<laddr_packed_t*>(p_value), old_root_addr);
    root->make_root_from(c, std::move(super), old_root_addr);
    ++(c.t.get_onode_tree_stats().extents_num_delta);
    return root;
  });
}

eagain_ifuture<Ref<tree_cursor_t>>
InternalNode::lookup_smallest(context_t c)
{
  impl->validate_non_empty();
  auto position = search_position_t::begin();
  const laddr_packed_t* p_child_addr;
  impl->get_slot(position, nullptr, &p_child_addr);
  return get_or_track_child(c, position, p_child_addr->value
  ).si_then([c](auto child) {
    return child->lookup_smallest(c);
  });
}

eagain_ifuture<Ref<tree_cursor_t>>
InternalNode::lookup_largest(context_t c)
{
  // NOTE: unlike LeafNode::lookup_largest(), this only works for the tail
  // internal node to return the tail child address.
  impl->validate_non_empty();
  assert(impl->is_level_tail());
  auto p_child_addr = impl->get_tail_value();
  return get_or_track_child(c, search_position_t::end(), p_child_addr->value
  ).si_then([c](auto child) {
    return child->lookup_largest(c);
  });
}

eagain_ifuture<Node::search_result_t>
InternalNode::lower_bound_tracked(
    context_t c, const key_hobj_t& key, MatchHistory& history)
{
  auto result = impl->lower_bound(key, history);
  return get_or_track_child(c, result.position, result.p_value->value
  ).si_then([c, &key, &history](auto child) {
    // XXX(multi-type): pass result.mstat to child
    return child->lower_bound_tracked(c, key, history);
  });
}

eagain_ifuture<> InternalNode::do_get_tree_stats(
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
      return trans_intr::repeat(
        [this, this_ref, c, &stats, &pos, &p_child_addr]()
        -> eagain_ifuture<seastar::stop_iteration> {
        return get_or_track_child(c, pos, p_child_addr->value
        ).si_then([c, &stats](auto child) {
          return child->do_get_tree_stats(c, stats);
        }).si_then([this, this_ref, &pos, &p_child_addr] {
          if (pos.is_end()) {
            return seastar::stop_iteration::yes;
          } else {
            impl->get_next_slot(pos, nullptr, &p_child_addr);
            if (pos.is_end()) {
              if (impl->is_level_tail()) {
                p_child_addr = impl->get_tail_value();
                return seastar::stop_iteration::no;
              } else {
                return seastar::stop_iteration::yes;
              }
            } else {
              return seastar::stop_iteration::no;
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

eagain_ifuture<> InternalNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const Node> this_ref = this;
  return InternalNode::allocate(c_other, L_ADDR_MIN, field_type_t::N0, true, impl->level()
  ).si_then([this, c_other, &tracker_other](auto fresh_other) {
    impl->test_copy_to(fresh_other.mut);
    auto cloned_root = fresh_other.node;
    return c_other.nm.get_super(c_other.t, tracker_other
    ).handle_error_interruptible(
      eagain_iertr::pass_further{},
      crimson::ct_error::assert_all{"Invalid error during test clone"}
    ).si_then([c_other, cloned_root](auto&& super_other) {
      assert(super_other);
      cloned_root->make_root_new(c_other, std::move(super_other));
      return cloned_root;
    });
  }).si_then([this_ref, this, c_other](auto cloned_root) {
    // clone tracked children
    // In some unit tests, the children are stubbed out that they
    // don't exist in NodeExtentManager, and are only tracked in memory.
    return trans_intr::do_for_each(
      tracked_child_nodes.begin(),
      tracked_child_nodes.end(),
      [this_ref, c_other, cloned_root](auto& kv) {
        assert(kv.first == kv.second->parent_info().position);
        return kv.second->test_clone_non_root(c_other, cloned_root);
      }
    );
  });
}

eagain_ifuture<> InternalNode::try_downgrade_root(
    context_t c, Ref<Node>&& this_ref)
{
  LOG_PREFIX(OTree::InternalNode::try_downgrade_root);
  assert(this_ref.get() == this);
  assert(is_root());
  assert(impl->is_level_tail());
  if (!impl->is_keys_empty()) {
    // I have more than 1 values, no need to downgrade
    return eagain_iertr::now();
  }

  // proceed downgrade root to the only child
  laddr_t child_addr = impl->get_tail_value()->value;
  return get_or_track_child(c, search_position_t::end(), child_addr
  ).si_then([c, this, FNAME,
               this_ref = std::move(this_ref)] (auto child) mutable {
    INFOT("downgrade {} to new root {}",
          c.t, get_name(), child->get_name());
    // Invariant, see InternalNode::erase_child()
    // the new internal root should have at least 2 children.
    assert(child->impl->is_level_tail());
    if (child->impl->node_type() == node_type_t::INTERNAL) {
      ceph_assert(!child->impl->is_keys_empty());
    }

    assert(tracked_child_nodes.size() == 1);
    child->deref_parent();
    auto super_to_move = deref_super();
    child->make_root_from(c, std::move(super_to_move), impl->laddr());
    --(c.t.get_onode_tree_stats().extents_num_delta);
    return retire(c, std::move(this_ref));
  });
}

eagain_ifuture<Ref<InternalNode>> InternalNode::insert_or_split(
    context_t c,
    const search_position_t& pos,
    const key_view_t& insert_key,
    Ref<Node> insert_child,
    Ref<Node> outdated_child)
{
  LOG_PREFIX(OTree::InternalNode::insert_or_split);
  // XXX: check the insert_child is unlinked from this node
#ifndef NDEBUG
  auto _insert_key = *insert_child->impl->get_pivot_index();
  assert(insert_key == _insert_key);
#endif
  auto insert_value = insert_child->impl->laddr();
  auto insert_pos = pos;
  DEBUGT("insert {} with insert_key={}, insert_child={}, insert_pos({}), "
         "outdated_child={} ...",
         c.t, get_name(), insert_key, insert_child->get_name(),
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
      validate_child_inconsistent(*outdated_child);
#ifndef NDEBUG
      do_untrack_child(*outdated_child);
      validate_tracked_children();
      do_track_child<false>(*outdated_child);
#endif
    } else {
      track_insert(insert_pos, insert_stage, insert_child);
      validate_tracked_children();
    }

    return eagain_iertr::make_ready_future<Ref<InternalNode>>(nullptr);
  }

  // proceed to split with insert
  // assume I'm already ref-counted by caller
  laddr_t left_hint, right_hint;
  {
    key_view_t left_key;
    impl->get_slot(search_position_t::begin(), &left_key, nullptr);
    left_hint = left_key.get_hint();
    key_view_t right_key;
    impl->get_largest_slot(nullptr, &right_key, nullptr);
    right_hint = right_key.get_hint();
  }
  return (is_root() ? upgrade_root(c, left_hint) : eagain_iertr::now()
  ).si_then([this, c, right_hint] {
    return InternalNode::allocate(
        c, right_hint, impl->field_type(), impl->is_level_tail(), impl->level());
  }).si_then([this, insert_key, insert_child, insert_pos,
                insert_stage=insert_stage, insert_size=insert_size,
                outdated_child, c, FNAME](auto fresh_right) mutable {
    // I'm the left_node and need to split into the right_node
    auto right_node = fresh_right.node;
    DEBUGT("proceed split {} to fresh {} with insert_child={},"
           " outdated_child={} ...",
           c.t, get_name(), right_node->get_name(),
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
#ifndef NDEBUG
      auto& _parent = outdated_child->parent_info().ptr;
      _parent->validate_child_inconsistent(*outdated_child);
      _parent->do_untrack_child(*outdated_child);
      validate_tracked_children();
      right_node->validate_tracked_children();
      _parent->do_track_child<false>(*outdated_child);
#endif
    } else {
      if (is_insert_left) {
        track_insert(insert_pos, insert_stage, insert_child);
      } else {
        right_node->track_insert(insert_pos, insert_stage, insert_child);
      }
      validate_tracked_children();
      right_node->validate_tracked_children();
    }
    ++(c.t.get_onode_tree_stats().extents_num_delta);
    return right_node;
  });
}

eagain_ifuture<Ref<Node>> InternalNode::get_or_track_child(
    context_t c, const search_position_t& position, laddr_t child_addr)
{
  LOG_PREFIX(OTree::InternalNode::get_or_track_child);
  Ref<Node> this_ref = this;
  return [this, position, child_addr, c, FNAME] {
    auto found = tracked_child_nodes.find(position);
    if (found != tracked_child_nodes.end()) {
      TRACET("loaded child tracked {} at pos({}) addr={:x}",
              c.t, found->second->get_name(), position, child_addr);
      return eagain_iertr::make_ready_future<Ref<Node>>(found->second);
    }
    // the child is not loaded yet
    TRACET("loading child at pos({}) addr={:x} ...",
           c.t, position, child_addr);
    bool level_tail = position.is_end();
    return Node::load(c, child_addr, level_tail
    ).si_then([this, position, c, FNAME] (auto child) {
      TRACET("loaded child untracked {}",
             c.t, child->get_name());
      if (child->level() + 1 != level()) {
        ERRORT("loaded child {} error from parent {} at pos({}), level mismatch",
               c.t, child->get_name(), get_name(), position);
        ceph_abort("fatal error");
      }
      child->as_child(position, this);
      return child;
    });
  }().si_then([this_ref, this, position, child_addr] (auto child) {
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
  assert(!new_child->is_tracked());
  auto& pos = old_child->parent_info().position;
  auto this_ref = old_child->deref_parent();
  assert(this_ref == this);
  if (is_new_child_outdated) {
    // we need to keep track of the outdated child through
    // insert and split.
    new_child->as_child<false>(pos, this);
  } else {
    new_child->as_child(pos, this);
  }

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
    assert(index_key == *child.impl->get_pivot_index());
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
  assert(check_is_tracking(child));
  auto& child_pos = child.parent_info().position;
  // the tail value has no key to fix
  assert(!child_pos.is_end());
  assert(!child.impl->is_level_tail());

  key_view_t current_key;
  const laddr_packed_t* p_value;
  impl->get_slot(child_pos, &current_key, &p_value);
  key_view_t new_key = *child.impl->get_pivot_index();
  assert(current_key != new_key);
  assert(p_value->value == child.impl->laddr());
#endif
}

eagain_ifuture<InternalNode::fresh_node_t> InternalNode::allocate(
    context_t c, laddr_t hint, field_type_t field_type, bool is_level_tail, level_t level)
{
  return InternalNodeImpl::allocate(c, hint, field_type, is_level_tail, level
  ).si_then([](auto&& fresh_impl) {
    auto *derived_ptr = fresh_impl.impl.get();
    auto node = Ref<InternalNode>(new InternalNode(
          derived_ptr, std::move(fresh_impl.impl)));
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

extent_len_t LeafNode::get_node_size() const
{
  return impl->get_node_size();
}

std::tuple<key_view_t, const value_header_t*>
LeafNode::get_kv(const search_position_t& pos) const
{
  key_view_t key_view;
  const value_header_t* p_value_header;
  impl->get_slot(pos, &key_view, &p_value_header);
  return {key_view, p_value_header};
}

eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::get_next_cursor(context_t c, const search_position_t& pos)
{
  impl->validate_non_empty();
  search_position_t next_pos = pos;
  key_view_t index_key;
  const value_header_t* p_value_header = nullptr;
  impl->get_next_slot(next_pos, &index_key, &p_value_header);
  if (next_pos.is_end()) {
    if (unlikely(is_level_tail())) {
      return eagain_iertr::make_ready_future<Ref<tree_cursor_t>>(
          tree_cursor_t::create_end(this));
    } else {
      return get_next_cursor_from_parent(c);
    }
  } else {
    return eagain_iertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(next_pos, index_key, p_value_header));
  }
}

template <bool FORCE_MERGE>
eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::erase(context_t c, const search_position_t& pos, bool get_next)
{
  LOG_PREFIX(OTree::LeafNode::erase);
  assert(!pos.is_end());
  assert(!impl->is_keys_empty());
  Ref<Node> this_ref = this;
  DEBUGT("erase {}'s pos({}), get_next={} ...",
         c.t, get_name(), pos, get_next);
  ++(c.t.get_onode_tree_stats().num_erases);

  // get the next cursor
  return eagain_iertr::now().si_then([c, &pos, get_next, this] {
    if (get_next) {
      return get_next_cursor(c, pos);
    } else {
      return eagain_iertr::make_ready_future<Ref<tree_cursor_t>>();
    }
  }).si_then([c, &pos, this_ref = std::move(this_ref),
                this, FNAME] (Ref<tree_cursor_t> next_cursor) mutable {
    if (next_cursor && next_cursor->is_end()) {
      // reset the node reference from the end cursor
      next_cursor.reset();
    }
    return eagain_iertr::now().si_then(
        [c, &pos, this_ref = std::move(this_ref), this, FNAME] () mutable {
      assert_moveable(this_ref);
#ifndef NDEBUG
      assert(!impl->is_keys_empty());
      if (impl->has_single_value()) {
        assert(pos == search_position_t::begin());
      }
#endif
      if (!is_root() && impl->has_single_value()) {
        // we need to keep the root as an empty leaf node
        // fast path without mutating the extent
        // track_erase
        DEBUGT("{} has one value left, erase ...", c.t, get_name());
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
        return eagain_iertr::now();
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
    }).si_then([next_cursor] {
      return next_cursor;
    });
  });
}
template eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::erase<true>(context_t, const search_position_t&, bool);
template eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::erase<false>(context_t, const search_position_t&, bool);

eagain_ifuture<> LeafNode::extend_value(
    context_t c, const search_position_t& pos, value_size_t extend_size)
{
  ceph_abort("not implemented");
  return eagain_iertr::now();
}

eagain_ifuture<> LeafNode::trim_value(
    context_t c, const search_position_t& pos, value_size_t trim_size)
{
  ceph_abort("not implemented");
  return eagain_iertr::now();
}

std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
LeafNode::prepare_mutate_value_payload(context_t c)
{
  return impl->prepare_mutate_value_payload(c);
}

eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::lookup_smallest(context_t)
{
  if (unlikely(impl->is_keys_empty())) {
    assert(is_root());
    return seastar::make_ready_future<Ref<tree_cursor_t>>(
        tree_cursor_t::create_end(this));
  }
  auto pos = search_position_t::begin();
  key_view_t index_key;
  const value_header_t* p_value_header;
  impl->get_slot(pos, &index_key, &p_value_header);
  return seastar::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, index_key, p_value_header));
}

eagain_ifuture<Ref<tree_cursor_t>>
LeafNode::lookup_largest(context_t)
{
  if (unlikely(impl->is_keys_empty())) {
    assert(is_root());
    return seastar::make_ready_future<Ref<tree_cursor_t>>(
        tree_cursor_t::create_end(this));
  }
  search_position_t pos;
  key_view_t index_key;
  const value_header_t* p_value_header = nullptr;
  impl->get_largest_slot(&pos, &index_key, &p_value_header);
  return seastar::make_ready_future<Ref<tree_cursor_t>>(
      get_or_track_cursor(pos, index_key, p_value_header));
}

eagain_ifuture<Node::search_result_t>
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
  return seastar::make_ready_future<search_result_t>(ret);
}

eagain_ifuture<> LeafNode::do_get_tree_stats(context_t, tree_stats_t& stats)
{
  auto nstats = impl->get_stats();
  stats.size_persistent_leaf += nstats.size_persistent;
  stats.size_filled_leaf += nstats.size_filled;
  stats.size_logical_leaf += nstats.size_logical;
  stats.size_overhead_leaf += nstats.size_overhead;
  stats.size_value_leaf += nstats.size_value;
  stats.num_kvs_leaf += nstats.num_kvs;
  stats.num_nodes_leaf += 1;
  return eagain_iertr::now();
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

eagain_ifuture<> LeafNode::test_clone_root(
    context_t c_other, RootNodeTracker& tracker_other) const
{
  assert(is_root());
  assert(impl->is_level_tail());
  assert(impl->field_type() == field_type_t::N0);
  Ref<const Node> this_ref = this;
  return LeafNode::allocate(c_other, L_ADDR_MIN, field_type_t::N0, true
  ).si_then([this, c_other, &tracker_other](auto fresh_other) {
    impl->test_copy_to(fresh_other.mut);
    auto cloned_root = fresh_other.node;
    return c_other.nm.get_super(c_other.t, tracker_other
    ).handle_error_interruptible(
      eagain_iertr::pass_further{},
      crimson::ct_error::assert_all{"Invalid error during test clone"}
    ).si_then([c_other, cloned_root](auto&& super_other) {
      assert(super_other);
      cloned_root->make_root_new(c_other, std::move(super_other));
    });
  }).si_then([this_ref]{});
}

eagain_ifuture<Ref<tree_cursor_t>> LeafNode::insert_value(
    context_t c, const key_hobj_t& key, value_config_t vconf,
    const search_position_t& pos, const MatchHistory& history,
    match_stat_t mstat)
{
  LOG_PREFIX(OTree::LeafNode::insert_value);
#ifndef NDEBUG
  if (pos.is_end()) {
    assert(impl->is_level_tail());
  }
#endif
  DEBUGT("insert {} with insert_key={}, insert_value={}, insert_pos({}), "
         "history={}, mstat({}) ...",
         c.t, get_name(), key, vconf, pos, history, mstat);
  ++(c.t.get_onode_tree_stats().num_inserts);
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
    return eagain_iertr::make_ready_future<Ref<tree_cursor_t>>(ret);
  }
  // split and insert
  Ref<Node> this_ref = this;
  laddr_t left_hint, right_hint;
  {
    key_view_t left_key;
    impl->get_slot(search_position_t::begin(), &left_key, nullptr);
    left_hint = left_key.get_hint();
    key_view_t right_key;
    impl->get_largest_slot(nullptr, &right_key, nullptr);
    right_hint = right_key.get_hint();
  }
  return (is_root() ? upgrade_root(c, left_hint) : eagain_iertr::now()
  ).si_then([this, c, right_hint] {
    return LeafNode::allocate(c, right_hint, impl->field_type(), impl->is_level_tail());
  }).si_then([this_ref = std::move(this_ref), this, c, &key, vconf, FNAME,
                insert_pos, insert_stage=insert_stage, insert_size=insert_size](auto fresh_right) mutable {
    auto right_node = fresh_right.node;
    DEBUGT("proceed split {} to fresh {} ...",
           c.t, get_name(), right_node->get_name());
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

    ++(c.t.get_onode_tree_stats().extents_num_delta);
    return apply_split_to_parent(
        c, std::move(this_ref), std::move(right_node), false
    ).si_then([ret] {
      return ret;
    });
    // TODO (optimize)
    // try to acquire space from siblings before split... see btrfs
  });
}

eagain_ifuture<Ref<LeafNode>> LeafNode::allocate_root(
    context_t c, RootNodeTracker& root_tracker)
{
  LOG_PREFIX(OTree::LeafNode::allocate_root);
  return LeafNode::allocate(c, L_ADDR_MIN, field_type_t::N0, true
  ).si_then([c, &root_tracker, FNAME](auto fresh_node) {
    auto root = fresh_node.node;
    return c.nm.get_super(c.t, root_tracker
    ).handle_error_interruptible(
      eagain_iertr::pass_further{},
      crimson::ct_error::input_output_error::assert_failure([FNAME, c] {
        ERRORT("EIO during get_super()", c.t);
      })
    ).si_then([c, root](auto&& super) {
      assert(super);
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
    p_cursor = tree_cursor_t::create_tracked(
        this, position, key, p_value_header);
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
  assert(!impl->is_extent_retired());

  // We need to make sure user has freed all the cursors before submitting the
  // according transaction. Otherwise the below checks will have undefined
  // behaviors.
  auto [key, p_value_header] = get_kv(cursor.get_position());
  auto magic = p_value_header->magic;
  assert(key == cursor.get_key_view(magic));
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
  return tree_cursor_t::create_inserted(
      this, insert_pos);
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

eagain_ifuture<LeafNode::fresh_node_t> LeafNode::allocate(
    context_t c, laddr_t hint, field_type_t field_type, bool is_level_tail)
{
  return LeafNodeImpl::allocate(c, hint, field_type, is_level_tail
  ).si_then([](auto&& fresh_impl) {
    auto *derived_ptr = fresh_impl.impl.get();
    auto node = Ref<LeafNode>(new LeafNode(
          derived_ptr, std::move(fresh_impl.impl)));
    return fresh_node_t{node, fresh_impl.mut};
  });
}

}
