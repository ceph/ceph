// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// TODO: remove
#include <iostream>
#include <ostream>

#include "common/likely.h"
#include "node.h"
#include "node_extent_visitor.h"
#include "stages/node_layout.h"

namespace crimson::os::seastore::onode {

class NodeExtentMutable;

// TODO: decouple NodeT with Node

template <typename FieldType, node_type_t _NODE_TYPE, typename ConcreteType>
class NodeT : virtual public Node {
 public:
  using extent_t = NodeExtentT<FieldType, _NODE_TYPE>;
  using node_ertr = Node::node_ertr;
  template <class... ValuesT>
  using node_future = Node::node_future<ValuesT...>;
  using node_stage_t = typename extent_t::node_stage_t;
  using position_t = typename extent_t::position_t;
  using value_t = typename extent_t::value_t;
  static constexpr auto FIELD_TYPE = extent_t::FIELD_TYPE;
  static constexpr auto NODE_TYPE = _NODE_TYPE;

  struct fresh_node_t {
    Ref<ConcreteType> node;
    NodeExtentMutable mut;
    std::pair<Ref<Node>, NodeExtentMutable> make_pair() {
      return std::make_pair(Ref<Node>(node), mut);
    }
  };

  virtual ~NodeT() = default;

  bool is_level_tail() const override final { return extent.read().is_level_tail(); }
  field_type_t field_type() const override final { return FIELD_TYPE; }
  laddr_t laddr() const override final { return extent.get_laddr(); }
  level_t level() const override final { return extent.read().level(); }

  full_key_t<KeyT::VIEW> get_key_view(
      const search_position_t& position) const override final {
    full_key_t<KeyT::VIEW> ret;
    STAGE_T::get_key_view(
        extent.read(), cast_down<STAGE_T::STAGE>(position), ret);
    return ret;
  }

  full_key_t<KeyT::VIEW> get_largest_key_view() const override final {
    full_key_t<KeyT::VIEW> key_view;
    STAGE_T::lookup_largest_index(extent.read(), key_view);
    return key_view;
  }

  std::ostream& dump(std::ostream& os) const override final {
    auto& node_stage = extent.read();
    auto p_start = node_stage.p_start();
    os << *this << ":";
    os << "\n  header: " << node_stage_t::header_size() << "B";
    size_t size = 0u;
    if (node_stage.keys()) {
      STAGE_T::dump(node_stage, os, "  ", size, p_start);
    } else {
      if constexpr (NODE_TYPE == node_type_t::LEAF) {
        return os << " empty!";
      } else { // internal node
        if (!is_level_tail()) {
          return os << " empty!";
        } else {
          size += node_stage_t::header_size();
        }
      }
    }
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (is_level_tail()) {
        size += sizeof(laddr_t);
        auto value_ptr = node_stage.get_end_p_laddr();
        int offset = reinterpret_cast<const char*>(value_ptr) - p_start;
        os << "\n  tail value: 0x"
           << std::hex << *value_ptr << std::dec
           << " " << size << "B"
           << "  @" << offset << "B";
      }
    }
    return os;
  }

  std::ostream& dump_brief(std::ostream& os) const override final {
    auto& node_stage = extent.read();
    os << "Node" << NODE_TYPE << FIELD_TYPE
       << "@0x" << std::hex << laddr()
       << "+" << node_stage_t::EXTENT_SIZE << std::dec
       << (is_level_tail() ? "$" : "")
       << "(level=" << (unsigned)level()
       << ", filled=" << node_stage.total_size() - node_stage.free_size() << "B"
       << ", free=" << node_stage.free_size() << "B"
       << ")";
    return os;
  }

  const value_t* get_value_ptr(const search_position_t& position) const {
    auto& node_stage = extent.read();
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (position.is_end()) {
        assert(is_level_tail());
        return node_stage.get_end_p_laddr();
      }
    } else {
      assert(!position.is_end());
    }
    return STAGE_T::get_p_value(node_stage, cast_down<STAGE_T::STAGE>(position));
  }

  void test_make_destructable(
      context_t c, NodeExtentMutable& mut, Super::URef&& _super) override final {
    node_stage_t::update_is_level_tail(mut, extent.read(), true);
    make_root(c, std::move(_super));
  }

  static Ref<Node> load(NodeExtent::Ref extent, bool expect_is_level_tail) {
    Ref<ConcreteType> ret = new ConcreteType();
    ret->extent = extent_t::load(extent);
    assert(ret->is_level_tail() == expect_is_level_tail);
    return ret;
  }

 protected:
  // TODO: constructor
  extent_t extent;
};

template <typename FieldType, typename ConcreteType>
class InternalNodeT : public InternalNode,
                      public NodeT<FieldType, node_type_t::INTERNAL, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::INTERNAL, ConcreteType>;
  using extent_t = typename parent_t::extent_t;
  using fresh_node_t = typename parent_t::fresh_node_t;
  using node_stage_t = typename parent_t::node_stage_t;
  using position_t = typename parent_t::position_t;

  virtual ~InternalNodeT() = default;

  node_future<search_result_t> do_lower_bound(
      context_t c, const full_key_t<KeyT::HOBJ>& key,
      MatchHistory& history) override final {
    auto result = STAGE_T::lower_bound_normalized(
        this->extent.read(), key, history);
    auto& position = result.position;
    laddr_t child_addr;
    if (position.is_end()) {
      assert(this->is_level_tail());
      child_addr = *this->get_value_ptr(position);
    } else {
      assert(result.p_value);
      child_addr = *result.p_value;
    }
    return get_or_track_child(c, position, child_addr
    ).safe_then([c, &key, &history](auto child) {
      // XXX(multi-type): pass result.mstat to child
      return child->do_lower_bound(c, key, history);
    });
  }

  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t c) override final {
    auto position = search_position_t::begin();
    laddr_t child_addr = *this->get_value_ptr(position);
    return get_or_track_child(c, position, child_addr).safe_then([c](auto child) {
      return child->lookup_smallest(c);
    });
  }

  node_future<Ref<tree_cursor_t>> lookup_largest(context_t c) override final {
    // NOTE: unlike LeafNodeT::lookup_largest(), this only works for the tail
    // internal node to return the tail child address.
    auto position = search_position_t::end();
    laddr_t child_addr = *this->get_value_ptr(position);
    return get_or_track_child(c, position, child_addr).safe_then([c](auto child) {
      return child->lookup_largest(c);
    });
  }

  node_future<> apply_child_split(
      context_t c, const search_position_t& pos,
      const full_key_t<KeyT::VIEW>& left_key, Ref<Node> left_child,
      Ref<Node> right_child) override final {
    this->extent.prepare_mutate(c);
    auto& node_stage = this->extent.read();

    // update pos => l_addr to r_addr
    auto left_laddr = left_child->laddr();
    auto right_laddr = right_child->laddr();
    const laddr_t* p_rvalue = this->get_value_ptr(pos);
    this->extent.prepare_internal_split_replayable(
        left_laddr, right_laddr, const_cast<laddr_t*>(p_rvalue));
    this->replace_track(pos, left_child, right_child);

    // evaluate insertion
    position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
    match_stage_t insert_stage;
    node_offset_t insert_size;
    if (unlikely(!node_stage.keys())) {
      assert(insert_pos.is_end());
      insert_stage = STAGE_T::STAGE;
      insert_size = STAGE_T::template insert_size<KeyT::VIEW>(left_key, left_laddr);
    } else {
      std::tie(insert_stage, insert_size) =
        STAGE_T::evaluate_insert(node_stage, left_key, left_laddr, insert_pos, true);
    }

    // TODO: common part begin, move to NodeT
    auto free_size = node_stage.free_size();
    if (free_size >= insert_size) {
      auto p_value = this->extent.template insert_replayable<KeyT::VIEW>(
          left_key, left_laddr, insert_pos, insert_stage, insert_size);
      assert(node_stage.free_size() == free_size - insert_size);
      // TODO: common part end, move to NodeT

      assert(*p_value == left_laddr);
      auto insert_pos_normalized = normalize(std::move(insert_pos));
      assert(insert_pos_normalized <= pos);
      assert(get_key_view(insert_pos_normalized) == left_key);
      track_insert(insert_pos_normalized, insert_stage, left_child, right_child);
      this->validate_tracked_children();
      return node_ertr::now();
    }

    std::cout << "  try insert at: " << insert_pos
              << ", insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size
              << ", values=0x" << std::hex << left_laddr
              << ",0x" << right_laddr << std::dec << std::endl;

    Ref<InternalNodeT> this_ref = this;
    return (is_root() ? this->upgrade_root(c) : node_ertr::now()
    ).safe_then([this, c] {
      return ConcreteType::allocate(c, this->level(), this->is_level_tail());
    }).safe_then([this_ref, this, c, left_key, left_child, right_child, left_laddr,
                  insert_pos, insert_stage, insert_size](auto fresh_right) mutable {
      auto& node_stage = this->extent.read();
      size_t empty_size = node_stage.size_before(0);
      size_t available_size = node_stage.total_size() - empty_size;
      size_t target_split_size = empty_size + (available_size + insert_size) / 2;
      // TODO adjust NODE_BLOCK_SIZE according to this requirement
      assert(insert_size < available_size / 2);
      typename STAGE_T::StagedIterator split_at;
      bool insert_left = STAGE_T::locate_split(
          node_stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

      std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
                << ", now insert at: " << insert_pos
                << std::endl;

      auto append_at = split_at;
      // TODO(cross-node string dedup)
      typename STAGE_T::template StagedAppender<KeyT::VIEW> right_appender;
      right_appender.init(&fresh_right.mut, fresh_right.mut.get_write());
      const laddr_t* p_value = nullptr;
      if (!insert_left) {
        // right node: append [start(append_at), insert_pos)
        STAGE_T::template append_until<KeyT::VIEW>(
            append_at, right_appender, insert_pos, insert_stage);
        std::cout << "insert to right: " << insert_pos
                  << ", insert_stage=" << (int)insert_stage << std::endl;
        // right node: append [insert_pos(key, value)]
        bool is_front_insert = (insert_pos == position_t::begin());
        bool is_end = STAGE_T::template append_insert<KeyT::VIEW>(
            left_key, left_laddr, append_at, right_appender,
            is_front_insert, insert_stage, p_value);
        assert(append_at.is_end() == is_end);
      }

      // right node: append (insert_pos, end)
      auto pos_end = position_t::end();
      STAGE_T::template append_until<KeyT::VIEW>(
          append_at, right_appender, pos_end, STAGE_T::STAGE);
      assert(append_at.is_end());
      right_appender.wrap();
      fresh_right.node->dump(std::cout) << std::endl;

      // mutate left node
      if (insert_left) {
        p_value = this->extent.template split_insert_replayable<KeyT::VIEW>(
            split_at, left_key, left_laddr, insert_pos, insert_stage, insert_size);
      } else {
        this->extent.split_replayable(split_at);
      }
      this->dump(std::cout) << std::endl;
      assert(p_value);
      // TODO: common part end, move to NodeT

      auto split_pos_normalized = normalize(split_at.get_pos());
      auto insert_pos_normalized = normalize(std::move(insert_pos));
      std::cout << "split at " << split_pos_normalized
                << ", insert at " << insert_pos_normalized
                << ", insert_left=" << insert_left
                << ", insert_stage=" << (int)insert_stage << std::endl;
      track_split(split_pos_normalized, fresh_right.node);
      if (insert_left) {
        track_insert(insert_pos_normalized, insert_stage, left_child);
      } else {
        fresh_right.node->track_insert(insert_pos_normalized, insert_stage, left_child);
      }

      this->validate_tracked_children();
      fresh_right.node->validate_tracked_children();

      // propagate index to parent
      return this->insert_parent(c, fresh_right.node);
      // TODO (optimize)
      // try to acquire space from siblings before split... see btrfs
    });
  }

  static node_future<fresh_node_t> allocate(
      context_t c, level_t level, bool is_level_tail) {
    assert(level != 0u);
    return extent_t::allocate(c, level, is_level_tail
    ).safe_then([](auto&& fresh_extent) {
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->extent = std::move(fresh_extent.extent);
      return fresh_node_t{ret, fresh_extent.mut};
    });
  }

 private:
  const laddr_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }

};

class InternalNode0 final : public InternalNodeT<node_fields_0_t, InternalNode0> {
 public:
  node_future<> test_clone_root(
      context_t c_other, RootNodeTracker& tracker_other) const override final {
    assert(is_root());
    assert(is_level_tail());
    Ref<const InternalNode0> this_ref = this;
    return InternalNode0::allocate(c_other, level(), true
    ).safe_then([this, c_other, &tracker_other](auto fresh_other) {
      this->extent.test_copy_to(fresh_other.mut);
      auto cloned_root = fresh_other.node;
      return c_other.nm.get_super(c_other.t, tracker_other
      ).safe_then([c_other, cloned_root](auto&& super_other) {
        cloned_root->make_root_new(c_other, std::move(super_other));
        return cloned_root;
      });
    }).safe_then([this_ref, this, c_other](auto cloned_root) {
      // In some unit tests, the children are stubbed out that they
      // don't exist in NodeExtentManager, and are only tracked in memory.
      return test_clone_children(c_other, cloned_root);
    });
  }

  static node_future<Ref<InternalNode0>> allocate_root(
      context_t c, level_t old_root_level,
      laddr_t old_root_addr, Super::URef&& super) {
    return allocate(c, old_root_level + 1, true
    ).safe_then([c, old_root_addr,
                 super = std::move(super)](auto fresh_root) mutable {
      auto root = fresh_root.node;
      const laddr_t* p_value = root->get_value_ptr(search_position_t::end());
      fresh_root.mut.copy_in_absolute(
          const_cast<laddr_t*>(p_value), old_root_addr);
      root->make_root_from(c, std::move(super), old_root_addr);
      return root;
    });
  }
};

class InternalNode1 final : public InternalNodeT<node_fields_1_t, InternalNode1> {};
class InternalNode2 final : public InternalNodeT<node_fields_2_t, InternalNode2> {};
class InternalNode3 final : public InternalNodeT<internal_fields_3_t, InternalNode3> {};

template <typename FieldType, typename ConcreteType>
class LeafNodeT: public LeafNode,
                 public NodeT<FieldType, node_type_t::LEAF, ConcreteType> {
 public:
  using parent_t = NodeT<FieldType, node_type_t::LEAF, ConcreteType>;
  using extent_t = typename parent_t::extent_t;
  using fresh_node_t = typename parent_t::fresh_node_t;
  using node_stage_t = typename parent_t::node_stage_t;
  using position_t = typename parent_t::position_t;

  virtual ~LeafNodeT() = default;

  node_future<search_result_t> do_lower_bound(
      context_t, const full_key_t<KeyT::HOBJ>& key,
      MatchHistory& history) override final {
    auto& node_stage = this->extent.read();
    if (unlikely(node_stage.keys() == 0)) {
      assert(this->is_root());
      history.set<STAGE_LEFT>(MatchKindCMP::NE);
      auto p_cursor = get_or_track_cursor(search_position_t::end(), nullptr);
      return node_ertr::make_ready_future<search_result_t>(
          search_result_t{p_cursor, MatchKindBS::NE});
    }

    auto result = STAGE_T::lower_bound_normalized(node_stage, key, history);
    if (result.is_end()) {
      assert(this->is_level_tail());
    } else {
      assert(result.p_value);
    }
    auto p_cursor = get_or_track_cursor(result.position, result.p_value);
    return node_ertr::make_ready_future<search_result_t>(
        search_result_t{p_cursor, result.match()});
  }

  node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override final {
    auto& node_stage = this->extent.read();
    if (unlikely(node_stage.keys() == 0)) {
      assert(this->is_root());
      auto pos = search_position_t::end();
      return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
          get_or_track_cursor(pos, nullptr));
    }

    auto pos = search_position_t::begin();
    const onode_t* p_value = this->get_value_ptr(pos);
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(pos, p_value));
  }

  node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override final {
    auto& node_stage = this->extent.read();
    if (unlikely(node_stage.keys() == 0)) {
      assert(this->is_root());
      auto pos = search_position_t::end();
      return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
          get_or_track_cursor(pos, nullptr));
    }

    search_position_t pos;
    const onode_t* p_value = nullptr;
    STAGE_T::lookup_largest_normalized(node_stage, pos, p_value);
    return node_ertr::make_ready_future<Ref<tree_cursor_t>>(
        get_or_track_cursor(pos, p_value));
  }

  node_future<Ref<tree_cursor_t>> insert_value(
      context_t c, const full_key_t<KeyT::HOBJ>& key, const onode_t& value,
      const search_position_t& pos, const MatchHistory& history) override final {
#ifndef NDEBUG
    if (pos.is_end()) {
      assert(this->is_level_tail());
    }
#endif
    this->extent.prepare_mutate(c);
    auto& node_stage = this->extent.read();

    position_t insert_pos = cast_down<STAGE_T::STAGE>(pos);
    auto [insert_stage, insert_size] =
      STAGE_T::evaluate_insert(key, value, history, insert_pos);

    // TODO: common part begin, move to NodeT
    auto free_size = node_stage.free_size();
    if (free_size >= insert_size) {
      auto p_value = this->extent.template insert_replayable<KeyT::HOBJ>(
          key, value, insert_pos, insert_stage, insert_size);
      assert(node_stage.free_size() == free_size - insert_size);
      // TODO: common part end, move to NodeT

      assert(p_value->size == value.size);
      auto insert_pos_normalized = normalize(std::move(insert_pos));
      assert(insert_pos_normalized <= pos);
      assert(get_key_view(insert_pos_normalized) == key);
      auto ret = track_insert(insert_pos_normalized, insert_stage, p_value);
      this->validate_tracked_cursors();
      return node_ertr::make_ready_future<Ref<tree_cursor_t>>(ret);
    }

    std::cout << "  try insert at: " << insert_pos
              << ", insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size
              << std::endl;

    Ref<LeafNodeT> this_ref = this;
    return (is_root() ? this->upgrade_root(c) : node_ertr::now()
    ).safe_then([this, c] {
      return ConcreteType::allocate(c, this->is_level_tail());
    }).safe_then([this_ref, this, c, &key, &value, &history,
                  insert_pos, insert_stage, insert_size](auto fresh_right) mutable {
      auto& node_stage = this->extent.read();
      size_t empty_size = node_stage.size_before(0);
      size_t available_size = node_stage.total_size() - empty_size;
      size_t target_split_size = empty_size + (available_size + insert_size) / 2;
      // TODO adjust NODE_BLOCK_SIZE according to this requirement
      assert(insert_size < available_size / 2);
      typename STAGE_T::StagedIterator split_at;
      bool insert_left = STAGE_T::locate_split(
          node_stage, target_split_size, insert_pos, insert_stage, insert_size, split_at);

      std::cout << "  split at: " << split_at << ", insert_left=" << insert_left
                << ", now insert at: " << insert_pos
                << std::endl;

      auto append_at = split_at;
      // TODO(cross-node string dedup)
      typename STAGE_T::template StagedAppender<KeyT::HOBJ> right_appender;
      right_appender.init(&fresh_right.mut, fresh_right.mut.get_write());
      const onode_t* p_value = nullptr;
      if (!insert_left) {
        // right node: append [start(append_at), insert_pos)
        STAGE_T::template append_until<KeyT::HOBJ>(
            append_at, right_appender, insert_pos, insert_stage);
        std::cout << "insert to right: " << insert_pos
                  << ", insert_stage=" << (int)insert_stage << std::endl;
        // right node: append [insert_pos(key, value)]
        bool is_front_insert = (insert_pos == position_t::begin());
        bool is_end = STAGE_T::template append_insert<KeyT::HOBJ>(
            key, value, append_at, right_appender,
            is_front_insert, insert_stage, p_value);
        assert(append_at.is_end() == is_end);
      }

      // right node: append (insert_pos, end)
      auto pos_end = position_t::end();
      STAGE_T::template append_until<KeyT::HOBJ>(
          append_at, right_appender, pos_end, STAGE_T::STAGE);
      assert(append_at.is_end());
      right_appender.wrap();
      fresh_right.node->dump(std::cout) << std::endl;

      // mutate left node
      if (insert_left) {
        p_value = this->extent.template split_insert_replayable<KeyT::HOBJ>(
            split_at, key, value, insert_pos, insert_stage, insert_size);
      } else {
        this->extent.split_replayable(split_at);
      }
      this->dump(std::cout) << std::endl;
      assert(p_value);
      // TODO: common part end, move to NodeT

      auto split_pos_normalized = normalize(split_at.get_pos());
      auto insert_pos_normalized = normalize(std::move(insert_pos));
      std::cout << "split at " << split_pos_normalized
                << ", insert at " << insert_pos_normalized
                << ", insert_left=" << insert_left
                << ", insert_stage=" << (int)insert_stage << std::endl;
      track_split(split_pos_normalized, fresh_right.node);
      Ref<tree_cursor_t> ret;
      if (insert_left) {
        assert(this->get_key_view(insert_pos_normalized) == key);
        ret = track_insert(insert_pos_normalized, insert_stage, p_value);
      } else {
        assert(fresh_right.node->get_key_view(insert_pos_normalized) == key);
        ret = fresh_right.node->track_insert(insert_pos_normalized, insert_stage, p_value);
      }

      this->validate_tracked_cursors();
      fresh_right.node->validate_tracked_cursors();

      // propagate index to parent
      return this->insert_parent(c, fresh_right.node).safe_then([ret] {
        return ret;
      });
      // TODO (optimize)
      // try to acquire space from siblings before split... see btrfs
    });
  }

  static node_future<fresh_node_t> allocate(context_t c, bool is_level_tail) {
    return extent_t::allocate(c, 0u, is_level_tail
    ).safe_then([](auto&& fresh_extent) {
      auto ret = Ref<ConcreteType>(new ConcreteType());
      ret->extent = std::move(fresh_extent.extent);
      return fresh_node_t{ret, fresh_extent.mut};
    });
  }

 private:
  const onode_t* get_p_value(const search_position_t& pos) const override final {
    return this->get_value_ptr(pos);
  }
};
class LeafNode0 final : public LeafNodeT<node_fields_0_t, LeafNode0> {
 public:
  node_future<> test_clone_root(
      context_t c_other, RootNodeTracker& tracker_other) const override final {
    assert(this->is_root());
    assert(is_level_tail());
    Ref<const LeafNode0> this_ref = this;
    return LeafNode0::allocate(c_other, true
    ).safe_then([this, c_other, &tracker_other](auto fresh_other) {
      this->extent.test_copy_to(fresh_other.mut);
      auto cloned_root = fresh_other.node;
      return c_other.nm.get_super(c_other.t, tracker_other
      ).safe_then([c_other, cloned_root](auto&& super_other) {
        cloned_root->make_root_new(c_other, std::move(super_other));
      });
    }).safe_then([this_ref]{});
  }

  static node_future<> mkfs(context_t c, RootNodeTracker& root_tracker) {
    return allocate(c, true
    ).safe_then([c, &root_tracker](auto fresh_node) {
      auto root = fresh_node.node;
      return c.nm.get_super(c.t, root_tracker
      ).safe_then([c, root](auto&& super) {
        root->make_root_new(c, std::move(super));
      });
    });
  }
};
class LeafNode1 final : public LeafNodeT<node_fields_1_t, LeafNode1> {};
class LeafNode2 final : public LeafNodeT<node_fields_2_t, LeafNode2> {};
class LeafNode3 final : public LeafNodeT<leaf_fields_3_t, LeafNode3> {};

inline Node::node_future<Ref<Node>> load_node(
    context_t c, laddr_t addr, bool expect_is_level_tail) {
  // NOTE:
  // *option1: all types of node have the same length;
  // option2: length is defined by node/field types;
  // option3: length is totally flexible;
  return c.nm.read_extent(c.t, addr, NODE_BLOCK_SIZE
  ).safe_then([expect_is_level_tail](auto extent) {
    const auto header = reinterpret_cast<const node_header_t*>(extent->get_read());
    auto _field_type = header->get_field_type();
    if (!_field_type.has_value()) {
      throw std::runtime_error("load failed: bad field type");
    }
    auto _node_type = header->get_node_type();
    if (_field_type == field_type_t::N0) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode0::load(extent, expect_is_level_tail);
      } else {
        return InternalNode0::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N1) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode1::load(extent, expect_is_level_tail);
      } else {
        return InternalNode1::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N2) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode2::load(extent, expect_is_level_tail);
      } else {
        return InternalNode2::load(extent, expect_is_level_tail);
      }
    } else if (_field_type == field_type_t::N3) {
      if (_node_type == node_type_t::LEAF) {
        return LeafNode3::load(extent, expect_is_level_tail);
      } else {
        return InternalNode3::load(extent, expect_is_level_tail);
      }
    } else {
      assert(false);
    }
  });
}

}
