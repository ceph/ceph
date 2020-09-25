// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// TODO: remove
#include <iostream>
#include <ostream>

#include "common/likely.h"
#include "node_extent_visitor.h"
#include "node_impl.h"
#include "stages/node_stage_layout.h"

namespace crimson::os::seastore::onode {

template <node_type_t NODE_TYPE> struct insert_key_type;
template <> struct insert_key_type<node_type_t::INTERNAL> {
  static constexpr auto type = KeyT::VIEW; };
template <> struct insert_key_type<node_type_t::LEAF> {
  static constexpr auto type = KeyT::HOBJ; };

template <node_type_t NODE_TYPE> struct node_impl_type;
template <> struct node_impl_type<node_type_t::INTERNAL> {
  using type = InternalNodeImpl; };
template <> struct node_impl_type<node_type_t::LEAF> {
  using type = LeafNodeImpl; };

template <node_type_t NODE_TYPE> struct node_marker_type;
template <> struct node_marker_type<node_type_t::INTERNAL> {
  using type = InternalNodeImpl::internal_marker_t; };
template <> struct node_marker_type<node_type_t::LEAF> {
  using type = LeafNodeImpl::leaf_marker_t; };

template <typename FieldType, node_type_t NODE_TYPE>
class NodeLayoutT final : public InternalNodeImpl, public LeafNodeImpl {
 public:
  using URef = std::unique_ptr<NodeLayoutT>;
  using extent_t = NodeExtentT<FieldType, NODE_TYPE>;
  using parent_t = typename node_impl_type<NODE_TYPE>::type;
  using marker_t = typename node_marker_type<NODE_TYPE>::type;
  using node_stage_t = typename extent_t::node_stage_t;
  using position_t = typename extent_t::position_t;
  using value_t = typename extent_t::value_t;
  static constexpr auto FIELD_TYPE = extent_t::FIELD_TYPE;
  static constexpr auto KEY_TYPE = insert_key_type<NODE_TYPE>::type;
  static constexpr auto STAGE = STAGE_T::STAGE;

  NodeLayoutT(const NodeLayoutT&) = delete;
  NodeLayoutT(NodeLayoutT&&) = delete;
  NodeLayoutT& operator=(const NodeLayoutT&) = delete;
  NodeLayoutT& operator=(NodeLayoutT&&) = delete;
  ~NodeLayoutT() override = default;

  static URef load(NodeExtentRef extent, bool expect_is_level_tail) {
    std::unique_ptr<NodeLayoutT> ret(
        new NodeLayoutT(extent_t::loaded(extent), extent));
    assert(ret->is_level_tail() == expect_is_level_tail);
    return ret;
  }

  using alloc_ertr = NodeExtentManager::tm_ertr;
  static alloc_ertr::future<typename parent_t::fresh_impl_t> allocate(
      context_t c, bool is_level_tail, level_t level) {
    // NOTE:
    // *option1: all types of node have the same length;
    // option2: length is defined by node/field types;
    // option3: length is totally flexible;
    return c.nm.alloc_extent(c.t, node_stage_t::EXTENT_SIZE
    ).safe_then([is_level_tail, level](auto extent) {
      auto [state, mut] = extent_t::allocated(extent, is_level_tail, level);
      return typename parent_t::fresh_impl_t{
        std::unique_ptr<parent_t>(new NodeLayoutT(state, extent)), mut};
    });
  }

 protected:
  /*
   * NodeImpl
   */
  field_type_t field_type() const override { return FIELD_TYPE; }
  laddr_t laddr() const override { return extent.get_laddr(); }
  void prepare_mutate(context_t c) override { return extent.prepare_mutate(c); }
  bool is_level_tail() const override { return extent.read().is_level_tail(); }
  bool is_empty() const override { return extent.read().keys() == 0; }
  level_t level() const override { return extent.read().level(); }
  node_offset_t free_size() const override { return extent.read().free_size(); }

  key_view_t get_key_view(const search_position_t& position) const override {
    key_view_t ret;
    STAGE_T::get_key_view(extent.read(), cast_down<STAGE>(position), ret);
    return ret;
  }

  key_view_t get_largest_key_view() const override {
    key_view_t index_key;
    STAGE_T::template lookup_largest_slot<false, true, false>(
        extent.read(), nullptr, &index_key, nullptr);
    return index_key;
  }

  std::ostream& dump(std::ostream& os) const override {
    auto& node_stage = extent.read();
    auto p_start = node_stage.p_start();
    dump_brief(os);
    os << ":\n  header: " << node_stage_t::header_size() << "B";
    size_t size = 0u;
    if (node_stage.keys()) {
      STAGE_T::dump(node_stage, os, "  ", size, p_start);
    } else {
      if constexpr (NODE_TYPE == node_type_t::LEAF) {
        return os << " empty!";
      } else { // internal node
        if (!node_stage.is_level_tail()) {
          return os << " empty!";
        } else {
          size += node_stage_t::header_size();
        }
      }
    }
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (node_stage.is_level_tail()) {
        size += sizeof(laddr_t);
        auto value_ptr = node_stage.get_end_p_laddr();
        int offset = reinterpret_cast<const char*>(value_ptr) - p_start;
        os << "\n  tail value: 0x"
           << std::hex << value_ptr->value << std::dec
           << " " << size << "B"
           << "  @" << offset << "B";
      }
    }
    return os;
  }

  std::ostream& dump_brief(std::ostream& os) const override {
    auto& node_stage = extent.read();
    os << "Node" << NODE_TYPE << FIELD_TYPE
       << "@0x" << std::hex << extent.get_laddr()
       << "+" << node_stage_t::EXTENT_SIZE << std::dec
       << (node_stage.is_level_tail() ? "$" : "")
       << "(level=" << (unsigned)node_stage.level()
       << ", filled=" << node_stage.total_size() - node_stage.free_size() << "B"
       << ", free=" << node_stage.free_size() << "B"
       << ")";
    return os;
  }

  void validate_layout() const override {
#ifndef NDEBUG
    STAGE_T::validate(extent.read());
#endif
  }

  void test_copy_to(NodeExtentMutable& to) const override {
    extent.test_copy_to(to);
  }

  void test_set_tail(NodeExtentMutable& mut) override {
    node_stage_t::update_is_level_tail(mut, extent.read(), true);
  }

  /*
   * Common
   */
  const value_t* get_p_value(const search_position_t& position,
                             key_view_t* index_key=nullptr, marker_t={}) const override {
    auto& node_stage = extent.read();
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(!index_key);
      if (position.is_end()) {
        assert(is_level_tail());
        return node_stage.get_end_p_laddr();
      }
    } else {
      assert(!position.is_end());
    }
    if (index_key) {
      return STAGE_T::template get_p_value<true>(
          node_stage, cast_down<STAGE>(position), index_key);
    } else {
      return STAGE_T::get_p_value(node_stage, cast_down<STAGE>(position));
    }
  }

  lookup_result_t<NODE_TYPE> lower_bound(
      const key_hobj_t& key, MatchHistory& history,
      key_view_t* index_key=nullptr, marker_t={}) const override {
    auto& node_stage = extent.read();
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      if (unlikely(node_stage.keys() == 0)) {
        history.set<STAGE_LEFT>(MatchKindCMP::NE);
        return lookup_result_t<NODE_TYPE>::end();
      }
    }

    typename STAGE_T::result_t result_raw;
    if (index_key) {
      result_raw = STAGE_T::template lower_bound<true>(
          node_stage, key, history, index_key);
#ifndef NDEBUG
      if (!result_raw.is_end()) {
        full_key_t<KeyT::VIEW> index;
        STAGE_T::get_key_view(node_stage, result_raw.position, index);
        assert(index == *index_key);
      }
#endif
    } else {
      result_raw = STAGE_T::lower_bound(node_stage, key, history);
    }
#ifndef NDEBUG
    if (result_raw.is_end()) {
      assert(result_raw.mstat == MSTAT_END);
    } else {
      full_key_t<KeyT::VIEW> index;
      STAGE_T::get_key_view(node_stage, result_raw.position, index);
      assert_mstat(key, index, result_raw.mstat);
    }
#endif

    // calculate MSTAT_NE3
    if constexpr (FIELD_TYPE == field_type_t::N0) {
      // currently only internal node checks mstat
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        if (result_raw.mstat == MSTAT_NE2) {
          auto cmp = compare_to<KeyT::HOBJ>(
              key, node_stage[result_raw.position.index].shard_pool);
          assert(cmp != MatchKindCMP::PO);
          if (cmp != MatchKindCMP::EQ) {
            result_raw.mstat = MSTAT_NE3;
          }
        }
      }
    }

    auto result = normalize(std::move(result_raw));
    if (result.is_end()) {
      assert(node_stage.is_level_tail());
      assert(result.p_value == nullptr);
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        result.p_value = node_stage.get_end_p_laddr();
      }
    } else {
      assert(result.p_value != nullptr);
    }
    return result;
  }

  const value_t* insert(
      const full_key_t<KEY_TYPE>& key, const value_t& value,
      search_position_t& insert_pos, match_stage_t& insert_stage,
      node_offset_t& insert_size) override {
    std::cout << "INSERT start: insert_pos(" << insert_pos
              << "), insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size << " ..." << std::endl;
    auto ret = extent.template insert_replayable<KEY_TYPE>(
        key, value, cast_down<STAGE>(insert_pos), insert_stage, insert_size);
#if 0
    dump(std::cout) << std::endl;
#endif
    std::cout << "INSERT done: insert_pos(" << insert_pos
              << "), insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size
              << std::endl << std::endl;
    validate_layout();
    assert(get_key_view(insert_pos) == key);
    return ret;
  }

  std::tuple<search_position_t, bool, const value_t*> split_insert(
      NodeExtentMutable& right_mut, NodeImpl& right_impl,
      const full_key_t<KEY_TYPE>& key, const value_t& value,
      search_position_t& _insert_pos, match_stage_t& insert_stage,
      node_offset_t& insert_size) override {
    std::cout << "SPLIT-INSERT start: insert_pos(" << _insert_pos
              << "), insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size
              << std::endl;
    auto& insert_pos = cast_down<STAGE>(_insert_pos);
    auto& node_stage = extent.read();
    typename STAGE_T::StagedIterator split_at;
    bool is_insert_left;
    {
      size_t empty_size = node_stage.size_before(0);
      size_t total_size = node_stage.total_size();
      size_t available_size = total_size - empty_size;
      size_t filled_size = total_size - node_stage.free_size() - empty_size;
      size_t target_split_size = empty_size + (filled_size + insert_size) / 2;
      // TODO adjust NODE_BLOCK_SIZE according to this requirement
      assert(insert_size < available_size / 2);

      size_t split_size = 0;
      std::optional<bool> _is_insert_left;
      split_at.set(node_stage);
      STAGE_T::recursively_locate_split_inserted(
          split_size, 0, target_split_size, insert_pos,
          insert_stage, insert_size, _is_insert_left, split_at);
      is_insert_left = *_is_insert_left;
      std::cout << "located_split: split_at(" << split_at << "), insert_pos(" << insert_pos
                << "), is_insert_left=" << is_insert_left
                << ", estimated_split_size=" << split_size
                << "(target=" << target_split_size
                << ", current=" << node_stage.size_before(node_stage.keys())
                << ")" << std::endl;
      assert(split_size <= target_split_size);
    }

    auto append_at = split_at;
    // TODO(cross-node string dedup)
    typename STAGE_T::template StagedAppender<KEY_TYPE> right_appender;
    right_appender.init(&right_mut, right_mut.get_write());
    const value_t* p_value = nullptr;
    if (!is_insert_left) {
      // right node: append [start(append_at), insert_pos)
      STAGE_T::template append_until<KEY_TYPE>(
          append_at, right_appender, insert_pos, insert_stage);
      std::cout << "append-insert right: insert_pos(" << insert_pos
                << "), insert_stage=" << (int)insert_stage
                << " ..." << std::endl;
      // right node: append [insert_pos(key, value)]
      bool is_front_insert = (insert_pos == position_t::begin());
      bool is_end = STAGE_T::template append_insert<KEY_TYPE>(
          key, value, append_at, right_appender,
          is_front_insert, insert_stage, p_value);
      assert(append_at.is_end() == is_end);
    } else {
      std::cout << "append right ..." << std::endl;
    }

    // right node: append (insert_pos, end)
    auto pos_end = position_t::end();
    STAGE_T::template append_until<KEY_TYPE>(
        append_at, right_appender, pos_end, STAGE);
    assert(append_at.is_end());
    right_appender.wrap();
    right_impl.dump(std::cout) << std::endl;
    right_impl.validate_layout();

    // mutate left node
    if (is_insert_left) {
      std::cout << "trim-insert left: insert_pos(" << insert_pos
                << "), insert_stage=" << (int)insert_stage
                << " ..." << std::endl;
      p_value = extent.template split_insert_replayable<KEY_TYPE>(
          split_at, key, value, insert_pos, insert_stage, insert_size);
      assert(get_key_view(_insert_pos) == key);
    } else {
      std::cout << "trim left ..." << std::endl;
      assert(right_impl.get_key_view(_insert_pos) == key);
      extent.split_replayable(split_at);
    }
    dump(std::cout) << std::endl;
    validate_layout();
    assert(p_value);

    auto split_pos = normalize(split_at.get_pos());
    std::cout << "SPLIT-INSERT done: split_pos(" << split_pos
              << "), insert_pos(" << _insert_pos
              << "), insert_stage=" << (int)insert_stage
              << ", insert_size=" << insert_size
              << std::endl << std::endl;
    return {split_pos, is_insert_left, p_value};
  }

  /*
   * InternalNodeImpl
   */
  void replace_child_addr(
      const search_position_t& pos, laddr_t dst, laddr_t src) override {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      const laddr_packed_t* p_value = get_p_value(pos);
      assert(p_value->value == src);
      extent.update_child_addr_replayable(dst, const_cast<laddr_packed_t*>(p_value));
    } else {
      assert(false && "impossible path");
    }
  }

  std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_view_t& key, const laddr_t& value,
      search_position_t& insert_pos) const override {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      auto packed_value = laddr_packed_t{value};
      auto& node_stage = extent.read();
      match_stage_t insert_stage;
      node_offset_t insert_size;
      if (unlikely(!node_stage.keys())) {
        assert(insert_pos.is_end());
        insert_stage = STAGE;
        insert_size = STAGE_T::template insert_size<KeyT::VIEW>(key, packed_value);
      } else {
        std::tie(insert_stage, insert_size) = STAGE_T::evaluate_insert(
            node_stage, key, packed_value, cast_down<STAGE>(insert_pos), true);
      }
      return {insert_stage, insert_size};
    } else {
      assert(false && "impossible path");
    }
  }

  /*
   * LeafNodeImpl
   */
  void get_largest_slot(search_position_t& pos,
                        key_view_t& index_key, const onode_t** pp_value) const override {
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      STAGE_T::template lookup_largest_slot<true, true, true>(
          extent.read(), &cast_down_fill_0<STAGE>(pos), &index_key, pp_value);
    } else {
      assert(false && "impossible path");
    }
  }

  std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_hobj_t& key, const onode_t& value, const MatchHistory& history,
      search_position_t& insert_pos) const override {
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      return STAGE_T::evaluate_insert(
          key, value, history, cast_down<STAGE>(insert_pos));
    } else {
      assert(false && "impossible path");
    }
  }

 private:
  NodeLayoutT(typename extent_t::state_t state, NodeExtentRef extent)
    : extent{state, extent} {}

  extent_t extent;
};

using InternalNode0 = NodeLayoutT<node_fields_0_t, node_type_t::INTERNAL>;
using InternalNode1 = NodeLayoutT<node_fields_1_t, node_type_t::INTERNAL>;
using InternalNode2 = NodeLayoutT<node_fields_2_t, node_type_t::INTERNAL>;
using InternalNode3 = NodeLayoutT<internal_fields_3_t, node_type_t::INTERNAL>;
using LeafNode0 = NodeLayoutT<node_fields_0_t, node_type_t::LEAF>;
using LeafNode1 = NodeLayoutT<node_fields_1_t, node_type_t::LEAF>;
using LeafNode2 = NodeLayoutT<node_fields_2_t, node_type_t::LEAF>;
using LeafNode3 = NodeLayoutT<leaf_fields_3_t, node_type_t::LEAF>;

}
