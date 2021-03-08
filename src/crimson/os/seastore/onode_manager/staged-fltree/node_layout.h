// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>
#include <sstream>

#include "common/likely.h"
#include "crimson/common/log.h"
#include "node_extent_accessor.h"
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

/**
 * NodeLayoutT
 *
 * Contains templated and concrete implementations for both InternalNodeImpl
 * and LeafNodeImpl under a specific node layout.
 */
template <typename FieldType, node_type_t NODE_TYPE>
class NodeLayoutT final : public InternalNodeImpl, public LeafNodeImpl {
 public:
  using URef = std::unique_ptr<NodeLayoutT>;
  using extent_t = NodeExtentAccessorT<FieldType, NODE_TYPE>;
  using parent_t = typename node_impl_type<NODE_TYPE>::type;
  using marker_t = typename node_marker_type<NODE_TYPE>::type;
  using node_stage_t = typename extent_t::node_stage_t;
  using position_t = typename extent_t::position_t;
  using value_input_t = typename extent_t::value_input_t;
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
    std::unique_ptr<NodeLayoutT> ret(new NodeLayoutT(extent));
    assert(ret->is_level_tail() == expect_is_level_tail);
    return ret;
  }

  using alloc_ertr = NodeExtentManager::tm_ertr;
  static alloc_ertr::future<typename parent_t::fresh_impl_t> allocate(
      context_t c, bool is_level_tail, level_t level) {
    // NOTE: Currently, all the node types have the same size for simplicity.
    // But depending on the requirement, we may need to make node size
    // configurable by field_type_t and node_type_t, or totally flexible.
    return c.nm.alloc_extent(c.t, node_stage_t::EXTENT_SIZE
    ).safe_then([is_level_tail, level](auto extent) {
      assert(extent->is_initial_pending());
      auto mut = extent->get_mutable();
      node_stage_t::bootstrap_extent(
          mut, FIELD_TYPE, NODE_TYPE, is_level_tail, level);
      return typename parent_t::fresh_impl_t{
        std::unique_ptr<parent_t>(new NodeLayoutT(extent)), mut};
    });
  }

 protected:
  /*
   * NodeImpl
   */
  node_type_t node_type() const override { return NODE_TYPE; }
  field_type_t field_type() const override { return FIELD_TYPE; }
  laddr_t laddr() const override { return extent.get_laddr(); }
  const char* read() const override { return extent.read().p_start(); }
  bool is_duplicate() const override { return extent.is_duplicate(); }
  void prepare_mutate(context_t c) override { return extent.prepare_mutate(c); }
  bool is_level_tail() const override { return extent.read().is_level_tail(); }
  bool is_empty() const override { return extent.read().keys() == 0; }
  level_t level() const override { return extent.read().level(); }
  node_offset_t free_size() const override { return extent.read().free_size(); }

  std::optional<key_view_t> get_pivot_index() const override {
    assert(!is_empty());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (is_level_tail()) {
        return std::nullopt;
      }
    }
    key_view_t pivot_index;
    get_largest_slot(nullptr, &pivot_index, nullptr);
    return {pivot_index};
  }

  node_stats_t get_stats() const override {
    node_stats_t stats;
    auto& node_stage = extent.read();
    key_view_t index_key;
    if (node_stage.keys()) {
      STAGE_T::get_stats(node_stage, stats, index_key);
    }
    stats.size_persistent = node_stage_t::EXTENT_SIZE;
    stats.size_filled = filled_size();
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      if (is_level_tail()) {
        stats.size_logical += sizeof(value_t);
        stats.size_value += sizeof(value_t);
        stats.num_kvs += 1;
      }
    }
    return stats;
  }

  std::ostream& dump(std::ostream& os) const override {
    auto& node_stage = extent.read();
    auto p_start = node_stage.p_start();
    dump_brief(os);
    auto stats = get_stats();
    os << " num_kvs=" << stats.num_kvs
       << ", logical=" << stats.size_logical
       << "B, overhead=" << stats.size_overhead
       << "B, value=" << stats.size_value << "B";
    os << ":\n  header: " << node_stage_t::header_size() << "B";
    size_t size = 0u;
    if (node_stage.keys()) {
      STAGE_T::dump(node_stage, os, "  ", size, p_start);
    } else {
      size += node_stage_t::header_size();
      if (NODE_TYPE == node_type_t::LEAF || !node_stage.is_level_tail()) {
        os << " empty!";
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
    assert(size == filled_size());
    return os;
  }

  std::ostream& dump_brief(std::ostream& os) const override {
    auto& node_stage = extent.read();
    os << "Node" << NODE_TYPE << FIELD_TYPE
       << "@0x" << std::hex << extent.get_laddr()
       << "+" << node_stage_t::EXTENT_SIZE << std::dec
       << (node_stage.is_level_tail() ? "$" : "")
       << "(level=" << (unsigned)node_stage.level()
       << ", filled=" << filled_size() << "B"
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
  void get_slot(const search_position_t& pos,
                key_view_t* p_index_key = nullptr,
                const value_t** pp_value = nullptr) const override {
    assert(!is_empty());
    assert(!pos.is_end());
    if (p_index_key && pp_value) {
      STAGE_T::template get_slot<true, true>(
          extent.read(), cast_down<STAGE>(pos), p_index_key, pp_value);
    } else if (!p_index_key && pp_value) {
      STAGE_T::template get_slot<false, true>(
          extent.read(), cast_down<STAGE>(pos), nullptr, pp_value);
    } else if (p_index_key && !pp_value) {
      STAGE_T::template get_slot<true, false>(
          extent.read(), cast_down<STAGE>(pos), p_index_key, nullptr);
    } else {
      ceph_abort("impossible path");
    }
  }

  void get_next_slot(search_position_t& pos,
                     key_view_t* p_index_key = nullptr,
                     const value_t** pp_value = nullptr) const override {
    assert(!is_empty());
    assert(!pos.is_end());
    bool find_next;
    if (p_index_key && pp_value) {
      find_next = STAGE_T::template get_next_slot<true, true>(
          extent.read(), cast_down<STAGE>(pos), p_index_key, pp_value);
    } else if (!p_index_key && pp_value) {
      find_next = STAGE_T::template get_next_slot<false, true>(
          extent.read(), cast_down<STAGE>(pos), nullptr, pp_value);
    } else {
      ceph_abort("not implemented");
    }
    if (find_next) {
      pos = search_position_t::end();
    }
  }

  void get_largest_slot(search_position_t* p_pos = nullptr,
                        key_view_t* p_index_key = nullptr,
                        const value_t** pp_value = nullptr) const override {
    assert(!is_empty());
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(!is_level_tail());
    }
    if (p_pos && p_index_key && pp_value) {
      STAGE_T::template get_largest_slot<true, true, true>(
          extent.read(), &cast_down_fill_0<STAGE>(*p_pos), p_index_key, pp_value);
    } else if (!p_pos && p_index_key && !pp_value) {
      STAGE_T::template get_largest_slot<false, true, false>(
          extent.read(), nullptr, p_index_key, nullptr);
    } else {
      ceph_abort("not implemented");
    }
  }


  lookup_result_t<NODE_TYPE> lower_bound(
      const key_hobj_t& key, MatchHistory& history,
      key_view_t* index_key=nullptr, marker_t={}) const override {
    auto& node_stage = extent.read();
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      if (unlikely(node_stage.keys() == 0)) {
        history.set<STAGE_LEFT>(MatchKindCMP::LT);
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
        STAGE_T::template get_slot<true, false>(
            node_stage, result_raw.position, &index, nullptr);
        assert(index.compare_to(*index_key) == MatchKindCMP::EQ);
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
      STAGE_T::template get_slot<true, false>(
          node_stage, result_raw.position, &index, nullptr);
      assert_mstat(key, index, result_raw.mstat);
    }
#endif

    // calculate MSTAT_LT3
    if constexpr (FIELD_TYPE == field_type_t::N0) {
      // currently only internal node checks mstat
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        if (result_raw.mstat == MSTAT_LT2) {
          auto cmp = compare_to<KeyT::HOBJ>(
              key, node_stage[result_raw.position.index].shard_pool);
          assert(cmp != MatchKindCMP::GT);
          if (cmp != MatchKindCMP::EQ) {
            result_raw.mstat = MSTAT_LT3;
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
      const full_key_t<KEY_TYPE>& key, const value_input_t& value,
      search_position_t& insert_pos, match_stage_t& insert_stage,
      node_offset_t& insert_size) override {
    logger().debug("OTree::Layout::Insert: begin at "
                   "insert_pos({}), insert_stage={}, insert_size={}B ...",
                   insert_pos, insert_stage, insert_size);
    if (unlikely(logger().is_enabled(seastar::log_level::trace))) {
      std::ostringstream sos;
      dump(sos);
      logger().trace("OTree::Layout::Insert: -- dump\n{}", sos.str());
    }
    auto ret = extent.template insert_replayable<KEY_TYPE>(
        key, value, cast_down<STAGE>(insert_pos), insert_stage, insert_size);
    logger().debug("OTree::Layout::Insert: done  at "
                   "insert_pos({}), insert_stage={}, insert_size={}B",
                   insert_pos, insert_stage, insert_size);
    if (unlikely(logger().is_enabled(seastar::log_level::trace))) {
      std::ostringstream sos;
      dump(sos);
      logger().trace("OTree::Layout::Insert: -- dump\n{}", sos.str());
    }
    validate_layout();
#ifndef NDEBUG
    full_key_t<KeyT::VIEW> index;
    get_slot(insert_pos, &index, nullptr);
    assert(index.compare_to(key) == MatchKindCMP::EQ);
#endif
    return ret;
  }

  std::tuple<search_position_t, bool, const value_t*> split_insert(
      NodeExtentMutable& right_mut, NodeImpl& _right_impl,
      const full_key_t<KEY_TYPE>& key, const value_input_t& value,
      search_position_t& _insert_pos, match_stage_t& insert_stage,
      node_offset_t& insert_size) override {
    assert(_right_impl.node_type() == NODE_TYPE);
    assert(_right_impl.field_type() == FIELD_TYPE);
    auto& right_impl = dynamic_cast<NodeLayoutT&>(_right_impl);
    logger().info("OTree::Layout::Split: begin at "
                  "insert_pos({}), insert_stage={}, insert_size={}B, "
                  "{:#x}=>{:#x} ...",
                  _insert_pos, insert_stage, insert_size,
                  laddr(), right_impl.laddr());
    if (unlikely(logger().is_enabled(seastar::log_level::debug))) {
      std::ostringstream sos;
      dump(sos);
      logger().debug("OTree::Layout::Split: -- dump\n{}", sos.str());
    }
#ifdef UNIT_TESTS_BUILT
    auto insert_stage_pre = insert_stage;
#endif

    auto& insert_pos = cast_down<STAGE>(_insert_pos);
    auto& node_stage = extent.read();
    typename STAGE_T::StagedIterator split_at;
    bool is_insert_left;
    size_t split_size;
    size_t target_split_size;
    {
      size_t empty_size = node_stage.size_before(0);
      size_t filled_kv_size = filled_size() - empty_size;
      /** NODE_BLOCK_SIZE considerations
       *
       * Generally,
       * target_split_size = (filled_size + insert_size) / 2
       * We can have two locate_split() strategies:
       * A. the simpler one is to locate the largest split position where
       *    the estimated left_node_size <= target_split_size;
       * B. the fair one takes a further step to calculate the next slot of
       *    P KiB, and if left_node_size + P/2 < target_split_size, compensate
       *    the split position to include the next slot; (TODO)
       *
       * Say that the node_block_size = N KiB, the largest allowed
       * insert_size = 1/I * N KiB (I > 1). We want to identify the minimal 'I'
       * that won't lead to "double split" effect, meaning after a split,
       * the right node size is still larger than N KiB and need to split
       * again. I think "double split" makes split much more complicated and
       * we can no longer identify whether the node is safe under concurrent
       * operations.
       *
       * We need to evaluate the worst case in order to identify 'I'. This means:
       * - filled_size ~= N KiB
       * - insert_size == N/I KiB
       * - target_split_size ~= (I+1)/2I * N KiB
       * To simplify the below calculations, node_block_size is normalized to 1.
       *
       * With strategy A, the worst case is when left_node_size cannot include
       * the next slot that will just overflow the target_split_size:
       * - left_node_size + 1/I ~= (I+1)/2I
       * - left_node_size ~= (I-1)/2I
       * - right_node_size ~= 1 + 1/I - left_node_size ~= (I+3)/2I
       * The right_node_size cannot larger than the node_block_size in the
       * worst case, which means (I+3)/2I < 1, so I > 3, meaning the largest
       * possible insert_size must be smaller than 1/3 of the node_block_size.
       *
       * With strategy B, the worst case is when left_node_size cannot include
       * the next slot that will just overflow the threshold
       * target_split_size - 1/2I, thus:
       * - left_node_size ~= (I+1)/2I - 1/2I ~= 1/2
       * - right_node_size ~= 1 + 1/I - 1/2 ~= (I+2)/2I < node_block_size(1)
       * - I > 2
       * This means the largest possible insert_size must be smaller than 1/2 of
       * the node_block_size, which is better than strategy A.

       * In order to avoid "double split", there is another side-effect we need
       * to take into consideration: if split happens with snap-gen indexes, the
       * according ns-oid string needs to be copied to the right node. That is
       * to say: right_node_size + string_size < node_block_size.
       *
       * Say that the largest allowed string size is 1/S of the largest allowed
       * insert_size N/I KiB. If we go with stragety B, the equation should be
       * changed to:
       * - right_node_size ~= (I+2)/2I + 1/(I*S) < 1
       * - I > 2 + 2/S (S > 1)
       *
       * Now back to NODE_BLOCK_SIZE calculation, if we have limits of at most
       * X KiB ns-oid string and Y KiB of value to store in this BTree, then:
       * - largest_insert_size ~= X+Y KiB
       * - 1/S == X/(X+Y)
       * - I > (4X+2Y)/(X+Y)
       * - node_block_size(N) == I * insert_size > 4X+2Y KiB
       *
       * In conclusion,
       * (TODO) the current node block size (4 KiB) is too small to
       * store entire 2 KiB ns-oid string. We need to consider a larger
       * node_block_size.
       *
       * We are setting X = Y = 640 B in order not to break the current
       * implementations with 4KiB node.
       *
       * (TODO) Implement smarter logics to check when "double split" happens.
       */
      target_split_size = empty_size + (filled_kv_size + insert_size) / 2;
      assert(insert_size < (node_stage.total_size() - empty_size) / 2);

      std::optional<bool> _is_insert_left;
      split_at.set(node_stage);
      split_size = 0;
      bool locate_nxt = STAGE_T::recursively_locate_split_inserted(
          split_size, 0, target_split_size, insert_pos,
          insert_stage, insert_size, _is_insert_left, split_at);
      is_insert_left = *_is_insert_left;
      logger().debug("OTree::Layout::Split: -- located "
          "split_at({}), insert_pos({}), is_insert_left={}, "
          "split_size={}B(target={}B, current={}B)",
          split_at, insert_pos, is_insert_left,
          split_size, target_split_size, filled_size());
      // split_size can be larger than target_split_size in strategy B
      // assert(split_size <= target_split_size);
      if (locate_nxt) {
        assert(insert_stage == STAGE);
        assert(split_at.get().is_last());
        split_at.set_end();
        assert(insert_pos.index == split_at.index());
      }
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
      logger().debug("OTree::Layout::Split: -- right appended until "
                     "insert_pos({}), insert_stage={}, insert/append the rest ...",
                     insert_pos, insert_stage);
      // right node: append [insert_pos(key, value)]
      bool is_front_insert = (insert_pos == position_t::begin());
      [[maybe_unused]] bool is_end = STAGE_T::template append_insert<KEY_TYPE>(
          key, value, append_at, right_appender,
          is_front_insert, insert_stage, p_value);
      assert(append_at.is_end() == is_end);
    } else {
      logger().debug("OTree::Layout::Split: -- right appending ...");
    }

    // right node: append (insert_pos, end)
    auto pos_end = position_t::end();
    STAGE_T::template append_until<KEY_TYPE>(
        append_at, right_appender, pos_end, STAGE);
    assert(append_at.is_end());
    right_appender.wrap();
    if (unlikely(logger().is_enabled(seastar::log_level::debug))) {
      std::ostringstream sos;
      right_impl.dump(sos);
      logger().debug("OTree::Layout::Split: -- right node dump\n{}", sos.str());
    }
    right_impl.validate_layout();

    // mutate left node
    if (is_insert_left) {
      logger().debug("OTree::Layout::Split: -- left trim/insert at "
                     "insert_pos({}), insert_stage={} ...",
                     insert_pos, insert_stage);
      p_value = extent.template split_insert_replayable<KEY_TYPE>(
          split_at, key, value, insert_pos, insert_stage, insert_size);
#ifndef NDEBUG
      full_key_t<KeyT::VIEW> index;
      get_slot(_insert_pos, &index, nullptr);
      assert(index.compare_to(key) == MatchKindCMP::EQ);
#endif
    } else {
      logger().debug("OTree::Layout::Split: -- left trim ...");
#ifndef NDEBUG
      full_key_t<KeyT::VIEW> index;
      right_impl.get_slot(_insert_pos, &index, nullptr);
      assert(index.compare_to(key) == MatchKindCMP::EQ);
#endif
      extent.split_replayable(split_at);
    }
    if (unlikely(logger().is_enabled(seastar::log_level::debug))) {
      std::ostringstream sos;
      dump(sos);
      logger().debug("OTree::Layout::Split: -- left node dump\n{}", sos.str());
    }
    validate_layout();
    assert(p_value);

    auto split_pos = normalize(split_at.get_pos());
    logger().info("OTree::Layout::Split: done  at "
                  "insert_pos({}), insert_stage={}, insert_size={}B, split_at({}), "
                  "is_insert_left={}, split_size={}B(target={}B)",
                  _insert_pos, insert_stage, insert_size, split_pos,
                  is_insert_left, split_size, target_split_size);
    assert(split_size == filled_size());

#ifdef UNIT_TESTS_BUILT
    InsertType insert_type;
    search_position_t last_pos;
    if (is_insert_left) {
      STAGE_T::template get_largest_slot<true, false, false>(
          extent.read(), &cast_down_fill_0<STAGE>(last_pos), nullptr, nullptr);
    } else {
      node_stage_t right_stage{reinterpret_cast<FieldType*>(right_mut.get_write())};
      STAGE_T::template get_largest_slot<true, false, false>(
          right_stage, &cast_down_fill_0<STAGE>(last_pos), nullptr, nullptr);
    }
    if (_insert_pos == search_position_t::begin()) {
      insert_type = InsertType::BEGIN;
    } else if (_insert_pos == last_pos) {
      insert_type = InsertType::LAST;
    } else {
      insert_type = InsertType::MID;
    }
    last_split = {split_pos, insert_stage_pre, is_insert_left, insert_type};
#endif
    return {split_pos, is_insert_left, p_value};
  }

  /*
   * InternalNodeImpl
   */
  const laddr_packed_t* get_tail_value() const override {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      assert(is_level_tail());
      return extent.read().get_end_p_laddr();
    } else {
      ceph_abort("impossible path");
    }
  }

  void replace_child_addr(
      const search_position_t& pos, laddr_t dst, laddr_t src) override {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      const laddr_packed_t* p_value;
      if (pos.is_end()) {
        assert(is_level_tail());
        p_value = get_tail_value();
      } else {
        get_slot(pos, nullptr, &p_value);
      }
      assert(p_value->value == src);
      extent.update_child_addr_replayable(dst, const_cast<laddr_packed_t*>(p_value));
    } else {
      ceph_abort("impossible path");
    }
  }

  std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_view_t& key, const laddr_t& value,
      search_position_t& insert_pos) const override {
    if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
      auto& node_stage = extent.read();
      match_stage_t insert_stage;
      node_offset_t insert_size;
      if (unlikely(!node_stage.keys())) {
        assert(insert_pos.is_end());
        insert_stage = STAGE;
        insert_size = STAGE_T::template insert_size<KeyT::VIEW>(key, value);
      } else {
        std::tie(insert_stage, insert_size) = STAGE_T::evaluate_insert(
            node_stage, key, value, cast_down<STAGE>(insert_pos), false);
      }
      return {insert_stage, insert_size};
    } else {
      ceph_abort("impossible path");
    }
  }

  /*
   * LeafNodeImpl
   */
  std::tuple<match_stage_t, node_offset_t> evaluate_insert(
      const key_hobj_t& key, const value_config_t& value,
      const MatchHistory& history, match_stat_t mstat,
      search_position_t& insert_pos) const override {
    if constexpr (NODE_TYPE == node_type_t::LEAF) {
      if (unlikely(is_empty())) {
        assert(insert_pos.is_end());
        return {STAGE, STAGE_T::template insert_size<KeyT::HOBJ>(key, value)};
      } else {
        return STAGE_T::evaluate_insert(
            key, value, history, mstat, cast_down<STAGE>(insert_pos));
      }
    } else {
      ceph_abort("impossible path");
    }
  }

  std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  prepare_mutate_value_payload(context_t c) {
    return extent.prepare_mutate_value_payload(c);
  }

 private:
  NodeLayoutT(NodeExtentRef extent) : extent{extent} {}

  node_offset_t filled_size() const {
    auto& node_stage = extent.read();
    auto ret = node_stage.size_before(node_stage.keys());
    assert(ret == node_stage.total_size() - node_stage.free_size());
    return ret;
  }

  static seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }

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
