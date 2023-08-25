// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <compare>
#include <optional>
#include <ostream>
#include <sstream>
#include <type_traits>

#include "common/likely.h"

#include "sub_items_stage.h"
#include "item_iterator_stage.h"

namespace crimson::os::seastore::onode {

struct search_result_bs_t {
  index_t index;
  MatchKindBS match;
};
template <typename FGetKey>
search_result_bs_t binary_search(
    const key_hobj_t& key,
    index_t begin, index_t end, FGetKey&& f_get_key) {
  assert(begin <= end);
  while (begin < end) {
    auto total = begin + end;
    auto mid = total >> 1;
    // do not copy if return value is reference
    decltype(f_get_key(mid)) target = f_get_key(mid);
    auto match = key <=> target;
    if (match == std::strong_ordering::less) {
      end = mid;
    } else if (match == std::strong_ordering::greater) {
      begin = mid + 1;
    } else {
      return {mid, MatchKindBS::EQ};
    }
  }
  return {begin , MatchKindBS::NE};
}

template <typename PivotType, typename FGet>
search_result_bs_t binary_search_r(
    index_t rend, index_t rbegin, FGet&& f_get, const PivotType& key) {
  assert(rend <= rbegin);
  while (rend < rbegin) {
    auto total = rend + rbegin + 1;
    auto mid = total >> 1;
    // do not copy if return value is reference
    decltype(f_get(mid)) target = f_get(mid);
    int match = target - key;
    if (match < 0) {
      rend = mid;
    } else if (match > 0) {
      rbegin = mid - 1;
    } else {
      return {mid, MatchKindBS::EQ};
    }
  }
  return {rbegin, MatchKindBS::NE};
}

inline bool matchable(field_type_t type, match_stat_t mstat) {
  assert(mstat >= MSTAT_MIN && mstat <= MSTAT_MAX);
  /*
   * compressed prefix by field type:
   * N0: NONE
   * N1: pool/shard
   * N2: pool/shard crush
   * N3: pool/shard crush ns/oid
   *
   * if key matches the node's compressed prefix, return true
   * else, return false
   */
#ifndef NDEBUG
  if (mstat == MSTAT_END) {
    assert(type == field_type_t::N0);
  }
#endif
  return mstat + to_unsigned(type) < 4;
}

inline void assert_mstat(
    const key_hobj_t& key,
    const key_view_t& index,
    match_stat_t mstat) {
  assert(mstat >= MSTAT_MIN && mstat <= MSTAT_LT2);
  // key < index ...
  switch (mstat) {
   case MSTAT_EQ:
    break;
   case MSTAT_LT0:
    assert(key < index.snap_gen_packed());
    break;
   case MSTAT_LT1:
    assert(key < index.ns_oid_view());
    break;
   case MSTAT_LT2:
    if (index.has_shard_pool()) {
      assert((key < shard_pool_crush_t{
               index.shard_pool_packed(), index.crush_packed()}));
    } else {
      assert(key < index.crush_packed());
    }
    break;
   default:
    ceph_abort("impossible path");
  }
  // key == index ...
  switch (mstat) {
   case MSTAT_EQ:
    assert(key == index.snap_gen_packed());
   case MSTAT_LT0:
    if (!index.has_ns_oid())
      break;
    assert(index.ns_oid_view().type() == ns_oid_view_t::Type::MAX ||
           key == index.ns_oid_view());
   case MSTAT_LT1:
    if (!index.has_crush())
      break;
    assert(key == index.crush_packed());
    if (!index.has_shard_pool())
      break;
    assert(key == index.shard_pool_packed());
   default:
    break;
  }
}

#define NXT_STAGE_T staged<next_param_t>

enum class TrimType { BEFORE, AFTER, AT };

/**
 * staged
 *
 * Implements recursive logic that modifies or reads the node layout
 * (N0/N1/N2/N3 * LEAF/INTERNAL) with the multi-stage design. The specific
 * stage implementation is flexible. So the implementations for different
 * stages can be assembled independently, as long as they follow the
 * definitions of container interfaces.
 *
 * Multi-stage is designed to index different portions of onode keys
 * stage-by-stage. There are at most 3 stages for a node:
 * - STAGE_LEFT:   index shard-pool-crush for N0, or index crush for N1 node;
 * - STAGE_STRING: index ns-oid for N0/N1/N2 nodes;
 * - STAGE_RIGHT:  index snap-gen for N0/N1/N2/N3 nodes;
 *
 * The intention is to consolidate the high-level indexing implementations at
 * the level of stage, so we don't need to write them repeatedly for every
 * stage and for every node type.
 */
template <typename Params>
struct staged {
  static_assert(Params::STAGE >= STAGE_BOTTOM);
  static_assert(Params::STAGE <= STAGE_TOP);
  using container_t = typename Params::container_t;
  using key_get_type = typename container_t::key_get_type;
  using next_param_t = typename Params::next_param_t;
  using position_t = staged_position_t<Params::STAGE>;
  using result_t = staged_result_t<Params::NODE_TYPE, Params::STAGE>;
  using value_input_t = value_input_type_t<Params::NODE_TYPE>;
  using value_t = value_type_t<Params::NODE_TYPE>;
  static constexpr auto CONTAINER_TYPE = container_t::CONTAINER_TYPE;
  static constexpr bool IS_BOTTOM = (Params::STAGE == STAGE_BOTTOM);
  static constexpr auto NODE_TYPE = Params::NODE_TYPE;
  static constexpr auto STAGE = Params::STAGE;

  template <bool is_exclusive>
  static void _left_or_right(index_t& split_index, index_t insert_index,
                             std::optional<bool>& is_insert_left) {
    assert(!is_insert_left.has_value());
    assert(is_valid_index(split_index));
    if constexpr (is_exclusive) {
      if (split_index <= insert_index) {
        // ...[s_index-1] |!| (i_index) [s_index]...
        // offset i_position to right
        is_insert_left = false;
      } else {
        // ...[s_index-1] (i_index)) |?[s_index]| ...
        // ...(i_index)...[s_index-1] |?[s_index]| ...
        is_insert_left = true;
        --split_index;
      }
    } else {
      if (split_index < insert_index) {
        // ...[s_index-1] |?[s_index]| ...[(i_index)[s_index_k]...
        is_insert_left = false;
      } else if (split_index > insert_index) {
        // ...[(i_index)s_index-1] |?[s_index]| ...
        // ...[(i_index)s_index_k]...[s_index-1] |?[s_index]| ...
        is_insert_left = true;
      } else {
        // ...[s_index-1] |?[(i_index)s_index]| ...
        // i_to_left = std::nullopt;
      }
    }
  }

  template <ContainerType CTYPE, typename Enable = void> class _iterator_t;
  template <ContainerType CTYPE>
  class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::INDEXABLE>> {
   /*
    * indexable container type system:
    *   CONTAINER_TYPE = ContainerType::INDEXABLE
    *   keys() const -> index_t
    *   operator[](index_t) const -> key_get_type
    *   size_before(index_t) const -> extent_len_t
    *   size_overhead_at(index_t) const -> node_offset_t
    *   (IS_BOTTOM) get_p_value(index_t) const -> const value_t*
    *   (!IS_BOTTOM) size_to_nxt_at(index_t) const -> node_offset_t
    *   (!IS_BOTTOM) get_nxt_container(index_t) const
    *   encode(p_node_start, encoded)
    *   decode(p_node_start, node_size, delta) -> container_t
    * static:
    *   header_size() -> node_offset_t
    *   estimate_insert(key, value) -> node_offset_t
    *   (IS_BOTTOM) insert_at(mut, src, key, value,
    *                         index, size, p_left_bound) -> const value_t*
    *   (!IS_BOTTOM) insert_prefix_at(mut, src, key,
    *                         index, size, p_left_bound) -> memory_range_t
    *   (!IS_BOTTOM) update_size_at(mut, src, index, size)
    *   trim_until(mut, container, index) -> trim_size
    *   (!IS_BOTTOM) trim_at(mut, container, index, trimmed) -> trim_size
    *   erase_at(mut, container, index, p_left_bound) -> erase_size
    *
    * Appender::append(const container_t& src, from, items)
    */
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {
      assert(container.keys());
    }

    index_t index() const {
      return _index;
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container[_index];
    }
    node_offset_t size_to_nxt() const {
      assert(!is_end());
      return container.size_to_nxt_at(_index);
    }
    template <typename T = typename NXT_STAGE_T::container_t>
    std::enable_if_t<!IS_BOTTOM, T> get_nxt_container() const {
      assert(!is_end());
      return container.get_nxt_container(_index);
    }
    template <typename T = value_t>
    std::enable_if_t<IS_BOTTOM, const T*> get_p_value() const {
      assert(!is_end());
      return container.get_p_value(_index);
    }
    bool is_last() const {
      return _index + 1 == container.keys();
    }
    bool is_end() const { return _index == container.keys(); }
    node_offset_t size() const {
      assert(!is_end());
      assert(header_size() == container.size_before(0));
      assert(container.size_before(_index + 1) > container.size_before(_index));
      return container.size_before(_index + 1) -
             container.size_before(_index);
    }
    node_offset_t size_overhead() const {
      assert(!is_end());
      return container.size_overhead_at(_index);
    }

    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++_index;
      return *this;
    }
    void seek_at(index_t index) {
      assert(index < container.keys());
      seek_till_end(index);
    }
    void seek_till_end(index_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      assert(index <= container.keys());
      _index = index;
    }
    void seek_last() {
      assert(!is_end());
      assert(index() == 0);
      _index = container.keys() - 1;
    }
    void set_end() {
      assert(!is_end());
      assert(is_last());
      ++_index;
    }
    // Note: possible to return an end iterator
    MatchKindBS seek(const key_hobj_t& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      index_t end_index = container.keys();
      if (exclude_last) {
        assert(end_index);
        --end_index;
        assert(key < container[end_index]);
      }
      auto ret = binary_search(key, _index, end_index,
          [this] (index_t index) { return container[index]; });
      _index = ret.index;
      return ret.match;
    }

    template <IsFullKey Key, typename T = value_t>
    std::enable_if_t<IS_BOTTOM, const T*> insert(
        NodeExtentMutable& mut,
        const Key& key,
        const value_input_t& value,
        node_offset_t insert_size,
        const char* p_left_bound) {
      return container_t::insert_at(
          mut, container, key, value, _index, insert_size, p_left_bound);
    }

    template <IsFullKey Key, typename T = memory_range_t>
    std::enable_if_t<!IS_BOTTOM, T> insert_prefix(
        NodeExtentMutable& mut, const Key& key,
        node_offset_t size, const char* p_left_bound) {
      return container_t::insert_prefix_at(
          mut, container, key, _index, size, p_left_bound);
    }

    template <typename T = void>
    std::enable_if_t<!IS_BOTTOM, T>
    update_size(NodeExtentMutable& mut, int insert_size) {
      assert(!is_end());
      container_t::update_size_at(mut, container, _index, insert_size);
    }

    // Note: possible to return an end iterator when is_exclusive is true
    template <bool is_exclusive>
    size_t seek_split_inserted(
        size_t start_size, size_t extra_size, size_t target_size,
        index_t& insert_index, size_t insert_size,
        std::optional<bool>& is_insert_left) {
      assert(!is_end());
      assert(index() == 0);
      // replace insert_index placeholder
      if constexpr (!is_exclusive) {
        if (insert_index == INDEX_LAST) {
          insert_index = container.keys() - 1;
        }
      } else {
        if (insert_index == INDEX_END) {
          insert_index = container.keys();
        }
      }
      assert(insert_index <= container.keys());

      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1,
                              insert_index, insert_size] (index_t index) {
        size_t current_size;
        if (unlikely(index == 0)) {
          current_size = start_size;
        } else {
          current_size = start_size_1;
          if (index > insert_index) {
            current_size += insert_size;
            if constexpr (is_exclusive) {
              --index;
            }
          }
          // already includes header size
          current_size += container.size_before(index);
        }
        return current_size;
      };
      index_t s_end;
      if constexpr (is_exclusive) {
        s_end = container.keys();
      } else {
        s_end = container.keys() - 1;
      }
      _index = binary_search_r(0, s_end, f_get_used_size, target_size).index;
      size_t current_size = f_get_used_size(_index);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(_index, insert_index, is_insert_left);
      return current_size;
    }

    size_t seek_split(size_t start_size, size_t extra_size, size_t target_size) {
      assert(!is_end());
      assert(index() == 0);
      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1] (index_t index) {
        size_t current_size;
        if (unlikely(index == 0)) {
          current_size = start_size;
        } else {
          // already includes header size
          current_size = start_size_1 + container.size_before(index);
        }
        return current_size;
      };
      _index = binary_search_r(
          0, container.keys() - 1, f_get_used_size, target_size).index;
      size_t current_size = f_get_used_size(_index);
      assert(current_size <= target_size);
      return current_size;
    }

    // Note: possible to return an end iterater if to_index == INDEX_END
    template <KeyT KT>
    void copy_out_until(
        typename container_t::template Appender<KT>& appender, index_t& to_index) {
      auto num_keys = container.keys();
      index_t items;
      if (to_index == INDEX_END) {
        items = num_keys - _index;
        appender.append(container, _index, items);
        _index = num_keys;
        to_index = _index;
      } else if (to_index == INDEX_LAST) {
        assert(!is_end());
        items = num_keys - 1 - _index;
        appender.append(container, _index, items);
        _index = num_keys - 1;
        to_index = _index;
      } else {
        assert(_index <= to_index);
        assert(to_index <= num_keys);
        items = to_index - _index;
        appender.append(container, _index, items);
        _index = to_index;
      }
    }

    node_offset_t trim_until(NodeExtentMutable& mut) {
      return container_t::trim_until(mut, container, _index);
    }

    template <typename T = node_offset_t>
    std::enable_if_t<!IS_BOTTOM, T>
    trim_at(NodeExtentMutable& mut, node_offset_t trimmed) {
      return container_t::trim_at(mut, container, _index, trimmed);
    }

    node_offset_t erase(NodeExtentMutable& mut, const char* p_left_bound) {
      assert(!is_end());
      return container_t::erase_at(mut, container, _index, p_left_bound);
    }

    template <KeyT KT>
    typename container_t::template Appender<KT>
    get_appender(NodeExtentMutable* p_mut) {
      assert(_index + 1 == container.keys());
      return typename container_t::template Appender<KT>(p_mut, container);
    }

    template <KeyT KT>
    typename container_t::template Appender<KT>
    get_appender_opened(NodeExtentMutable* p_mut) {
      if constexpr (!IS_BOTTOM) {
        assert(_index + 1 == container.keys());
        return typename container_t::template Appender<KT>(p_mut, container, true);
      } else {
        ceph_abort("impossible path");
      }
    }

    void encode(const char* p_node_start, ceph::bufferlist& encoded) const {
      container.encode(p_node_start, encoded);
      ceph::encode(_index, encoded);
    }

    static me_t decode(const char* p_node_start,
                       extent_len_t node_size,
                       ceph::bufferlist::const_iterator& delta) {
      auto container = container_t::decode(
          p_node_start, node_size, delta);
      auto ret = me_t(container);
      index_t index;
      ceph::decode(index, delta);
      ret.seek_till_end(index);
      return ret;
    }

    static node_offset_t header_size() {
      return container_t::header_size();
    }

    template <IsFullKey Key>
    static node_offset_t estimate_insert(
        const Key& key, const value_input_t& value) {
      return container_t::estimate_insert(key, value);
    }

   private:
    container_t container;
    index_t _index = 0;
  };

  template <ContainerType CTYPE>
  class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::ITERATIVE>> {
    /*
     * iterative container type system (!IS_BOTTOM):
     *   CONTAINER_TYPE = ContainerType::ITERATIVE
     *   index() const -> index_t
     *   get_key() const -> key_get_type
     *   size() const -> node_offset_t
     *   size_to_nxt() const -> node_offset_t
     *   size_overhead() const -> node_offset_t
     *   get_nxt_container() const
     *   has_next() const -> bool
     *   encode(p_node_start, encoded)
     *   decode(p_node_start, node_length, delta) -> container_t
     *   operator++()
     * static:
     *   header_size() -> node_offset_t
     *   estimate_insert(key, value) -> node_offset_t
     *   insert_prefix(mut, src, key, is_end, size, p_left_bound) -> memory_range_t
     *   update_size(mut, src, size)
     *   trim_until(mut, container) -> trim_size
     *   trim_at(mut, container, trimmed) -> trim_size
     *   erase(mut, container, p_left_bound) -> erase_size
     */
    // currently the iterative iterator is only implemented with STAGE_STRING
    // for in-node space efficiency
    static_assert(STAGE == STAGE_STRING);
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {}

    index_t index() const {
      if (is_end()) {
        return container.index() + 1;
      } else {
        return container.index();
      }
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container.get_key();
    }
    node_offset_t size_to_nxt() const {
      assert(!is_end());
      return container.size_to_nxt();
    }
    const typename NXT_STAGE_T::container_t get_nxt_container() const {
      assert(!is_end());
      return container.get_nxt_container();
    }
    bool is_last() const {
      assert(!is_end());
      return !container.has_next();
    }
    bool is_end() const {
#ifndef NDEBUG
      if (_is_end) {
        assert(!container.has_next());
      }
#endif
      return _is_end;
    }
    node_offset_t size() const {
      assert(!is_end());
      return container.size();
    }
    node_offset_t size_overhead() const {
      assert(!is_end());
      return container.size_overhead();
    }

    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++container;
      return *this;
    }
    void seek_at(index_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      while (index > 0) {
        assert(container.has_next());
        ++container;
        --index;
      }
    }
    void seek_till_end(index_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      while (index > 0) {
        if (!container.has_next()) {
          assert(index == 1);
          set_end();
          break;
        }
        ++container;
        --index;
      }
    }
    void seek_last() {
      assert(!is_end());
      assert(index() == 0);
      while (container.has_next()) {
        ++container;
      }
    }
    void set_end() {
      assert(!is_end());
      assert(is_last());
      _is_end = true;
    }
    // Note: possible to return an end iterator
    MatchKindBS seek(const key_hobj_t& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      do {
        if (exclude_last && is_last()) {
          assert(key < get_key());
          return MatchKindBS::NE;
        }
        auto match = key <=> get_key();
        if (match == std::strong_ordering::less) {
          return MatchKindBS::NE;
        } else if (match == std::strong_ordering::equal) {
          return MatchKindBS::EQ;
        } else {
          if (container.has_next()) {
            ++container;
          } else {
            // end
            break;
          }
        }
      } while (true);
      assert(!exclude_last);
      set_end();
      return MatchKindBS::NE;
    }

    template <IsFullKey Key>
    memory_range_t insert_prefix(
        NodeExtentMutable& mut, const Key& key,
        node_offset_t size, const char* p_left_bound) {
      return container_t::insert_prefix(
          mut, container, key, is_end(), size, p_left_bound);
    }

    void update_size(NodeExtentMutable& mut, int insert_size) {
      assert(!is_end());
      container_t::update_size(mut, container, insert_size);
    }

    // Note: possible to return an end iterator when is_exclusive is true
    // insert_index can still be INDEX_LAST or INDEX_END
    template <bool is_exclusive>
    size_t seek_split_inserted(
        size_t start_size, size_t extra_size, size_t target_size,
        index_t& insert_index, size_t insert_size,
        std::optional<bool>& is_insert_left) {
      assert(!is_end());
      assert(index() == 0);
      size_t current_size = start_size;
      index_t split_index = 0;
      extra_size += header_size();
      do {
        if constexpr (!is_exclusive) {
          if (is_last()) {
            assert(split_index == index());
            if (insert_index == INDEX_LAST) {
              insert_index = index();
            }
            assert(insert_index <= index());
            break;
          }
        }

        size_t nxt_size = current_size;
        if (split_index == 0) {
          nxt_size += extra_size;
        }
        if (split_index == insert_index) {
          nxt_size += insert_size;
          if constexpr (is_exclusive) {
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            ++split_index;
          }
        }
        nxt_size += size();
        if (nxt_size > target_size) {
          break;
        }
        current_size = nxt_size;

        if constexpr (is_exclusive) {
          if (is_last()) {
            assert(split_index == index());
            set_end();
            split_index = index();
            if (insert_index == INDEX_END) {
              insert_index = index();
            }
            assert(insert_index == index());
            break;
          } else {
            ++(*this);
            ++split_index;
          }
        } else {
          ++(*this);
          ++split_index;
        }
      } while (true);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(split_index, insert_index, is_insert_left);
      assert(split_index == index());
      return current_size;
    }

    size_t seek_split(size_t start_size, size_t extra_size, size_t target_size) {
      assert(!is_end());
      assert(index() == 0);
      size_t current_size = start_size;
      do {
        if (is_last()) {
          break;
        }

        size_t nxt_size = current_size;
        if (index() == 0) {
          nxt_size += extra_size;
        }
        nxt_size += size();
        if (nxt_size > target_size) {
          break;
        }
        current_size = nxt_size;
        ++(*this);
      } while (true);
      assert(current_size <= target_size);
      return current_size;
    }

    // Note: possible to return an end iterater if to_index == INDEX_END
    template <KeyT KT>
    void copy_out_until(
        typename container_t::template Appender<KT>& appender, index_t& to_index) {
      if (is_end()) {
        assert(!container.has_next());
        if (to_index == INDEX_END) {
          to_index = index();
        }
        assert(to_index == index());
        return;
      }
      index_t items;
      if (to_index == INDEX_END || to_index == INDEX_LAST) {
        items = to_index;
      } else {
        assert(is_valid_index(to_index));
        assert(index() <= to_index);
        items = to_index - index();
      }
      if (appender.append(container, items)) {
        set_end();
      }
      to_index = index();
    }

    node_offset_t trim_until(NodeExtentMutable& mut) {
      if (is_end()) {
        return 0;
      }
      return container_t::trim_until(mut, container);
    }

    node_offset_t trim_at(NodeExtentMutable& mut, node_offset_t trimmed) {
      assert(!is_end());
      return container_t::trim_at(mut, container, trimmed);
    }

    node_offset_t erase(NodeExtentMutable& mut, const char* p_left_bound) {
      assert(!is_end());
      return container_t::erase(mut, container, p_left_bound);
    }

    template <KeyT KT>
    typename container_t::template Appender<KT>
    get_appender(NodeExtentMutable* p_mut) {
      return typename container_t::template Appender<KT>(p_mut, container, false);
    }

    template <KeyT KT>
    typename container_t::template Appender<KT>
    get_appender_opened(NodeExtentMutable* p_mut) {
      if constexpr (!IS_BOTTOM) {
        return typename container_t::template Appender<KT>(p_mut, container, true);
      } else {
        ceph_abort("impossible path");
      }
    }

    void encode(const char* p_node_start, ceph::bufferlist& encoded) const {
      container.encode(p_node_start, encoded);
      uint8_t is_end = _is_end;
      ceph::encode(is_end, encoded);
    }

    static me_t decode(const char* p_node_start,
                       extent_len_t node_size,
                       ceph::bufferlist::const_iterator& delta) {
      auto container = container_t::decode(
          p_node_start, node_size, delta);
      auto ret = me_t(container);
      uint8_t is_end;
      ceph::decode(is_end, delta);
      if (is_end) {
        ret.set_end();
      }
      return ret;
    }

    static node_offset_t header_size() {
      return container_t::header_size();
    }

    template <IsFullKey Key>
    static node_offset_t estimate_insert(const Key& key,
                                         const value_input_t& value) {
      return container_t::estimate_insert(key, value);
    }

   private:
    container_t container;
    bool _is_end = false;
  };

  /*
   * iterator_t encapsulates both indexable and iterative implementations
   * from a *non-empty* container.
   * cstr(const container_t&)
   * access:
   *   index() -> index_t
   *   get_key() -> key_get_type (const reference or value type)
   *   is_last() -> bool
   *   is_end() -> bool
   *   size() -> node_offset_t
   *   size_overhead() -> node_offset_t
   *   (IS_BOTTOM) get_p_value() -> const value_t*
   *   (!IS_BOTTOM) get_nxt_container() -> container_range_t
   *   (!IS_BOTTOM) size_to_nxt() -> node_offset_t
   * seek:
   *   operator++() -> iterator_t&
   *   seek_at(index)
   *   seek_till_end(index)
   *   seek_last()
   *   set_end()
   *   seek(key, exclude_last) -> MatchKindBS
   * insert:
   *   (IS_BOTTOM) insert(mut, key, value, size, p_left_bound) -> p_value
   *   (!IS_BOTTOM) insert_prefix(mut, key, size, p_left_bound) -> memory_range_t
   *   (!IS_BOTTOM) update_size(mut, size)
   * split:
   *   seek_split_inserted<bool is_exclusive>(
   *       start_size, extra_size, target_size, insert_index, insert_size,
   *       std::optional<bool>& is_insert_left)
   *           -> insert to left/right/unknown (!exclusive)
   *           -> insert to left/right         (exclusive, can be end)
   *     -> split_size
   *   seek_split(start_size, extra_size, target_size) -> split_size
   *   copy_out_until(appender, to_index) (can be end)
   *   trim_until(mut) -> trim_size
   *   (!IS_BOTTOM) trim_at(mut, trimmed) -> trim_size
   * erase:
   *   erase(mut, p_left_bound) -> erase_size
   * merge:
   *   get_appender(p_mut) -> Appender
   *   (!IS_BOTTOM)get_appender_opened(p_mut) -> Appender
   * denc:
   *   encode(p_node_start, encoded)
   *   decode(p_node_start, node_size, delta) -> iterator_t
   * static:
   *   header_size() -> node_offset_t
   *   estimate_insert(key, value) -> node_offset_t
   */
  using iterator_t = _iterator_t<CONTAINER_TYPE>;
  /* TODO: detailed comments
   * - trim_until(mut) -> trim_size
   *   * keep 0 to i - 1, and remove the rest, return the size trimmed.
   *   * if this is the end iterator, do nothing and return 0.
   *   * if this is the start iterator, normally needs to go to the higher
   *     stage to trim the entire container.
   * - trim_at(mut, trimmed) -> trim_size
   *   * trim happens inside the current iterator, causing the size reduced by
   *     <trimmed>, return the total size trimmed.
   */

  /*
   * Lookup internals (hide?)
   */

  static bool is_keys_one(
      const container_t& container) {      // IN
    auto iter = iterator_t(container);
    iter.seek_last();
    if (iter.index() == 0) {
      if constexpr (IS_BOTTOM) {
        // ok, there is only 1 key
        return true;
      } else {
        auto nxt_container = iter.get_nxt_container();
        return NXT_STAGE_T::is_keys_one(nxt_container);
      }
    } else {
      // more than 1 keys
      return false;
    }
  }

  template <bool GET_KEY>
  static result_t smallest_result(
      const iterator_t& iter, key_view_t* p_index_key) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto nxt_container = iter.get_nxt_container();
    auto pos_smallest = NXT_STAGE_T::position_t::begin();
    const value_t* p_value;
    NXT_STAGE_T::template get_slot<GET_KEY, true>(
        nxt_container, pos_smallest, p_index_key, &p_value);
    if constexpr (GET_KEY) {
      assert(p_index_key);
      p_index_key->set(iter.get_key());
    } else {
      assert(!p_index_key);
    }
    return result_t{{iter.index(), pos_smallest}, p_value, STAGE};
  }

  template <bool GET_KEY>
  static result_t nxt_lower_bound(
      const key_hobj_t& key, iterator_t& iter,
      MatchHistory& history, key_view_t* index_key) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto nxt_container = iter.get_nxt_container();
    auto nxt_result = NXT_STAGE_T::template lower_bound<GET_KEY>(
        nxt_container, key, history, index_key);
    if (nxt_result.is_end()) {
      if (iter.is_last()) {
        return result_t::end();
      } else {
        return smallest_result<GET_KEY>(++iter, index_key);
      }
    } else {
      if constexpr (GET_KEY) {
        index_key->set(iter.get_key());
      }
      return result_t::from_nxt(iter.index(), nxt_result);
    }
  }

  template <bool GET_POS, bool GET_KEY, bool GET_VAL>
  static void get_largest_slot(
      const container_t& container,        // IN
      position_t* p_position,              // OUT
      key_view_t* p_index_key,             // OUT
      const value_t** pp_value) {          // OUT
    auto iter = iterator_t(container);
    iter.seek_last();
    if constexpr (GET_KEY) {
      assert(p_index_key);
      p_index_key->set(iter.get_key());
    } else {
      assert(!p_index_key);
    }
    if constexpr (GET_POS) {
      assert(p_position);
      p_position->index = iter.index();
    } else {
      assert(!p_position);
    }
    if constexpr (IS_BOTTOM) {
      if constexpr (GET_VAL) {
        assert(pp_value);
        *pp_value = iter.get_p_value();
      } else {
        assert(!pp_value);
      }
    } else {
      auto nxt_container = iter.get_nxt_container();
      if constexpr (GET_POS) {
        NXT_STAGE_T::template get_largest_slot<true, GET_KEY, GET_VAL>(
            nxt_container, &p_position->nxt, p_index_key, pp_value);
      } else {
        NXT_STAGE_T::template get_largest_slot<false, GET_KEY, GET_VAL>(
            nxt_container, nullptr, p_index_key, pp_value);
      }
    }
  }

  template <bool GET_KEY, bool GET_VAL>
  static void get_slot(
      const container_t& container,        // IN
      const position_t& pos,               // IN
      key_view_t* p_index_key,             // OUT
      const value_t** pp_value) {          // OUT
    auto iter = iterator_t(container);
    iter.seek_at(pos.index);

    if constexpr (GET_KEY) {
      assert(p_index_key);
      p_index_key->set(iter.get_key());
    } else {
      assert(!p_index_key);
    }

    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      NXT_STAGE_T::template get_slot<GET_KEY, GET_VAL>(
          nxt_container, pos.nxt, p_index_key, pp_value);
    } else {
      if constexpr (GET_VAL) {
        assert(pp_value);
        *pp_value = iter.get_p_value();
      } else {
        assert(!pp_value);
      }
    }
  }

  template <bool GET_KEY = false>
  static result_t lower_bound(
      const container_t& container,
      const key_hobj_t& key,
      MatchHistory& history,
      key_view_t* index_key = nullptr) {
    bool exclude_last = false;
    if (history.get<STAGE>().has_value()) {
      if (*history.get<STAGE>() == MatchKindCMP::EQ) {
        // lookup is short-circuited
        if constexpr (!IS_BOTTOM) {
          assert(history.get<STAGE - 1>().has_value());
          if (history.is_GT<STAGE - 1>()) {
            auto iter = iterator_t(container);
            bool test_key_equal;
            if constexpr (STAGE == STAGE_STRING) {
              // TODO(cross-node string dedup)
              // test_key_equal = (iter.get_key().type() == ns_oid_view_t::Type::MIN);
              auto cmp = key <=> iter.get_key();
              assert(cmp != std::strong_ordering::greater);
              test_key_equal = (cmp == 0);
            } else {
              auto cmp = key <=> iter.get_key();
              // From history, key[stage] == parent[stage][index - 1]
              // which should be the smallest possible value for all
              // index[stage][*]
              assert(cmp != std::strong_ordering::greater);
              test_key_equal = (cmp == 0);
            }
            if (test_key_equal) {
              return nxt_lower_bound<GET_KEY>(key, iter, history, index_key);
            } else {
              // key[stage] < index[stage][left-most]
              return smallest_result<GET_KEY>(iter, index_key);
            }
          }
        }
        // IS_BOTTOM || !history.is_GT<STAGE - 1>()
        auto iter = iterator_t(container);
        iter.seek_last();
        if constexpr (STAGE == STAGE_STRING) {
          // TODO(cross-node string dedup)
          // assert(iter.get_key().type() == ns_oid_view_t::Type::MAX);
          assert(key == iter.get_key());
        } else {
          assert(key == iter.get_key());
        }
        if constexpr (GET_KEY) {
          index_key->set(iter.get_key());
        }
        if constexpr (IS_BOTTOM) {
          auto value_ptr = iter.get_p_value();
          return result_t{{iter.index()}, value_ptr, MSTAT_EQ};
        } else {
          auto nxt_container = iter.get_nxt_container();
          auto nxt_result = NXT_STAGE_T::template lower_bound<GET_KEY>(
              nxt_container, key, history, index_key);
          // !history.is_GT<STAGE - 1>() means
          // key[stage+1 ...] <= index[stage+1 ...][*]
          assert(!nxt_result.is_end());
          return result_t::from_nxt(iter.index(), nxt_result);
        }
      } else if (*history.get<STAGE>() == MatchKindCMP::LT) {
        exclude_last = true;
      }
    }
    auto iter = iterator_t(container);
    auto bs_match = iter.seek(key, exclude_last);
    if (iter.is_end()) {
      assert(!exclude_last);
      assert(bs_match == MatchKindBS::NE);
      history.set<STAGE>(MatchKindCMP::GT);
      return result_t::end();
    }
    history.set<STAGE>(bs_match == MatchKindBS::EQ ?
                       MatchKindCMP::EQ : MatchKindCMP::LT);
    if constexpr (IS_BOTTOM) {
      if constexpr (GET_KEY) {
        index_key->set(iter.get_key());
      }
      auto value_ptr = iter.get_p_value();
      return result_t{{iter.index()}, value_ptr,
                      (bs_match == MatchKindBS::EQ ? MSTAT_EQ : MSTAT_LT0)};
    } else {
      if (bs_match == MatchKindBS::EQ) {
        return nxt_lower_bound<GET_KEY>(key, iter, history, index_key);
      } else {
        return smallest_result<GET_KEY>(iter, index_key);
      }
    }
  }

  template <IsFullKey Key>
  static node_offset_t insert_size(const Key& key,
                                   const value_input_t& value) {
    if constexpr (IS_BOTTOM) {
      return iterator_t::estimate_insert(key, value);
    } else {
      return iterator_t::estimate_insert(key, value) +
             NXT_STAGE_T::iterator_t::header_size() +
             NXT_STAGE_T::insert_size(key, value);
    }
  }

  template <IsFullKey Key>
  static node_offset_t insert_size_at(match_stage_t stage,
                                      const Key& key,
                                      const value_input_t& value) {
    if (stage == STAGE) {
      return insert_size(key, value);
    } else {
      assert(stage < STAGE);
      return NXT_STAGE_T::template insert_size_at(stage, key, value);
    }
  }

  template <typename T = std::tuple<match_stage_t, node_offset_t>>
  static std::enable_if_t<NODE_TYPE == node_type_t::INTERNAL, T> evaluate_insert(
      const container_t& container, const key_view_t& key,
      const value_input_t& value, position_t& position, bool evaluate_last) {
    auto iter = iterator_t(container);
    auto& index = position.index;
    if (evaluate_last || index == INDEX_END) {
      iter.seek_last();
      index = iter.index();
      // evaluate the previous index
    } else {
      assert(is_valid_index(index));
      // evaluate the current index
      iter.seek_at(index);
      auto match = key <=> iter.get_key();
      if (match == 0) {
        if constexpr (IS_BOTTOM) {
          ceph_abort("insert conflict at current index!");
        } else {
          // insert into the current index
          auto nxt_container = iter.get_nxt_container();
          return NXT_STAGE_T::evaluate_insert(
              nxt_container, key, value, position.nxt, false);
        }
      } else {
        assert(match == std::strong_ordering::less);
        if (index == 0) {
          // already the first index, so insert at the current index
          return {STAGE, insert_size(key, value)};
        }
        --index;
        iter = iterator_t(container);
        iter.seek_at(index);
        // proceed to evaluate the previous index
      }
    }

    // XXX(multi-type): when key is from a different type of node
    auto match = key <=> iter.get_key();
    if (match == std::strong_ordering::greater) {
      // key doesn't match both indexes, so insert at the current index
      ++index;
      return {STAGE, insert_size(key, value)};
    } else {
      assert(match == std::strong_ordering::equal);
      if constexpr (IS_BOTTOM) {
        // ceph_abort?
        ceph_abort("insert conflict at the previous index!");
      } else {
        // insert into the previous index
        auto nxt_container = iter.get_nxt_container();
        return NXT_STAGE_T::evaluate_insert(
            nxt_container, key, value, position.nxt, true);
      }
    }
  }

  template <typename T = bool>
  static std::enable_if_t<NODE_TYPE == node_type_t::LEAF, T>
  compensate_insert_position_at(match_stage_t stage, position_t& position) {
    auto& index = position.index;
    if (stage == STAGE) {
      assert(index == 0);
      // insert at the end of the current stage
      index = INDEX_END;
      return true;
    } else {
      if constexpr (IS_BOTTOM) {
        ceph_abort("impossible path");
      } else {
        assert(stage < STAGE);
        bool compensate = NXT_STAGE_T::
          compensate_insert_position_at(stage, position.nxt);
        if (compensate) {
          assert(is_valid_index(index));
          if (index == 0) {
            // insert into the *last* index of the current stage
            index = INDEX_LAST;
            return true;
          } else {
            --index;
            return false;
          }
        } else {
          return false;
        }
      }
    }
  }

  static void patch_insert_end(position_t& insert_pos, match_stage_t insert_stage) {
    assert(insert_stage <= STAGE);
    if (insert_stage == STAGE) {
      insert_pos.index = INDEX_END;
    } else if constexpr (!IS_BOTTOM) {
      insert_pos.index = INDEX_LAST;
      NXT_STAGE_T::patch_insert_end(insert_pos.nxt, insert_stage);
    }
  }

  template <typename T = std::tuple<match_stage_t, node_offset_t>>
  static std::enable_if_t<NODE_TYPE == node_type_t::LEAF, T> evaluate_insert(
      const key_hobj_t& key, const value_config_t& value,
      const MatchHistory& history, match_stat_t mstat, position_t& position) {
    match_stage_t insert_stage = STAGE_TOP;
    while (*history.get_by_stage(insert_stage) == MatchKindCMP::EQ) {
      assert(insert_stage != STAGE_BOTTOM && "insert conflict!");
      --insert_stage;
    }

    if (history.is_GT()) {
      if (position.is_end()) {
        // no need to compensate insert position
        assert(insert_stage <= STAGE && "impossible insert stage");
      } else if (position == position_t::begin()) {
        // I must be short-circuited by staged::smallest_result()
        // in staged::lower_bound(), so we need to rely on mstat instead
        assert(mstat >= MSTAT_LT0 && mstat <= MSTAT_LT3);
        if (mstat == MSTAT_LT0) {
          insert_stage = STAGE_RIGHT;
        } else if (mstat == MSTAT_LT1) {
          insert_stage = STAGE_STRING;
        } else {
          insert_stage = STAGE_LEFT;
        }
        // XXX(multi-type): need to upgrade node type before inserting an
        // incompatible index at front.
        assert(insert_stage <= STAGE && "incompatible insert");
      } else {
        assert(insert_stage <= STAGE && "impossible insert stage");
        [[maybe_unused]] bool ret = compensate_insert_position_at(insert_stage, position);
        assert(!ret);
      }
    }

    if (position.is_end()) {
      patch_insert_end(position, insert_stage);
    }

    node_offset_t insert_size = insert_size_at(insert_stage, key, value);

    return {insert_stage, insert_size};
  }

  template <KeyT KT>
  static const value_t* insert_new(
      NodeExtentMutable& mut, const memory_range_t& range,
      const full_key_t<KT>& key, const value_input_t& value) {
    char* p_insert = const_cast<char*>(range.p_end);
    const value_t* p_value = nullptr;
    StagedAppender<KT> appender;
    appender.init_empty(&mut, p_insert);
    appender.append(key, value, p_value);
    [[maybe_unused]] const char* p_insert_front = appender.wrap();
    assert(p_insert_front == range.p_start);
    return p_value;
  }

  template <KeyT KT, bool SPLIT>
  static const value_t* proceed_insert_recursively(
      NodeExtentMutable& mut, const container_t& container,
      const full_key_t<KT>& key, const value_input_t& value,
      position_t& position, match_stage_t& stage,
      node_offset_t& _insert_size, const char* p_left_bound) {
    // proceed insert from right to left
    assert(stage <= STAGE);
    auto iter = iterator_t(container);
    auto& index = position.index;

    bool do_insert = false;
    if (stage == STAGE) {
      if (index == INDEX_END) {
        iter.seek_last();
        iter.set_end();
        index = iter.index();
      } else {
        assert(is_valid_index(index));
        iter.seek_till_end(index);
      }
      do_insert = true;
    } else { // stage < STAGE
      if (index == INDEX_LAST) {
        iter.seek_last();
        index = iter.index();
      } else {
        assert(is_valid_index(index));
        iter.seek_till_end(index);
      }
      if constexpr (SPLIT) {
        if (iter.is_end()) {
          // insert at the higher stage due to split
          do_insert = true;
          _insert_size = insert_size(key, value);
          stage = STAGE;
        }
      } else {
        assert(!iter.is_end());
      }
    }

    if (do_insert) {
      if constexpr (!IS_BOTTOM) {
        position.nxt = position_t::nxt_t::begin();
      }
      assert(_insert_size == insert_size(key, value));
      if constexpr (IS_BOTTOM) {
        return iter.insert(
            mut, key, value, _insert_size, p_left_bound);
      } else {
        auto range = iter.insert_prefix(
            mut, key, _insert_size, p_left_bound);
        return NXT_STAGE_T::template insert_new<KT>(mut, range, key, value);
      }
    } else {
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        auto p_value = NXT_STAGE_T::template proceed_insert_recursively<KT, SPLIT>(
            mut, nxt_container, key, value,
            position.nxt, stage, _insert_size, p_left_bound);
        iter.update_size(mut, _insert_size);
        return p_value;
      } else {
        ceph_abort("impossible path");
      }
    }
  }

  template <KeyT KT, bool SPLIT>
  static const value_t* proceed_insert(
      NodeExtentMutable& mut, const container_t& container,
      const full_key_t<KT>& key, const value_input_t& value,
      position_t& position, match_stage_t& stage, node_offset_t& _insert_size) {
    auto p_left_bound = container.p_left_bound();
    if (unlikely(!container.keys())) {
      if (position.is_end()) {
        position = position_t::begin();
        assert(stage == STAGE);
        assert(_insert_size == insert_size(key, value));
      } else if (position == position_t::begin()) {
        // when insert into a trimmed and empty left node
        stage = STAGE;
        _insert_size = insert_size(key, value);
      } else {
        ceph_abort("impossible path");
      }
      if constexpr (IS_BOTTOM) {
        return container_t::insert_at(
            mut, container, key, value, 0, _insert_size, p_left_bound);
      } else {
        auto range = container_t::template insert_prefix_at(
            mut, container, key, 0, _insert_size, p_left_bound);
        return NXT_STAGE_T::template insert_new<KT>(mut, range, key, value);
      }
    } else {
      return proceed_insert_recursively<KT, SPLIT>(
          mut, container, key, value,
          position, stage, _insert_size, p_left_bound);
    }
  }

  static std::ostream& dump(const container_t& container,
                            std::ostream& os,
                            const std::string& prefix,
                            size_t& size,
                            const char* p_start) {
    auto iter = iterator_t(container);
    assert(!iter.is_end());
    std::string prefix_blank(prefix.size(), ' ');
    const std::string* p_prefix = &prefix;
    size += iterator_t::header_size();
    do {
      std::ostringstream sos;
      sos << *p_prefix << iter.get_key() << ": ";
      std::string i_prefix = sos.str();
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        size += iter.size_to_nxt();
        NXT_STAGE_T::dump(nxt_container, os, i_prefix, size, p_start);
      } else {
        auto value_ptr = iter.get_p_value();
        int offset = reinterpret_cast<const char*>(value_ptr) - p_start;
        size += iter.size();
        os << "\n" << i_prefix;
        if constexpr (NODE_TYPE == node_type_t::LEAF) {
          os << *value_ptr;
        } else {
          os << "0x" << std::hex << value_ptr->value << std::dec;
        }
        os << " " << size << "B"
           << "  @" << offset << "B";
      }
      if (iter.is_last()) {
        break;
      } else {
        ++iter;
        p_prefix = &prefix_blank;
      }
    } while (true);
    return os;
  }

  static void validate(const container_t& container) {
    auto iter = iterator_t(container);
    assert(!iter.is_end());
    auto key = iter.get_key();
    do {
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        NXT_STAGE_T::validate(nxt_container);
      }
      if (iter.is_last()) {
        break;
      } else {
        ++iter;
        assert(key < iter.get_key());
        key = iter.get_key();
      }
    } while (true);
  }

  static void get_stats(const container_t& container, node_stats_t& stats,
                        key_view_t& index_key) {
    auto iter = iterator_t(container);
    assert(!iter.is_end());
    stats.size_overhead += iterator_t::header_size();
    do {
      index_key.replace(iter.get_key());
      stats.size_overhead += iter.size_overhead();
      if constexpr (!IS_BOTTOM) {
        auto nxt_container = iter.get_nxt_container();
        NXT_STAGE_T::get_stats(nxt_container, stats, index_key);
      } else {
        ++stats.num_kvs;
        size_t kv_logical_size = index_key.size_logical();
        size_t value_size;
        if constexpr (NODE_TYPE == node_type_t::LEAF) {
          value_size = iter.get_p_value()->allocation_size();
        } else {
          value_size = sizeof(value_t);
        }
        stats.size_value += value_size;
        kv_logical_size += value_size;
        stats.size_logical += kv_logical_size;
      }
      if (iter.is_last()) {
        break;
      } else {
        ++iter;
      }
    } while (true);
  }

  template <bool GET_KEY, bool GET_VAL>
  static bool get_next_slot(
      const container_t& container,         // IN
      position_t& pos,                      // IN&OUT
      key_view_t* p_index_key,              // OUT
      const value_t** pp_value) {           // OUT
    auto iter = iterator_t(container);
    assert(!iter.is_end());
    iter.seek_at(pos.index);
    bool find_next;
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      find_next = NXT_STAGE_T::template get_next_slot<GET_KEY, GET_VAL>(
          nxt_container, pos.nxt, p_index_key, pp_value);
    } else {
      find_next = true;
    }

    if (find_next) {
      if (iter.is_last()) {
        return true;
      } else {
        pos.index = iter.index() + 1;
        if constexpr (!IS_BOTTOM) {
          pos.nxt = NXT_STAGE_T::position_t::begin();
        }
        get_slot<GET_KEY, GET_VAL>(
            container, pos, p_index_key, pp_value);
        return false;
      }
    } else { // !find_next && !IS_BOTTOM
      if constexpr (GET_KEY) {
        assert(p_index_key);
        p_index_key->set(iter.get_key());
      } else {
        assert(!p_index_key);
      }
      return false;
    }
  }

  template <bool GET_KEY, bool GET_VAL>
  static void get_prev_slot(
      const container_t& container,         // IN
      position_t& pos,                      // IN&OUT
      key_view_t* p_index_key,              // OUT
      const value_t** pp_value) {           // OUT
    assert(pos != position_t::begin());
    assert(!pos.is_end());
    auto& index = pos.index;
    auto iter = iterator_t(container);
    if constexpr (!IS_BOTTOM) {
      auto& nxt_pos = pos.nxt;
      if (nxt_pos == NXT_STAGE_T::position_t::begin()) {
        assert(index);
        --index;
        iter.seek_at(index);
        auto nxt_container = iter.get_nxt_container();
        NXT_STAGE_T::template get_largest_slot<true, GET_KEY, GET_VAL>(
            nxt_container, &nxt_pos, p_index_key, pp_value);
      } else {
        iter.seek_at(index);
        auto nxt_container = iter.get_nxt_container();
        NXT_STAGE_T::template get_prev_slot<GET_KEY, GET_VAL>(
            nxt_container, nxt_pos, p_index_key, pp_value);
      }
    } else {
      assert(index);
      --index;
      iter.seek_at(index);
      if constexpr (GET_VAL) {
        assert(pp_value);
        *pp_value = iter.get_p_value();
      } else {
        assert(!pp_value);
      }
    }
    if constexpr (GET_KEY) {
      p_index_key->set(iter.get_key());
    } else {
      assert(!p_index_key);
    }
  }

  struct _BaseEmpty {};
  class _BaseWithNxtIterator {
   protected:
    typename NXT_STAGE_T::StagedIterator _nxt;
  };
  class StagedIterator
      : std::conditional_t<IS_BOTTOM, _BaseEmpty, _BaseWithNxtIterator> {
   public:
    StagedIterator() = default;
    bool valid() const { return iter.has_value(); }
    index_t index() const {
      return iter->index();
    }
    bool is_end() const { return iter->is_end(); }
    bool in_progress() const {
      assert(valid());
      assert(!is_end());
      if constexpr (!IS_BOTTOM) {
        if (this->_nxt.valid()) {
          if (this->_nxt.index() == 0) {
            return this->_nxt.in_progress();
          } else {
            return true;
          }
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    key_get_type get_key() const { return iter->get_key(); }

    iterator_t& get() { return *iter; }
    void set(const container_t& container) {
      assert(!valid());
      iter = iterator_t(container);
    }
    void set_end() { iter->set_end(); }
    typename NXT_STAGE_T::StagedIterator& nxt() {
      if constexpr (!IS_BOTTOM) {
        if (!this->_nxt.valid()) {
          auto nxt_container = iter->get_nxt_container();
          this->_nxt.set(nxt_container);
        }
        return this->_nxt;
      } else {
        ceph_abort("impossible path");
      }
    }
    typename NXT_STAGE_T::StagedIterator& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        return this->_nxt;
      } else {
        ceph_abort("impossible path");
      }
    }
    StagedIterator& operator++() {
      if (iter->is_last()) {
        iter->set_end();
      } else {
        ++(*iter);
      }
      if constexpr (!IS_BOTTOM) {
        this->_nxt.reset();
      }
      return *this;
    }
    void reset() {
      if (valid()) {
        iter.reset();
        if constexpr (!IS_BOTTOM) {
          this->_nxt.reset();
        }
      }
    }

    template<typename OutputIt>
    auto do_format_to(OutputIt out, bool is_top) const {
      if (valid()) {
        if (iter->is_end()) {
          return fmt::format_to(out, "END");
        } else {
          out = fmt::format_to(out, "{}", index());
        }
      } else {
        if (is_top) {
          return fmt::format_to(out, "invalid StagedIterator!");
        } else {
          out = fmt::format_to(out, "0!");
        }
      }
      if constexpr (!IS_BOTTOM) {
        out = fmt::format_to(out, ", ");
        return this->_nxt.do_format_to(out, false);
      } else {
        return out;
      }
    }

    position_t get_pos() const {
      if (valid()) {
        if constexpr (IS_BOTTOM) {
          return position_t{index()};
        } else {
          return position_t{index(), this->_nxt.get_pos()};
        }
      } else {
        return position_t::begin();
      }
    }
    void encode(const char* p_node_start, ceph::bufferlist& encoded) const {
      uint8_t present = static_cast<bool>(iter);
      ceph::encode(present, encoded);
      if (iter.has_value()) {
        iter->encode(p_node_start, encoded);
        if constexpr (!IS_BOTTOM) {
          this->_nxt.encode(p_node_start, encoded);
        }
      }
    }
    static StagedIterator decode(const char* p_node_start,
                                 extent_len_t node_size,
                                 ceph::bufferlist::const_iterator& delta) {
      StagedIterator ret;
      uint8_t present;
      ceph::decode(present, delta);
      if (present) {
        ret.iter = iterator_t::decode(
            p_node_start, node_size, delta);
        if constexpr (!IS_BOTTOM) {
          ret._nxt = NXT_STAGE_T::StagedIterator::decode(
              p_node_start, node_size, delta);
        }
      }
      return ret;
    }
   private:
    std::optional<iterator_t> iter;
  };

  static bool recursively_locate_split(
      size_t& current_size, size_t extra_size,
      size_t target_size, StagedIterator& split_at) {
    assert(current_size <= target_size);
    iterator_t& split_iter = split_at.get();
    current_size = split_iter.seek_split(current_size, extra_size, target_size);
    assert(current_size <= target_size);
    assert(!split_iter.is_end());
    if (split_iter.index() == 0) {
      extra_size += iterator_t::header_size();
    } else {
      extra_size = 0;
    }
    bool locate_nxt;
    if constexpr (!IS_BOTTOM) {
      locate_nxt = NXT_STAGE_T::recursively_locate_split(
          current_size, extra_size + split_iter.size_to_nxt(),
          target_size, split_at.nxt());
    } else { // IS_BOTTOM
      // located upper_bound, fair split strategy
      size_t nxt_size = split_iter.size() + extra_size;
      assert(current_size + nxt_size > target_size);
      if (current_size + nxt_size/2 < target_size) {
        // include next
        current_size += nxt_size;
        locate_nxt = true;
      } else {
        // exclude next
        locate_nxt = false;
      }
    }
    if (locate_nxt) {
      if (split_iter.is_last()) {
        return true;
      } else {
        ++split_at;
        return false;
      }
    } else {
      return false;
    }
  }

  static bool recursively_locate_split_inserted(
      size_t& current_size, size_t extra_size, size_t target_size,
      position_t& insert_pos, match_stage_t insert_stage, size_t insert_size,
      std::optional<bool>& is_insert_left, StagedIterator& split_at) {
    assert(current_size <= target_size);
    assert(!is_insert_left.has_value());
    iterator_t& split_iter = split_at.get();
    auto& insert_index = insert_pos.index;
    if (insert_stage == STAGE) {
      current_size = split_iter.template seek_split_inserted<true>(
          current_size, extra_size, target_size,
          insert_index, insert_size, is_insert_left);
      assert(is_insert_left.has_value());
      assert(current_size <= target_size);
      if (split_iter.index() == 0) {
        if (insert_index == 0) {
          if (*is_insert_left == false) {
            extra_size += iterator_t::header_size();
          } else {
            extra_size = 0;
          }
        } else {
          extra_size += iterator_t::header_size();
        }
      } else {
        extra_size = 0;
      }
      if (*is_insert_left == false && split_iter.index() == insert_index) {
        // split_iter can be end
        // found the lower-bound of target_size
        // ...[s_index-1] |!| (i_index) [s_index]...

        // located upper-bound, fair split strategy
        // look at the next slot (the insert item)
        size_t nxt_size = insert_size + extra_size;
        assert(current_size + nxt_size > target_size);
        if (current_size + nxt_size/2 < target_size) {
          // include next
          *is_insert_left = true;
          current_size += nxt_size;
          if (split_iter.is_end()) {
            // ...[s_index-1] (i_index) |!|
            return true;
          } else {
            return false;
          }
        } else {
          // exclude next
          return false;
        }
      } else {
        // Already considered insert effect in the current stage.
        // Look into the next stage to identify the target_size lower-bound w/o
        // insert effect.
        assert(!split_iter.is_end());
        bool locate_nxt;
        if constexpr (!IS_BOTTOM) {
          locate_nxt = NXT_STAGE_T::recursively_locate_split(
              current_size, extra_size + split_iter.size_to_nxt(),
              target_size, split_at.nxt());
        } else { // IS_BOTTOM
          // located upper-bound, fair split strategy
          // look at the next slot
          size_t nxt_size = split_iter.size() + extra_size;
          assert(current_size + nxt_size > target_size);
          if (current_size + nxt_size/2 < target_size) {
            // include next
            current_size += nxt_size;
            locate_nxt = true;
          } else {
            // exclude next
            locate_nxt = false;
          }
        }
        if (locate_nxt) {
          if (split_iter.is_last()) {
            auto end_index = split_iter.index() + 1;
            if (insert_index == INDEX_END) {
              insert_index = end_index;
            }
            assert(insert_index <= end_index);
            if (insert_index == end_index) {
              assert(*is_insert_left == false);
              split_iter.set_end();
              // ...[s_index-1] |!| (i_index)
              return false;
            } else {
              assert(*is_insert_left == true);
              return true;
            }
          } else {
            ++split_at;
            return false;
          }
        } else {
          return false;
        }
      }
    } else {
      if constexpr (!IS_BOTTOM) {
        assert(insert_stage < STAGE);
        current_size = split_iter.template seek_split_inserted<false>(
            current_size, extra_size, target_size,
            insert_index, insert_size, is_insert_left);
        assert(!split_iter.is_end());
        assert(current_size <= target_size);
        if (split_iter.index() == 0) {
          extra_size += iterator_t::header_size();
        } else {
          extra_size = 0;
        }
        bool locate_nxt;
        if (!is_insert_left.has_value()) {
          // Considered insert effect in the current stage, and insert happens
          // in the lower stage.
          // Look into the next stage to identify the target_size lower-bound w/
          // insert effect.
          assert(split_iter.index() == insert_index);
          locate_nxt = NXT_STAGE_T::recursively_locate_split_inserted(
              current_size, extra_size + split_iter.size_to_nxt(), target_size,
              insert_pos.nxt, insert_stage, insert_size,
              is_insert_left, split_at.nxt());
          assert(is_insert_left.has_value());
#ifndef NDEBUG
          if (locate_nxt) {
            assert(*is_insert_left == true);
          }
#endif
        } else {
          // is_insert_left.has_value() == true
          // Insert will *not* happen in the lower stage.
          // Need to look into the next stage to identify the target_size
          // lower-bound w/ insert effect
          assert(split_iter.index() != insert_index);
          locate_nxt = NXT_STAGE_T::recursively_locate_split(
              current_size, extra_size + split_iter.size_to_nxt(),
              target_size, split_at.nxt());
#ifndef NDEBUG
          if (split_iter.index() < insert_index) {
            assert(*is_insert_left == false);
          } else {
            assert(*is_insert_left == true);
          }
#endif
        }
        if (locate_nxt) {
          if (split_iter.is_last()) {
            return true;
          } else {
            ++split_at;
            return false;
          }
        } else {
          return false;
        }
      } else {
        ceph_abort("impossible path");
        return false;;
      }
    }
  }

  /*
   * container appender type system
   *   container_t::Appender(NodeExtentMutable& mut, char* p_append)
   *   append(const container_t& src, index_t from, index_t items)
   *   wrap() -> char*
   * IF !IS_BOTTOM:
   *   open_nxt(const key_get_type&)
   *   open_nxt(const full_key_t&)
   *       -> std::tuple<NodeExtentMutable&, char*>
   *   wrap_nxt(char* p_append)
   * ELSE
   *   append(const full_key_t& key, const value_input_t& value)
   */
  template <KeyT KT>
  struct _BaseWithNxtAppender {
    typename NXT_STAGE_T::template StagedAppender<KT> _nxt;
  };
  template <KeyT KT>
  class StagedAppender
      : std::conditional_t<IS_BOTTOM, _BaseEmpty, _BaseWithNxtAppender<KT>> {
   public:
    StagedAppender() = default;
    ~StagedAppender() {
      assert(!require_wrap_nxt);
      assert(!valid());
    }
    bool valid() const { return appender.has_value(); }
    index_t index() const {
      assert(valid());
      return _index;
    }
    bool in_progress() const { return require_wrap_nxt; }
    // TODO: pass by reference
    void init_empty(NodeExtentMutable* p_mut, char* p_start) {
      assert(!valid());
      appender = typename container_t::template Appender<KT>(p_mut, p_start);
      _index = 0;
    }
    void init_tail(NodeExtentMutable* p_mut,
                   const container_t& container,
                   match_stage_t stage) {
      assert(!valid());
      auto iter = iterator_t(container);
      iter.seek_last();
      if (stage == STAGE) {
        appender = iter.template get_appender<KT>(p_mut);
        _index = iter.index() + 1;
        if constexpr (!IS_BOTTOM) {
          assert(!this->_nxt.valid());
        }
      } else {
        assert(stage < STAGE);
        if constexpr (!IS_BOTTOM) {
          appender = iter.template get_appender_opened<KT>(p_mut);
          _index = iter.index();
          require_wrap_nxt = true;
          auto nxt_container = iter.get_nxt_container();
          this->_nxt.init_tail(p_mut, nxt_container, stage);
        } else {
          ceph_abort("impossible path");
        }
      }
    }
    // possible to make src_iter end if to_index == INDEX_END
    void append_until(StagedIterator& src_iter, index_t& to_index) {
      assert(!require_wrap_nxt);
      auto s_index = src_iter.index();
      src_iter.get().template copy_out_until<KT>(*appender, to_index);
      assert(src_iter.index() == to_index);
      assert(to_index >= s_index);
      auto increment = (to_index - s_index);
      if (increment) {
        _index += increment;
        if constexpr (!IS_BOTTOM) {
          src_iter.get_nxt().reset();
        }
      }
    }
    void append(const full_key_t<KT>& key,
                const value_input_t& value, const value_t*& p_value) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        auto& nxt = open_nxt(key);
        nxt.append(key, value, p_value);
        wrap_nxt();
      } else {
        appender->append(key, value, p_value);
        ++_index;
      }
    }
    char* wrap() {
      assert(valid());
      assert(_index > 0);
      if constexpr (!IS_BOTTOM) {
        if (require_wrap_nxt) {
          wrap_nxt();
        }
      }
      auto ret = appender->wrap();
      appender.reset();
      return ret;
    }
    typename NXT_STAGE_T::template StagedAppender<KT>&
    open_nxt(key_get_type paritial_key) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        require_wrap_nxt = true;
        auto [p_mut, p_append] = appender->open_nxt(paritial_key);
        this->_nxt.init_empty(p_mut, p_append);
        return this->_nxt;
      } else {
        ceph_abort("impossible path");
      }
    }
    typename NXT_STAGE_T::template StagedAppender<KT>&
    open_nxt(const full_key_t<KT>& key) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        require_wrap_nxt = true;
        auto [p_mut, p_append] = appender->open_nxt(key);
        this->_nxt.init_empty(p_mut, p_append);
        return this->_nxt;
      } else {
        ceph_abort("impossible path");
      }
    }
    typename NXT_STAGE_T::template StagedAppender<KT>& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        assert(require_wrap_nxt);
        return this->_nxt;
      } else {
        ceph_abort("impossible path");
      }
    }
    void wrap_nxt() {
      if constexpr (!IS_BOTTOM) {
        assert(require_wrap_nxt);
        require_wrap_nxt = false;
        auto p_append = this->_nxt.wrap();
        appender->wrap_nxt(p_append);
        ++_index;
      } else {
        ceph_abort("impossible path");
      }
    }
   private:
    std::optional<typename container_t::template Appender<KT>> appender;
    index_t _index;
    bool require_wrap_nxt = false;
  };

  template <KeyT KT>
  static void _append_range(
      StagedIterator& src_iter, StagedAppender<KT>& appender, index_t& to_index) {
    if (src_iter.is_end()) {
      // append done
      assert(to_index == INDEX_END);
      to_index = src_iter.index();
    } else if constexpr (!IS_BOTTOM) {
      if (appender.in_progress()) {
        // appender has appended something at the current item,
        // cannot append the current item as-a-whole
        index_t to_index_nxt = INDEX_END;
        NXT_STAGE_T::template _append_range<KT>(
            src_iter.nxt(), appender.get_nxt(), to_index_nxt);
        ++src_iter;
        appender.wrap_nxt();
      } else if (src_iter.in_progress()) {
        // src_iter is not at the beginning of the current item,
        // cannot append the current item as-a-whole
        index_t to_index_nxt = INDEX_END;
        NXT_STAGE_T::template _append_range<KT>(
            src_iter.get_nxt(), appender.open_nxt(src_iter.get_key()), to_index_nxt);
        ++src_iter;
        appender.wrap_nxt();
      } else {
        // we can safely append the current item as-a-whole
      }
    }
    appender.append_until(src_iter, to_index);
  }

  template <KeyT KT>
  static void _append_into(StagedIterator& src_iter, StagedAppender<KT>& appender,
                           position_t& position, match_stage_t stage) {
    assert(position.index == src_iter.index());
    // reaches the last item
    if (stage == STAGE) {
      // done, end recursion
      if constexpr (!IS_BOTTOM) {
        position.nxt = position_t::nxt_t::begin();
      }
    } else {
      assert(stage < STAGE);
      // proceed append in the next stage
      NXT_STAGE_T::template append_until<KT>(
          src_iter.nxt(), appender.open_nxt(src_iter.get_key()),
          position.nxt, stage);
    }
  }

  template <KeyT KT>
  static void append_until(StagedIterator& src_iter, StagedAppender<KT>& appender,
                           position_t& position, match_stage_t stage) {
    index_t from_index = src_iter.index();
    index_t& to_index = position.index;
    assert(from_index <= to_index);
    if constexpr (IS_BOTTOM) {
      assert(stage == STAGE);
      appender.append_until(src_iter, to_index);
    } else {
      assert(stage <= STAGE);
      if (src_iter.index() == to_index) {
        _append_into<KT>(src_iter, appender, position, stage);
      } else {
        if (to_index == INDEX_END) {
          assert(stage == STAGE);
        } else if (to_index == INDEX_LAST) {
          assert(stage < STAGE);
        }
        _append_range<KT>(src_iter, appender, to_index);
        _append_into<KT>(src_iter, appender, position, stage);
      }
    }
    to_index -= from_index;
  }

  template <KeyT KT>
  static bool append_insert(
      const full_key_t<KT>& key, const value_input_t& value,
      StagedIterator& src_iter, StagedAppender<KT>& appender,
      bool is_front_insert, match_stage_t& stage, const value_t*& p_value) {
    assert(src_iter.valid());
    if (stage == STAGE) {
      appender.append(key, value, p_value);
      if (src_iter.is_end()) {
        return true;
      } else {
        return false;
      }
    } else {
      assert(stage < STAGE);
      if constexpr (!IS_BOTTOM) {
        auto nxt_is_end = NXT_STAGE_T::template append_insert<KT>(
            key, value, src_iter.get_nxt(), appender.get_nxt(),
            is_front_insert, stage, p_value);
        if (nxt_is_end) {
          appender.wrap_nxt();
          ++src_iter;
          if (is_front_insert) {
            stage = STAGE;
          }
          if (src_iter.is_end()) {
            return true;
          }
        }
        return false;
      } else {
        ceph_abort("impossible path");
      }
    }
  }

  /* TrimType:
   *   BEFORE: remove the entire container, normally means the according higher
   *           stage iterator needs to be trimmed as-a-whole.
   *   AFTER: retain the entire container, normally means the trim should be
   *          start from the next iterator at the higher stage.
   *   AT: trim happens in the current container, and the according higher
   *       stage iterator needs to be adjusted by the trimmed size.
   */
  static std::tuple<TrimType, node_offset_t>
  recursively_trim(NodeExtentMutable& mut, StagedIterator& trim_at) {
    if (!trim_at.valid()) {
      return {TrimType::BEFORE, 0u};
    }
    if (trim_at.is_end()) {
      return {TrimType::AFTER, 0u};
    }

    auto& iter = trim_at.get();
    if constexpr (!IS_BOTTOM) {
      auto [type, trimmed] = NXT_STAGE_T::recursively_trim(
          mut, trim_at.get_nxt());
      node_offset_t trim_size;
      if (type == TrimType::AFTER) {
        if (iter.is_last()) {
          return {TrimType::AFTER, 0u};
        }
        ++trim_at;
        trim_size = iter.trim_until(mut);
      } else if (type == TrimType::BEFORE) {
        if (iter.index() == 0) {
          return {TrimType::BEFORE, 0u};
        }
        trim_size = iter.trim_until(mut);
      } else {
        trim_size = iter.trim_at(mut, trimmed);
      }
      return {TrimType::AT, trim_size};
    } else {
      if (iter.index() == 0) {
        return {TrimType::BEFORE, 0u};
      } else {
        auto trimmed = iter.trim_until(mut);
        return {TrimType::AT, trimmed};
      }
    }
  }

  static void trim(NodeExtentMutable& mut, StagedIterator& trim_at) {
    auto [type, trimmed] = recursively_trim(mut, trim_at);
    if (type == TrimType::BEFORE) {
      assert(trim_at.valid());
      auto& iter = trim_at.get();
      iter.trim_until(mut);
    }
  }

  static std::optional<std::tuple<match_stage_t, node_offset_t, bool>>
  proceed_erase_recursively(
      NodeExtentMutable& mut,
      const container_t& container,     // IN
      const char* p_left_bound,         // IN
      position_t& pos) {                // IN&OUT
    auto iter = iterator_t(container);
    auto& index = pos.index;
    assert(is_valid_index(index));
    iter.seek_at(index);
    bool is_last = iter.is_last();

    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      auto ret = NXT_STAGE_T::proceed_erase_recursively(
          mut, nxt_container, p_left_bound, pos.nxt);
      if (ret.has_value()) {
        // erased at lower level
        auto [r_stage, r_erase_size, r_done] = *ret;
        assert(r_erase_size != 0);
        iter.update_size(mut, -r_erase_size);
        if (r_done) {
          // done, the next_pos is calculated
          return ret;
        } else {
          if (is_last) {
            // need to find the next pos at upper stage
            return ret;
          } else {
            // done, calculate the next pos
            ++index;
            pos.nxt = NXT_STAGE_T::position_t::begin();
            return {{r_stage, r_erase_size, true}};
          }
        }
      }
      // not erased at lower level
    }

    // not erased yet
    if (index == 0 && is_last) {
      // need to erase from the upper stage
      return std::nullopt;
    } else {
      auto erase_size = iter.erase(mut, p_left_bound);
      assert(erase_size != 0);
      if (is_last) {
        // need to find the next pos at upper stage
        return {{STAGE, erase_size, false}};
      } else {
        // done, calculate the next pos (should be correct already)
        if constexpr (!IS_BOTTOM) {
          assert(pos.nxt == NXT_STAGE_T::position_t::begin());
        }
        return {{STAGE, erase_size, true}};
      }
    }
  }

  static match_stage_t erase(
      NodeExtentMutable& mut,
      const container_t& node_stage,    // IN
      position_t& erase_pos) {          // IN&OUT
    auto p_left_bound = node_stage.p_left_bound();
    auto ret = proceed_erase_recursively(
        mut, node_stage, p_left_bound, erase_pos);
    if (ret.has_value()) {
      auto [r_stage, r_erase_size, r_done] = *ret;
      std::ignore = r_erase_size;
      if (r_done) {
        assert(!erase_pos.is_end());
        return r_stage;
      } else {
        // erased the last kv
        erase_pos = position_t::end();
        return r_stage;
      }
    } else {
      assert(node_stage.keys() == 1);
      node_stage.erase_at(mut, node_stage, 0, p_left_bound);
      erase_pos = position_t::end();
      return STAGE;
    }
  }

  static std::tuple<match_stage_t, node_offset_t> evaluate_merge(
      const key_view_t& left_pivot_index,
      const container_t& right_container) {
    auto r_iter = iterator_t(right_container);
    r_iter.seek_at(0);
    node_offset_t compensate = r_iter.header_size();
    auto cmp = left_pivot_index <=> r_iter.get_key();
    if (cmp == std::strong_ordering::equal) {
      if constexpr (!IS_BOTTOM) {
        // the index is equal, compensate and look at the lower stage
        compensate += r_iter.size_to_nxt();
        auto r_nxt_container = r_iter.get_nxt_container();
        auto [ret_stage, ret_compensate] = NXT_STAGE_T::evaluate_merge(
            left_pivot_index, r_nxt_container);
        compensate += ret_compensate;
        return {ret_stage, compensate};
      } else {
        ceph_abort("impossible path: left_pivot_key == right_first_key");
      }
    } else if (cmp == std::strong_ordering::less) {
      // ok, do merge here
      return {STAGE, compensate};
    } else {
      ceph_abort("impossible path: left_pivot_key < right_first_key");
    }
  }
};

/**
 * Configurations for struct staged
 *
 * staged_params_* assembles different container_t implementations (defined by
 * stated::_iterator_t) by STAGE, and constructs the final multi-stage
 * implementations for different node layouts defined by
 * node_extent_t<FieldType, NODE_TYPE>.
 *
 * The specialized implementations for different layouts are accessible through
 * the helper type node_to_stage_t<node_extent_t<FieldType, NODE_TYPE>>.
 *
 * Specifically, the settings of 8 layouts are:
 *
 * The layout (N0, LEAF/INTERNAL) has 3 stages:
 * - STAGE_LEFT:   node_extent_t<node_fields_0_t, LEAF/INTERNAL>
 * - STAGE_STRING: item_iterator_t<LEAF/INTERNAL>
 * - STAGE_RIGHT:  sub_items_t<LEAF/INTERNAL>
 *
 * The layout (N1, LEAF/INTERNAL) has 3 stages:
 * - STAGE_LEFT:   node_extent_t<node_fields_1_t, LEAF/INTERNAL>
 * - STAGE_STRING: item_iterator_t<LEAF/INTERNAL>
 * - STAGE_RIGHT:  sub_items_t<LEAF/INTERNAL>
 *
 * The layout (N2, LEAF/INTERNAL) has 2 stages:
 * - STAGE_STRING: node_extent_t<node_fields_2_t, LEAF/INTERNAL>
 * - STAGE_RIGHT:  sub_items_t<LEAF/INTERNAL>
 *
 * The layout (N3, LEAF) has 1 stage:
 * - STAGE_RIGHT:  node_extent_t<leaf_fields_3_t, LEAF>
 *
 * The layout (N3, INTERNAL) has 1 stage:
 * - STAGE_RIGHT:  node_extent_t<internal_fields_3_t, INTERNAL>
 */

template <node_type_t _NODE_TYPE>
struct staged_params_subitems {
  using container_t = sub_items_t<_NODE_TYPE>;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  static constexpr auto STAGE = STAGE_RIGHT;

  // dummy type in order to make our type system work
  // any better solution to get rid of this?
  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <node_type_t _NODE_TYPE>
struct staged_params_item_iterator {
  using container_t = item_iterator_t<_NODE_TYPE>;
  static constexpr auto NODE_TYPE = _NODE_TYPE;
  static constexpr auto STAGE = STAGE_STRING;

  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_01 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_LEFT;

  using next_param_t = staged_params_item_iterator<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_2 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_STRING;

  using next_param_t = staged_params_subitems<NODE_TYPE>;
};

template <typename NodeType>
struct staged_params_node_3 {
  using container_t = NodeType;
  static constexpr auto NODE_TYPE = NodeType::NODE_TYPE;
  static constexpr auto STAGE = STAGE_RIGHT;

  // dummy type in order to make our type system work
  // any better solution to get rid of this?
  using next_param_t = staged_params_node_3<NodeType>;
};

template <typename NodeType, typename Enable = void> struct _node_to_stage_t;
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N0 ||
                     NodeType::FIELD_TYPE == field_type_t::N1>> {
  using type = staged<staged_params_node_01<NodeType>>;
};
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N2>> {
  using type = staged<staged_params_node_2<NodeType>>;
};
template <typename NodeType>
struct _node_to_stage_t<NodeType,
    std::enable_if_t<NodeType::FIELD_TYPE == field_type_t::N3>> {
  using type = staged<staged_params_node_3<NodeType>>;
};
template <typename NodeType>
using node_to_stage_t = typename _node_to_stage_t<NodeType>::type;

}

template<typename T>
concept HasDoFormatTo = requires(T x, std::back_insert_iterator<fmt::memory_buffer> out) {
  { x.do_format_to(out, true) } -> std::same_as<decltype(out)>;
};
namespace fmt {
// placed in the fmt namespace due to an ADL bug in g++ < 12
// (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92944).
template <HasDoFormatTo T> struct formatter<T> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const T& staged_iterator, FormatContext& ctx) {
    return staged_iterator.do_format_to(ctx.out(), true);
  }
};
}
