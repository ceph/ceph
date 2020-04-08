// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <optional>
#include <ostream>
// TODO: remove
#include <iostream>
#include <sstream>
#include <type_traits>

#include "common/likely.h"

#include "sub_items_stage.h"
#include "item_iterator_stage.h"

namespace crimson::os::seastore::onode {

struct search_result_bs_t {
  size_t index;
  MatchKindBS match;
};
template <typename FGetKey>
search_result_bs_t binary_search(
    const full_key_t<KeyT::HOBJ>& key,
    size_t begin, size_t end, FGetKey&& f_get_key) {
  assert(begin <= end);
  while (begin < end) {
    auto total = begin + end;
    auto mid = total >> 1;
    // do not copy if return value is reference
    decltype(f_get_key(mid)) target = f_get_key(mid);
    auto match = compare_to<KeyT::HOBJ>(key, target);
    if (match == MatchKindCMP::NE) {
      end = mid;
    } else if (match == MatchKindCMP::PO) {
      begin = mid + 1;
    } else {
      return {mid, MatchKindBS::EQ};
    }
  }
  return {begin , MatchKindBS::NE};
}

template <typename PivotType, typename FGet>
search_result_bs_t binary_search_r(
    size_t rend, size_t rbegin, FGet&& f_get, const PivotType& key) {
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

using match_stat_t = int8_t;
constexpr match_stat_t MSTAT_END = -2; // index is search_position_t::end()
constexpr match_stat_t MSTAT_EQ  = -1; // key == index
constexpr match_stat_t MSTAT_NE0 =  0; // key == index [pool/shard crush ns/oid]; key < index [snap/gen]
constexpr match_stat_t MSTAT_NE1 =  1; // key == index [pool/shard crush]; key < index [ns/oid]
constexpr match_stat_t MSTAT_NE2 =  2; // key < index [pool/shard crush ns/oid] ||
                                       // key == index [pool/shard]; key < index [crush]
constexpr match_stat_t MSTAT_NE3 =  3; // key < index [pool/shard]
constexpr match_stat_t MSTAT_MIN = MSTAT_END;
constexpr match_stat_t MSTAT_MAX = MSTAT_NE3;

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
    const full_key_t<KeyT::HOBJ>& key,
    const full_key_t<KeyT::VIEW>& index,
    match_stat_t mstat) {
  assert(mstat >= MSTAT_MIN && mstat <= MSTAT_NE2);
  // key < index ...
  switch (mstat) {
   case MSTAT_EQ:
    break;
   case MSTAT_NE0:
    assert(compare_to<KeyT::HOBJ>(key, index.snap_gen_packed()) == MatchKindCMP::NE);
    break;
   case MSTAT_NE1:
    assert(compare_to<KeyT::HOBJ>(key, index.ns_oid_view()) == MatchKindCMP::NE);
    break;
   case MSTAT_NE2:
    if (index.has_shard_pool()) {
      assert(compare_to<KeyT::HOBJ>(key, shard_pool_crush_t{
               index.shard_pool_packed(), index.crush_packed()}) == MatchKindCMP::NE);
    } else {
      assert(compare_to<KeyT::HOBJ>(key, index.crush_packed()) == MatchKindCMP::NE);
    }
    break;
   default:
    assert(false);
  }
  // key == index ...
  switch (mstat) {
   case MSTAT_EQ:
    assert(compare_to<KeyT::HOBJ>(key, index.snap_gen_packed()) == MatchKindCMP::EQ);
   case MSTAT_NE0:
    if (!index.has_ns_oid())
      break;
    assert(index.ns_oid_view().type() == ns_oid_view_t::Type::MAX ||
           compare_to<KeyT::HOBJ>(key, index.ns_oid_view()) == MatchKindCMP::EQ);
   case MSTAT_NE1:
    if (!index.has_crush())
      break;
    assert(compare_to<KeyT::HOBJ>(key, index.crush_packed()) == MatchKindCMP::EQ);
    if (!index.has_shard_pool())
      break;
    assert(compare_to<KeyT::HOBJ>(key, index.shard_pool_packed()) == MatchKindCMP::EQ);
   default:
    break;
  }
}

template <node_type_t NODE_TYPE, match_stage_t STAGE>
struct staged_result_t {
  using me_t = staged_result_t<NODE_TYPE, STAGE>;
  bool is_end() const { return position.is_end(); }
  MatchKindBS match() const {
    assert(mstat >= MSTAT_MIN && mstat <= MSTAT_MAX);
    return (mstat == MSTAT_EQ ? MatchKindBS::EQ : MatchKindBS::NE);
  }

  static me_t end() {
    return {staged_position_t<STAGE>::end(), nullptr, MSTAT_END};
  }
  template <typename T = me_t>
  static std::enable_if_t<STAGE != STAGE_BOTTOM, T> from_nxt(
      size_t index, const staged_result_t<NODE_TYPE, STAGE - 1>& nxt_stage_result) {
    return {{index, nxt_stage_result.position},
            nxt_stage_result.p_value,
            nxt_stage_result.mstat};
  }

  staged_position_t<STAGE> position;
  const value_type_t<NODE_TYPE>* p_value;
  match_stat_t mstat;
};

template <node_type_t NODE_TYPE>
staged_result_t<NODE_TYPE, STAGE_TOP>&& normalize(
    staged_result_t<NODE_TYPE, STAGE_TOP>&& result) { return std::move(result); }

template <node_type_t NODE_TYPE, match_stage_t STAGE,
          typename = std::enable_if_t<STAGE != STAGE_TOP>>
staged_result_t<NODE_TYPE, STAGE_TOP> normalize(
    staged_result_t<NODE_TYPE, STAGE>&& result) {
  // FIXME: assert result.mstat correct
  return {normalize(std::move(result.position)), result.p_value, result.mstat};
}

/*
 * staged infrastructure
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

#define NXT_STAGE_T staged<next_param_t>

enum class TrimType { BEFORE, AFTER, AT };

template <typename Params>
struct staged {
  static_assert(Params::STAGE >= STAGE_BOTTOM);
  static_assert(Params::STAGE <= STAGE_TOP);
  using container_t = typename Params::container_t;
  using key_get_type = typename container_t::key_get_type;
  using next_param_t = typename Params::next_param_t;
  using position_t = staged_position_t<Params::STAGE>;
  using result_t = staged_result_t<Params::NODE_TYPE, Params::STAGE>;
  using value_t = value_type_t<Params::NODE_TYPE>;
  static constexpr auto CONTAINER_TYPE = container_t::CONTAINER_TYPE;
  static constexpr bool IS_BOTTOM = (Params::STAGE == STAGE_BOTTOM);
  static constexpr auto NODE_TYPE = Params::NODE_TYPE;
  static constexpr auto STAGE = Params::STAGE;

  template <bool is_exclusive>
  static void _left_or_right(size_t& s_index, size_t i_index,
                             std::optional<bool>& i_to_left) {
    assert(!i_to_left.has_value());
    assert(s_index != INDEX_END);
    if constexpr (is_exclusive) {
      if (s_index <= i_index) {
        // ...[s_index-1] |!| (i_index) [s_index]...
        // offset i_position to right
        i_to_left = false;
      } else {
        // ...[s_index-1] (i_index)) |?[s_index]| ...
        // ...(i_index)...[s_index-1] |?[s_index]| ...
        i_to_left = true;
        --s_index;
      }
    } else {
      if (s_index < i_index) {
        // ...[s_index-1] |?[s_index]| ...[(i_index)[s_index_k]...
        i_to_left = false;
      } else if (s_index > i_index) {
        // ...[(i_index)s_index-1] |?[s_index]| ...
        // ...[(i_index)s_index_k]...[s_index-1] |?[s_index]| ...
        i_to_left = true;
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
    *   keys() const -> size_t
    *   operator[](size_t) const -> key_get_type
    *   size_before(size_t) const -> size_t
    *   (IS_BOTTOM) get_p_value(size_t) const -> const value_t*
    *   (!IS_BOTTOM) size_to_nxt_at(size_t) const -> size_t
    *   (!IS_BOTTOM) get_nxt_container(size_t) const
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
    *
    * Appender::append(const container_t& src, from, items)
    */
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {
      assert(container.keys());
    }

    size_t index() const {
      return _index;
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container[_index];
    }
    size_t size_to_nxt() const {
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
    size_t size() const {
      assert(!is_end());
      assert(header_size() == container.size_before(0));
      return container.size_before(_index + 1) -
             container.size_before(_index);
    }

    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++_index;
      return *this;
    }
    void seek_at(size_t index) {
      assert(index < container.keys());
      seek_till_end(index);
    }
    void seek_till_end(size_t index) {
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
    MatchKindBS seek(const full_key_t<KeyT::HOBJ>& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      size_t end_index = container.keys();
      if (exclude_last) {
        assert(end_index);
        --end_index;
        assert(compare_to<KeyT::HOBJ>(key, container[end_index]) == MatchKindCMP::NE);
      }
      auto ret = binary_search(key, _index, end_index,
          [this] (size_t index) { return container[index]; });
      _index = ret.index;
      return ret.match;
    }

    template <KeyT KT, typename T = value_t>
    std::enable_if_t<IS_BOTTOM, const T*> insert(
        NodeExtentMutable& mut, const full_key_t<KT>& key,
        const value_t& value, node_offset_t insert_size, const char* p_left_bound) {
      return container_t::template insert_at<KT>(
          mut, container, key, value, _index, insert_size, p_left_bound);
    }

    template <KeyT KT, typename T = memory_range_t>
    std::enable_if_t<!IS_BOTTOM, T> insert_prefix(
        NodeExtentMutable& mut, const full_key_t<KT>& key,
        node_offset_t size, const char* p_left_bound) {
      return container_t::template insert_prefix_at<KT>(
          mut, container, key, _index, size, p_left_bound);
    }

    template <typename T = void>
    std::enable_if_t<!IS_BOTTOM, T>
    update_size(NodeExtentMutable& mut, node_offset_t insert_size) {
      assert(!is_end());
      container_t::update_size_at(mut, container, _index, insert_size);
    }

    // Note: possible to return an end iterator when is_exclusive is true
    template <bool is_exclusive>
    size_t seek_split_inserted(size_t start_size, size_t extra_size,
                               size_t target_size, size_t& i_index, size_t i_size,
                               std::optional<bool>& i_to_left) {
      assert(!is_end());
      assert(index() == 0);
      assert(i_index <= container.keys() || i_index == INDEX_END);
      // replace the unknown INDEX_END value
      if (i_index == INDEX_END) {
        if constexpr (!is_exclusive) {
          i_index = container.keys() - 1;
        } else {
          i_index = container.keys();
        }
      }
      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1,
                              i_index, i_size] (size_t index) {
        size_t current_size;
        if (unlikely(index == 0)) {
          current_size = start_size;
        } else {
          current_size = start_size_1;
          if (index > i_index) {
            current_size += i_size;
            if constexpr (is_exclusive) {
              --index;
            }
          }
          // already includes header size
          current_size += container.size_before(index);
        }
        return current_size;
      };
      size_t s_end;
      if constexpr (is_exclusive) {
        s_end = container.keys();
      } else {
        s_end = container.keys() - 1;
      }
      _index = binary_search_r(0, s_end, f_get_used_size, target_size).index;
      size_t current_size = f_get_used_size(_index);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(_index, i_index, i_to_left);
      return current_size;
    }

    size_t seek_split(size_t start_size, size_t extra_size, size_t target_size) {
      assert(!is_end());
      assert(index() == 0);
      auto start_size_1 = start_size + extra_size;
      auto f_get_used_size = [this, start_size, start_size_1] (size_t index) {
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

    // Note: possible to return an end iterater if
    // to_index == INDEX_END && to_stage == STAGE
    template <KeyT KT>
    void copy_out_until(typename container_t::template Appender<KT>& appender,
                        size_t& to_index,
                        match_stage_t to_stage) {
      assert(to_stage <= STAGE);
      auto num_keys = container.keys();
      size_t items;
      if (to_index == INDEX_END) {
        if (to_stage == STAGE) {
          items = num_keys - _index;
          appender.append(container, _index, items);
          _index = num_keys;
        } else {
          assert(!is_end());
          items = num_keys - 1 - _index;
          appender.append(container, _index, items);
          _index = num_keys - 1;
        }
        to_index = _index;
      } else {
        assert(_index <= to_index);
        items = to_index - _index;
        appender.append(container, _index, items);
        _index = to_index;
      }
    }

    size_t trim_until(NodeExtentMutable& mut) {
      return container_t::trim_until(mut, container, _index);
    }

    template <typename T = size_t>
    std::enable_if_t<!IS_BOTTOM, T>
    trim_at(NodeExtentMutable& mut, size_t trimmed) {
      return container_t::trim_at(mut, container, _index, trimmed);
    }

    static node_offset_t header_size() {
      return container_t::header_size();
    }

    template <KeyT KT>
    static size_t estimate_insert(const full_key_t<KT>& key, const value_t& value) {
      return container_t::template estimate_insert<KT>(key, value);
    }

   private:
    container_t container;
    size_t _index = 0;
  };

  template <ContainerType CTYPE>
  class _iterator_t<CTYPE, std::enable_if_t<CTYPE == ContainerType::ITERATIVE>> {
    /*
     * iterative container type system (!IS_BOTTOM):
     *   CONTAINER_TYPE = ContainerType::ITERATIVE
     *   index() const -> size_t
     *   get_key() const -> key_get_type
     *   size() const -> size_t
     *   size_to_nxt() const -> size_t
     *   get_nxt_container() const
     *   has_next() const -> bool
     *   operator++()
     * static:
     *   header_size() -> node_offset_t
     *   estimate_insert(key, value) -> node_offset_t
     *   insert_prefix(mut, src, key, is_end, size, p_left_bound) -> memory_range_t
     *   update_size(mut, src, size)
     *   trim_until(mut, container) -> trim_size
     *   trim_at(mut, container, trimmed) -> trim_size
     */
    // currently the iterative iterator is only implemented with STAGE_STRING
    // for in-node space efficiency
    static_assert(STAGE == STAGE_STRING);
   public:
    using me_t = _iterator_t<CTYPE>;

    _iterator_t(const container_t& container) : container{container} {
      assert(index() == 0);
    }

    size_t index() const {
      if (is_end()) {
        return end_index;
      } else {
        return container.index();
      }
    }
    key_get_type get_key() const {
      assert(!is_end());
      return container.get_key();
    }
    size_t size_to_nxt() const {
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
    bool is_end() const { return _is_end; }
    size_t size() const {
      assert(!is_end());
      return container.size();
    }

    me_t& operator++() {
      assert(!is_end());
      assert(!is_last());
      ++container;
      return *this;
    }
    void seek_at(size_t index) {
      assert(!is_end());
      assert(this->index() == 0);
      while (index > 0) {
        assert(container.has_next());
        ++container;
        --index;
      }
    }
    void seek_till_end(size_t index) {
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
      end_index = container.index() + 1;
    }
    // Note: possible to return an end iterator
    MatchKindBS seek(const full_key_t<KeyT::HOBJ>& key, bool exclude_last) {
      assert(!is_end());
      assert(index() == 0);
      do {
        if (exclude_last && is_last()) {
          assert(compare_to<KeyT::HOBJ>(key, get_key()) == MatchKindCMP::NE);
          return MatchKindBS::NE;
        }
        auto match = compare_to<KeyT::HOBJ>(key, get_key());
        if (match == MatchKindCMP::NE) {
          return MatchKindBS::NE;
        } else if (match == MatchKindCMP::EQ) {
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

    template <KeyT KT>
    memory_range_t insert_prefix(
        NodeExtentMutable& mut, const full_key_t<KT>& key,
        node_offset_t size, const char* p_left_bound) {
      return container_t::template insert_prefix<KT>(
          mut, container, key, is_end(), size, p_left_bound);
    }

    void update_size(NodeExtentMutable& mut, node_offset_t insert_size) {
      assert(!is_end());
      container_t::update_size(mut, container, insert_size);
    }

    // Note: possible to return an end iterator when is_exclusive is true
    template <bool is_exclusive>
    size_t seek_split_inserted(size_t start_size, size_t extra_size,
                               size_t target_size, size_t& i_index, size_t i_size,
                               std::optional<bool>& i_to_left) {
      assert(!is_end());
      assert(index() == 0);
      size_t current_size = start_size;
      size_t s_index = 0;
      extra_size += header_size();
      do {
        if constexpr (!is_exclusive) {
          if (is_last()) {
            assert(s_index == index());
            if (i_index == INDEX_END) {
              i_index = index();
            }
            assert(i_index <= index());
            break;
          }
        }

        size_t nxt_size = current_size;
        if (s_index == 0) {
          nxt_size += extra_size;
        }
        if (s_index == i_index) {
          nxt_size += i_size;
          if constexpr (is_exclusive) {
            if (nxt_size > target_size) {
              break;
            }
            current_size = nxt_size;
            ++s_index;
          }
        }
        nxt_size += size();
        if (nxt_size > target_size) {
          break;
        }
        current_size = nxt_size;

        if constexpr (is_exclusive) {
          if (is_last()) {
            assert(s_index == index());
            set_end();
            s_index = index();
            if (i_index == INDEX_END) {
              i_index = index();
            }
            assert(i_index == index());
            break;
          } else {
            ++(*this);
            ++s_index;
          }
        } else {
          ++(*this);
          ++s_index;
        }
      } while (true);
      assert(current_size <= target_size);

      _left_or_right<is_exclusive>(s_index, i_index, i_to_left);
      assert(s_index == index());
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

    // Note: possible to return an end iterater if
    // to_index == INDEX_END && to_stage == STAGE
    template <KeyT KT>
    void copy_out_until(typename container_t::template Appender<KT>& appender,
                        size_t& to_index,
                        match_stage_t to_stage) {
      assert(to_stage <= STAGE);
      if (is_end()) {
        assert(!container.has_next());
        assert(to_stage == STAGE);
        assert(to_index == index() || to_index == INDEX_END);
        to_index = index();
        return;
      }
      typename container_t::index_t type;
      size_t items;
      if (to_index == INDEX_END) {
        if (to_stage == STAGE) {
          type = container_t::index_t::end;
        } else {
          type = container_t::index_t::last;
        }
        items = INDEX_END;
      } else {
        assert(index() <= to_index);
        type = container_t::index_t::none;
        items = to_index - index();
      }
      if (appender.append(container, items, type)) {
        set_end();
      }
      to_index = index();
    }

    size_t trim_until(NodeExtentMutable& mut) {
      if (is_end()) {
        return 0;
      }
      return container_t::trim_until(mut, container);
    }

    size_t trim_at(NodeExtentMutable& mut, size_t trimmed) {
      assert(!is_end());
      return container_t::trim_at(mut, container, trimmed);
    }

    static node_offset_t header_size() {
      return container_t::header_size();
    }

    template <KeyT KT>
    static node_offset_t estimate_insert(const full_key_t<KT>& key, const value_t& value) {
      return container_t::template estimate_insert<KT>(key, value);
    }

   private:
    container_t container;
    bool _is_end = false;
    size_t end_index;
  };

  /*
   * iterator_t encapsulates both indexable and iterative implementations
   * from a *non-empty* container.
   * cstr(const container_t&)
   * access:
   *   index() -> size_t
   *   get_key() -> key_get_type (const reference or value type)
   *   is_last() -> bool
   *   is_end() -> bool
   *   size() -> size_t
   *   (IS_BOTTOM) get_p_value() -> const value_t*
   *   (!IS_BOTTOM) get_nxt_container() -> nxt_stage::container_t
   *   (!IS_BOTTOM) size_to_nxt() -> size_t
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
   * split;
   *   seek_split_inserted<bool is_exclusive>(
   *       start_size, extra_size, target_size, i_index, i_size,
   *       std::optional<bool>& i_to_left)
   *           -> insert to left/right/unknown (!exclusive)
   *           -> insert to left/right         (exclusive, can be end)
   *     -> split_size
   *   seek_split(start_size, extra_size, target_size) -> split_size
   *   copy_out_until(appender, to_index, to_stage) (can be end)
   *   trim_until(mut) -> trim_size
   *   (!IS_BOTTOM) trim_at(mut, trimmed) -> trim_size
   * static:
   *   header_size() -> node_offset_t
   *   estimate_insert(key, value) -> node_offset_t
   */
  using iterator_t = _iterator_t<CONTAINER_TYPE>;

  /*
   * Lookup internals (hide?)
   */

  static result_t smallest_result(const iterator_t& iter) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto pos_smallest = NXT_STAGE_T::position_t::begin();
    auto nxt_container = iter.get_nxt_container();
    auto value_ptr = NXT_STAGE_T::get_p_value(nxt_container, pos_smallest);
    return result_t{{iter.index(), pos_smallest}, value_ptr, STAGE};
  }

  static result_t nxt_lower_bound(
      const full_key_t<KeyT::HOBJ>& key, iterator_t& iter, MatchHistory& history) {
    static_assert(!IS_BOTTOM);
    assert(!iter.is_end());
    auto nxt_container = iter.get_nxt_container();
    auto nxt_result = NXT_STAGE_T::lower_bound(nxt_container, key, history);
    if (nxt_result.is_end()) {
      if (iter.is_last()) {
        return result_t::end();
      } else {
        return smallest_result(++iter);
      }
    } else {
      return result_t::from_nxt(iter.index(), nxt_result);
    }
  }

  static void lookup_largest(
      const container_t& container, position_t& position, const value_t*& p_value) {
    auto iter = iterator_t(container);
    iter.seek_last();
    position.index = iter.index();
    if constexpr (IS_BOTTOM) {
      p_value = iter.get_p_value();
    } else {
      auto nxt_container = iter.get_nxt_container();
      NXT_STAGE_T::lookup_largest(nxt_container, position.nxt, p_value);
    }
  }

  static void lookup_largest_index(
      const container_t& container, full_key_t<KeyT::VIEW>& output) {
    auto iter = iterator_t(container);
    iter.seek_last();
    output.set(iter.get_key());
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      NXT_STAGE_T::lookup_largest_index(nxt_container, output);
    }
  }

  static const value_t* get_p_value(
      const container_t& container, const position_t& position) {
    auto iter = iterator_t(container);
    iter.seek_at(position.index);
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      return NXT_STAGE_T::get_p_value(nxt_container, position.nxt);
    } else {
      return iter.get_p_value();
    }
  }

  static void get_key_view(
      const container_t& container,
      const position_t& position,
      full_key_t<KeyT::VIEW>& output) {
    auto iter = iterator_t(container);
    iter.seek_at(position.index);
    output.set(iter.get_key());
    if constexpr (!IS_BOTTOM) {
      auto nxt_container = iter.get_nxt_container();
      return NXT_STAGE_T::get_key_view(nxt_container, position.nxt, output);
    }
  }

  static result_t lower_bound(
      const container_t& container,
      const full_key_t<KeyT::HOBJ>& key,
      MatchHistory& history) {
    bool exclude_last = false;
    if (history.get<STAGE>().has_value()) {
      if (*history.get<STAGE>() == MatchKindCMP::EQ) {
        // lookup is short-circuited
        if constexpr (!IS_BOTTOM) {
          assert(history.get<STAGE - 1>().has_value());
          if (history.is_PO<STAGE - 1>()) {
            auto iter = iterator_t(container);
            bool test_key_equal;
            if constexpr (STAGE == STAGE_STRING) {
              // TODO(cross-node string dedup)
              // test_key_equal = (iter.get_key().type() == ns_oid_view_t::Type::MIN);
              auto cmp = compare_to<KeyT::HOBJ>(key, iter.get_key());
              assert(cmp != MatchKindCMP::PO);
              test_key_equal = (cmp == MatchKindCMP::EQ);
            } else {
              auto cmp = compare_to<KeyT::HOBJ>(key, iter.get_key());
              // From history, key[stage] == parent[stage][index - 1]
              // which should be the smallest possible value for all
              // index[stage][*]
              assert(cmp != MatchKindCMP::PO);
              test_key_equal = (cmp == MatchKindCMP::EQ);
            }
            if (test_key_equal) {
              return nxt_lower_bound(key, iter, history);
            } else {
              // key[stage] < index[stage][left-most]
              return smallest_result(iter);
            }
          }
        }
        // IS_BOTTOM || !history.is_PO<STAGE - 1>()
        auto iter = iterator_t(container);
        iter.seek_last();
        if constexpr (STAGE == STAGE_STRING) {
          // TODO(cross-node string dedup)
          // assert(iter.get_key().type() == ns_oid_view_t::Type::MAX);
          assert(compare_to<KeyT::HOBJ>(key, iter.get_key()) == MatchKindCMP::EQ);
        } else {
          assert(compare_to<KeyT::HOBJ>(key, iter.get_key()) == MatchKindCMP::EQ);
        }
        if constexpr (IS_BOTTOM) {
          auto value_ptr = iter.get_p_value();
          return result_t{{iter.index()}, value_ptr, MSTAT_EQ};
        } else {
          auto nxt_container = iter.get_nxt_container();
          auto nxt_result = NXT_STAGE_T::lower_bound(nxt_container, key, history);
          // !history.is_PO<STAGE - 1>() means
          // key[stage+1 ...] <= index[stage+1 ...][*]
          assert(!nxt_result.is_end());
          return result_t::from_nxt(iter.index(), nxt_result);
        }
      } else if (*history.get<STAGE>() == MatchKindCMP::NE) {
        exclude_last = true;
      }
    }
    auto iter = iterator_t(container);
    auto bs_match = iter.seek(key, exclude_last);
    if (iter.is_end()) {
      assert(!exclude_last);
      assert(bs_match == MatchKindBS::NE);
      history.set<STAGE>(MatchKindCMP::PO);
      return result_t::end();
    }
    history.set<STAGE>(bs_match == MatchKindBS::EQ ?
                       MatchKindCMP::EQ : MatchKindCMP::NE);
    if constexpr (IS_BOTTOM) {
      auto value_ptr = iter.get_p_value();
      return result_t{{iter.index()}, value_ptr,
                      (bs_match == MatchKindBS::EQ ? MSTAT_EQ : MSTAT_NE0)};
    } else {
      if (bs_match == MatchKindBS::EQ) {
        return nxt_lower_bound(key, iter, history);
      } else {
        return smallest_result(iter);
      }
    }
  }

  template <KeyT KT>
  static node_offset_t insert_size(const full_key_t<KT>& key, const value_t& value) {
    if constexpr (IS_BOTTOM) {
      return iterator_t::template estimate_insert<KT>(key, value);
    } else {
      return iterator_t::template estimate_insert<KT>(key, value) +
             NXT_STAGE_T::iterator_t::header_size() +
             NXT_STAGE_T::template insert_size<KT>(key, value);
    }
  }

  template <KeyT KT>
  static node_offset_t insert_size_at(
      match_stage_t stage, const full_key_t<KeyT::HOBJ>& key, const value_t& value) {
    if (stage == STAGE) {
      return insert_size<KT>(key, value);
    } else {
      assert(stage < STAGE);
      return NXT_STAGE_T::template insert_size_at<KT>(stage, key, value);
    }
  }

  template <typename T = std::tuple<match_stage_t, node_offset_t>>
  static std::enable_if_t<NODE_TYPE == node_type_t::INTERNAL, T> evaluate_insert(
      const container_t& container, const full_key_t<KeyT::VIEW>& key,
      const value_t& value, position_t& position, bool is_current) {
    auto iter = iterator_t(container);
    auto& index = position.index;
    if (!is_current) {
      index = INDEX_END;
    }
    if (index == INDEX_END) {
      iter.seek_last();
      index = iter.index();
      // evaluate the previous index
    } else {
      // evaluate the current index
      iter.seek_at(index);
      auto match = compare_to<KeyT::VIEW>(key, iter.get_key());
      if (match == MatchKindCMP::EQ) {
        if constexpr (IS_BOTTOM) {
          // ceph_abort?
          assert(false && "insert conflict at current index!");
        } else {
          // insert into the current index
          auto nxt_container = iter.get_nxt_container();
          return NXT_STAGE_T::evaluate_insert(
              nxt_container, key, value, position.nxt, true);
        }
      } else {
        assert(is_current && match == MatchKindCMP::NE);
        if (index == 0) {
          // already the first index, so insert at the current index
          return {STAGE, insert_size<KeyT::VIEW>(key, value)};
        }
        --index;
        iter = iterator_t(container);
        iter.seek_at(index);
        // proceed to evaluate the previous index
      }
    }

    // XXX(multi-type): when key is from a different type of node
    auto match = compare_to<KeyT::VIEW>(key, iter.get_key());
    if (match == MatchKindCMP::PO) {
      // key doesn't match both indexes, so insert at the current index
      ++index;
      return {STAGE, insert_size<KeyT::VIEW>(key, value)};
    } else {
      assert(match == MatchKindCMP::EQ);
      if constexpr (IS_BOTTOM) {
        // ceph_abort?
        assert(false && "insert conflict at the previous index!");
      } else {
        // insert into the previous index
        auto nxt_container = iter.get_nxt_container();
        return NXT_STAGE_T::evaluate_insert(
            nxt_container, key, value, position.nxt, false);
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
        assert(false && "impossible");
      } else {
        assert(stage < STAGE);
        bool compensate = NXT_STAGE_T::
          compensate_insert_position_at(stage, position.nxt);
        if (compensate) {
          assert(index != INDEX_END);
          if (index == 0) {
            // insert into the *last* index of the current stage
            index = INDEX_END;
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

  template <typename T = std::tuple<match_stage_t, node_offset_t>>
  static std::enable_if_t<NODE_TYPE == node_type_t::LEAF, T> evaluate_insert(
      const full_key_t<KeyT::HOBJ>& key, const onode_t& value,
      const MatchHistory& history, position_t& position) {
    match_stage_t insert_stage = STAGE_TOP;
    while (*history.get_by_stage(insert_stage) == MatchKindCMP::EQ) {
      assert(insert_stage != STAGE_BOTTOM && "insert conflict!");
      --insert_stage;
    }

    if (history.is_PO()) {
      if (position.is_end()) {
        // no need to compensate insert position
        assert(insert_stage <= STAGE && "impossible insert stage");
      } else if (position == position_t::begin()) {
        // I must be short-circuited by staged::smallest_result()
        // in staged::lower_bound()

        // XXX(multi-type): need to upgrade node type before inserting an
        // incompatible index at front.
        assert(insert_stage <= STAGE && "incompatible insert");

        // insert at begin and at the top stage
        insert_stage = STAGE;
      } else {
        assert(insert_stage <= STAGE && "impossible insert stage");
        bool ret = compensate_insert_position_at(insert_stage, position);
        assert(!ret);
      }
    }

    node_offset_t insert_size = insert_size_at<KeyT::HOBJ>(insert_stage, key, value);

    return {insert_stage, insert_size};
  }

  template <KeyT KT>
  static const value_t* insert_new(
      NodeExtentMutable& mut, const memory_range_t& range,
      const full_key_t<KT>& key, const value_t& value) {
    char* p_insert = const_cast<char*>(range.p_end);
    const value_t* p_value = nullptr;
    StagedAppender<KT> appender;
    appender.init(&mut, p_insert);
    appender.append(key, value, p_value);
    const char* p_insert_front = appender.wrap();
    assert(p_insert_front == range.p_start);
    return p_value;
  }

  template <KeyT KT, bool SPLIT>
  static const value_t* proceed_insert_recursively(
      NodeExtentMutable& mut, const container_t& container,
      const full_key_t<KT>& key, const value_t& value,
      position_t& position, match_stage_t& stage,
      node_offset_t& _insert_size, const char* p_left_bound) {
    // proceed insert from right to left
    assert(stage <= STAGE);
    auto iter = iterator_t(container);
    auto& index = position.index;
    if (index == INDEX_END) {
      iter.seek_last();
    } else {
      iter.seek_till_end(index);
    }

    bool do_insert = false;
    if (stage == STAGE) {
      if (index == INDEX_END) {
        iter.set_end();
      }
      do_insert = true;
    } else { // stage < STAGE
      if constexpr (SPLIT) {
        if (iter.is_end()) {
          // insert at the higher stage due to split
          do_insert = true;
          _insert_size = insert_size<KT>(key, value);
          stage = STAGE;
        }
      } else {
        assert(!iter.is_end());
      }
    }
    if (index == INDEX_END) {
      index = iter.index();
    }

    if (do_insert) {
      if constexpr (!IS_BOTTOM) {
        position.nxt = position_t::nxt_t::begin();
      }
      assert(_insert_size == insert_size<KT>(key, value));
      if constexpr (IS_BOTTOM) {
        return iter.template insert<KT>(
            mut, key, value, _insert_size, p_left_bound);
      } else {
        auto range = iter.template insert_prefix<KT>(
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
        assert(false && "impossible path");
      }
    }
  }

  template <KeyT KT, bool SPLIT>
  static const value_t* proceed_insert(
      NodeExtentMutable& mut, const container_t& container,
      const full_key_t<KT>& key, const value_t& value,
      position_t& position, match_stage_t& stage, node_offset_t& _insert_size) {
    auto p_left_bound = container.p_left_bound();
    if (unlikely(!container.keys())) {
      assert(position == position_t::end());
      assert(stage == STAGE);
      position = position_t::begin();
      if constexpr (IS_BOTTOM) {
        return container_t::template insert_at<KT>(
            mut, container, key, value, 0, _insert_size, p_left_bound);
      } else {
        auto range = container_t::template insert_prefix_at<KT>(
            mut, container, key, 0, _insert_size, p_left_bound);
        return NXT_STAGE_T::template insert_new<KT>(mut, range, key, value);
      }
    } else {
      return proceed_insert_recursively<KT, SPLIT>(
          mut, container, key, value,
          position, stage, _insert_size, p_left_bound);
    }
  }

  /*
   * Lookup interfaces
   */

  static void lookup_largest_normalized(
      const container_t& container,
      search_position_t& position,
      const value_t*& p_value) {
    if constexpr (STAGE == STAGE_LEFT) {
      lookup_largest(container, position, p_value);
      return;
    }
    position.index = 0;
    auto& pos_nxt = position.nxt;
    if constexpr (STAGE == STAGE_STRING) {
      lookup_largest(container, pos_nxt, p_value);
      return;
    }
    pos_nxt.index = 0;
    auto& pos_nxt_nxt = pos_nxt.nxt;
    if constexpr (STAGE == STAGE_RIGHT) {
      lookup_largest(container, pos_nxt_nxt, p_value);
      return;
    }
    assert(false);
  }

  static staged_result_t<NODE_TYPE, STAGE_TOP> lower_bound_normalized(
      const container_t& container,
      const full_key_t<KeyT::HOBJ>& key,
      MatchHistory& history) {
    auto&& result = lower_bound(container, key, history);
#ifndef NDEBUG
    if (result.is_end()) {
      assert(result.mstat == MSTAT_END);
    } else {
      full_key_t<KeyT::VIEW> index;
      get_key_view(container, result.position, index);
      assert_mstat(key, index, result.mstat);
    }
#endif
    if constexpr (container_t::FIELD_TYPE == field_type_t::N0) {
      // currently only internal node checks mstat
      if constexpr (NODE_TYPE == node_type_t::INTERNAL) {
        if (result.mstat == MSTAT_NE2) {
          auto cmp = compare_to<KeyT::HOBJ>(
              key, container[result.position.index].shard_pool);
          assert(cmp != MatchKindCMP::PO);
          if (cmp != MatchKindCMP::EQ) {
            result.mstat = MSTAT_NE3;
          }
        }
      }
    }
    return normalize(std::move(result));
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
          os << "0x" << std::hex << *value_ptr << std::dec;
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
    size_t index() const {
      return iter->index();
    }
    bool is_end() const { return iter->is_end(); }
    bool in_progress() const {
      assert(valid());
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
        assert(false);
      }
    }
    typename NXT_STAGE_T::StagedIterator& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        return this->_nxt;
      } else {
        assert(false);
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
    std::ostream& print(std::ostream& os, bool is_top) const {
      if (valid()) {
        if (iter->is_end()) {
          return os << "END";
        } else {
          os << index();
        }
      } else {
        if (is_top) {
          return os << "invalid StagedIterator!";
        } else {
          os << "0!";
        }
      }
      if constexpr (!IS_BOTTOM) {
        os << ", ";
        return this->_nxt.print(os, false);
      } else {
        return os;
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
    friend std::ostream& operator<<(std::ostream& os, const StagedIterator& iter) {
      return iter.print(os, true);
    }
   private:
    std::optional<iterator_t> iter;
    size_t end_index;
  };

  static void recursively_locate_split(
      size_t& current_size, size_t extra_size,
      size_t target_size, StagedIterator& split_at) {
    assert(current_size <= target_size);
    iterator_t& iter = split_at.get();
    current_size = iter.seek_split(current_size, extra_size, target_size);
    if constexpr (!IS_BOTTOM) {
      NXT_STAGE_T::recursively_locate_split(
          current_size, extra_size + iter.size_to_nxt(),
          target_size, split_at.nxt());
    }
  }

  static void recursively_locate_split_inserted(
      size_t& current_size, size_t extra_size, size_t target_size,
      position_t& i_position, match_stage_t i_stage, size_t i_size,
      std::optional<bool>& i_to_left, StagedIterator& split_at) {
    assert(current_size <= target_size);
    assert(!i_to_left.has_value());
    iterator_t& iter = split_at.get();
    auto& i_index = i_position.index;
    if (i_stage == STAGE) {
      current_size = iter.template seek_split_inserted<true>(
          current_size, extra_size, target_size,
          i_index, i_size, i_to_left);
      assert(i_to_left.has_value());
      if (*i_to_left == false && iter.index() == i_index) {
        // ...[s_index-1] |!| (i_index) [s_index]...
        return;
      }
      assert(!iter.is_end());
      if (iter.index() == 0) {
        extra_size += iterator_t::header_size();
      } else {
        extra_size = 0;
      }
    } else {
      if constexpr (!IS_BOTTOM) {
        assert(i_stage < STAGE);
        current_size = iter.template seek_split_inserted<false>(
            current_size, extra_size, target_size,
            i_index, i_size, i_to_left);
        assert(!iter.is_end());
        if (iter.index() == 0) {
          extra_size += iterator_t::header_size();
        } else {
          extra_size = 0;
        }
        if (!i_to_left.has_value()) {
          assert(iter.index() == i_index);
          NXT_STAGE_T::recursively_locate_split_inserted(
              current_size, extra_size + iter.size_to_nxt(), target_size,
              i_position.nxt, i_stage, i_size, i_to_left, split_at.nxt());
          assert(i_to_left.has_value());
          return;
        }
      } else {
        assert(false && "impossible path");
      }
    }
    if constexpr (!IS_BOTTOM) {
      NXT_STAGE_T::recursively_locate_split(
          current_size, extra_size + iter.size_to_nxt(),
          target_size, split_at.nxt());
    }
    return;
  }

  static bool locate_split(
      const container_t& container, size_t target_size,
      position_t& i_position, match_stage_t i_stage, size_t i_size,
      StagedIterator& split_at) {
    split_at.set(container);
    size_t current_size = 0;
    std::optional<bool> i_to_left;
    recursively_locate_split_inserted(
        current_size, 0, target_size,
        i_position, i_stage, i_size, i_to_left, split_at);
    std::cout << "  locate_split(): size_to_left=" << current_size
              << ", target_split_size=" << target_size
              << ", original_size=" << container.size_before(container.keys())
              << std::endl;
    assert(current_size <= target_size);
    return *i_to_left;
  }

  /*
   * container appender type system
   *   container_t::Appender(NodeExtentMutable& mut, char* p_append)
   *   append(const container_t& src, size_t from, size_t items)
   *   wrap() -> char*
   * IF !IS_BOTTOM:
   *   open_nxt(const key_get_type&)
   *   open_nxt(const full_key_t&)
   *       -> std::tuple<NodeExtentMutable&, char*>
   *   wrap_nxt(char* p_append)
   * ELSE
   *   append(const full_key_t& key, const value_t& value)
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
    size_t index() const {
      assert(valid());
      return _index;
    }
    bool in_progress() const { return require_wrap_nxt; }
    // TODO: pass by reference
    void init(NodeExtentMutable* p_mut, char* p_start) {
      assert(!valid());
      appender = typename container_t::template Appender<KT>(p_mut, p_start);
      _index = 0;
    }
    // possible to make src_iter end if
    // to_index == INDEX_END && to_stage == STAGE
    void append_until(
        StagedIterator& src_iter, size_t& to_index, match_stage_t to_stage) {
      assert(!require_wrap_nxt);
      assert(to_stage <= STAGE);
      auto s_index = src_iter.index();
      src_iter.get().template copy_out_until<KT>(*appender, to_index, to_stage);
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
                const value_t& value, const value_t*& p_value) {
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
        this->_nxt.init(p_mut, p_append);
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    typename NXT_STAGE_T::template StagedAppender<KT>&
    open_nxt(const full_key_t<KT>& key) {
      assert(!require_wrap_nxt);
      if constexpr (!IS_BOTTOM) {
        require_wrap_nxt = true;
        auto [p_mut, p_append] = appender->open_nxt(key);
        this->_nxt.init(p_mut, p_append);
        return this->_nxt;
      } else {
        assert(false);
      }
    }
    typename NXT_STAGE_T::template StagedAppender<KT>& get_nxt() {
      if constexpr (!IS_BOTTOM) {
        assert(require_wrap_nxt);
        return this->_nxt;
      } else {
        assert(false);
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
        assert(false);
      }
    }
   private:
    std::optional<typename container_t::template Appender<KT>> appender;
    size_t _index;
    bool require_wrap_nxt = false;
  };

  template <KeyT KT>
  static void _append_range(StagedIterator& src_iter, StagedAppender<KT>& appender,
                            size_t& to_index, match_stage_t stage) {
    if (src_iter.is_end()) {
      assert(to_index == INDEX_END);
      assert(stage == STAGE);
      to_index = src_iter.index();
    } else if constexpr (!IS_BOTTOM) {
      if (appender.in_progress()) {
        // we are in the progress of appending
        auto to_index_nxt = INDEX_END;
        NXT_STAGE_T::template _append_range<KT>(
            src_iter.nxt(), appender.get_nxt(),
            to_index_nxt, STAGE - 1);
        ++src_iter;
        appender.wrap_nxt();
      } else if (src_iter.in_progress()) {
        // cannot append the current item as-a-whole
        auto to_index_nxt = INDEX_END;
        NXT_STAGE_T::template _append_range<KT>(
            src_iter.nxt(), appender.open_nxt(src_iter.get_key()),
            to_index_nxt, STAGE - 1);
        ++src_iter;
        appender.wrap_nxt();
      }
    }
    appender.append_until(src_iter, to_index, stage);
  }

  template <KeyT KT>
  static void _append_into(StagedIterator& src_iter, StagedAppender<KT>& appender,
                           position_t& position, match_stage_t stage) {
    // reaches the last item
    if (stage == STAGE) {
      // done, end recursion
      if constexpr (!IS_BOTTOM) {
        position.nxt = position_t::nxt_t::begin();
      }
    } else {
      assert(stage < STAGE);
      // process append in the next stage
      NXT_STAGE_T::template append_until<KT>(
          src_iter.nxt(), appender.open_nxt(src_iter.get_key()),
          position.nxt, stage);
    }
  }

  template <KeyT KT>
  static void append_until(StagedIterator& src_iter, StagedAppender<KT>& appender,
                           position_t& position, match_stage_t stage) {
    size_t from_index = src_iter.index();
    size_t& to_index = position.index;
    assert(from_index <= to_index);
    if constexpr (IS_BOTTOM) {
      assert(stage == STAGE);
      appender.append_until(src_iter, to_index, stage);
    } else {
      assert(stage <= STAGE);
      if (src_iter.index() == to_index) {
        _append_into<KT>(src_iter, appender, position, stage);
      } else {
        _append_range<KT>(src_iter, appender, to_index, stage);
        _append_into<KT>(src_iter, appender, position, stage);
      }
    }
    to_index -= from_index;
  }

  template <KeyT KT>
  static bool append_insert(
      const full_key_t<KT>& key, const value_t& value,
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
        assert(false && "impossible path");
      }
    }
  }

  static std::tuple<TrimType, size_t>
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
      size_t trim_size;
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
    if (type == TrimType::AFTER) {
      auto& iter = trim_at.get();
      assert(iter.is_end());
      iter.trim_until(mut);
    }
  }
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
