// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <optional>
#include <ostream>

#include "common/fmt_common.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fwd.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_types.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/value.h"

namespace crimson::os::seastore::onode {

using match_stage_t = int8_t;
constexpr match_stage_t STAGE_LEFT = 2;   // shard/pool/crush
constexpr match_stage_t STAGE_STRING = 1; // nspace/oid
constexpr match_stage_t STAGE_RIGHT = 0;  // snap/gen
constexpr auto STAGE_TOP = STAGE_LEFT;
constexpr auto STAGE_BOTTOM = STAGE_RIGHT;
constexpr bool is_valid_stage(match_stage_t stage) {
  return std::clamp(stage, STAGE_BOTTOM, STAGE_TOP) == stage;
}
// TODO: replace by
// using match_history_t = int8_t;
//     left_m, str_m, right_m
//  3: GT,
//  2: EQ,     GT,
//  1: EQ,     EQ,    GT
//  0: EQ,     EQ,    EQ
// -1: EQ,     EQ,    LT
// -2: EQ,     LT,
// -3: LT,

struct MatchHistory {
  template <match_stage_t STAGE>
  const std::optional<MatchKindCMP>& get() const {
    static_assert(is_valid_stage(STAGE));
    if constexpr (STAGE == STAGE_RIGHT) {
      return right_match;
    } else if (STAGE == STAGE_STRING) {
      return string_match;
    } else {
      return left_match;
    }
  }

  const std::optional<MatchKindCMP>&
  get_by_stage(match_stage_t stage) const {
    assert(is_valid_stage(stage));
    if (stage == STAGE_RIGHT) {
      return right_match;
    } else if (stage == STAGE_STRING) {
      return string_match;
    } else {
      return left_match;
    }
  }

  template <match_stage_t STAGE = STAGE_TOP>
  const bool is_GT() const;

  template <match_stage_t STAGE>
  void set(MatchKindCMP match) {
    static_assert(is_valid_stage(STAGE));
    if constexpr (STAGE < STAGE_TOP) {
      assert(*get<STAGE + 1>() == MatchKindCMP::EQ);
    }
    assert(!get<STAGE>().has_value() || *get<STAGE>() != MatchKindCMP::EQ);
    const_cast<std::optional<MatchKindCMP>&>(get<STAGE>()) = match;
  }

  std::ostream& dump(std::ostream& os) const {
    os << "history(";
    dump_each(os, left_match) << ", ";
    dump_each(os, string_match) << ", ";
    dump_each(os, right_match) << ")";
    return os;
  }

  std::ostream& dump_each(
      std::ostream& os, const std::optional<MatchKindCMP>& match) const {
    if (!match.has_value()) {
      return os << "--";
    } else if (*match == MatchKindCMP::LT) {
      return os << "LT";
    } else if (*match == MatchKindCMP::EQ) {
      return os << "EQ";
    } else if (*match == MatchKindCMP::GT) {
      return os << "GT";
    } else {
      ceph_abort("impossble path");
    }
  }

  std::optional<MatchKindCMP> left_match;
  std::optional<MatchKindCMP> string_match;
  std::optional<MatchKindCMP> right_match;
};
inline std::ostream& operator<<(std::ostream& os, const MatchHistory& pos) {
  return pos.dump(os);
}

template <match_stage_t STAGE>
struct _check_GT_t {
  static bool eval(const MatchHistory* history) {
    return history->get<STAGE>() &&
           (*history->get<STAGE>() == MatchKindCMP::GT ||
            (*history->get<STAGE>() == MatchKindCMP::EQ &&
             _check_GT_t<STAGE - 1>::eval(history)));
  }
};
template <>
struct _check_GT_t<STAGE_RIGHT> {
  static bool eval(const MatchHistory* history) {
    return history->get<STAGE_RIGHT>() &&
           *history->get<STAGE_RIGHT>() == MatchKindCMP::GT;
  }
};
template <match_stage_t STAGE>
const bool MatchHistory::is_GT() const {
  static_assert(is_valid_stage(STAGE));
  if constexpr (STAGE < STAGE_TOP) {
    assert(get<STAGE + 1>() == MatchKindCMP::EQ);
  }
  return _check_GT_t<STAGE>::eval(this);
}

template <match_stage_t STAGE>
struct staged_position_t {
  static_assert(is_valid_stage(STAGE));
  using me_t = staged_position_t<STAGE>;
  using nxt_t = staged_position_t<STAGE - 1>;
  bool is_end() const {
    if (index == INDEX_END) {
      return true;
    } else {
      assert(is_valid_index(index));
      return false;
    }
  }
  index_t& index_by_stage(match_stage_t stage) {
    assert(stage <= STAGE);
    if (STAGE == stage) {
      return index;
    } else {
      return nxt.index_by_stage(stage);
    }
  }

  auto operator<=>(const me_t& o) const = default;

  void assert_next_to(const me_t& prv) const {
#ifndef NDEBUG
    if (is_end()) {
      assert(!prv.is_end());
    } else if (index == prv.index) {
      assert(!nxt.is_end());
      nxt.assert_next_to(prv.nxt);
    } else if (index == prv.index + 1) {
      assert(!prv.nxt.is_end());
      assert(nxt == nxt_t::begin());
    } else {
      assert(false);
    }
#endif
  }

  me_t& operator-=(const me_t& o) {
    assert(is_valid_index(o.index));
    assert(index >= o.index);
    if (index != INDEX_END) {
      assert(is_valid_index(index));
      index -= o.index;
      if (index == 0) {
        nxt -= o.nxt;
      }
    }
    return *this;
  }

  me_t& operator+=(const me_t& o) {
    assert(is_valid_index(index));
    assert(is_valid_index(o.index));
    index += o.index;
    nxt += o.nxt;
    return *this;
  }

  void encode(ceph::bufferlist& encoded) const {
    ceph::encode(index, encoded);
    nxt.encode(encoded);
  }

  static me_t decode(ceph::bufferlist::const_iterator& delta) {
    me_t ret;
    ceph::decode(ret.index, delta);
    ret.nxt = nxt_t::decode(delta);
    return ret;
  }

  static me_t begin() { return {0u, nxt_t::begin()}; }
  static me_t end() {
    return {INDEX_END, nxt_t::end()};
  }

  index_t index;
  nxt_t nxt;

  std::string fmt_print() const {
    if (index == INDEX_END) {
      return fmt::format("END, {}", nxt.fmt_print());
    }
    if (index == INDEX_LAST) {
      return fmt::format("LAST, {}", nxt.fmt_print());
    }
    assert(is_valid_index(index));
    return fmt::format("{}, {}", index, nxt.fmt_print());
  }
};

template <>
struct staged_position_t<STAGE_BOTTOM> {
  using me_t = staged_position_t<STAGE_BOTTOM>;
  bool is_end() const {
    if (index == INDEX_END) {
      return true;
    } else {
      assert(is_valid_index(index));
      return false;
    }
  }
  index_t& index_by_stage(match_stage_t stage) {
    assert(stage == STAGE_BOTTOM);
    return index;
  }

  auto operator<=>(const me_t&) const = default;

  me_t& operator-=(const me_t& o) {
    assert(is_valid_index(o.index));
    assert(index >= o.index);
    if (index != INDEX_END) {
      assert(is_valid_index(index));
      index -= o.index;
    }
    return *this;
  }

  me_t& operator+=(const me_t& o) {
    assert(is_valid_index(index));
    assert(is_valid_index(o.index));
    index += o.index;
    return *this;
  }

  void assert_next_to(const me_t& prv) const {
#ifndef NDEBUG
    if (is_end()) {
      assert(!prv.is_end());
    } else {
      assert(index == prv.index + 1);
    }
#endif
  }

  void encode(ceph::bufferlist& encoded) const {
    ceph::encode(index, encoded);
  }

  static me_t decode(ceph::bufferlist::const_iterator& delta) {
    me_t ret;
    ceph::decode(ret.index, delta);
    return ret;
  }

  static me_t begin() { return {0u}; }
  static me_t end() { return {INDEX_END}; }

  index_t index;
  std::string fmt_print() const {
    if (index == INDEX_END) {
      return "END";
    }
    if (index == INDEX_LAST) {
      return "LAST";
    }
    assert(is_valid_index(index));
    return std::to_string(index);
  }
};

using search_position_t = staged_position_t<STAGE_TOP>;

template <match_stage_t STAGE>
const staged_position_t<STAGE>& cast_down(const search_position_t& pos) {
  if constexpr (STAGE == STAGE_LEFT) {
    return pos;
  } else if constexpr (STAGE == STAGE_STRING) {
#ifndef NDEBUG
    if (pos.is_end()) {
      assert(pos.nxt.is_end());
    } else {
      assert(pos.index == 0u);
    }
#endif
    return pos.nxt;
  } else if constexpr (STAGE == STAGE_RIGHT) {
#ifndef NDEBUG
    if (pos.is_end()) {
      assert(pos.nxt.nxt.is_end());
    } else {
      assert(pos.index == 0u);
      assert(pos.nxt.index == 0u);
    }
#endif
    return pos.nxt.nxt;
  } else {
    ceph_abort("impossible path");
  }
}

template <match_stage_t STAGE>
staged_position_t<STAGE>& cast_down(search_position_t& pos) {
  const search_position_t& _pos = pos;
  return const_cast<staged_position_t<STAGE>&>(cast_down<STAGE>(_pos));
}

template <match_stage_t STAGE>
staged_position_t<STAGE>& cast_down_fill_0(search_position_t& pos) {
  if constexpr (STAGE == STAGE_LEFT) {
    return pos;
  } if constexpr (STAGE == STAGE_STRING) {
    pos.index = 0;
    return pos.nxt;
  } else if constexpr (STAGE == STAGE_RIGHT) {
    pos.index = 0;
    pos.nxt.index = 0;
    return pos.nxt.nxt;
  } else {
    ceph_abort("impossible path");
  }
}

inline search_position_t&& normalize(search_position_t&& pos) { return std::move(pos); }

template <match_stage_t STAGE, typename = std::enable_if_t<STAGE != STAGE_TOP>>
search_position_t normalize(staged_position_t<STAGE>&& pos) {
  if (pos.is_end()) {
    return search_position_t::end();
  }
  if constexpr (STAGE == STAGE_STRING) {
    return {0u, std::move(pos)};
  } else if (STAGE == STAGE_RIGHT) {
    return {0u, {0u, std::move(pos)}};
  } else {
    ceph_abort("impossible path");
  }
}

struct memory_range_t {
  const char* p_start;
  const char* p_end;
};

struct container_range_t {
  memory_range_t range;
  extent_len_t node_size;
};

enum class ContainerType { ITERATIVE, INDEXABLE };

// the input type to construct the value during insert.
template <node_type_t> struct value_input_type;
template<> struct value_input_type<node_type_t::INTERNAL> { using type = laddr_t; };
template<> struct value_input_type<node_type_t::LEAF> { using type = value_config_t; };
template <node_type_t NODE_TYPE>
using value_input_type_t = typename value_input_type<NODE_TYPE>::type;

template <node_type_t> struct value_type;
template<> struct value_type<node_type_t::INTERNAL> { using type = laddr_packed_t; };
template<> struct value_type<node_type_t::LEAF> { using type = value_header_t; };
template <node_type_t NODE_TYPE>
using value_type_t = typename value_type<NODE_TYPE>::type;

template <node_type_t NODE_TYPE, match_stage_t STAGE>
struct staged_result_t {
  using me_t = staged_result_t<NODE_TYPE, STAGE>;
  bool is_end() const { return position.is_end(); }

  static me_t end() {
    return {staged_position_t<STAGE>::end(), nullptr, MSTAT_END};
  }
  template <typename T = me_t>
  static std::enable_if_t<STAGE != STAGE_BOTTOM, T> from_nxt(
      index_t index, const staged_result_t<NODE_TYPE, STAGE - 1>& nxt_stage_result) {
    return {{index, nxt_stage_result.position},
            nxt_stage_result.p_value,
            nxt_stage_result.mstat};
  }

  staged_position_t<STAGE> position;
  const value_type_t<NODE_TYPE>* p_value;
  match_stat_t mstat;
};

template <node_type_t NODE_TYPE>
using lookup_result_t = staged_result_t<NODE_TYPE, STAGE_TOP>;

template <node_type_t NODE_TYPE>
lookup_result_t<NODE_TYPE>&& normalize(
    lookup_result_t<NODE_TYPE>&& result) { return std::move(result); }

template <node_type_t NODE_TYPE, match_stage_t STAGE,
          typename = std::enable_if_t<STAGE != STAGE_TOP>>
lookup_result_t<NODE_TYPE> normalize(
    staged_result_t<NODE_TYPE, STAGE>&& result) {
  // FIXME: assert result.mstat correct
  return {normalize(std::move(result.position)), result.p_value, result.mstat};
}

struct node_stats_t {
  size_t size_persistent = 0;
  size_t size_filled = 0;
  // filled by staged::get_stats()
  size_t size_logical = 0;
  size_t size_overhead = 0;
  size_t size_value = 0;
  unsigned num_kvs = 0;
};

} // namespace crimson::os::seastore::onode

namespace std {
template <crimson::os::seastore::onode::match_stage_t STAGE>
std::ostream& operator<<(
    std::ostream& os,
    const crimson::os::seastore::onode::staged_position_t<STAGE>& pos) {
  return os << pos.fmt_print();
}
} // namespace std


namespace fmt {
template <crimson::os::seastore::onode::match_stage_t S>
struct formatter<crimson::os::seastore::onode::staged_position_t<S>> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::os::seastore::onode::staged_position_t<S>& k,
              FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}", k.fmt_print());
  }
};

#if FMT_VERSION >= 90000
template <> struct formatter<crimson::os::seastore::onode::MatchHistory> : ostream_formatter {};
#endif
}  // namespace fmt
