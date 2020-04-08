// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <optional>
#include <ostream>

#include "crimson/os/seastore/onode_manager/staged-fltree/fwd.h"

namespace crimson::os::seastore::onode {

using match_stage_t = uint8_t;
constexpr match_stage_t STAGE_LEFT = 2u;   // shard/pool/crush
constexpr match_stage_t STAGE_STRING = 1u; // nspace/oid
constexpr match_stage_t STAGE_RIGHT = 0u;  // snap/gen
constexpr auto STAGE_TOP = STAGE_LEFT;
constexpr auto STAGE_BOTTOM = STAGE_RIGHT;

// TODO: replace by
// using match_history_t = int8_t;
//     left_m, str_m, right_m
//  3: PO,
//  2: EQ,     PO,
//  1: EQ,     EQ,    PO
//  0: EQ,     EQ,    EQ
// -1: EQ,     EQ,    NE
// -2: EQ,     NE,
// -3: NE,

struct MatchHistory {
  template <match_stage_t STAGE>
  const std::optional<MatchKindCMP>& get() const {
    static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
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
    assert(stage >= STAGE_BOTTOM && stage <= STAGE_TOP);
    if (stage == STAGE_RIGHT) {
      return right_match;
    } else if (stage == STAGE_STRING) {
      return string_match;
    } else {
      return left_match;
    }
  }

  template <match_stage_t STAGE = STAGE_TOP>
  const bool is_PO() const;

  template <match_stage_t STAGE>
  void set(MatchKindCMP match) {
    static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
    if constexpr (STAGE < STAGE_TOP) {
      assert(*get<STAGE + 1>() == MatchKindCMP::EQ);
    }
    assert(!get<STAGE>().has_value() || *get<STAGE>() != MatchKindCMP::EQ);
    const_cast<std::optional<MatchKindCMP>&>(get<STAGE>()) = match;
  }

  std::optional<MatchKindCMP> left_match;
  std::optional<MatchKindCMP> string_match;
  std::optional<MatchKindCMP> right_match;
};

template <match_stage_t STAGE>
struct _check_PO_t {
  static bool eval(const MatchHistory* history) {
    return history->get<STAGE>() &&
           (*history->get<STAGE>() == MatchKindCMP::PO ||
            (*history->get<STAGE>() == MatchKindCMP::EQ &&
             _check_PO_t<STAGE - 1>::eval(history)));
  }
};
template <>
struct _check_PO_t<STAGE_RIGHT> {
  static bool eval(const MatchHistory* history) {
    return history->get<STAGE_RIGHT>() &&
           *history->get<STAGE_RIGHT>() == MatchKindCMP::PO;
  }
};
template <match_stage_t STAGE>
const bool MatchHistory::is_PO() const {
  static_assert(STAGE >= STAGE_BOTTOM && STAGE <= STAGE_TOP);
  if constexpr (STAGE < STAGE_TOP) {
    assert(get<STAGE + 1>() == MatchKindCMP::EQ);
  }
  return _check_PO_t<STAGE>::eval(this);
}

template <match_stage_t STAGE>
struct staged_position_t {
  static_assert(STAGE > STAGE_BOTTOM && STAGE <= STAGE_TOP);
  using me_t = staged_position_t<STAGE>;
  using nxt_t = staged_position_t<STAGE - 1>;
  bool is_end() const { return index == INDEX_END; }
  size_t& index_by_stage(match_stage_t stage) {
    assert(stage <= STAGE);
    if (STAGE == stage) {
      return index;
    } else {
      return nxt.index_by_stage(stage);
    }
  }

  int cmp(const me_t& o) const {
    if (index > o.index) {
      return 1;
    } else if (index < o.index) {
      return -1;
    } else {
      return nxt.cmp(o.nxt);
    }
  }
  bool operator>(const me_t& o) const { return cmp(o) > 0; }
  bool operator>=(const me_t& o) const { return cmp(o) >= 0; }
  bool operator<(const me_t& o) const { return cmp(o) < 0; }
  bool operator<=(const me_t& o) const { return cmp(o) <= 0; }
  bool operator==(const me_t& o) const { return cmp(o) == 0; }
  bool operator!=(const me_t& o) const { return cmp(o) != 0; }

  me_t& operator-=(const me_t& o) {
    assert(o.index != INDEX_END);
    assert(index >= o.index);
    if (index != INDEX_END) {
      index -= o.index;
      if (index == 0) {
        nxt -= o.nxt;
      }
    }
    return *this;
  }

  static me_t begin() { return {0u, nxt_t::begin()}; }
  static me_t end() {
    return {INDEX_END, nxt_t::end()};
  }

  size_t index;
  nxt_t nxt;
};
template <match_stage_t STAGE>
std::ostream& operator<<(std::ostream& os, const staged_position_t<STAGE>& pos) {
  if (pos.index == INDEX_END) {
    os << "END";
  } else {
    os << pos.index;
  }
  return os << ", " << pos.nxt;
}

template <>
struct staged_position_t<STAGE_BOTTOM> {
  using me_t = staged_position_t<STAGE_BOTTOM>;
  bool is_end() const { return index == INDEX_END; }
  size_t& index_by_stage(match_stage_t stage) {
    assert(stage == STAGE_BOTTOM);
    return index;
  }

  int cmp(const staged_position_t<STAGE_BOTTOM>& o) const {
    if (index > o.index) {
      return 1;
    } else if (index < o.index) {
      return -1;
    } else {
      return 0;
    }
  }
  bool operator>(const me_t& o) const { return cmp(o) > 0; }
  bool operator>=(const me_t& o) const { return cmp(o) >= 0; }
  bool operator<(const me_t& o) const { return cmp(o) < 0; }
  bool operator<=(const me_t& o) const { return cmp(o) <= 0; }
  bool operator==(const me_t& o) const { return cmp(o) == 0; }
  bool operator!=(const me_t& o) const { return cmp(o) != 0; }

  me_t& operator-=(const me_t& o) {
    assert(o.index != INDEX_END);
    assert(index >= o.index);
    if (index != INDEX_END) {
      index -= o.index;
    }
    return *this;
  }

  static me_t begin() { return {0u}; }
  static me_t end() { return {INDEX_END}; }

  size_t index;
};
template <>
inline std::ostream& operator<<(std::ostream& os, const staged_position_t<STAGE_BOTTOM>& pos) {
  if (pos.index == INDEX_END) {
    return os << "END";
  } else {
    return os << pos.index;
  }
}

using search_position_t = staged_position_t<STAGE_TOP>;

template <match_stage_t STAGE, typename = std::enable_if_t<STAGE == STAGE_TOP>>
const search_position_t& cast_down(const search_position_t& pos) { return pos; }

template <match_stage_t STAGE, typename = std::enable_if_t<STAGE != STAGE_TOP>>
const staged_position_t<STAGE>& cast_down(const search_position_t& pos) {
  if constexpr (STAGE == STAGE_STRING) {
#ifndef NDEBUG
    if (pos.is_end()) {
      assert(pos.nxt.is_end());
    } else {
      assert(pos.index == 0u);
    }
#endif
    return pos.nxt;
  } else if (STAGE == STAGE_RIGHT) {
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
    assert(false);
  }
}

template <match_stage_t STAGE>
staged_position_t<STAGE>& cast_down(search_position_t& pos) {
  const search_position_t& _pos = pos;
  return const_cast<staged_position_t<STAGE>&>(cast_down<STAGE>(_pos));
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
    assert(false);
  }
}

struct memory_range_t {
  const char* p_start;
  const char* p_end;
};

enum class ContainerType { ITERATIVE, INDEXABLE };

struct onode_t;

template <node_type_t> struct value_type;
template<> struct value_type<node_type_t::INTERNAL> { using type = laddr_t; };
template<> struct value_type<node_type_t::LEAF> { using type = onode_t; };
template <node_type_t NODE_TYPE>
using value_type_t = typename value_type<NODE_TYPE>::type;

}
