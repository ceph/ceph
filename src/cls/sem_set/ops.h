// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <string>
#include <unordered_set>
#include <unordered_map>

#include "include/encoding.h"

namespace cls::sem_set {

inline constexpr auto max_keys = 1'000u;

namespace buffer = ceph::buffer;

/// Input to increment and decrement operations
struct incdec {
  std::unordered_set<std::string> keys;

  incdec() = default;

  incdec(std::string s)
    : keys({std::move(s)}) {}

  incdec(std::initializer_list<std::string> l)
    : keys(l) {}

  incdec(std::unordered_set<std::string> s)
    : keys(std::move(s)) {}

  template<std::input_iterator I>
  incdec(I begin, I end)
    requires std::is_convertible_v<typename I::value_type, std::string>
    : keys(begin, end) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(keys, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(keys, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(incdec);

using increment = incdec;
using decrement = incdec;

struct list_op {
  std::uint64_t count;
  std::string cursor;

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(count, bl);
    encode(cursor, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(count, bl);
    decode(cursor, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(list_op);

struct list_ret {
  std::unordered_map<std::string, std::uint64_t> kvs;
  std::string cursor;

  list_ret() = default;

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(kvs, bl);
    encode(cursor, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(kvs, bl);
    decode(cursor, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(list_ret);

inline constexpr auto CLASS = "sem_set";
inline constexpr auto INCREMENT = "increment";
inline constexpr auto DECREMENT = "decrement";
inline constexpr auto LIST = "list";

} // namespace cls::sem_set
