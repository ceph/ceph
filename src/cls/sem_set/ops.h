// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <string>

#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>

#include "include/encoding.h"

namespace cls::sem_set {
using namespace std::literals;

inline constexpr auto max_keys = 1'000u;

namespace buffer = ceph::buffer;

struct increment {
  boost::container::flat_set<std::string> keys;

  increment() = default;

  increment(std::string s)
    : keys({std::move(s)}) {}

  increment(decltype(keys) s)
    : keys(std::move(s)) {}

  template<std::input_iterator I>
  increment(I begin, I end)
    requires std::is_convertible_v<typename std::iterator_traits<I>::value_type,
				   std::string>
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
WRITE_CLASS_ENCODER(increment);

struct decrement {
  boost::container::flat_set<std::string> keys;
  ceph::timespan grace;

  decrement() = default;

  decrement(std::string s, ceph::timespan grace = 0ns)
    : keys({std::move(s)}), grace(grace) {}

  decrement(decltype(keys) s, ceph::timespan grace = 0ns)
    : keys(std::move(s)), grace(grace) {}

  template<std::input_iterator I>
  decrement(I begin, I end, ceph::timespan grace = 0ns)
    requires std::is_convertible_v<typename std::iterator_traits<I>::value_type,
				   std::string>
    : keys(begin, end), grace(grace) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(keys, bl);
    encode(grace, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(keys, bl);
    decode(grace, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(decrement);

struct reset {
  std::unordered_map<std::string, std::uint64_t> keys;

  reset() = default;

  reset(std::string s, uint64_t val = 0)
    : keys({{std::move(s), val}}) {}

  reset(decltype(keys) s)
    : keys(std::move(s)) {}

  template<std::input_iterator I>
  reset(I begin, I end)
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
WRITE_CLASS_ENCODER(reset);

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
  boost::container::flat_map<std::string, std::uint64_t> kvs;
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
inline constexpr auto RESET = "reset";
inline constexpr auto LIST = "list";

} // namespace cls::sem_set
