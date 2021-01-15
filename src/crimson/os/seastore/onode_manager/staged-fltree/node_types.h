// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <ostream>

#include "fwd.h"

namespace crimson::os::seastore::onode {

constexpr uint8_t FIELD_TYPE_MAGIC = 0x25;
enum class field_type_t : uint8_t {
  N0 = FIELD_TYPE_MAGIC,
  N1,
  N2,
  N3,
  _MAX
};
inline uint8_t to_unsigned(field_type_t type) {
  auto value = static_cast<uint8_t>(type);
  assert(value >= FIELD_TYPE_MAGIC);
  assert(value < static_cast<uint8_t>(field_type_t::_MAX));
  return value - FIELD_TYPE_MAGIC;
}
inline std::ostream& operator<<(std::ostream &os, field_type_t type) {
  const char* const names[] = {"0", "1", "2", "3"};
  auto index = to_unsigned(type);
  os << names[index];
  return os;
}

enum class node_type_t : uint8_t {
  LEAF = 0,
  INTERNAL
};
inline std::ostream& operator<<(std::ostream &os, const node_type_t& type) {
  const char* const names[] = {"L", "I"};
  auto index = static_cast<uint8_t>(type);
  assert(index <= 1u);
  os << names[index];
  return os;
}

struct laddr_packed_t {
  laddr_t value;
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const laddr_packed_t& laddr) {
  return os << "laddr_packed(0x" << std::hex << laddr.value << std::dec << ")";
}

using match_stat_t = int8_t;
constexpr match_stat_t MSTAT_END = -2; // index is search_position_t::end()
constexpr match_stat_t MSTAT_EQ  = -1; // key == index
constexpr match_stat_t MSTAT_LT0 =  0; // key == index [pool/shard crush ns/oid]; key < index [snap/gen]
constexpr match_stat_t MSTAT_LT1 =  1; // key == index [pool/shard crush]; key < index [ns/oid]
constexpr match_stat_t MSTAT_LT2 =  2; // key < index [pool/shard crush ns/oid] ||
                                       // key == index [pool/shard]; key < index [crush]
constexpr match_stat_t MSTAT_LT3 =  3; // key < index [pool/shard]
constexpr match_stat_t MSTAT_MIN = MSTAT_END;
constexpr match_stat_t MSTAT_MAX = MSTAT_LT3;

enum class node_delta_op_t : uint8_t {
  INSERT,
  SPLIT,
  SPLIT_INSERT,
  UPDATE_CHILD_ADDR,
  SUBOP_UPDATE_VALUE = 0xff,
};

}
