// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

using level_t = uint8_t;

}
