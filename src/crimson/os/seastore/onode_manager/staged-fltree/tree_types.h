// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

namespace crimson::os::seastore::onode {

// TODO: replace
struct onode_t {
  // onode should be smaller than a node
  uint16_t size; // address up to 64 KiB sized node
  uint16_t id;
  // omap, extent_map, inline data

  bool operator==(const onode_t& o) const { return size == o.size && id == o.id; }
  bool operator!=(const onode_t& o) const { return !(*this == o); }
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const onode_t& node) {
  return os << "onode(" << node.id << ", " << node.size << "B)";
}

}
