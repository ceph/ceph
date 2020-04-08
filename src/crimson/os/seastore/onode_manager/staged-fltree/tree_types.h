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

using shard_t = int8_t;
using pool_t = int64_t;
using crush_hash_t = uint32_t;
using snap_t = uint64_t;
using gen_t = uint64_t;

// TODO: replace with ghobject_t
struct onode_key_t {
  shard_t shard;
  pool_t pool;
  crush_hash_t crush;
  std::string nspace;
  std::string oid;
  snap_t snap;
  gen_t gen;

  int cmp(const onode_key_t& o) const {
    auto l = std::tie(shard, pool, crush, nspace, oid, snap, gen);
    auto r = std::tie(o.shard, o.pool, o.crush, o.nspace, o.oid, o.snap, o.gen);
    if (l < r) {
      return -1;
    } else if (l > r) {
      return 1;
    } else {
      return 0;
    }
  }
  bool operator>(const onode_key_t& o) const { return cmp(o) > 0; }
  bool operator>=(const onode_key_t& o) const { return cmp(o) >= 0; }
  bool operator<(const onode_key_t& o) const { return cmp(o) < 0; }
  bool operator<=(const onode_key_t& o) const { return cmp(o) <= 0; }
  bool operator==(const onode_key_t& o) const { return cmp(o) == 0; }
  bool operator!=(const onode_key_t& o) const { return cmp(o) != 0; }
};
inline std::ostream& operator<<(std::ostream& os, const onode_key_t& key) {
  os << "key(" << (unsigned)key.shard << "," << key.pool << "," << key.crush << "; ";
  if (key.nspace.size() <= 12) {
    os << "\"" << key.nspace << "\",";
  } else {
    os << "\"" << key.nspace.substr(0, 4) << ".."
       << key.nspace.substr(key.nspace.size() - 2, 2)
       << "/" << key.nspace.size() << "B\",";
  }
  if (key.oid.size() <= 12) {
    os << "\"" << key.oid << "\"; ";
  } else {
    os << "\"" << key.oid.substr(0, 4) << ".."
       << key.oid.substr(key.oid.size() - 2, 2)
       << "/" << key.oid.size() << "B\"; ";
  }
  os << key.snap << "," << key.gen << ")";
  return os;
}

}
