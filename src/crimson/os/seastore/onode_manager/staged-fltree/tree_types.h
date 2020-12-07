// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

namespace crimson::os::seastore::onode {

// TODO: Redesign according to real requirement from onode manager
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

struct tree_stats_t {
  size_t size_persistent_leaf = 0;
  size_t size_persistent_internal = 0;
  size_t size_filled_leaf = 0;
  size_t size_filled_internal = 0;
  size_t size_logical_leaf = 0;
  size_t size_logical_internal = 0;
  size_t size_overhead_leaf = 0;
  size_t size_overhead_internal = 0;
  size_t size_value_leaf = 0;
  size_t size_value_internal = 0;
  unsigned num_kvs_leaf = 0;
  unsigned num_kvs_internal = 0;
  unsigned num_nodes_leaf = 0;
  unsigned num_nodes_internal = 0;
  unsigned height = 0;

  size_t size_persistent() const {
    return size_persistent_leaf + size_persistent_internal; }
  size_t size_filled() const {
    return size_filled_leaf + size_filled_internal; }
  size_t size_logical() const {
    return size_logical_leaf + size_logical_internal; }
  size_t size_overhead() const {
    return size_overhead_leaf + size_overhead_internal; }
  size_t size_value() const {
    return size_value_leaf + size_value_internal; }
  unsigned num_kvs() const {
    return num_kvs_leaf + num_kvs_internal; }
  unsigned num_nodes() const {
    return num_nodes_leaf + num_nodes_internal; }

  double ratio_fullness() const {
    return (double)size_filled() / size_persistent(); }
  double ratio_key_compression() const {
    return (double)(size_filled() - size_value()) / (size_logical() - size_value()); }
  double ratio_overhead() const {
    return (double)size_overhead() / size_filled(); }
  double ratio_keys_leaf() const {
    return (double)num_kvs_leaf / num_kvs(); }
  double ratio_nodes_leaf() const {
    return (double)num_nodes_leaf / num_nodes(); }
  double ratio_filled_leaf() const {
    return (double)size_filled_leaf / size_filled(); }
};
inline std::ostream& operator<<(std::ostream& os, const tree_stats_t& stats) {
  os << "Tree stats:"
     << "\n  height = " << stats.height
     << "\n  num values = " << stats.num_kvs_leaf
     << "\n  num nodes  = " << stats.num_nodes()
     << " (leaf=" << stats.num_nodes_leaf
     << ", internal=" << stats.num_nodes_internal << ")"
     << "\n  size persistent = " << stats.size_persistent() << "B"
     << "\n  size filled     = " << stats.size_filled() << "B"
     << " (value=" << stats.size_value_leaf << "B"
     << ", rest=" << stats.size_filled() - stats.size_value_leaf << "B)"
     << "\n  size logical    = " << stats.size_logical() << "B"
     << "\n  size overhead   = " << stats.size_overhead() << "B"
     << "\n  ratio fullness  = " << stats.ratio_fullness()
     << "\n  ratio keys leaf = " << stats.ratio_keys_leaf()
     << "\n  ratio nodes leaf  = " << stats.ratio_nodes_leaf()
     << "\n  ratio filled leaf = " << stats.ratio_filled_leaf()
     << "\n  ratio key compression = " << stats.ratio_key_compression();
  assert(stats.num_kvs_internal + 1 == stats.num_nodes());
  return os;
}

}
