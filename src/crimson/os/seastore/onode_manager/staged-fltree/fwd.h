// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <string>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore::onode {

using eagain_ertr = crimson::errorator<
  crimson::ct_error::eagain>;
template <class ValueT=void>
using eagain_future = eagain_ertr::future<ValueT>;

using crimson::os::seastore::Transaction;
using crimson::os::seastore::TransactionRef;
using crimson::os::seastore::laddr_t;
using crimson::os::seastore::L_ADDR_MIN;
using crimson::os::seastore::L_ADDR_NULL;
using crimson::os::seastore::extent_len_t;

class DeltaRecorder;
class NodeExtent;
class NodeExtentManager;
class RootNodeTracker;
struct ValueBuilder;
using DeltaRecorderURef = std::unique_ptr<DeltaRecorder>;
using NodeExtentRef = crimson::os::seastore::TCachedExtentRef<NodeExtent>;
using NodeExtentManagerURef = std::unique_ptr<NodeExtentManager>;
using RootNodeTrackerURef = std::unique_ptr<RootNodeTracker>;
struct context_t {
  NodeExtentManager& nm;
  const ValueBuilder& vb;
  Transaction& t;
};

class LeafNodeImpl;
class InternalNodeImpl;
class NodeImpl;
using LeafNodeImplURef = std::unique_ptr<LeafNodeImpl>;
using InternalNodeImplURef = std::unique_ptr<InternalNodeImpl>;
using NodeImplURef = std::unique_ptr<NodeImpl>;

using level_t = uint8_t;
constexpr auto MAX_LEVEL = std::numeric_limits<level_t>::max();

// a type only to index within a node, 32 bits should be enough
using index_t = uint32_t;
constexpr auto INDEX_END = std::numeric_limits<index_t>::max();
constexpr auto INDEX_LAST = INDEX_END - 0x4;
constexpr auto INDEX_UPPER_BOUND = INDEX_END - 0x8;
inline bool is_valid_index(index_t index) { return index < INDEX_UPPER_BOUND; }

// we support up to 64 KiB tree nodes
using node_offset_t = uint16_t;
constexpr node_offset_t DISK_BLOCK_SIZE = 1u << 12;
constexpr auto MAX_NODE_SIZE =
    (extent_len_t)std::numeric_limits<node_offset_t>::max() + 1;
inline bool is_valid_node_size(extent_len_t node_size) {
  return (node_size > 0 &&
          node_size <= MAX_NODE_SIZE &&
          node_size % DISK_BLOCK_SIZE == 0);
}

using string_size_t = uint16_t;

enum class MatchKindBS : int8_t { NE = -1, EQ = 0 };

enum class MatchKindCMP : int8_t { LT = -1, EQ = 0, GT };
inline MatchKindCMP toMatchKindCMP(int value) {
  if (value > 0) {
    return MatchKindCMP::GT;
  } else if (value < 0) {
    return MatchKindCMP::LT;
  } else {
    return MatchKindCMP::EQ;
  }
}
template <typename Type>
MatchKindCMP toMatchKindCMP(const Type& l, const Type& r) {
  if (l > r) {
    return MatchKindCMP::GT;
  } else if (l < r) {
    return MatchKindCMP::LT;
  } else {
    return MatchKindCMP::EQ;
  }
}

inline MatchKindCMP toMatchKindCMP(
    std::string_view l, std::string_view r) {
  return toMatchKindCMP(l.compare(r));
}

inline MatchKindCMP reverse(MatchKindCMP cmp) {
  if (cmp == MatchKindCMP::LT) {
    return MatchKindCMP::GT;
  } else if (cmp == MatchKindCMP::GT) {
    return MatchKindCMP::LT;
  } else {
    return cmp;
  }
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

template <typename PtrType>
void reset_ptr(PtrType& ptr, const char* origin_base,
               const char* new_base, extent_len_t node_size) {
  assert((const char*)ptr > origin_base);
  assert((const char*)ptr - origin_base < (int)node_size);
  ptr = reinterpret_cast<PtrType>(
      (const char*)ptr - origin_base + new_base);
}

}
