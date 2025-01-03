// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "fwd.h"
#include "node_extent_mutable.h"
#include "node_types.h"
#include "stages/node_stage_layout.h"
#include "super.h"

/**
 * node_extent_manager.h
 *
 * Contains general interfaces for different backends (Dummy and Seastore).
 */

namespace crimson::os::seastore::onode {

using crimson::os::seastore::LogicalCachedExtent;
class NodeExtent : public LogicalCachedExtent {
 public:
  virtual ~NodeExtent() = default;
  const node_header_t& get_header() const {
    return *reinterpret_cast<const node_header_t*>(get_read());
  }
  const char* get_read() const {
    return get_bptr().c_str();
  }
  NodeExtentMutable get_mutable() {
    assert(is_mutable());
    return do_get_mutable();
  }

  virtual DeltaRecorder* get_recorder() const = 0;
  virtual NodeExtentRef mutate(context_t, DeltaRecorderURef&&) = 0;

 protected:
  template <typename... T>
  NodeExtent(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  NodeExtentMutable do_get_mutable() {
    return NodeExtentMutable(get_bptr().c_str(), get_length());
  }

  std::ostream& print_detail_l(std::ostream& out) const final {
    return out << ", fltree_header=" << get_header();
  }

  /**
   * Abstracted interfaces to implement:
   * - CacheExtent::duplicate_for_write() -> CachedExtentRef
   * - CacheExtent::get_type() -> extent_types_t
   * - CacheExtent::get_delta() -> ceph::bufferlist
   * - LogicalCachedExtent::apply_delta(const ceph::bufferlist) -> void
   */
};

using crimson::os::seastore::TransactionManager;
class NodeExtentManager {
  using base_iertr = TransactionManager::base_iertr;
 public:
  virtual ~NodeExtentManager() = default;

  virtual bool is_read_isolated() const = 0;

  using read_iertr = base_iertr::extend<
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_iertr::future<NodeExtentRef> read_extent(
      Transaction&, laddr_t) = 0;

  using alloc_iertr = base_iertr;
  virtual alloc_iertr::future<NodeExtentRef> alloc_extent(
      Transaction&, laddr_t hint, extent_len_t) = 0;

  using retire_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  virtual retire_iertr::future<> retire_extent(
      Transaction&, NodeExtentRef) = 0;

  using getsuper_iertr = base_iertr;
  virtual getsuper_iertr::future<Super::URef> get_super(
      Transaction&, RootNodeTracker&) = 0;

  virtual std::ostream& print(std::ostream& os) const = 0;

  static NodeExtentManagerURef create_dummy(bool is_sync);
  static NodeExtentManagerURef create_seastore(
      TransactionManager &tm, laddr_t min_laddr = L_ADDR_MIN, double p_eagain = 0.0);
};
inline std::ostream& operator<<(std::ostream& os, const NodeExtentManager& nm) {
  return nm.print(os);
}

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::onode::NodeExtent> : fmt::ostream_formatter {};
#endif
