// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "fwd.h"
#include "super.h"
#include "node_extent_mutable.h"
#include "node_types.h"

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
  std::pair<node_type_t, field_type_t> get_types() const;
  const char* get_read() const {
    return get_bptr().c_str();
  }
  NodeExtentMutable get_mutable() {
    assert(is_pending());
    return do_get_mutable();
  }

  virtual DeltaRecorder* get_recorder() const = 0;
  virtual NodeExtentRef mutate(context_t, DeltaRecorderURef&&) = 0;

 protected:
  template <typename... T>
  NodeExtent(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  NodeExtentMutable do_get_mutable() {
    assert(is_pending() || // during mutation
           is_clean());    // during replay
    return NodeExtentMutable(get_bptr().c_str(), get_length());
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
 public:
  virtual ~NodeExtentManager() = default;
  using tm_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange,
    crimson::ct_error::eagain>;
  template <class ValueT=void>
  using tm_future = tm_ertr::future<ValueT>;

  virtual bool is_read_isolated() const = 0;
  virtual tm_future<NodeExtentRef> read_extent(
      Transaction&, laddr_t, extent_len_t) = 0;
  virtual tm_future<NodeExtentRef> alloc_extent(Transaction&, extent_len_t) = 0;
  virtual tm_future<Super::URef> get_super(Transaction&, RootNodeTracker&) = 0;
  virtual std::ostream& print(std::ostream& os) const = 0;

  static NodeExtentManagerURef create_dummy(bool is_sync);
  static NodeExtentManagerURef create_seastore(
      TransactionManager& tm, laddr_t min_laddr = L_ADDR_MIN);
};
inline std::ostream& operator<<(std::ostream& os, const NodeExtentManager& nm) {
  return nm.print(os);
}

}
