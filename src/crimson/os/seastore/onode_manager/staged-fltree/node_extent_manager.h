// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/type_helpers.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "fwd.h"
#include "super.h"
#include "node_extent_mutable.h"

namespace crimson::os::seastore::onode {

using crimson::os::seastore::LogicalCachedExtent;
class NodeExtent : public LogicalCachedExtent {
 public:
  using Ref = crimson::os::seastore::TCachedExtentRef<NodeExtent>;
  virtual ~NodeExtent() = default;
  const char* get_read() const {
    return get_bptr().c_str();
  }
  auto get_mutable() {
    assert(is_pending());
    return NodeExtentMutable(*this);
  }
  virtual Ref mutate(context_t/* DeltaBuffer::Ref */) = 0;

 protected:
  template <typename... T>
  NodeExtent(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}

  /**
   * abstracted:
   * - CacheExtent::duplicate_for_write() -> CachedExtentRef
   * - CacheExtent::get_type() -> extent_types_t
   * - CacheExtent::get_delta() -> ceph::bufferlist
   * - LogicalCachedExtent::apply_delta(const ceph::bufferlist) -> void
   */

 private:
  friend class NodeExtentMutable;
};

using crimson::os::seastore::TransactionManager;
class NodeExtentManager {
 public:
  virtual ~NodeExtentManager() = default;
  using tm_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  template <class... ValuesT>
  using tm_future = tm_ertr::future<ValuesT...>;

  virtual bool is_read_isolated() const = 0;
  virtual tm_future<NodeExtent::Ref> read_extent(
      Transaction&, laddr_t, extent_len_t) = 0;
  virtual tm_future<NodeExtent::Ref> alloc_extent(Transaction&, extent_len_t) = 0;
  virtual tm_future<Super::URef> get_super(Transaction&, RootNodeTracker&) = 0;

  static NodeExtentManagerURef create_dummy();
  static NodeExtentManagerURef create_seastore(
      TransactionManager& tm, laddr_t min_laddr = L_ADDR_MIN);
};

}
