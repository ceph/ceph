// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdint>
#include <boost/container/small_vector.hpp>

#include "crimson/os/seastore/transaction_manager.h"
#include "onode_delta.h"

namespace crimson::os::seastore {

// TODO s/CachedExtent/LogicalCachedExtent/
struct OnodeBlock final : LogicalCachedExtent {
  using Ref = TCachedExtentRef<OnodeBlock>;

  template <typename... T>
  OnodeBlock(T&&... t) : LogicalCachedExtent(std::forward<T>(t)...) {}
  OnodeBlock(OnodeBlock&& block) noexcept
    : LogicalCachedExtent{std::move(block)},
      deltas{std::move(block.deltas)}
  {}
  OnodeBlock(const OnodeBlock& block, CachedExtent::share_buffer_t tag) noexcept
    : LogicalCachedExtent{block, tag},
      share_buffer{true}
  {}

  CachedExtentRef duplicate_for_write() final {
    return new OnodeBlock{*this, CachedExtent::share_buffer_t{}};
  }

  // could materialize the pending changes to the underlying buffer here,
  // but since we write the change to the buffer immediately, let skip
  // this for now.
  void prepare_write() final {}

  // queries
  static constexpr extent_types_t TYPE = extent_types_t::ONODE_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  // have to stash all the changes before on_delta_write() is called,
  // otherwise we could pollute the extent with pending mutations
  // before the transaction carrying these mutations is committed to
  // disk
  ceph::bufferlist get_delta() final;
  void on_initial_write() final;
  void on_delta_write(paddr_t record_block_offset) final;
  void apply_delta(const ceph::bufferlist &bl) final;

  void sync() {
    apply_pending_changes(false);
  }
  void mutate(delta_t&& d);
  using mutate_func_t = std::function<void (char*, const delta_t&)>;
  void set_delta_applier(mutate_func_t&& func) {
    mutate_func = std::move(func);
  }
private:
  // before looking at the extent, we need to make sure the content is up to date
  void apply_pending_changes(bool do_cleanup);
  // assuming we don't stash too many deltas to a single block
  // otherwise a fullwrite op is necessary
  boost::container::small_vector<std::unique_ptr<delta_t>, 2> deltas;
  mutate_func_t mutate_func;
  bool share_buffer = false;
};

}
