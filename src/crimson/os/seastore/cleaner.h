// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/extent_reader.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

class SegmentProvider;

class SpaceTrackerI {
public:
  virtual int64_t allocate(
    paddr_t addr,
    extent_len_t len) = 0;

  virtual int64_t release(
    paddr_t addr,
    extent_len_t len) = 0;

  virtual int64_t get_usage(
    paddr_t addr) const = 0;

  virtual bool equals(const SpaceTrackerI &other) const = 0;

  virtual std::unique_ptr<SpaceTrackerI> make_empty() const = 0;

  virtual void dump_usage(paddr_t) const = 0;

  virtual double calc_utilization(paddr_t) const = 0;

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class Cleaner {

public:
  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;

    virtual TransactionRef create_transaction(
        Transaction::src_t, const char*) = 0;

    /// Creates empty transaction with interruptible context
    template <typename Func>
    auto with_transaction_intr(
        Transaction::src_t src,
        const char* name,
        Func &&f) {
      return seastar::do_with(
        create_transaction(src, name),
        [f=std::forward<Func>(f)](auto &ref_t) mutable {
          return with_trans_intr(
            *ref_t,
            [f=std::forward<Func>(f)](auto& t) mutable {
              return f(t);
            }
          );
        }
      );
    }

    /// See Cache::get_next_dirty_extents
    using get_next_dirty_extents_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
      Transaction &t,     ///< [in] current transaction
      journal_seq_t bound,///< [in] return extents with dirty_from < bound
      size_t max_bytes    ///< [in] return up to max_bytes of extents
    ) = 0;

    using extent_mapping_ertr = crimson::errorator<
      crimson::ct_error::input_output_error,
      crimson::ct_error::eagain>;
    using extent_mapping_iertr = trans_iertr<
      crimson::errorator<
	crimson::ct_error::input_output_error>
      >;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  extent may be invalid, implementation must correctly
     * handle finding the current instance if it is still alive and
     * otherwise ignore it.
     */
    using rewrite_extent_iertr = extent_mapping_iertr;
    using rewrite_extent_ret = rewrite_extent_iertr::future<>;
    virtual rewrite_extent_ret rewrite_extent(
      Transaction &t,
      CachedExtentRef extent) = 0;

    /**
     * get_extent_if_live
     *
     * Returns extent at specified location if still referenced by
     * lba_manager and not removed by t.
     *
     * See TransactionManager::get_extent_if_live and
     * LBAManager::get_physical_extent_if_live.
     */
    using get_extent_if_live_iertr = extent_mapping_iertr;
    using get_extent_if_live_ret = get_extent_if_live_iertr::future<
      CachedExtentRef>;
    virtual get_extent_if_live_ret get_extent_if_live(
      Transaction &t,
      extent_types_t type,
      paddr_t addr,
      laddr_t laddr,
      seastore_off_t len) = 0;

    /**
     * release_segment
     *
     * Release segment.
     */
    using release_segment_ertr = SegmentManager::release_ertr;
    using release_segment_ret = release_segment_ertr::future<>;
    virtual release_segment_ret release_segment(
      segment_id_t id) = 0;

    /**
     * submit_transaction_direct
     *
     * Submits transaction without any space throttling.
     */
    using submit_transaction_direct_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using submit_transaction_direct_ret =
      submit_transaction_direct_iertr::future<>;
    virtual submit_transaction_direct_ret submit_transaction_direct(
      Transaction &t) = 0;
  };

  virtual void set_extent_callback(ExtentCallbackInterface *cb) = 0;

  using mount_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mount_ret = mount_ertr::future<>;
  virtual mount_ret mount(device_id_t pdevice_id) = 0;

  virtual void init_mkfs(journal_seq_t head) = 0;

  virtual void set_journal_head(journal_seq_t head) = 0;

  virtual bool debug_check_space(const SpaceTrackerI &tracker) = 0;
  virtual SpaceTrackerIRef get_empty_space_tracker() const = 0;
  virtual void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    seastar::lowres_system_clock::time_point last_modified
      = seastar::lowres_system_clock::time_point(),
    seastar::lowres_system_clock::time_point last_rewritten
      = seastar::lowres_system_clock::time_point(),
    bool init_scan = false) = 0;
  virtual void mark_space_free(
    paddr_t addr,
    extent_len_t len) = 0;

  virtual void complete_init() = 0;
  virtual seastar::future<> stop() = 0;
  virtual seastar::future<> reserve_projected_usage(size_t projected_usage) = 0;
  virtual void release_projected_usage(size_t projected_usage) = 0;

  virtual void update_journal_tail_target(journal_seq_t target) = 0;

  virtual store_statfs_t stat() const = 0;

  using release_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  virtual release_ertr::future<> release_if_needed(Transaction& t) = 0;

  virtual ~Cleaner() {}
};

using CleanerRef = std::unique_ptr<Cleaner>;

namespace cleaner {
struct seg_cleaner_config_t;
CleanerRef make_segmented(
  seg_cleaner_config_t config,
  ExtentReaderRef&& scanner,
  bool detailed = false);
}

}
