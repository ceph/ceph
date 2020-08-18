// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {
class Transaction;

class SegmentCleaner : public JournalSegmentProvider {
public:
  /// Config
  struct config_t {
    size_t num_segments = 0;
    size_t segment_size = 0;
    size_t target_journal_segments = 0;
    size_t max_journal_segments = 0;

    static config_t default_from_segment_manager(
      SegmentManager &manager) {
      return config_t{
	manager.get_num_segments(),
	static_cast<size_t>(manager.get_segment_size()),
	2,
	4};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    /**
     * get_next_dirty_extent
     *
     * returns all extents with dirty_from < bound
     */
    using get_next_dirty_extents_ertr = crimson::errorator<>;
    using get_next_dirty_extents_ret = get_next_dirty_extents_ertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
      journal_seq_t bound ///< [in] return extents with dirty_from < bound
    ) = 0;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  extent may be invalid, implementation must correctly
     * handle finding the current instance if it is still alive and
     * otherwise ignore it.
     */
    using rewrite_extent_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    using rewrite_extent_ret = rewrite_extent_ertr::future<>;
    virtual rewrite_extent_ret rewrite_extent(
      Transaction &t,
      CachedExtentRef extent) = 0;
  };

private:
  segment_id_t next = 0;
  const config_t config;

  journal_seq_t journal_tail_target;
  journal_seq_t journal_tail_committed;
  journal_seq_t journal_head;

  ExtentCallbackInterface *ecb = nullptr;

public:
  SegmentCleaner(config_t config)
    : config(config) {}

  get_segment_ret get_segment() final;

  // hack for testing until we get real space handling
  void set_next(segment_id_t _next) {
    next = _next;
  }
  segment_id_t get_next() const {
    return next;
  }


  void put_segment(segment_id_t segment) final;

  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  void update_journal_tail_committed(journal_seq_t committed) final;

  void update_journal_tail_target(journal_seq_t target);

  void init_journal_tail(journal_seq_t tail) {
    journal_tail_target = journal_tail_committed = tail;
  }

  void set_journal_head(journal_seq_t head) {
    assert(journal_head == journal_seq_t() || head >= journal_head);
    journal_head = head;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  /**
   * do_immediate_work
   *
   * Should be invoked prior to submission of any transaction,
   * will piggy-back work required to maintain deferred work
   * constraints.
   */
  using do_immediate_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_immediate_work_ret = do_immediate_work_ertr::future<>;
  do_immediate_work_ret do_immediate_work(
    Transaction &t);


  /**
   * do_deferred_work
   *
   * Should be called at idle times -- will perform background
   * operations based on deferred work constraints.
   *
   * If returned timespan is non-zero, caller should pause calling
   * back into do_deferred_work before returned timespan has elapsed,
   * or a foreground operation occurs.
   */
  using do_deferred_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_deferred_work_ret = do_deferred_work_ertr::future<
    ceph::timespan
    >;
  do_deferred_work_ret do_deferred_work(
    Transaction &t);

private:
  journal_seq_t get_dirty_tail() const {
    auto ret = journal_head;
    ret.segment_seq -= std::min(
      static_cast<size_t>(ret.segment_seq),
      config.target_journal_segments);
    return ret;
  }

  journal_seq_t get_dirty_tail_limit() const {
    auto ret = journal_head;
    ret.segment_seq -= std::min(
      static_cast<size_t>(ret.segment_seq),
      config.max_journal_segments);
    return ret;
  }
};

}
