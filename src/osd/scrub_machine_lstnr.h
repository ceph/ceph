// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

/**
 * \file the PgScrubber interface used by the scrub FSM
 */

#include "common/version.h"
#include "include/Context.h"

#include "osd_types.h"

namespace Scrub {

/// used when PgScrubber is called by the scrub-machine, to tell the FSM
/// how to continue
enum class FsmNext { do_discard, next_chunk, goto_notactive };

/// the interface exposed by the PgScrubber into its internal
/// preemption_data object
struct preemption_t {

  virtual ~preemption_t(){};

  virtual bool is_preemptable() const = 0;

  virtual bool was_preempted() const = 0;

  virtual void adjust_parameters() = 0;

  /**
   *  Try to preempt the scrub.
   *  'true' (i.e. - preempted) if:
   *   preemptable && not already preempted
   */
  virtual bool do_preempt() = 0;

  /**
   *  disables preemptions.
   *  Returns 'true' if we were already preempted
   */
  virtual bool disable_and_test() = 0;
};

}  // namespace Scrub

struct ScrubMachineListener {

  virtual ~ScrubMachineListener(){};

  virtual bool select_range() = 0;

  // walk the log to find the latest update that affects our chunk
  virtual eversion_t search_log_for_updates() const = 0;

  virtual eversion_t get_last_update_applied() const = 0;

  virtual void requeue_waiting() const = 0;

  virtual int pending_active_pushes() const = 0;

  virtual int build_primary_map_chunk() = 0;

  virtual int build_replica_map_chunk(bool qu_priority) = 0;

  virtual void scrub_compare_maps() = 0;

  virtual void on_init() = 0;

  virtual void on_replica_init() = 0;

  virtual void replica_handling_done() = 0;

  virtual void add_delayed_scheduling() = 0;

  virtual bool get_replicas_maps(bool replica_can_preempt) = 0;

  virtual Scrub::FsmNext on_digest_updates() = 0;

  virtual void send_replica_map(bool was_preempted) = 0;

  virtual void replica_update_start_epoch() = 0;

  virtual bool has_pg_marked_new_updates() const = 0;

  virtual void set_subset_last_update(eversion_t e) = 0;

  virtual Scrub::preemption_t* get_preemptor() = 0;

  /**
   *  a "technical" collection of the steps performed once all
   *  rep maps are available:
   *  - the maps are compared
   *  - the scrub region markers (start_ & end_) are advanced
   *  - callbacks and ops that were pending are free to run
   */
  virtual void done_comparing_maps() = 0;

  /// \todo handle the following:
  // waiting_on_whom
  // pg_whoami_
  // cleanup()
  // was_epoch_changed()
};
