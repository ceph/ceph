// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
// clang-format off
/*
  ┌───────────────────────┐
  │ OSD                   │
  │ OSDService            │
  │                       │
  │ ┌─────────────────────┤
  │ │                     │
  │ │   OsdScrub          │
  │ │                    ─┼───┐
  │ │                     │   │
  └─┴─────────────────────┘   │   Owns & uses the following
                              │   ScrubQueue interfaces:
                              │
                              │
                              │   - resource management (*1)
                              │
                              │   - environment conditions (*2)
                              │
                              │   - scrub scheduling (*3)
                              │
                              │
                              │
   ScrubQueue                 │
  ┌───────────────────────────▼────────────┐
  │                                        │
  │                                        │
  │  not_before_queue_t to_scrub <>────────┼────────┐
  │                                        │        │
  │                                        │        │
  │  OSD_wide resource counters            │        │
  │                                        │        │
  │                                        │        │
  │  "env scrub conditions" monitoring     │        │
  │                                        │        │
  │                                        │        │
  │                                        │        │
  │                                        │        │
  └─▲──────────────────────────────────────┘        │
    │                                               │
    │                                               │
    │uses interface <4>                             │
    │                                               │
    │                                               │
    │            ┌──────────────────────────────────┘
    │            │
    │            │
    │      ┌─────▼──────┐
    │      │Copy of     │
    │      │job's       ├┐
    │      │sched targts││
    │      │            │┼┐
    │      │            │┼┘◄────────────────────────┐
    └──────┤            ││                          │
           │            ││                          │
           │            ││                          │
           │            ││                          │
           └┬───────────┼│                          │
            └─┼┼┼┼┼┼┼┼┼┼┼│                          │
              └──────────┘                          │
                                                    │
                                                    │                                                    │
                                                    │
  ┌─────────────────────────────────┐               │
  │                               <>│               │
  │PgScrubber                       │               │
  │               ┌─────────────────┴───┐           │
  │               │ScrubJob             │           │
  │               │                     │           │
  │               │     ┌───────────────┤           │
  │               │     │Sched target   ├───────────┘
  └───────────────┤     └───────────────┤
                  │                     │           ^
                  │     ┌───────────────┤           |
                  │     │Sched target   ├───────────┘
                  │     └───────────────┤
                  └─────────────────────┘


ScrubQueue interfaces (main functions):

<1> - OSD/PG resources management:

  - can_inc_scrubs()
  - {inc/dec}_scrubs_{local/remote}()
  - dump_scrub_reservations()

<2> - environment conditions:

  - update_loadavg()

  - scrub_load_below_threshold()
  - scrub_time_permit()

<3> - scheduling scrubs:

  - select_pg_and_scrub()
  - dump_scrubs()

<4> - manipulating a job's state:

  - remove_from_osd_queue()
  - update_job()

 */
// clang-format on

#include <algorithm>
#include <optional>

#include "common/AsyncReserver.h"
#include "common/not_before_queue.h"
#include "utime.h"
#include "osd/scrubber/scrub_job.h"
#include "osd/PG.h"

namespace Scrub {

using namespace ::std::literals;

// the OSD services provided to the scrub scheduler
class ScrubSchedListener {
 public:
  virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')

  /**
   * locks the named PG, returning an RAII wrapper that unlocks upon
   * destruction.
   * returns nullopt if failing to lock.
   */
  virtual std::optional<PGLockWrapper> get_locked_pg(spg_t pgid) = 0;

  /**
   * allow access to the scrub_reserver, the AsyncReserver that keeps track
   * of 'remote replica reservations'.
   */
  virtual AsyncReserver<spg_t, Finisher>& get_scrub_reserver() = 0;

  virtual ~ScrubSchedListener() {}
};

}  // namespace Scrub


/**
 * the queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain
 * time.
 */
class ScrubQueue {
 public:
  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;

  friend class TestOSDScrub;
  friend class ScrubSchedTestWrapper; ///< unit-tests structure
  using sched_params_t = Scrub::sched_params_t;


  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(spg_t pgid);

  /// A predicate over the entries in the queue
  using EntryPred =
      std::function<bool(const ::Scrub::SchedEntry&, bool only_eligibles)>;

  /// a predicate to check entries against some common temporary restrictions
  using EligibilityPred = std::function<
      bool(const Scrub::SchedEntry&, const Scrub::OSDRestrictions&, utime_t)>;

  /**
   * the set of all PGs named by the entries in the queue (but only those
   * entries that satisfy the predicate)
   */
  std::set<spg_t> get_pgs(const EntryPred&) const;

  /**
   * Add the scrub job (both SchedTargets) to the list of jobs (i.e. list of
   * PGs) to be periodically scrubbed by the OSD.
   */
  void enqueue_scrub_job(const Scrub::ScrubJob& sjob);

  /**
   * copy the scheduling element (the SchedEntry sub-object) part of
   * the SchedTarget to the queue.
   */
  void enqueue_target(const Scrub::SchedTarget& trgt);

  void dequeue_target(spg_t pgid, scrub_level_t s_or_d);

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;

 public:
  void dump_scrubs(ceph::Formatter* f) const;

  void for_each_job(
      std::function<void(const Scrub::SchedEntry&)> fn,
      int max_jobs) const;

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);
  int get_blocked_pgs_count() const;

  /**
   * find the nearest scheduling entry that is ready to
   * to be scrubbed (taking 'restrictions' into account).
   * The selected entry in the queue is dequeued and returned.
   * nullopt is returned if no such entry exists.
   */
  std::optional<Scrub::SchedEntry> pop_ready_entry(
    EligibilityPred eligibility_pred,
    Scrub::OSDRestrictions restrictions,
    utime_t time_now);

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;

#ifdef WITH_CRIMSON
  auto& conf() const { return local_conf(); }
#else
  auto& conf() const { return cct->_conf; }
#endif

  /**
   *  jobs_lock protects the job container.
   *
   *  Note that PG locks should not be acquired while holding jobs_lock.
   */
  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  not_before_queue_t<Scrub::SchedEntry> to_scrub;

  /**
   * The scrubbing of PGs might be delayed if the scrubbed chunk of objects is
   * locked by some other operation. A bug might cause this to be an infinite
   * delay. If that happens, the OSDs "scrub resources" (i.e. the
   * counters that limit the number of concurrent scrub operations) might
   * be exhausted.
   * We do issue a cluster-log warning in such occasions, but that message is
   * easy to miss. The 'some pg is blocked' global flag is used to note the
   * existence of such a situation in the scrub-queue log messages.
   */
  std::atomic_int_fast16_t blocked_scrubs_cnt{0};

  /**
   * remove the entry from the queue.
   * returns: true if it was there, false otherwise.
   */
  bool remove_entry_unlocked(spg_t pgid, scrub_level_t s_or_d);

protected: // used by the unit-tests
  /**
   * unit-tests will override this function to return a mock time
   */
  virtual utime_t time_now() const { return ceph_clock_now(); }
};
