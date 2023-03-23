// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_types_fmt.h"
#include "osd/scrubber/scrub_queue_if.h"
#include "osd/scrubber/scrub_resources.h"

namespace Scrub {
class ScrubSchedListener;
class ScrubJob;
struct SchedEntry;
}  // namespace Scrub

// clang-format off
/*
 * The main components involved in managing a "scrub initiation loop"
 * (the process, initiated from within the OSD tick, to find a PG to
 * scrub - but one that actually manages to reserve its replicas and
 * start scrubbing) are:
 *
 *  OSD
 * |
 * |
 * |
 * |
 * |    ScrubQueue
 * |    -----------
 * |
 * |           |
 * |           |
 * +-> initiate_a_scrub()
 *             |
 *             +--->  selects a PG
 *             |
 *             +--->  initializes m_initiation_loop (an object detailing the
 *             |      state of the current loop, i.e. the current attempt to
 *             |      find a PG to scrub)
 *             |
 *             |      +------------------------+
 *             |      | - restrictions         |
 *             |      | - max tries            |
 *             |      | - started at           |
 *             |      | - ....                 |
 *             |      |                        |
 *             |      |                        |
 *             |      |                        |
 *             |      +------------------------+
 *             |                                 PG & PgScrubber
 *             |                                ------------------
 *             +-------------------------------> start_scrubbing()
 *             |                                  +
 *             |                                  start_scrub()
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |creates a wrapper object around
 *             |                                  | |"I am the chosen one in the
 *             |                                  | | current loop" (m_schedloop_step)
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |Guaranteed to cause (dtor) one of:
 *             |        loop ID (matching ^)      | |
 *             |  <-------------------------------+-+-- try the next one in queue
 *             |        scrub_next_in_queue()     | |   (as I cannot be scrubbed)
 *             |                                  | |
 *             |                                  | |Or:
 *             |                                  | |
 *             |  <-------------------------------+-+-- loop concluded due to failure
 *             |                                  | |
 *             |        initiation_loop_done()    | |Or:
 *             |                                  | |   I am happily scrubbing. Loop
 *             |  <-------------------------------+-+-- concluded.
 *             |                                  | |   (*) after replica reservation
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |
 *             |                                  | |
 */
// clang-format on

struct ScrubQueueStats {
  uint16_t num_ready{0};
  uint16_t num_total{0};
};

/**
 * the interface used by the ScrubQueue to access the underlying
 * queue of scrub targets.
 * Separated from the actual implementation for three reasons:
 * - more than one implementation is possible (
 *   and more than one implementation was tried during development);
 * - allows for cleaner division into 'commits'; and
 * - supports unit-testing of the ScrubQueue
 */
struct ScrubQueueImp_IF {
  using SchedEntry = Scrub::SchedEntry;

  virtual void enqueue_entry(const SchedEntry& entry) = 0;

  /*
   * \retval true  entry was found and removed
   */
  virtual bool remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  // note: not 'const', as get_stats() updates the time (the 'not before'
  // threshold)
  virtual ScrubQueueStats get_stats(utime_t scrub_clock_now) = 0;

  /**
   * @return a scrub target that is ready to be scrubbed, or std::nullopt.
   *
   * The selected target is removed from the queue.
   */
  virtual std::optional<SchedEntry> pop_ready_pg(utime_t scrub_clock_now) = 0;

  /**
   * Creates a new 'scrubs' section in the provided formatter, and
   * dumps all queue entries (line by line, eligible entries first)
   * into it.
   */
  virtual void dump_scrubs(ceph::Formatter* f) const = 0;

  using EntryPred = std::function<bool(const SchedEntry&, bool is_eligible)>;

  /**
   * the set of all PGs named by the entries in the queue (but only those
   * entries that satisfy the predicate)
   */
  virtual std::set<spg_t> get_pgs(EntryPred) const = 0;

  /// a copy of all entries in the queue that satisfy the predicate
  virtual std::vector<SchedEntry> get_entries(EntryPred) const = 0;

  virtual ~ScrubQueueImp_IF() = default;
};

class ScrubQueueImp;

/**
 * The 'ScrubQueue' is a "sub-component" of the OSD. It is responsible (mainly) `
 * for selecting the PGs to be scrubbed and initiating the scrub operation.
 * 
 * Other responsibilities "traditionally" associated with the scrub-queue are:
 * - monitoring system load, and
 * - monitoring the number of scrubs performed by the OSD, as either
 *   primary or replica roles.
 * 
 * The main functionality is implemented in two layers:
 * - an upper layer (the 'ScrubQueue' class) responsible for initiating a
 *   scrub on the top-most (priority-wise) eligible PG;
 * - a prioritized container of "scheduling targets". A target conveys both the
 *   PG to be scrubbed, and the scrub type (deep or shallow). It contains the
 *   information required for prioritizing a specific scrub request
 *   compared to all other queued requests.
 *
 * \note: the following invariants hold:
 * - there are at most two objects queued for each PG (one for each scrub
 *   level).
 * - when a queue element is removed, the corresponding object held by the
 *   PgScrubber is (not necessarily immediately) marked as 'not in the queue'.
 */
class ScrubQueue : public Scrub::ScrubQueueOps {

  /**
   * ScrubStartLoop: an aux structure to help with the bookkeeping involved with
   * an on-going 'scrub initiation loop'.
   */
  struct ScrubStartLoop {
    ScrubStartLoop(
	utime_t now,
	int budget,
	Scrub::OSDRestrictions preconds,
	spg_t first_tried,
	scrub_level_t first_level_tried)
	: loop_id{now}
	, retries_budget{budget}
	, env_restrictions{preconds}
	, first_pg_tried{first_tried}
	, first_level_tried{first_level_tried}
    {}

    Scrub::loop_token_t loop_id;  // its ID - and its start time

    /// how many scrub queue entries would we try at most
    int retries_budget;

    /// restrictions on the next scrub imposed by OSD environment
    Scrub::OSDRestrictions env_restrictions;

    /// noting the 1'st entry tried in this loop, to avoid looping on the same
    /// sched target
    spg_t first_pg_tried;
    scrub_level_t first_level_tried;

    int attempted{0};  // how many retries were done

    ///\todo consider adding 'last update' time, to detect a stuck loop
  };

 public:
  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;

  friend class TestOSDScrub;
  friend class ScrubQueueTestWrapper;  ///< unit-tests structure

  using SchedEntry = Scrub::SchedEntry;
  using SchedulingQueue = std::deque<SchedEntry>;

  std::ostream& gen_prefix(std::ostream& out) const;

  // //////////////////////////////////////////////////////////////
  // the ScrubQueueOps interface (doc in scrub_queue_if.h)

  utime_t scrub_clock_now() const override;

  Scrub::sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const override;

  void remove_entry(spg_t pgid, scrub_level_t s_or_d) final;

  void enqueue_target(SchedEntry t) final;

  void enqueue_targets(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) final;

  void scrub_next_in_queue(Scrub::loop_token_t loop_id) final;

  void initiation_loop_done(Scrub::loop_token_t loop_id) final;

  size_t count_queued(spg_t pgid) const final;

  // ///////////////////////////////////////////////////////////////
  // outside the scope of the I/F used by the ScrubJob:

  /**
   * the main entry point for the OSD. Called in OSD::tick_without_osd_lock()
   * to determine if there are PGs that are ready to be scrubbed, and to
   * initiate a scrub of one of those that are ready.
   */
  void initiate_a_scrub(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active);

  /*
   * handles a change to the configuration parameters affecting the scheduling
   * of scrubs.
   */
  void on_config_times_change();

  void dump_scrubs(ceph::Formatter* f) const;

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  // resource reservation management

  Scrub::ScrubResources& resource_bookkeeper();
  const Scrub::ScrubResources& resource_bookkeeper() const;
  /// and the logger function used by that bookkeeper:
  void log_fwd(std::string_view text);

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

 private:
  int get_blocked_pgs_count() const;

 public:
  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times.
   * Scrubbing will continue, but the delays will be controlled by a separate
   * (read - with higher value) configuration element
   * (osd_scrub_extended_sleep).
   */
  std::chrono::milliseconds required_sleep_time(bool high_priority_scrub) const;

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average() {
    return m_load_tracker.update_load_average();
  }

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;
  Scrub::ScrubResources m_osd_resources;

#ifdef WITH_SEASTAR
  auto& conf() const { return local_conf(); }
#else
  auto& conf() const { return cct->_conf; }
#endif

  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  // the underlying implementation of the scrub queue
  std::unique_ptr<ScrubQueueImp_IF> m_queue_impl;

  /**
   * m_initiation_loop, when set, indicates that we are traversing the scrub
   * queue looking for a PG to scrub. It also maintains the loop ID (its start
   * time) and the number of retries left.
   */
  std::optional<ScrubStartLoop> m_initiation_loop;

  /**
   * m_loop_lock protects 'm_initiation_loop'
   *
   * \attn never take 'jobs_lock' while holding this lock!
   */
  ceph::mutex m_loop_lock{ceph::make_mutex("ScrubQueue::m_loop_lock")};

  std::string log_prefix;

  /**
   * The set of "environmental" restrictions that possibly affect the
   * scheduling of scrubs (e.g. preventing some non-urgent scrubs from being
   * scheduled).
   * If restrictions_on_scrubbing() determines that no scrubbing is possible,
   * std::nullopt is returned, which should result in no more attempted scrubs
   * at this tick (this 'scheduling loop').
   */
  std::optional<Scrub::OSDRestrictions> restrictions_on_scrubbing(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

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

  std::atomic_bool a_pg_is_reserving{false};

  /**
   * \returns true if the current time is within the scrub time window
   *
   * Using the 'current scrub time' as maintained by the ScrubQueue
   * object (which is the same as the 'current time' used by the OSD -
   * unless we are in a unit-test).
   */
  [[nodiscard]] bool scrub_time_permit() const;
  [[nodiscard]] bool scrub_time_permit(utime_t t) const;

  // note: sizeof(ScrubQueueStats)==4
  void debug_log_queue(ScrubQueueStats queue_stats) const;


  /**
   * tracking the average load on the CPU. Used both by the
   * OSD logger, and by the scrub queue (as no scrubbing is allowed if
   * the load is too high).
   */
  class LoadTracker {
    CephContext* cct;
    const ceph::common::ConfigProxy& cnf;
    std::string log_prefix;
    double daily_loadavg{0.0};

   public:
    explicit LoadTracker(
        CephContext* cct,
	const ceph::common::ConfigProxy& config,
        int node_id);

    std::optional<double> update_load_average();

    [[nodiscard]] bool scrub_load_below_threshold() const;

    std::ostream& gen_prefix(std::ostream& out) const;
  };
  LoadTracker m_load_tracker;

 public:  // used by the unit-tests
  /**
   * unit-tests override this function to return a mock time
   */
  virtual utime_t time_now() const { return ceph_clock_now(); }
};
