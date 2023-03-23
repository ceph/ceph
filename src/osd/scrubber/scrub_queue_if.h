// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

namespace Scrub {
class ScrubSchedListener;
struct SchedEntry;
struct sched_conf_t;


/**
 *  the interface used by the PgScrubber and by the ScrubJob (a component
 *  of the PgScrubber) to access the scrub scheduling functionality.
 *  Separated from the actual implementation, mostly due to cyclic dependencies.
 */
struct ScrubQueueOps {

  // a mockable ceph_clock_now(), to allow unit-testing of the scrub scheduling
  virtual utime_t scrub_clock_now() const = 0;

  virtual void scrub_next_in_queue(utime_t loop_id) = 0;

  /**
   * let the ScrubQueue know that it should terminate the
   * current search for a scrub candidate (the search that was initiated
   * from the tick_without_osd_lock()).
   * Will be called once a triggered scrub has past the point of securing
   * replicas.
   */
  virtual void initiation_loop_done(utime_t loop_id) = 0;

  virtual sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const = 0;

  virtual void remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  /**
   * Insert both targets into the queue (but only if urgency>off)
   * Note: asserts if any of the targets has urgency==off.
   */
  virtual void enqueue_targets(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) = 0;

  virtual void enqueue_target(SchedEntry t) = 0;

  virtual ~ScrubQueueOps() = default;

  /// a debug interface, used to verify the entries # for a PG
  virtual size_t count_queued(spg_t pgid) const = 0;
};

/**
 * a wrapper for the 'participation in the scrub scheduling loop' state.
 * A scrubber holding this object is the one currently selected by the OSD
 * (i.e. by the ScrubQueue object) to scrub. The ScrubQueue will not try
 * the next PG in the queue, until and if the current PG releases the object
 * with a failure indication (via go_for_next_in_queue()).
 * conclude_candidates_selection() (a success indication) or a destruction of
 * the wrapper object will stop the scrub-scheduling loop.
 */
class SchedLoopHolder {
 public:
  SchedLoopHolder(ScrubQueueOps& queue, utime_t loop_id)
      : m_loop_id{loop_id}
      , m_queue{queue}
  {}

  /*
   * the dtor will signal 'success', as in 'do not continue the loop'.
   * It is assumed that all relevant failures call 'go_for_next_in_queue()'
   * explicitly, and that the destruction of a 'loaded' object is a bug.
   * Treating that as a 'do not continue' limits the possible damage.
   */
  ~SchedLoopHolder();

  /*
   * The loop should be discontinued. Either because of a success
   * (the PG is now scrubbing) or because of a failure that would
   * prevent us from trying more targets
   */
  void conclude_candidates_selection();

  void go_for_next_in_queue();

  /// for logs/debugging
  std::optional<utime_t> loop_id() const { return m_loop_id; }

 private:
  /**
   * the ID of the loop (which is also the loop's original creation time)
   * Reset to 'nullopt' when the loop is concluded.
   */
  std::optional<utime_t> m_loop_id;

  ScrubQueueOps& m_queue;
};

}  // namespace Scrub

// required by the not_before queue implementation:
bool operator<(const Scrub::SchedEntry& lhs, const Scrub::SchedEntry& rhs);

namespace fmt {
template <>
struct formatter<Scrub::SchedLoopHolder> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedLoopHolder& loop, FormatContext& ctx)
  {
    if (loop.loop_id()) {
      return format_to(ctx.out(), "loop_id:{}", *loop.loop_id());
    } else {
      return format_to(ctx.out(), "loop_id:None");
    }
  }
};
}  // namespace fmt
