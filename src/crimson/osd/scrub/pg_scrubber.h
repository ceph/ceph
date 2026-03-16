// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#pragma once

#include <seastar/core/shared_future.hh>

#include "crimson/common/operation.h"
#include "msg/Message.h"
#include "osd/scrubber_common.h"
#include "scrub_machine.h"
#include "osd/scrubber/scrub_job.h"
#include "scrub_queue.h"
#include "osd/scrubber/scrub_resources.h"

namespace crimson::osd {
class PG;
class ScrubScan;
class ScrubFindRange;
class ScrubReserveRange;
}

namespace crimson::osd::scrub {

struct blocked_range_t {
  hobject_t begin;
  hobject_t end;
  seastar::shared_promise<> p;
};

/**
 * the scrub operation flags. Primary only.
 * Set at scrub start. Checked in multiple locations - mostly
 * at finish.
 */
struct scrub_flags_t {

  unsigned int priority{0};

  /**
   * set by set_op_parameters() for deep scrubs, if the hardware
   * supports auto repairing and osd_scrub_auto_repair is enabled.
   */
  bool auto_repair{false};

  /// this flag indicates that we are scrubbing post repair to verify everything
  /// is fixed (otherwise - PG_STATE_FAILED_REPAIR will be asserted.)
  /// Update (July 2024): now reflects an 'after-repair' urgency.
  bool check_repair{false};

  /// checked at the end of the scrub, to possibly initiate a deep-scrub
  bool deep_scrub_on_error{false};
};

class PGScrubber : public crimson::BlockerT<PGScrubber>, ScrubContext {
  friend class ::crimson::osd::ScrubScan;
  friend class ::crimson::osd::ScrubFindRange;
  friend class ::crimson::osd::ScrubReserveRange;

  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;
  template <typename T = void>
  using ifut =
    ::crimson::interruptible::interruptible_future<
      ::crimson::osd::IOInterruptCondition, T>;

  PG &pg;

  /// PG alias for logging in header functions
  DoutPrefixProvider &dpp;

  ScrubMachine machine;

  std::optional<blocked_range_t> blocked;

  std::optional<eversion_t> waiting_for_update;

  /// the sub-object that manages this PG's scheduling parameters.
  /// An Optional instead of a regular member, as we wish to directly
  /// control the order of construction/destruction.
  std::optional<ScrubJob> m_scrub_job;
    /**
   * once we acquire the local OSD resource, this is set to a wrapper that
   * guarantees that the resource will be released when the scrub is done
   */
  std::unique_ptr<LocalResourceWrapper> m_local_osd_resource;
  bool m_queued_or_active{false};
  std::optional<SchedTarget> m_active_target;
  epoch_t m_epoch_start{0};  ///< the actual epoch when scrubbing started
  scrub_flags_t m_flags;
  bool m_is_deep{false};
  bool m_is_repair{false};
  enum class delay_both_targets_t { no, yes };

  template <typename E>
  void handle_event(E &&e)
  {
    LOG_PREFIX(PGScrubber::handle_event);
    SUBDEBUGDPP(osd, "handle_event: {}", dpp, e);
    machine.process_event(std::forward<E>(e));
  }

public:
  static constexpr const char *type_name = "PGScrubber";
  using Blocker = PGScrubber;
  void dump_detail(Formatter *f) const;

  static inline bool is_scrub_message(Message &m) {
    switch (m.get_type()) {
    case MSG_OSD_REP_SCRUB:
    case MSG_OSD_REP_SCRUBMAP:
      return true;
    default:
      return false;
    }
    return false;
  }

  static utime_t scrub_must_stamp() { return utime_t(1, 1); }
  PGScrubber(PG &pg);
  virtual ~PGScrubber();

  /// setup scrub machine state
  void initiate() { machine.initiate(); }

  /// notify machine on primary that PG is active+clean
  void on_primary_active_clean();

  /// notify machine on replica that PG is active
  void on_replica_activate();

  /// notify machine of interval change
  void on_interval_change();

  /// notify machine that PG has committed up to versino v
  void on_log_update(eversion_t v);

  schedule_result_t start_scrub(
    scrub_level_t s_or_d,
    OSDRestrictions osd_restrictions,
    ScrubPGPreconds pg_cond);

  /// handle scrub request
  void handle_scrub_requested(bool deep);


  /// handle other scrub message
  void handle_scrub_message(Message &m);

  /// notify machine of a mutation of on_object resulting in delta_stats
  void handle_op_stats(
    const hobject_t &on_object,
    object_stat_sum_t delta_stats);

  /// maybe block an op trying to mutate hoid until chunk is complete
  ifut<> wait_scrub(
    PGScrubber::BlockingEvent::TriggerI&& trigger,
    const hobject_t &hoid);

private:
  DoutPrefixProvider &get_dpp() final { return dpp; }

  void schedule_scrub_with_osd() final;
  void update_scrub_job() final;
  void rm_from_osd_scrubbing() final;

  void notify_scrub_start(bool deep) final;
  void notify_scrub_end(bool deep) final;

  void requeue_penalized(
      scrub_level_t s_or_d,
      delay_both_targets_t delay_both,
      delay_cause_t cause,
      utime_t scrub_clock_now);
  bool reserve_local(const SchedTarget& trgt);

  const std::set<pg_shard_t> &get_ids_to_scrub() const final;

  chunk_validation_policy_t get_policy() const final;

  void request_range(const hobject_t &start) final;
  void reserve_range(const hobject_t &start, const hobject_t &end) final;
  void release_range() final;
  void scan_range(
    pg_shard_t target,
    eversion_t version,
    bool deep,
    const hobject_t &start,
    const hobject_t &end) final;
  bool await_update(const eversion_t &version) final;
  void generate_and_submit_chunk_result(
    const hobject_t &begin,
    const hobject_t &end,
    bool deep) final;
  void emit_chunk_result(
    const request_range_result_t &range,
    chunk_result_t &&result) final;
  void emit_scrub_result(
    bool deep,
    object_stat_sum_t scrub_stats) final;

  sched_conf_t populate_config_params() const;
  void update_targets(utime_t scrub_clock_now);
  bool is_queued_or_active() const  {
    return m_queued_or_active;
  };
  void set_op_parameters(ScrubPGPreconds pg_cond);
  void set_queued_or_active() {
    m_queued_or_active = true;
  }
  void clear_queued_or_active()
  {
    if (m_queued_or_active) {
      m_queued_or_active = false;
    }
  }
  void cleanup_on_finish();
  void clear_pgscrub_state();
  void reset_internal_state();
  std::string_view registration_state() const;
};

} // namespace crimson::osd::scrub
#include <fmt/format.h>

namespace fmt {

template <>
struct formatter<crimson::osd::scrub::blocked_range_t> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const crimson::osd::scrub::blocked_range_t& r,
              FormatContext& ctx) const {
    return format_to(
        ctx.out(),
        "blocked_range[{} -> {}]",
        r.begin,
        r.end);
  }
};

} // namespace fmt
