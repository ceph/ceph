// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <array>
#include <chrono>
#include <cstdint>

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_types.hh>

namespace crimson::osd {

// Stages of a client write on the primary, in order.
enum class client_write_stage_t : uint8_t {
  DISPATCH = 0, // arrival until the op runs on the PG's shard
  PG_READY,     // osdmap and PG active waits
  THROTTLE,     // mClock admission
  PG_ORDER,     // entering the PG's get_obc stage
  OBJ_ORDER,    // entering the object's process stage (one write at a time)
  PREPARE,      // missing/dup/scrub checks, obc load and lock
  EXECUTE,      // running the ops
  SUBMIT,       // local transaction submit and repop sends
  WAIT_REPOP,   // waiting for local commit and all replicas
  REPLY,        // send_reply stage and reply send
  TOTAL,
  MAX
};

// Stages of a repop on a replica, in order.
enum class rep_op_stage_t : uint8_t {
  DISPATCH = 0, // arrival until the op runs on the PG's shard
  PG_ORDER,     // entering the PG's repop process stage and osdmap wait
  THROTTLE,     // mClock admission
  PREPARE,      // building and submitting the local transaction
  COMMIT,       // waiting for the local commit
  REPLY,        // send_reply stage and reply send
  TOTAL,
  MAX
};

// Replication as seen from the primary.
enum class repl_lat_t : uint8_t {
  REPOP_RTT = 0, // repop sent until that replica's reply, per replica
  LOCAL_COMMIT,  // repops sent until the primary's own commit
  WAIT_PEERS,    // primary's own commit until all replicas acked
  MAX
};

class op_stage_stats_t {
public:
  using clock_t = seastar::lowres_clock;

  // Collects a sample per stage, to be added once the op completes.
  template <typename Stage>
  class timer_t {
  public:
    explicit timer_t(clock_t::time_point start = clock_t::now())
      : start(start), last(start) {}

    void mark(Stage stage) {
      auto now = clock_t::now();
      durs[static_cast<std::size_t>(stage)] += now - last;
      marked[static_cast<std::size_t>(stage)] = true;
      last = now;
    }

  private:
    friend class op_stage_stats_t;
    static constexpr auto N = static_cast<std::size_t>(Stage::MAX);
    clock_t::time_point start;
    clock_t::time_point last;
    std::array<clock_t::duration, N> durs{};
    std::array<bool, N> marked{};
  };

  void register_metrics();

  void add(const timer_t<client_write_stage_t> &t);
  void add(const timer_t<rep_op_stage_t> &t);
  void add(repl_lat_t what, clock_t::duration dur);

private:
  // Ops slower than these in total are also counted in the "slow" and
  // "very_slow" tiers, to show which stages the tail spends time in.
  static constexpr auto SLOW = std::chrono::milliseconds(10);
  static constexpr auto VERY_SLOW = std::chrono::milliseconds(50);
  enum tier_t : std::size_t { ALL = 0, TIER_SLOW, TIER_VERY_SLOW, TIER_MAX };

  template <typename Stage>
  using tiers_t = std::array<
    std::array<seastar::metrics::histogram,
               static_cast<std::size_t>(Stage::MAX)>,
    TIER_MAX>;

  template <typename Stage>
  static void add_timer(tiers_t<Stage> &hists, const timer_t<Stage> &t);

  tiers_t<client_write_stage_t> client_write;
  tiers_t<rep_op_stage_t> rep_op;
  std::array<seastar::metrics::histogram,
             static_cast<std::size_t>(repl_lat_t::MAX)> repl;

  seastar::metrics::metric_groups metrics;
};

} // namespace crimson::osd
