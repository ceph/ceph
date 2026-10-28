// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/op_stage_stats.h"

#include <utility>

#include "crimson/common/lat_histogram.h"

namespace crimson::osd {

namespace sm = seastar::metrics;
using crimson::common::add_lat_sample;
using crimson::common::init_lat_histogram;

template <typename Stage>
void op_stage_stats_t::add_timer(tiers_t<Stage> &hists, const timer_t<Stage> &t)
{
  constexpr auto TOTAL = static_cast<std::size_t>(Stage::TOTAL);
  const auto total = t.last - t.start;
  auto add_tier = [&](auto &tier) {
    for (std::size_t i = 0; i < TOTAL; ++i) {
      if (t.marked[i]) {
        add_lat_sample(tier[i], t.durs[i]);
      }
    }
    add_lat_sample(tier[TOTAL], total);
  };
  add_tier(hists[ALL]);
  if (total >= SLOW) {
    add_tier(hists[TIER_SLOW]);
  }
  if (total >= VERY_SLOW) {
    add_tier(hists[TIER_VERY_SLOW]);
  }
}

void op_stage_stats_t::add(const timer_t<client_write_stage_t> &t)
{
  add_timer(client_write, t);
}

void op_stage_stats_t::add(const timer_t<rep_op_stage_t> &t)
{
  add_timer(rep_op, t);
}

void op_stage_stats_t::add(repl_lat_t what, clock_t::duration dur)
{
  add_lat_sample(repl[static_cast<std::size_t>(what)], dur);
}

void op_stage_stats_t::register_metrics()
{
  const std::pair<client_write_stage_t, const char*> client_write_stages[] = {
    {client_write_stage_t::DISPATCH,   "dispatch"},
    {client_write_stage_t::PG_READY,   "pg_ready"},
    {client_write_stage_t::THROTTLE,   "throttle"},
    {client_write_stage_t::PG_ORDER,   "pg_order"},
    {client_write_stage_t::OBJ_ORDER,  "obj_order"},
    {client_write_stage_t::PREPARE,    "prepare"},
    {client_write_stage_t::EXECUTE,    "execute"},
    {client_write_stage_t::SUBMIT,     "submit"},
    {client_write_stage_t::WAIT_REPOP, "wait_repop"},
    {client_write_stage_t::REPLY,      "reply"},
    {client_write_stage_t::TOTAL,      "total"},
  };
  const std::pair<rep_op_stage_t, const char*> rep_op_stages[] = {
    {rep_op_stage_t::DISPATCH, "dispatch"},
    {rep_op_stage_t::PG_ORDER, "pg_order"},
    {rep_op_stage_t::THROTTLE, "throttle"},
    {rep_op_stage_t::PREPARE,  "prepare"},
    {rep_op_stage_t::COMMIT,   "commit"},
    {rep_op_stage_t::REPLY,    "reply"},
    {rep_op_stage_t::TOTAL,    "total"},
  };
  const char* tier_names[] = {"all", "slow", "very_slow"};

  auto add_stages = [this, &tier_names](
    auto &tiers, const auto &stages, const char *name, const char *desc) {
    for (std::size_t tier = 0; tier < TIER_MAX; ++tier) {
      for (auto &[stage, stage_name] : stages) {
        auto &hist = tiers[tier][static_cast<std::size_t>(stage)];
        init_lat_histogram(hist);
        metrics.add_group("osd_op", {
          sm::make_histogram(name,
            [&hist]() -> sm::histogram& { return hist; },
            sm::description(desc),
            {sm::label_instance("stage", stage_name),
             sm::label_instance("tail", tier_names[tier])}),
        });
      }
    }
  };
  add_stages(client_write, client_write_stages, "client_write_stage_lat_ms",
    "per-stage latency of client writes on the primary");
  add_stages(rep_op, rep_op_stages, "rep_op_stage_lat_ms",
    "per-stage latency of repops on a replica");

  const std::pair<repl_lat_t, const char*> repl_lats[] = {
    {repl_lat_t::REPOP_RTT,    "repop_rtt"},
    {repl_lat_t::LOCAL_COMMIT, "local_commit"},
    {repl_lat_t::WAIT_PEERS,   "wait_peers"},
  };
  for (auto &[what, what_name] : repl_lats) {
    auto &hist = repl[static_cast<std::size_t>(what)];
    init_lat_histogram(hist);
    metrics.add_group("osd_op", {
      sm::make_histogram("replication_lat_ms",
        [&hist]() -> sm::histogram& { return hist; },
        sm::description("replication latency seen from the primary"),
        {sm::label_instance("what", what_name)}),
    });
  }
}

} // namespace crimson::osd
