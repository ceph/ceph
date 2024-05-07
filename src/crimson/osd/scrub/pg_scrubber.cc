// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <fmt/ranges.h>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/scrub_events.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "pg_scrubber.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

void PGScrubber::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();
}

PGScrubber::PGScrubber(PG &pg) : pg(pg), dpp(pg), machine(*this) {}

void PGScrubber::on_primary_active_clean()
{
  LOG_PREFIX(PGScrubber::on_primary_active_clean);
  DEBUGDPP("", pg);
  handle_event(events::primary_activate_t{});
}

void PGScrubber::on_replica_activate()
{
  LOG_PREFIX(PGScrubber::on_replica_activate);
  DEBUGDPP("", pg);
  handle_event(events::replica_activate_t{});
}

void PGScrubber::on_interval_change()
{
  LOG_PREFIX(PGScrubber::on_interval_change);
  DEBUGDPP("", pg);
  /* Once reservations and scheduling are introduced, we'll need an
   * IntervalChange event to drop remote resources (they'll be automatically
   * released on the other side) */
  handle_event(events::reset_t{});
  waiting_for_update = std::nullopt;
  ceph_assert(!blocked);
}

void PGScrubber::on_log_update(eversion_t v)
{
  LOG_PREFIX(PGScrubber::on_interval_change);
  if (waiting_for_update && v >= *waiting_for_update) {
    DEBUGDPP("waiting_for_update: {}, v: {}", pg, *waiting_for_update, v);
    handle_event(await_update_complete_t{});
    waiting_for_update = std::nullopt;
  }
}

void PGScrubber::handle_scrub_requested(bool deep)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  DEBUGDPP("deep: {}", pg, deep);
  handle_event(events::start_scrub_t{deep});
}

void PGScrubber::handle_scrub_message(Message &_m)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  switch (_m.get_type()) {
  case MSG_OSD_REP_SCRUB: {
    MOSDRepScrub &m = *static_cast<MOSDRepScrub*>(&_m);
    DEBUGDPP("MOSDRepScrub: {}", pg, m);
    handle_event(events::replica_scan_t{
	m.start, m.end, m.scrub_from, m.deep
      });
    break;
  }
  case MSG_OSD_REP_SCRUBMAP: {
    MOSDRepScrubMap &m = *static_cast<MOSDRepScrubMap*>(&_m);
    DEBUGDPP("MOSDRepScrubMap: {}", pg, m);
    ScrubMap map;
    auto iter = m.get_data().cbegin();
    ::decode(map, iter);
    handle_event(scan_range_complete_t{
	m.from, std::move(map)
      });
    break;
  }
  default:
    DEBUGDPP("invalid message: {}", pg, _m);
    ceph_assert(is_scrub_message(_m));
  }
}

void PGScrubber::handle_op_stats(
  const hobject_t &on_object,
  object_stat_sum_t delta_stats) {
  handle_event(events::op_stats_t{on_object, delta_stats});
}

PGScrubber::ifut<> PGScrubber::wait_scrub(
  PGScrubber::BlockingEvent::TriggerI&& trigger,
  const hobject_t &hoid)
{
  LOG_PREFIX(PGScrubber::wait_scrub);
  if (blocked && (hoid >= blocked->begin) && (hoid < blocked->end)) {
    DEBUGDPP("blocked: {}, hoid: {}", pg, *blocked, hoid);
    return trigger.maybe_record_blocking(
      blocked->p.get_shared_future(),
      *this);
  } else {
    return seastar::now();
  }
}

void PGScrubber::notify_scrub_start(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_start);
  DEBUGDPP("deep: {}", pg, deep);
  pg.peering_state.state_set(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_set(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}

void PGScrubber::notify_scrub_end(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_end);
  DEBUGDPP("deep: {}", pg, deep);
  pg.peering_state.state_clear(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_clear(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}

const std::set<pg_shard_t> &PGScrubber::get_ids_to_scrub() const
{
  return pg.peering_state.get_actingset();
}

chunk_validation_policy_t PGScrubber::get_policy() const
{
  return chunk_validation_policy_t{
    pg.get_primary(),
    std::nullopt /* stripe_info, populate when EC is implemented */,
    crimson::common::local_conf().get_val<Option::size_t>(
      "osd_max_object_size"),
    crimson::common::local_conf().get_val<std::string>(
      "osd_hit_set_namespace"),
    crimson::common::local_conf().get_val<Option::size_t>(
      "osd_deep_scrub_large_omap_object_value_sum_threshold"),
    crimson::common::local_conf().get_val<uint64_t>(
      "osd_deep_scrub_large_omap_object_key_threshold")
  };
}

void PGScrubber::request_range(const hobject_t &start)
{
  LOG_PREFIX(PGScrubber::request_range);
  DEBUGDPP("start: {}", pg, start);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubFindRange
    >(start, &pg);
}

/* TODO: This isn't actually enough.  Here, classic would
 * hold the pg lock from the wait_scrub through to IO submission.
 * ClientRequest, however, isn't in the processing ExclusivePhase
 * bit yet, and so this check may miss ops between the wait_scrub
 * check and adding the IO to the log. */

void PGScrubber::reserve_range(const hobject_t &start, const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::reserve_range);
  DEBUGDPP("start: {}, end: {}", pg, start, end);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubReserveRange
    >(start, end, &pg);
}

void PGScrubber::release_range()
{
  LOG_PREFIX(PGScrubber::release_range);
  ceph_assert(blocked);
  DEBUGDPP("blocked: {}", pg, *blocked);
  pg.background_process_lock.unlock();
  blocked->p.set_value();
  blocked = std::nullopt;
}

void PGScrubber::scan_range(
  pg_shard_t target,
  eversion_t version,
  bool deep,
  const hobject_t &start,
  const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::scan_range);
  DEBUGDPP("target: {}, version: {}, deep: {}, start: {}, end: {}",
	   pg, target, version, deep, start, end);
  if (target == pg.get_pg_whoami()) {
    std::ignore = pg.shard_services.start_operation_may_interrupt<
      interruptor, ScrubScan
      >(&pg, deep, true /* local */, start, end);
  } else {
    std::ignore = pg.shard_services.send_to_osd(
      target.osd,
      crimson::make_message<MOSDRepScrub>(
	spg_t(pg.get_pgid().pgid, target.shard),
	version,
	pg.get_osdmap_epoch(),
	pg.get_osdmap_epoch(),
	start,
	end,
	deep,
	false /* allow preemption -- irrelevant for replicas TODO */,
	64 /* priority, TODO */,
	false /* high_priority TODO */),
      pg.get_osdmap_epoch());
  }
}

bool PGScrubber::await_update(const eversion_t &version)
{
  LOG_PREFIX(PGScrubber::await_update);
  DEBUGDPP("version: {}", pg, version);
  ceph_assert(!waiting_for_update);
  auto& log = pg.peering_state.get_pg_log().get_log().log;
  eversion_t current = log.empty() ? eversion_t() : log.rbegin()->version;
  if (version <= current) {
    return true;
  } else {
    waiting_for_update = version;
    return false;
  }
}

void PGScrubber::generate_and_submit_chunk_result(
  const hobject_t &begin,
  const hobject_t &end,
  bool deep)
{
  LOG_PREFIX(PGScrubber::generate_and_submit_chunk_result);
  DEBUGDPP("begin: {}, end: {}, deep: {}", pg, begin, end, deep);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubScan
    >(&pg, deep, false /* local */, begin, end);
}

#define LOG_SCRUB_ERROR(MSG, ...) {					\
    auto errorstr = fmt::format(MSG, __VA_ARGS__);			\
    ERRORDPP("{}", pg, errorstr);					\
    pg.get_clog_error() << "pg " << pg.get_pgid() << ": " << errorstr;	\
  }

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
  LOG_PREFIX(PGScrubber::emit_chunk_result);
  if (result.has_errors()) {
    LOG_SCRUB_ERROR(
      "Scrub errors found. range: {}, result: {}",
      range, result);
  } else {
    DEBUGDPP("Chunk complete. range: {}", pg, range);
  }
}

void PGScrubber::emit_scrub_result(
  bool deep,
  object_stat_sum_t in_stats)
{
  LOG_PREFIX(PGScrubber::emit_scrub_result);
  DEBUGDPP("", pg);
  pg.peering_state.update_stats(
    [this, FNAME, deep, &in_stats](auto &history, auto &pg_stats) {
      foreach_scrub_maintained_stat(
	[deep, &pg_stats, &in_stats](
	  const auto &name, auto statptr, bool skip_for_shallow) {
	  if (deep && !skip_for_shallow) {
	    pg_stats.stats.sum.*statptr = in_stats.*statptr;
	  }
	});
      foreach_scrub_checked_stat(
	[this, FNAME, &pg_stats, &in_stats](
	  const auto &name, auto statptr, const auto &invalid_predicate) {
	  if (!invalid_predicate(pg_stats) &&
	      (in_stats.*statptr != pg_stats.stats.sum.*statptr)) {
	    LOG_SCRUB_ERROR(
	      "stat mismatch for {}: scrubbed value: {}, stored pg value: {}",
	      name, in_stats.*statptr, pg_stats.stats.sum.*statptr);
	    ++pg_stats.stats.sum.num_shallow_scrub_errors;
	  }
	});
      history.last_scrub = pg.peering_state.get_info().last_update;
      auto now = ceph_clock_now();
      history.last_scrub_stamp = now;
      if (deep) {
	history.last_deep_scrub_stamp = now;
      }
      return false; // notify_scrub_end will flush stats to osd
    });
}

}
