// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/backfill_facades.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/pg_recovery.h"

#include "osd/osd_types.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using std::map;
using std::set;

void PGRecovery::start_pglogbased_recovery()
{
  using PglogBasedRecovery = crimson::osd::PglogBasedRecovery;
  (void) pg->get_shard_services().start_operation<PglogBasedRecovery>(
    static_cast<crimson::osd::PG*>(pg),
    pg->get_shard_services(),
    pg->get_osdmap_epoch(),
    float(0.001));
}

PGRecovery::interruptible_future<bool>
PGRecovery::start_recovery_ops(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  size_t max_to_start)
{
  assert(pg->is_primary());
  assert(pg->is_peered());

  if (!pg->is_recovering() && !pg->is_backfilling()) {
    logger().debug("recovery raced and were queued twice, ignoring!");
    return seastar::make_ready_future<bool>(false);
  }

  // in ceph-osd the do_recovery() path handles both the pg log-based
  // recovery and the backfill, albeit they are separated at the layer
  // of PeeringState. In crimson-osd backfill has been cut from it, so
  // and do_recovery() is actually solely for pg log-based recovery.
  // At the time of writing it's considered to move it to FSM and fix
  // the naming as well.
  assert(!pg->is_backfilling());
  assert(!pg->get_peering_state().is_deleting());

  std::vector<interruptible_future<>> started;
  started.reserve(max_to_start);
  max_to_start -= start_primary_recovery_ops(trigger, max_to_start, &started);
  if (max_to_start > 0) {
    max_to_start -= start_replica_recovery_ops(trigger, max_to_start, &started);
  }
  using interruptor =
    crimson::interruptible::interruptor<crimson::osd::IOInterruptCondition>;
  return interruptor::parallel_for_each(started,
					[] (auto&& ifut) {
    return std::move(ifut);
  }).then_interruptible([this] {
    //TODO: maybe we should implement a recovery race interruptor in the future
    if (!pg->is_recovering() && !pg->is_backfilling()) {
      logger().debug("recovery raced and were queued twice, ignoring!");
      return seastar::make_ready_future<bool>(false);
    }

    bool done = !pg->get_peering_state().needs_recovery();
    if (done) {
      logger().debug("start_recovery_ops: AllReplicasRecovered for pg: {}",
                     pg->get_pgid());
      using LocalPeeringEvent = crimson::osd::LocalPeeringEvent;
      if (!pg->get_peering_state().needs_backfill()) {
        logger().debug("start_recovery_ops: AllReplicasRecovered for pg: {}",
                      pg->get_pgid());
        (void) pg->get_shard_services().start_operation<LocalPeeringEvent>(
          static_cast<crimson::osd::PG*>(pg),
          pg->get_pg_whoami(),
          pg->get_pgid(),
          pg->get_osdmap_epoch(),
          pg->get_osdmap_epoch(),
          PeeringState::AllReplicasRecovered{});
      } else {
        logger().debug("start_recovery_ops: RequestBackfill for pg: {}",
                      pg->get_pgid());
        (void) pg->get_shard_services().start_operation<LocalPeeringEvent>(
          static_cast<crimson::osd::PG*>(pg),
          pg->get_pg_whoami(),
          pg->get_pgid(),
          pg->get_osdmap_epoch(),
          pg->get_osdmap_epoch(),
          PeeringState::RequestBackfill{});
      }
    }
    return seastar::make_ready_future<bool>(!done);
  });
}

size_t PGRecovery::start_primary_recovery_ops(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  size_t max_to_start,
  std::vector<PGRecovery::interruptible_future<>> *out)
{
  if (!pg->is_recovering()) {
    return 0;
  }

  if (!pg->get_peering_state().have_missing()) {
    pg->get_peering_state().local_recovery_complete();
    return 0;
  }

  const auto &missing = pg->get_peering_state().get_pg_log().get_missing();

  logger().info("{} recovering {} in pg {}, missing {}", __func__,
                pg->get_recovery_backend()->total_recovering(),
                *static_cast<crimson::osd::PG*>(pg),
                missing);

  unsigned started = 0;
  int skipped = 0;

  map<version_t, hobject_t>::const_iterator p =
    missing.get_rmissing().lower_bound(pg->get_peering_state().get_pg_log().get_log().last_requested);
  while (started < max_to_start && p != missing.get_rmissing().end()) {
    // TODO: chain futures here to enable yielding to scheduler?
    hobject_t soid;
    version_t v = p->first;

    auto it_objects = pg->get_peering_state().get_pg_log().get_log().objects.find(p->second);
    if (it_objects != pg->get_peering_state().get_pg_log().get_log().objects.end()) {
      // look at log!
      pg_log_entry_t *latest = it_objects->second;
      assert(latest->is_update() || latest->is_delete());
      soid = latest->soid;
    } else {
      soid = p->second;
    }
    const pg_missing_item& item = missing.get_items().find(p->second)->second;
    ++p;

    hobject_t head = soid.get_head();

    bool head_missing = missing.is_missing(head);
    logger().info(
      "{} {} item.need {} {} {} {} {}",
      __func__,
      soid,
      item.need,
      missing.is_missing(soid) ? " (missing)":"",
      head_missing ? " (missing head)":"",
      pg->get_recovery_backend()->is_recovering(soid) ? " (recovering)":"",
      pg->get_recovery_backend()->is_recovering(head) ? " (recovering head)":"");

    // TODO: handle lost/unfound
    if (pg->get_recovery_backend()->is_recovering(soid)) {
      auto& recovery_waiter = pg->get_recovery_backend()->get_recovering(soid);
      out->emplace_back(recovery_waiter.wait_for_recovered(trigger));
      ++started;
    } else if (pg->get_recovery_backend()->is_recovering(head)) {
      ++skipped;
    } else {
      if (head_missing) {
	auto it = missing.get_items().find(head);
	assert(it != missing.get_items().end());
	auto head_need = it->second.need;
	out->emplace_back(recover_missing(trigger, head, head_need));
	++skipped;
      } else {
	out->emplace_back(recover_missing(trigger, soid, item.need));
      }
      ++started;
    }

    if (!skipped)
      pg->get_peering_state().set_last_requested(v);
  }

  logger().info("{} started {} skipped {}", __func__, started, skipped);

  return started;
}

size_t PGRecovery::start_replica_recovery_ops(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  size_t max_to_start,
  std::vector<PGRecovery::interruptible_future<>> *out)
{
  if (!pg->is_recovering()) {
    return 0;
  }
  uint64_t started = 0;

  assert(!pg->get_peering_state().get_acting_recovery_backfill().empty());

  auto recovery_order = get_replica_recovery_order();
  for (auto &peer : recovery_order) {
    assert(peer != pg->get_peering_state().get_primary());
    const auto& pm = pg->get_peering_state().get_peer_missing(peer);

    logger().debug("{}: peer osd.{} missing {} objects", __func__,
                 peer, pm.num_missing());
    logger().trace("{}: peer osd.{} missing {}", __func__,
                 peer, pm.get_items());

    // recover oldest first
    for (auto p = pm.get_rmissing().begin();
	 p != pm.get_rmissing().end() && started < max_to_start;
	 ++p) {
      const auto &soid = p->second;

      if (pg->get_peering_state().get_missing_loc().is_unfound(soid)) {
	logger().debug("{}: object {} still unfound", __func__, soid);
	continue;
      }

      const pg_info_t &pi = pg->get_peering_state().get_peer_info(peer);
      if (soid > pi.last_backfill) {
	if (!pg->get_recovery_backend()->is_recovering(soid)) {
	  logger().error(
	    "{}: object {} in missing set for backfill (last_backfill {})"
	    " but not in recovering",
	    __func__,
	    soid,
	    pi.last_backfill);
	  ceph_abort();
	}
	continue;
      }

      if (pg->get_recovery_backend()->is_recovering(soid)) {
	logger().debug("{}: already recovering object {}", __func__, soid);
	auto& recovery_waiter = pg->get_recovery_backend()->get_recovering(soid);
	out->emplace_back(recovery_waiter.wait_for_recovered(trigger));
	started++;
	continue;
      }

      if (pg->get_peering_state().get_missing_loc().is_deleted(soid)) {
	logger().debug("{}: soid {} is a delete, removing", __func__, soid);
	map<hobject_t,pg_missing_item>::const_iterator r =
	  pm.get_items().find(soid);
	started++;
	out->emplace_back(
	  prep_object_replica_deletes(trigger, soid, r->second.need));
	continue;
      }

      if (soid.is_snap() &&
	  pg->get_peering_state().get_pg_log().get_missing().is_missing(
	    soid.get_head())) {
	logger().debug("{}: head {} still missing on primary", __func__,
		     soid.get_head());
	continue;
      }

      if (pg->get_peering_state().get_pg_log().get_missing().is_missing(soid)) {
	logger().debug("{}: soid {} still missing on primary", __func__, soid);
	continue;
      }

      logger().debug("{}: recover_object_replicas({})", __func__,soid);
      map<hobject_t,pg_missing_item>::const_iterator r = pm.get_items().find(
	soid);
      started++;
      out->emplace_back(
	prep_object_replica_pushes(trigger, soid, r->second.need));
    }
  }

  return started;
}

PGRecovery::interruptible_future<>
PGRecovery::recover_missing(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  const hobject_t &soid, eversion_t need)
{
  logger().info("{} {} v {}", __func__, soid, need);
  auto [recovering, added] = pg->get_recovery_backend()->add_recovering(soid);
  if (added) {
    logger().info("{} {} v {}, new recovery", __func__, soid, need);
    if (pg->get_peering_state().get_missing_loc().is_deleted(soid)) {
      return recovering.wait_track_blocking(
	trigger,
	pg->get_recovery_backend()->recover_delete(soid, need));
    } else {
      return recovering.wait_track_blocking(
	trigger,
	pg->get_recovery_backend()->recover_object(soid, need)
	.handle_exception_interruptible(
	  [=, this, soid = std::move(soid)] (auto e) {
	  on_failed_recover({ pg->get_pg_whoami() }, soid, need);
	  return seastar::make_ready_future<>();
	})
      );
    }
  } else {
    return recovering.wait_for_recovered();
  }
}

RecoveryBackend::interruptible_future<> PGRecovery::prep_object_replica_deletes(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  const hobject_t& soid,
  eversion_t need)
{
  logger().info("{} {} v {}", __func__, soid, need);
  auto [recovering, added] = pg->get_recovery_backend()->add_recovering(soid);
  if (added) {
    logger().info("{} {} v {}, new recovery", __func__, soid, need);
    return recovering.wait_track_blocking(
      trigger,
      pg->get_recovery_backend()->push_delete(soid, need).then_interruptible(
	[=, this] {
	object_stat_sum_t stat_diff;
	stat_diff.num_objects_recovered = 1;
	on_global_recover(soid, stat_diff, true);
	return seastar::make_ready_future<>();
      })
    );
  } else {
    return recovering.wait_for_recovered();
  }
}

RecoveryBackend::interruptible_future<> PGRecovery::prep_object_replica_pushes(
  RecoveryBackend::RecoveryBlockingEvent::TriggerI& trigger,
  const hobject_t& soid,
  eversion_t need)
{
  logger().info("{} {} v {}", __func__, soid, need);
  auto [recovering, added] = pg->get_recovery_backend()->add_recovering(soid);
  if (added) {
    logger().info("{} {} v {}, new recovery", __func__, soid, need);
    return recovering.wait_track_blocking(
      trigger,
      pg->get_recovery_backend()->recover_object(soid, need)
      .handle_exception_interruptible(
	[=, this, soid = std::move(soid)] (auto e) {
	on_failed_recover({ pg->get_pg_whoami() }, soid, need);
	return seastar::make_ready_future<>();
      })
    );
  } else {
    return recovering.wait_for_recovered();
  }
}

RecoveryBackend::interruptible_future<>
PGRecovery::on_local_recover(
  const hobject_t& soid,
  const ObjectRecoveryInfo& recovery_info,
  const bool is_delete,
  ceph::os::Transaction& t)
{
  if (const auto &log = pg->get_peering_state().get_pg_log();
      !is_delete &&
      log.get_missing().is_missing(recovery_info.soid) &&
      log.get_missing().get_items().find(recovery_info.soid)->second.need > recovery_info.version) {
    assert(pg->is_primary());
    if (const auto* latest = log.get_log().objects.find(recovery_info.soid)->second;
        latest->op == pg_log_entry_t::LOST_REVERT) {
      ceph_abort("mark_unfound_lost (LOST_REVERT) is not implemented yet");
    }
  }

  return RecoveryBackend::interruptor::async(
    [soid, &recovery_info, is_delete, &t, this] {
    if (soid.is_snap()) {
      OSDriver::OSTransaction _t(pg->get_osdriver().get_transaction(&t));
      int r = pg->get_snap_mapper().remove_oid(soid, &_t);
      assert(r == 0 || r == -ENOENT);

      if (!is_delete) {
	set<snapid_t> snaps;
	auto p = recovery_info.ss.clone_snaps.find(soid.snap);
	assert(p != recovery_info.ss.clone_snaps.end());
	snaps.insert(p->second.begin(), p->second.end());
	pg->get_snap_mapper().add_oid(recovery_info.soid, snaps, &_t);
      }
    }

    pg->get_peering_state().recover_got(soid,
	recovery_info.version, is_delete, t);

    if (pg->is_primary()) {
      if (!is_delete) {
	auto& obc = pg->get_recovery_backend()->get_recovering(soid).obc; //TODO: move to pg backend?
	obc->obs.exists = true;
	obc->obs.oi = recovery_info.oi;
      }
      if (!pg->is_unreadable_object(soid)) {
	pg->get_recovery_backend()->get_recovering(soid).set_readable();
      }
      pg->publish_stats_to_osd();
    }
  });
}

void PGRecovery::on_global_recover (
  const hobject_t& soid,
  const object_stat_sum_t& stat_diff,
  const bool is_delete)
{
  logger().info("{} {}", __func__, soid);
  pg->get_peering_state().object_recovered(soid, stat_diff);
  pg->publish_stats_to_osd();
  auto& recovery_waiter = pg->get_recovery_backend()->get_recovering(soid);
  if (!is_delete)
    recovery_waiter.obc->drop_recovery_read();
  recovery_waiter.set_recovered();
  pg->get_recovery_backend()->remove_recovering(soid);
}

void PGRecovery::on_failed_recover(
  const set<pg_shard_t>& from,
  const hobject_t& soid,
  const eversion_t& v)
{
  for (auto pg_shard : from) {
    if (pg_shard != pg->get_pg_whoami()) {
      pg->get_peering_state().force_object_missing(pg_shard, soid, v);
    }
  }
}

void PGRecovery::on_peer_recover(
  pg_shard_t peer,
  const hobject_t &oid,
  const ObjectRecoveryInfo &recovery_info)
{
  crimson::get_logger(ceph_subsys_osd).debug(
      "{}: {}, {} on {}", __func__, oid,
      recovery_info.version, peer);
  pg->get_peering_state().on_peer_recover(peer, oid, recovery_info.version);
}

void PGRecovery::_committed_pushed_object(epoch_t epoch,
			      eversion_t last_complete)
{
  if (!pg->has_reset_since(epoch)) {
    pg->get_peering_state().recovery_committed_to(last_complete);
  } else {
    crimson::get_logger(ceph_subsys_osd).debug(
	"{} pg has changed, not touching last_complete_ondisk",
	__func__);
  }
}

template <class EventT>
void PGRecovery::start_backfill_recovery(const EventT& evt)
{
  using BackfillRecovery = crimson::osd::BackfillRecovery;
  std::ignore = pg->get_shard_services().start_operation<BackfillRecovery>(
    static_cast<crimson::osd::PG*>(pg),
    pg->get_shard_services(),
    pg->get_osdmap_epoch(),
    evt);
}

void PGRecovery::request_replica_scan(
  const pg_shard_t& target,
  const hobject_t& begin,
  const hobject_t& end)
{
  logger().debug("{}: target.osd={}", __func__, target.osd);
  auto msg = crimson::make_message<MOSDPGScan>(
    MOSDPGScan::OP_SCAN_GET_DIGEST,
    pg->get_pg_whoami(),
    pg->get_osdmap_epoch(),
    pg->get_last_peering_reset(),
    spg_t(pg->get_pgid().pgid, target.shard),
    begin,
    end);
  std::ignore = pg->get_shard_services().send_to_osd(
    target.osd,
    std::move(msg),
    pg->get_osdmap_epoch());
}

void PGRecovery::request_primary_scan(
  const hobject_t& begin)
{
  logger().debug("{}", __func__);
  using crimson::common::local_conf;
  std::ignore = pg->get_recovery_backend()->scan_for_backfill(
    begin,
    local_conf()->osd_backfill_scan_min,
    local_conf()->osd_backfill_scan_max
  ).then_interruptible([this] (BackfillInterval bi) {
    logger().debug("request_primary_scan:{}", __func__);
    using BackfillState = crimson::osd::BackfillState;
    start_backfill_recovery(BackfillState::PrimaryScanned{ std::move(bi) });
  });
}

void PGRecovery::enqueue_push(
  const hobject_t& obj,
  const eversion_t& v)
{
  logger().info("{}: obj={} v={}",
                 __func__, obj, v);
  auto [recovering, added] = pg->get_recovery_backend()->add_recovering(obj);
  if (!added)
    return;
  std::ignore = pg->get_recovery_backend()->recover_object(obj, v).\
  handle_exception_interruptible([] (auto) {
    ceph_abort_msg("got exception on backfill's push");
    return seastar::make_ready_future<>();
  }).then_interruptible([this, obj] {
    logger().debug("enqueue_push:{}", __func__);
    using BackfillState = crimson::osd::BackfillState;
    start_backfill_recovery(BackfillState::ObjectPushed(std::move(obj)));
  });
}

void PGRecovery::enqueue_drop(
  const pg_shard_t& target,
  const hobject_t& obj,
  const eversion_t& v)
{
  // allocate a pair if target is seen for the first time
  auto& req = backfill_drop_requests[target];
  if (!req) {
    req = crimson::make_message<MOSDPGBackfillRemove>(
      spg_t(pg->get_pgid().pgid, target.shard), pg->get_osdmap_epoch());
  }
  req->ls.emplace_back(obj, v);
}

void PGRecovery::maybe_flush()
{
  for (auto& [target, req] : backfill_drop_requests) {
    std::ignore = pg->get_shard_services().send_to_osd(
      target.osd,
      std::move(req),
      pg->get_osdmap_epoch());
  }
  backfill_drop_requests.clear();
}

void PGRecovery::update_peers_last_backfill(
  const hobject_t& new_last_backfill)
{
  logger().debug("{}: new_last_backfill={}",
                 __func__, new_last_backfill);
  // If new_last_backfill == MAX, then we will send OP_BACKFILL_FINISH to
  // all the backfill targets.  Otherwise, we will move last_backfill up on
  // those targets need it and send OP_BACKFILL_PROGRESS to them.
  for (const auto& bt : pg->get_peering_state().get_backfill_targets()) {
    if (const pg_info_t& pinfo = pg->get_peering_state().get_peer_info(bt);
        new_last_backfill > pinfo.last_backfill) {
      pg->get_peering_state().update_peer_last_backfill(bt, new_last_backfill);
      auto m = crimson::make_message<MOSDPGBackfill>(
        pinfo.last_backfill.is_max() ? MOSDPGBackfill::OP_BACKFILL_FINISH
                                     : MOSDPGBackfill::OP_BACKFILL_PROGRESS,
        pg->get_osdmap_epoch(),
        pg->get_last_peering_reset(),
        spg_t(pg->get_pgid().pgid, bt.shard));
      // Use default priority here, must match sub_op priority
      // TODO: if pinfo.last_backfill.is_max(), then
      //       start_recovery_op(hobject_t::get_max());
      m->last_backfill = pinfo.last_backfill;
      m->stats = pinfo.stats;
      std::ignore = pg->get_shard_services().send_to_osd(
        bt.osd, std::move(m), pg->get_osdmap_epoch());
      logger().info("{}: peer {} num_objects now {} / {}",
                    __func__,
                    bt,
                    pinfo.stats.stats.sum.num_objects,
                    pg->get_info().stats.stats.sum.num_objects);
    }
  }
}

bool PGRecovery::budget_available() const
{
  // TODO: the limits!
  return true;
}

void PGRecovery::backfilled()
{
  using LocalPeeringEvent = crimson::osd::LocalPeeringEvent;
  std::ignore = pg->get_shard_services().start_operation<LocalPeeringEvent>(
    static_cast<crimson::osd::PG*>(pg),
    pg->get_pg_whoami(),
    pg->get_pgid(),
    pg->get_osdmap_epoch(),
    pg->get_osdmap_epoch(),
    PeeringState::Backfilled{});
}

void PGRecovery::dispatch_backfill_event(
  boost::intrusive_ptr<const boost::statechart::event_base> evt)
{
  logger().debug("{}", __func__);
  backfill_state->process_event(evt);
}

void PGRecovery::on_backfill_reserved()
{
  logger().debug("{}", __func__);
  // PIMP and depedency injection for the sake unittestability.
  // I'm not afraid about the performance here.
  using BackfillState = crimson::osd::BackfillState;
  backfill_state = std::make_unique<BackfillState>(
    *this,
    std::make_unique<crimson::osd::PeeringFacade>(pg->get_peering_state()),
    std::make_unique<crimson::osd::PGFacade>(
      *static_cast<crimson::osd::PG*>(pg)));
  // yes, it's **not** backfilling yet. The PG_STATE_BACKFILLING
  // will be set after on_backfill_reserved() returns.
  // Backfill needs to take this into consideration when scheduling
  // events -- they must be mutually exclusive with PeeringEvent
  // instances. Otherwise the execution might begin without having
  // the state updated.
  ceph_assert(!pg->get_peering_state().is_backfilling());
  start_backfill_recovery(BackfillState::Triggered{});
}
