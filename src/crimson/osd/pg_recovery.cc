// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/backfill_facades.h"
#include "crimson/osd/osd_operations/background_recovery.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/pg_recovery.h"

#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

#include "osd/osd_types.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

void PGRecovery::start_pglogbased_recovery()
{
  using PglogBasedRecovery = crimson::osd::PglogBasedRecovery;
  (void) pg->get_shard_services().start_operation<PglogBasedRecovery>(
    static_cast<crimson::osd::PG*>(pg),
    pg->get_shard_services(),
    pg->get_osdmap_epoch());
}

crimson::osd::blocking_future<bool>
PGRecovery::start_recovery_ops(size_t max_to_start)
{
  assert(pg->is_primary());
  assert(pg->is_peered());
  assert(pg->is_recovering());
  // in ceph-osd the do_recovery() path handles both the pg log-based
  // recovery and the backfill, albeit they are separated at the layer
  // of PeeringState. In crimson-osd backfill has been cut from it, so
  // and do_recovery() is actually solely for pg log-based recovery.
  // At the time of writing it's considered to move it to FSM and fix
  // the naming as well.
  assert(!pg->is_backfilling());
  assert(!pg->get_peering_state().is_deleting());

  std::vector<crimson::osd::blocking_future<>> started;
  started.reserve(max_to_start);
  max_to_start -= start_primary_recovery_ops(max_to_start, &started);
  if (max_to_start > 0) {
    max_to_start -= start_replica_recovery_ops(max_to_start, &started);
  }
  return crimson::osd::join_blocking_futures(std::move(started)).then(
    [this] {
    bool done = !pg->get_peering_state().needs_recovery();
    if (done) {
      crimson::get_logger(ceph_subsys_osd).debug("start_recovery_ops: AllReplicasRecovered for pg: {}",
		     pg->get_pgid());
      using LocalPeeringEvent = crimson::osd::LocalPeeringEvent;
      if (!pg->get_peering_state().needs_backfill()) {
        logger().debug("start_recovery_ops: AllReplicasRecovered for pg: {}",
                      pg->get_pgid());
        (void) pg->get_shard_services().start_operation<LocalPeeringEvent>(
          static_cast<crimson::osd::PG*>(pg),
          pg->get_shard_services(),
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
          pg->get_shard_services(),
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
  size_t max_to_start,
  std::vector<crimson::osd::blocking_future<>> *out)
{
  if (!pg->is_recovering()) {
    return 0;
  }

  if (!pg->get_peering_state().have_missing()) {
    pg->get_peering_state().local_recovery_complete();
    return 0;
  }

  const auto &missing = pg->get_peering_state().get_pg_log().get_missing();

  crimson::get_logger(ceph_subsys_osd).info(
    "{} recovering {} in pg {}, missing {}",
    __func__,
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

    crimson::get_logger(ceph_subsys_osd).info(
      "{} {} item.need {} {} {} {} {}",
      __func__,
      soid,
      item.need,
      missing.is_missing(soid) ? " (missing)":"",
      missing.is_missing(head) ? " (missing head)":"",
      pg->get_recovery_backend()->is_recovering(soid) ? " (recovering)":"",
      pg->get_recovery_backend()->is_recovering(head) ? " (recovering head)":"");

    // TODO: handle lost/unfound
    if (!pg->get_recovery_backend()->is_recovering(soid)) {
      if (pg->get_recovery_backend()->is_recovering(head)) {
	++skipped;
      } else {
	auto futopt = recover_missing(soid, item.need);
	if (futopt) {
	  out->push_back(std::move(*futopt));
	  ++started;
	} else {
	  ++skipped;
	}
      }
    }

    if (!skipped)
      pg->get_peering_state().set_last_requested(v);
  }

  crimson::get_logger(ceph_subsys_osd).info(
    "{} started {} skipped {}",
    __func__,
    started,
    skipped);

  return started;
}

size_t PGRecovery::start_replica_recovery_ops(
  size_t max_to_start,
  std::vector<crimson::osd::blocking_future<>> *out)
{
  if (!pg->is_recovering()) {
    return 0;
  }
  uint64_t started = 0;

  assert(!pg->get_peering_state().get_acting_recovery_backfill().empty());

  auto recovery_order = get_replica_recovery_order();
  for (auto &peer : recovery_order) {
    assert(peer != pg->get_peering_state().get_primary());
    auto pm = pg->get_peering_state().get_peer_missing().find(peer);
    assert(pm != pg->get_peering_state().get_peer_missing().end());

    size_t m_sz = pm->second.num_missing();

    crimson::get_logger(ceph_subsys_osd).debug(
	"{}: peer osd.{} missing {} objects",
	__func__,
	peer,
	m_sz);
    crimson::get_logger(ceph_subsys_osd).trace(
	"{}: peer osd.{} missing {}", __func__,
	peer, pm->second.get_items());

    // recover oldest first
    const pg_missing_t &m(pm->second);
    for (auto p = m.get_rmissing().begin();
	 p != m.get_rmissing().end() && started < max_to_start;
	 ++p) {
      const auto &soid = p->second;

      if (pg->get_peering_state().get_missing_loc().is_unfound(soid)) {
	crimson::get_logger(ceph_subsys_osd).debug(
	    "{}: object {} still unfound", __func__, soid);
	continue;
      }

      const pg_info_t &pi = pg->get_peering_state().get_peer_info(peer);
      if (soid > pi.last_backfill) {
	if (!pg->get_recovery_backend()->is_recovering(soid)) {
	  crimson::get_logger(ceph_subsys_osd).error(
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
	crimson::get_logger(ceph_subsys_osd).debug(
	    "{}: already recovering object {}", __func__, soid);
	continue;
      }

      if (pg->get_peering_state().get_missing_loc().is_deleted(soid)) {
	crimson::get_logger(ceph_subsys_osd).debug(
	    "{}: soid {} is a delete, removing", __func__, soid);
	map<hobject_t,pg_missing_item>::const_iterator r =
	  m.get_items().find(soid);
	started += prep_object_replica_deletes(
	  soid, r->second.need, out);
	continue;
      }

      if (soid.is_snap() &&
	  pg->get_peering_state().get_pg_log().get_missing().is_missing(
	    soid.get_head())) {
	crimson::get_logger(ceph_subsys_osd).debug(
	    "{}: head {} still missing on primary",
	    __func__, soid.get_head());
	continue;
      }

      if (pg->get_peering_state().get_pg_log().get_missing().is_missing(soid)) {
	crimson::get_logger(ceph_subsys_osd).debug(
	    "{}: soid {} still missing on primary", __func__, soid);
	continue;
      }

      crimson::get_logger(ceph_subsys_osd).debug(
	"{}: recover_object_replicas({})",
	__func__,
	soid);
      map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(
	soid);
      started += prep_object_replica_pushes(
	soid, r->second.need, out);
    }
  }

  return started;
}

std::optional<crimson::osd::blocking_future<>> PGRecovery::recover_missing(
  const hobject_t &soid, eversion_t need)
{
  if (pg->get_peering_state().get_missing_loc().is_deleted(soid)) {
    return pg->get_recovery_backend()->get_recovering(soid).make_blocking_future(
	pg->get_recovery_backend()->recover_delete(soid, need));
  } else {
    return pg->get_recovery_backend()->get_recovering(soid).make_blocking_future(
      pg->get_recovery_backend()->recover_object(soid, need).handle_exception(
	[=, soid = std::move(soid)] (auto e) {
	on_failed_recover({ pg->get_pg_whoami() }, soid, need);
	return seastar::make_ready_future<>();
      })
    );
  }
}

size_t PGRecovery::prep_object_replica_deletes(
  const hobject_t& soid,
  eversion_t need,
  std::vector<crimson::osd::blocking_future<>> *in_progress)
{
  in_progress->push_back(
    pg->get_recovery_backend()->get_recovering(soid).make_blocking_future(
      pg->get_recovery_backend()->push_delete(soid, need).then([=] {
	object_stat_sum_t stat_diff;
	stat_diff.num_objects_recovered = 1;
	on_global_recover(soid, stat_diff, true);
	return seastar::make_ready_future<>();
      })
    )
  );
  return 1;
}

size_t PGRecovery::prep_object_replica_pushes(
  const hobject_t& soid,
  eversion_t need,
  std::vector<crimson::osd::blocking_future<>> *in_progress)
{
  in_progress->push_back(
    pg->get_recovery_backend()->get_recovering(soid).make_blocking_future(
      pg->get_recovery_backend()->recover_object(soid, need).handle_exception(
	[=, soid = std::move(soid)] (auto e) {
	on_failed_recover({ pg->get_pg_whoami() }, soid, need);
	return seastar::make_ready_future<>();
      })
    )
  );
  return 1;
}

void PGRecovery::on_local_recover(
  const hobject_t& soid,
  const ObjectRecoveryInfo& recovery_info,
  const bool is_delete,
  ceph::os::Transaction& t)
{
  pg->get_peering_state().recover_got(soid,
      recovery_info.version, is_delete, t);

  if (pg->is_primary()) {
    if (!is_delete) {
      auto& obc = pg->get_recovery_backend()->get_recovering(soid).obc; //TODO: move to pg backend?
      obc->obs.exists = true;
      obc->obs.oi = recovery_info.oi;
      // obc is loaded the excl lock
      obc->put_lock_type(RWState::RWEXCL);
      assert(obc->get_recovery_read().get0());
    }
    if (!pg->is_unreadable_object(soid)) {
      pg->get_recovery_backend()->get_recovering(soid).set_readable();
    }
  }
}

void PGRecovery::on_global_recover (
  const hobject_t& soid,
  const object_stat_sum_t& stat_diff,
  const bool is_delete)
{
  crimson::get_logger(ceph_subsys_osd).info("{} {}", __func__, soid);
  pg->get_peering_state().object_recovered(soid, stat_diff);
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
  auto msg = make_message<MOSDPGScan>(
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
  ).then([this] (BackfillInterval bi) {
    logger().debug("request_primary_scan:{}", __func__);
    using BackfillState = crimson::osd::BackfillState;
    start_backfill_recovery(BackfillState::PrimaryScanned{ std::move(bi) });
  });
}

void PGRecovery::enqueue_push(
  const pg_shard_t& target,
  const hobject_t& obj,
  const eversion_t& v)
{
  logger().debug("{}: target={} obj={} v={}",
                 __func__, target, obj, v);
  std::ignore = pg->get_recovery_backend()->recover_object(obj, v).\
  handle_exception([] (auto) {
    ceph_abort_msg("got exception on backfill's push");
    return seastar::make_ready_future<>();
  }).then([this, obj] {
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
  ceph_abort_msg("Not implemented");
}

void PGRecovery::update_peers_last_backfill(
  const hobject_t& new_last_backfill)
{
  ceph_abort_msg("Not implemented");
}

bool PGRecovery::budget_available() const
{
  // TODO: the limits!
  return true;
}

void PGRecovery::backfilled()
{
  shard_services.start_operation<LocalPeeringEvent>(
    this,
    shard_services,
    pg_whoami,
    pgid,
    get_osdmap_epoch(),
    get_osdmap_epoch(),
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
    std::make_unique<BackfillState::PeeringFacade>(pg->get_peering_state()),
    std::make_unique<BackfillState::PGFacade>(
      *static_cast<crimson::osd::PG*>(pg)));
  start_backfill_recovery(BackfillState::Triggered{});
}
