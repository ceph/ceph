// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>

#include "crimson/common/coroutine.h"
#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/osd_operations/background_recovery.h"

#include "messages/MOSDFastDispatchOp.h"
#include "osd/osd_types.h"

SET_SUBSYS(osd);

hobject_t RecoveryBackend::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version) const
{
  LOG_PREFIX(RecoveryBackend::get_temp_recovery_object);
  hobject_t hoid =
    target.make_temp_hobject(fmt::format("temp_recovering_{}_{}_{}_{}",
                                         pg.get_info().pgid,
                                         version,
                                         pg.get_info().history.same_interval_since,
                                         target.snap));
  DEBUGDPP("{}", pg, hoid);
  return hoid;
}

void RecoveryBackend::add_temp_obj(const hobject_t &oid)
{
  backend->add_temp_obj(oid);
}

void RecoveryBackend::clear_temp_obj(const hobject_t &oid)
{
  backend->clear_temp_obj(oid);
}

void RecoveryBackend::clean_up(ceph::os::Transaction& t,
			       interrupt_cause_t why)
{
  for_each_temp_obj([&](auto &soid) {
    t.remove(pg.get_collection_ref()->get_cid(),
	      ghobject_t(soid, ghobject_t::NO_GEN, pg.get_pg_whoami().shard));
  });
  clear_temp_objs();

  replica_push_targets.clear();

  for (auto& [soid, recovery_waiter] : recovering) {
    if ((recovery_waiter->pull_info
         && recovery_waiter->pull_info->is_complete())
	|| (!recovery_waiter->pull_info
	  && recovery_waiter->obc && recovery_waiter->obc->obs.exists)) {
      recovery_waiter->obc->interrupt(
	  ::crimson::common::actingset_changed(
	      pg.is_primary()));
      recovery_waiter->interrupt(why);
    }
  }
  recovering.clear();
}

void RecoveryBackend::WaitForObjectRecovery::interrupt(interrupt_cause_t why) {
  switch(why) {
  case interrupt_cause_t::INTERVAL_CHANGE:
    if (readable) {
      readable->set_exception(
	crimson::common::actingset_changed(pg.is_primary()));
      readable.reset();
    }
    if (recovered) {
      recovered->set_exception(
	crimson::common::actingset_changed(pg.is_primary()));
      recovered.reset();
    }
    if (pulled) {
      pulled->set_exception(
	crimson::common::actingset_changed(pg.is_primary()));
      pulled.reset();
    }
    for (auto& [pg_shard, pr] : pushes) {
      pr.set_exception(
	crimson::common::actingset_changed(pg.is_primary()));
    }
    pushes.clear();
    break;
  default:
    ceph_abort("impossible");
    break;
  }
}

void RecoveryBackend::WaitForObjectRecovery::stop() {
  if (readable) {
    readable->set_exception(
      crimson::common::system_shutdown_exception());
    readable.reset();
  }
  if (recovered) {
    recovered->set_exception(
      crimson::common::system_shutdown_exception());
    recovered.reset();
  }
  if (pulled) {
    pulled->set_exception(
      crimson::common::system_shutdown_exception());
    pulled.reset();
  }
  for (auto& [pg_shard, pr] : pushes) {
    pr.set_exception(
      crimson::common::system_shutdown_exception());
  }
  pushes.clear();
}

void RecoveryBackend::handle_backfill_finish(
  MOSDPGBackfill& m,
  crimson::net::ConnectionXcoreRef conn)
{
  LOG_PREFIX(RecoveryBackend::handle_backfill_finish);
  DEBUGDPP("", pg);
  ceph_assert(!pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 1);
  auto reply = crimson::make_message<MOSDPGBackfill>(
    MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
    pg.get_osdmap_epoch(),
    m.query_epoch,
    spg_t(pg.get_pgid().pgid, pg.get_primary().shard));
  reply->set_priority(pg.get_recovery_op_priority());
  std::ignore = conn->send(std::move(reply));
  shard_services.start_operation<crimson::osd::LocalPeeringEvent>(
    static_cast<crimson::osd::PG*>(&pg),
    pg.get_pg_whoami(),
    pg.get_pgid(),
    pg.get_osdmap_epoch(),
    pg.get_osdmap_epoch(),
    RecoveryDone{});
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill_progress(
  MOSDPGBackfill& m)
{
  LOG_PREFIX(RecoveryBackend::handle_backfill_progress);
  DEBUGDPP("", pg);
  ceph_assert(!pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 2);

  ObjectStore::Transaction t;
  pg.get_peering_state().update_backfill_progress(
    m.last_backfill,
    m.stats,
    m.op == MOSDPGBackfill::OP_BACKFILL_PROGRESS,
    t);
  DEBUGDPP("submitting transaction", pg);
  return shard_services.get_store().do_transaction(
    pg.get_collection_ref(), std::move(t)).or_terminate();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill_finish_ack(
  MOSDPGBackfill& m)
{
  LOG_PREFIX(RecoveryBackend::handle_backfill_finish_ack);
  DEBUGDPP("", pg);
  ceph_assert(pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 3);
  auto recovery_handler = pg.get_recovery_handler();
  recovery_handler->backfill_target_finished();
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill(
  MOSDPGBackfill& m,
  crimson::net::ConnectionXcoreRef conn)
{
  LOG_PREFIX(RecoveryBackend::handle_backfill);
  DEBUGDPP("", pg);
  if (pg.old_peering_msg(m.map_epoch, m.query_epoch)) {
    DEBUGDPP("discarding {}", pg, m);
    return seastar::now();
  }
  switch (m.op) {
    case MOSDPGBackfill::OP_BACKFILL_FINISH:
      handle_backfill_finish(m, conn);
      [[fallthrough]];
    case MOSDPGBackfill::OP_BACKFILL_PROGRESS:
      return handle_backfill_progress(m);
    case MOSDPGBackfill::OP_BACKFILL_FINISH_ACK:
      return handle_backfill_finish_ack(m);
    default:
      ceph_assert("unknown op type for pg backfill");
      return seastar::now();
  }
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill_remove(
  MOSDPGBackfillRemove& m)
{
  LOG_PREFIX(RecoveryBackend::handle_backfill_remove);
  DEBUGDPP("m.ls={}", pg, m.ls);
  assert(m.get_type() == MSG_OSD_PG_BACKFILL_REMOVE);

  ObjectStore::Transaction t;
  for ([[maybe_unused]] const auto& [soid, ver] : m.ls) {
    // TODO: the reserved space management. PG::try_reserve_recovery_space().
    co_await interruptor::async([this, soid=soid, &t] {
      pg.remove_maybe_snapmapped_object(t, soid);
    });
  }
  DEBUGDPP("submitting transaction", pg);
  co_await interruptor::make_interruptible(
    shard_services.get_store().do_transaction(
      pg.get_collection_ref(), std::move(t)).or_terminate());
}

RecoveryBackend::interruptible_future<PrimaryBackfillInterval>
RecoveryBackend::scan_for_backfill_primary(
  const hobject_t start,
  [[maybe_unused]] const std::int64_t min,
  const std::int64_t max,
  const std::set<pg_shard_t> &backfill_targets)
{
  LOG_PREFIX(RecoveryBackend::scan_for_backfill_primary);
  DEBUGDPP("starting from {}", pg, start);
  auto version_map = seastar::make_lw_shared<std::multimap<hobject_t,
		       std::pair<shard_id_t,eversion_t>>>();
  auto&& [objects, next] = co_await backend->list_objects(start, max);
  co_await interruptor::parallel_for_each(objects,
    seastar::coroutine::lambda([FNAME, this, version_map, backfill_targets]
    (const hobject_t& object) -> interruptible_future<> {
    DEBUGDPP("querying obj:{}", pg, object);
    auto obc_manager = pg.obc_loader.get_obc_manager(
      object, /* resolve_clone = */ false);

    auto found = co_await pg.obc_loader.load_and_lock(
      obc_manager, RWState::RWREAD
    ).si_then([] {
      return true;
    }).handle_error_interruptible(
      crimson::ct_error::enoent::handle([](auto) {
	return false;
      }),
      crimson::ct_error::assert_all(fmt::format("{} {} error when loading obc", pg, FNAME).c_str())
    );
    if (!found) {
      // if the object does not exist here, it must have been removed
      // between the collection_list_partial and here.  This can happen
      // for the first item in the range, which is usually last_backfill.
      co_return;
    }

    if (obc_manager.get_obc()->obs.exists) {
      auto version = obc_manager.get_obc()->obs.oi.version;
      auto shard_versions = obc_manager.get_obc()->obs.oi.shard_versions;
      if (shard_versions.empty()) {
	version_map->emplace(object, std::make_pair(shard_id_t::NO_SHARD,
						    version));
      } else {
	bool added_default = false;
	for (auto & shard: backfill_targets) {
	  if (shard_versions.contains(shard.shard)) {
	    version = shard_versions.at(shard.shard);
	    version_map->emplace(object, std::make_pair(shard.shard, version));
	  } else if (!added_default) {
	    version_map->emplace(object, std::make_pair(shard_id_t::NO_SHARD,
							version));
	    added_default = true;
	  }
	}
      }
      DEBUGDPP("found: {}  {}", pg,
               object, version);
      co_return;
    } else {
      // if the object does not exist here, it must have been removed
      // between the collection_list_partial and here.  This can happen
      // for the first item in the range, which is usually last_backfill.
      co_return;
    }
  }));
  PrimaryBackfillInterval bi;
  bi.begin = std::move(start);
  bi.end = std::move(next);
  bi.objects = std::move(*version_map);
  DEBUGDPP("{} PrimaryBackfillInterval filled, leaving, {}",
           "scan_for_backfill_primary",
           pg, bi);
  co_return std::move(bi);
}

RecoveryBackend::interruptible_future<ReplicaBackfillInterval>
RecoveryBackend::scan_for_backfill_replica(
  const hobject_t start,
  [[maybe_unused]] const std::int64_t min,
  const std::int64_t max)
{
  LOG_PREFIX(RecoveryBackend::scan_for_backfill_replica);
  DEBUGDPP("starting from {}", pg, start);
  auto version_map = seastar::make_lw_shared<std::map<hobject_t,
						      eversion_t>>();
  auto&& [objects, next] = co_await backend->list_objects(start, max);
  co_await interruptor::parallel_for_each(objects,
    seastar::coroutine::lambda([FNAME, this, version_map]
    (const hobject_t& object) -> interruptible_future<> {
    DEBUGDPP("querying obj:{}", pg, object);
    auto obc_manager = pg.obc_loader.get_obc_manager(
      object, /* resolve_clone = */ false);

    auto found = co_await pg.obc_loader.load_and_lock(
      obc_manager, RWState::RWREAD
    ).si_then([] {
      return true;
    }).handle_error_interruptible(
      crimson::ct_error::enoent::handle([](auto) {
	return false;
      }),
      crimson::ct_error::assert_all(fmt::format("{} {} error when loading obc", pg, FNAME).c_str())
    );
    if (!found) {
      // if the object does not exist here, it must have been removed
      // between the collection_list_partial and here.  This can happen
      // for the first item in the range, which is usually last_backfill.
      co_return;
    }

    if (obc_manager.get_obc()->obs.exists) {
      auto version = obc_manager.get_obc()->obs.oi.version;
      version_map->emplace(object, version);
      DEBUGDPP("found: {}  {}", pg,
               object, version);
      co_return;
    } else {
      // if the object does not exist here, it must have been removed
      // between the collection_list_partial and here.  This can happen
      // for the first item in the range, which is usually last_backfill.
      co_return;
    }
  }));
  ReplicaBackfillInterval bi;
  bi.begin = std::move(start);
  bi.end = std::move(next);
  bi.objects = std::move(*version_map);
  DEBUGDPP("{} ReplicaBackfillInterval filled, leaving, {}",
           "scan_for_backfill_replica",
           pg, bi);
  co_return std::move(bi);
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_scan_get_digest(
  MOSDPGScan& m,
  crimson::net::ConnectionXcoreRef conn)
{
  LOG_PREFIX(RecoveryBackend::handle_scan_get_digest);
  DEBUGDPP("", pg);
  if (false /* FIXME: check for backfill too full */) {
    std::ignore = shard_services.start_operation<crimson::osd::LocalPeeringEvent>(
      // TODO: abstract start_background_recovery
      static_cast<crimson::osd::PG*>(&pg),
      pg.get_pg_whoami(),
      pg.get_pgid(),
      pg.get_osdmap_epoch(),
      pg.get_osdmap_epoch(),
      PeeringState::BackfillTooFull());
    return seastar::now();
  }
  return scan_for_backfill_replica(
    std::move(m.begin),
    crimson::common::local_conf().get_val<std::int64_t>("osd_backfill_scan_min"),
    crimson::common::local_conf().get_val<std::int64_t>("osd_backfill_scan_max")
  ).then_interruptible(
    [this, query_epoch=m.query_epoch, conn
    ](auto backfill_interval) {
      auto reply = crimson::make_message<MOSDPGScan>(
	MOSDPGScan::OP_SCAN_DIGEST,
	pg.get_pg_whoami(),
	pg.get_osdmap_epoch(),
	query_epoch,
	spg_t(pg.get_info().pgid.pgid, pg.get_primary().shard),
	backfill_interval.begin,
	backfill_interval.end);
      encode(backfill_interval.objects, reply->get_data());
      return conn->send(std::move(reply));
    });
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_scan_digest(
  MOSDPGScan& m)
{
  LOG_PREFIX(RecoveryBackend::handle_scan_digest);
  DEBUGDPP("", pg);
  // Check that from is in backfill_targets vector
  ceph_assert(pg.is_backfill_target(m.from));

  ReplicaBackfillInterval bi;
  bi.begin = m.begin;
  bi.end = m.end;
  {
    auto p = m.get_data().cbegin();
    // take care to preserve ordering!
    bi.clear_objects();
    ::decode_noclear(bi.objects, p);
  }
  auto recovery_handler = pg.get_recovery_handler();
  recovery_handler->dispatch_backfill_event(
    crimson::osd::BackfillState::ReplicaScanned{
      m.from, std::move(bi) }.intrusive_from_this());
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_scan(
  MOSDPGScan& m,
  crimson::net::ConnectionXcoreRef conn)
{
  LOG_PREFIX(RecoveryBackend::handle_scan);
  DEBUGDPP("", pg);
  if (pg.old_peering_msg(m.map_epoch, m.query_epoch)) {
    DEBUGDPP("discarding {}", pg, m);
    return seastar::now();
  }
  switch (m.op) {
    case MOSDPGScan::OP_SCAN_GET_DIGEST:
      return handle_scan_get_digest(m, conn);
    case MOSDPGScan::OP_SCAN_DIGEST:
      return handle_scan_digest(m);
    default:
      // FIXME: move to errorator
      ceph_assert("unknown op type for pg scan");
      return seastar::now();
  }
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill_op(
  Ref<MOSDFastDispatchOp> m,
  crimson::net::ConnectionXcoreRef conn)
{
  switch (m->get_header().type) {
  case MSG_OSD_PG_BACKFILL:
    return handle_backfill(*boost::static_pointer_cast<MOSDPGBackfill>(m), conn);
  case MSG_OSD_PG_BACKFILL_REMOVE:
    return handle_backfill_remove(*boost::static_pointer_cast<MOSDPGBackfillRemove>(m));
  case MSG_OSD_PG_SCAN:
    return handle_scan(*boost::static_pointer_cast<MOSDPGScan>(m), conn);
  default:
    return seastar::make_exception_future<>(
	std::invalid_argument(fmt::format("invalid request type: {}",
					  (uint16_t)m->get_header().type)));
  }
}
