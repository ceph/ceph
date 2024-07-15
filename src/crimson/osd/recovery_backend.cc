// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>

#include "crimson/common/exception.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/osd_operations/background_recovery.h"

#include "messages/MOSDFastDispatchOp.h"
#include "osd/osd_types.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

hobject_t RecoveryBackend::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version) const
{
  hobject_t hoid =
    target.make_temp_hobject(fmt::format("temp_recovering_{}_{}_{}_{}",
                                         pg.get_info().pgid,
                                         version,
                                         pg.get_info().history.same_interval_since,
                                         target.snap));
  logger().debug("{} {}", __func__, hoid);
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
			       std::string_view why)
{
  for (auto& soid : temp_contents) {
    t.remove(pg.get_collection_ref()->get_cid(),
	      ghobject_t(soid, ghobject_t::NO_GEN, pg.get_pg_whoami().shard));
  }
  temp_contents.clear();

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
  logger().debug("{}", __func__);
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
  logger().debug("{}", __func__);
  ceph_assert(!pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 2);

  ObjectStore::Transaction t;
  pg.get_peering_state().update_backfill_progress(
    m.last_backfill,
    m.stats,
    m.op == MOSDPGBackfill::OP_BACKFILL_PROGRESS,
    t);
  logger().debug("RecoveryBackend::handle_backfill_progress: do_transaction...");
  return shard_services.get_store().do_transaction(
    pg.get_collection_ref(), std::move(t)).or_terminate();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill_finish_ack(
  MOSDPGBackfill& m)
{
  logger().debug("{}", __func__);
  ceph_assert(pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 3);
  // TODO:
  // finish_recovery_op(hobject_t::get_max());
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_backfill(
  MOSDPGBackfill& m,
  crimson::net::ConnectionXcoreRef conn)
{
  logger().debug("{}", __func__);
  if (pg.old_peering_msg(m.map_epoch, m.query_epoch)) {
    logger().debug("{}: discarding {}", __func__, m);
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
  logger().debug("{} m.ls={}", __func__, m.ls);
  assert(m.get_type() == MSG_OSD_PG_BACKFILL_REMOVE);

  ObjectStore::Transaction t;
  for ([[maybe_unused]] const auto& [soid, ver] : m.ls) {
    // TODO: the reserved space management. PG::try_reserve_recovery_space().
    t.remove(pg.get_collection_ref()->get_cid(),
	      ghobject_t(soid, ghobject_t::NO_GEN, pg.get_pg_whoami().shard));
  }
  logger().debug("RecoveryBackend::handle_backfill_remove: do_transaction...");
  return shard_services.get_store().do_transaction(
    pg.get_collection_ref(), std::move(t)).or_terminate();
}

RecoveryBackend::interruptible_future<BackfillInterval>
RecoveryBackend::scan_for_backfill(
  const hobject_t& start,
  [[maybe_unused]] const std::int64_t min,
  const std::int64_t max)
{
  logger().debug("{} starting from {}", __func__, start);
  auto version_map = seastar::make_lw_shared<std::map<hobject_t, eversion_t>>();
  return backend->list_objects(start, max).then_interruptible(
    [this, start, version_map] (auto&& ret) {
    auto&& [objects, next] = std::move(ret);
    return seastar::do_with(
      std::move(objects),
      [this, version_map](auto &objects) {
      return interruptor::parallel_for_each(objects,
	[this, version_map] (const hobject_t& object)
	-> interruptible_future<> {
	crimson::osd::ObjectContextRef obc;
	if (pg.is_primary()) {
	  obc = pg.obc_registry.maybe_get_cached_obc(object);
	}
	if (obc) {
	  if (obc->obs.exists) {
	    logger().debug("scan_for_backfill found (primary): {}  {}",
			   object, obc->obs.oi.version);
	    version_map->emplace(object, obc->obs.oi.version);
	  } else {
	    // if the object does not exist here, it must have been removed
	    // between the collection_list_partial and here.  This can happen
	    // for the first item in the range, which is usually last_backfill.
	  }
	  return seastar::now();
	} else {
	  return backend->load_metadata(object).safe_then_interruptible(
	    [version_map, object] (auto md) {
	    if (md->os.exists) {
	      logger().debug("scan_for_backfill found: {}  {}",
			     object, md->os.oi.version);
	      version_map->emplace(object, md->os.oi.version);
	    }
	    return seastar::now();
	  }, PGBackend::load_metadata_ertr::assert_all{});
	}
      });
    }).then_interruptible([version_map, start=std::move(start), next=std::move(next), this] {
      BackfillInterval bi;
      bi.begin = std::move(start);
      bi.end = std::move(next);
      bi.version = pg.get_info().last_update;
      bi.objects = std::move(*version_map);
      logger().debug("{} BackfillInterval filled, leaving",
                     "scan_for_backfill");
      return seastar::make_ready_future<BackfillInterval>(std::move(bi));
    });
  });
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_scan_get_digest(
  MOSDPGScan& m,
  crimson::net::ConnectionXcoreRef conn)
{
  logger().debug("{}", __func__);
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
  return scan_for_backfill(
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
  logger().debug("{}", __func__);
  // Check that from is in backfill_targets vector
  ceph_assert(pg.is_backfill_target(m.from));

  BackfillInterval bi;
  bi.begin = m.begin;
  bi.end = m.end;
  {
    auto p = m.get_data().cbegin();
    // take care to preserve ordering!
    bi.clear_objects();
    ::decode_noclear(bi.objects, p);
  }
  shard_services.start_operation<crimson::osd::BackfillRecovery>(
    static_cast<crimson::osd::PG*>(&pg),
    shard_services,
    pg.get_osdmap_epoch(),
    crimson::osd::BackfillState::ReplicaScanned{ m.from, std::move(bi) });
  return seastar::now();
}

RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_scan(
  MOSDPGScan& m,
  crimson::net::ConnectionXcoreRef conn)
{
  logger().debug("{}", __func__);
  if (pg.old_peering_msg(m.map_epoch, m.query_epoch)) {
    logger().debug("{}: discarding {}", __func__, m);
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
RecoveryBackend::handle_recovery_op(
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
					  m->get_header().type)));
  }
}
