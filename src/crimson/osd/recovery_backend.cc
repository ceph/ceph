// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>

#include "crimson/common/exception.h"
#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/pg_backend.h"

#include "messages/MOSDFastDispatchOp.h"
#include "osd/osd_types.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

hobject_t RecoveryBackend::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version)
{
  ostringstream ss;
  ss << "temp_recovering_" << pg.get_info().pgid << "_" << version
    << "_" << pg.get_info().history.same_interval_since << "_" << target.snap;
  hobject_t hoid = target.make_temp_hobject(ss.str());
  logger().debug("{} {}", __func__, hoid);
  return hoid;
}

void RecoveryBackend::clean_up(ceph::os::Transaction& t,
			       const std::string& why)
{
  for (auto& soid : temp_contents) {
    t.remove(pg.get_collection_ref()->get_cid(),
	      ghobject_t(soid, ghobject_t::NO_GEN, pg.get_pg_whoami().shard));
  }
  temp_contents.clear();

  for (auto& [soid, recovery_waiter] : recovering) {
    if (recovery_waiter.obc && recovery_waiter.obc->obs.exists) {
      recovery_waiter.obc->drop_recovery_read();
      recovery_waiter.interrupt(why);
    }
  }
  recovering.clear();
}

void RecoveryBackend::WaitForObjectRecovery::stop() {
  readable.set_exception(
      crimson::common::system_shutdown_exception());
  recovered.set_exception(
      crimson::common::system_shutdown_exception());
  pulled.set_exception(
      crimson::common::system_shutdown_exception());
  for (auto& [pg_shard, pr] : pushes) {
    pr.set_exception(
	crimson::common::system_shutdown_exception());
  }
}

seastar::future<BackfillInterval> RecoveryBackend::scan_for_backfill(
  const hobject_t& start,
  [[maybe_unused]] const std::int64_t min,
  const std::int64_t max)
{
  logger().debug("{} starting from {}", __func__, start);
  return seastar::do_with(
    std::map<hobject_t, eversion_t>{},
    [this, &start, max] (auto& version_map) {
      return backend->list_objects(start, max).then(
        [this, &start, &version_map] (auto&& ret) {
          auto& [objects, next] = ret;
          return seastar::do_for_each(
            objects,
            [this, &version_map] (const hobject_t& object) {
              crimson::osd::ObjectContextRef obc;
              if (pg.is_primary()) {
                obc = shard_services.obc_registry.maybe_get_cached_obc(object);
              }
              if (obc) {
                if (obc->obs.exists) {
                  logger().debug("scan_for_backfill found (primary): {}  {}",
                                 object, obc->obs.oi.version);
                  version_map[object] = obc->obs.oi.version;
                } else {
                  // if the object does not exist here, it must have been removed
                  // between the collection_list_partial and here.  This can happen
                  // for the first item in the range, which is usually last_backfill.
                }
                return seastar::now();
              } else {
                return backend->load_metadata(object).safe_then(
                  [&version_map, object] (auto md) {
                    if (md->os.exists) {
                      logger().debug("scan_for_backfill found: {}  {}",
                                     object, md->os.oi.version);
                      version_map[object] = md->os.oi.version;
                    }
                    return seastar::now();
                  }, PGBackend::load_metadata_ertr::assert_all{});
              }
          }).then(
            [&version_map, &start, next=std::move(next), this] {
              BackfillInterval bi;
              bi.begin = start;
              bi.end = std::move(next);
              bi.version = pg.get_info().last_update;
              bi.objects = std::move(version_map);
              logger().debug("{} BackfillInterval filled, leaving",
                             "scan_for_backfill");
              return seastar::make_ready_future<BackfillInterval>(std::move(bi));
            });
        });
    });
}

seastar::future<> RecoveryBackend::handle_scan_get_digest(
  MOSDPGScan& m)
{
  logger().debug("{}", __func__);
  if (false /* FIXME: check for backfill too full */) {
    std::ignore = shard_services.start_operation<crimson::osd::LocalPeeringEvent>(
      // TODO: abstract start_background_recovery
      static_cast<crimson::osd::PG*>(&pg),
      shard_services,
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
  ).then([this,
          query_epoch=m.query_epoch,
          conn=m.get_connection()] (auto backfill_interval) {
    auto reply = make_message<MOSDPGScan>(
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

seastar::future<> RecoveryBackend::handle_scan_digest(
  MOSDPGScan& m)
{
  logger().debug("{}", __func__);
  ceph_assert("Not implemented" == nullptr);
  return seastar::now();
}

seastar::future<> RecoveryBackend::handle_scan(
  MOSDPGScan& m)
{
  logger().debug("{}", __func__);
  switch (m.op) {
    case MOSDPGScan::OP_SCAN_GET_DIGEST:
      return handle_scan_get_digest(m);
    case MOSDPGScan::OP_SCAN_DIGEST:
      return handle_scan_digest(m);
    default:
      // FIXME: move to errorator
      ceph_assert("unknown op type for pg scan");
      return seastar::now();
  }
}

seastar::future<> RecoveryBackend::handle_recovery_op(
  Ref<MOSDFastDispatchOp> m)
{
  switch (m->get_header().type) {
  case MSG_OSD_PG_SCAN:
    return handle_scan(*boost::static_pointer_cast<MOSDPGScan>(m));
  default:
    return seastar::make_exception_future<>(
	std::invalid_argument(fmt::format("invalid request type: {}",
					  m->get_header().type)));
  }
}
