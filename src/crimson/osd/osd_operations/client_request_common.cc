// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/background_recovery.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

typename InterruptibleOperation::template interruptible_future<bool>
CommonClientRequest::do_recover_missing(
  Ref<PG> pg,
  const hobject_t& soid,
  const osd_reqid_t& reqid)
{
  logger().debug("{} reqid {} check for recovery, {}",
                 __func__, reqid, soid);
  assert(pg->is_primary());
  eversion_t ver;
  auto &peering_state = pg->get_peering_state();
  auto &missing_loc = peering_state.get_missing_loc();
  bool needs_recovery_or_backfill = false;

  if (pg->is_unreadable_object(soid)) {
    logger().debug("{} reqid {}, {} is unreadable",
                   __func__, reqid, soid);
    ceph_assert(missing_loc.needs_recovery(soid, &ver));
    needs_recovery_or_backfill = true;
  }

  if (pg->is_degraded_or_backfilling_object(soid)) {
    logger().debug("{} reqid {}, {} is degraded or backfilling",
                   __func__, reqid, soid);
    if (missing_loc.needs_recovery(soid, &ver)) {
      needs_recovery_or_backfill = true;
    }
  }

  if (!needs_recovery_or_backfill) {
    logger().debug("{} reqid {} nothing to recover {}",
                   __func__, reqid, soid);
    return seastar::make_ready_future<bool>(false);
  }

  if (pg->get_peering_state().get_missing_loc().is_unfound(soid)) {
    return seastar::make_ready_future<bool>(true);
  }
  logger().debug("{} reqid {} need to wait for recovery, {} version {}",
                 __func__, reqid, soid, ver);
  if (pg->get_recovery_backend()->is_recovering(soid)) {
    logger().debug("{} reqid {} object {} version {}, already recovering",
                   __func__, reqid, soid, ver);
    return pg->get_recovery_backend()->get_recovering(
      soid).wait_for_recovered(
    ).then([] {
      return seastar::make_ready_future<bool>(false);
    });
  } else {
    logger().debug("{} reqid {} object {} version {}, starting recovery",
                   __func__, reqid, soid, ver);
    auto [op, fut] =
      pg->get_shard_services().start_operation<UrgentRecovery>(
        soid, ver, pg, pg->get_shard_services(), pg->get_osdmap_epoch());
    return fut.then([] { return seastar::make_ready_future<bool>(false); });
  }
}

} // namespace crimson::osd
