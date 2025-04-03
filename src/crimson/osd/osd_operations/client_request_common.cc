// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/common/log.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/background_recovery.h"

SET_SUBSYS(osd);

namespace crimson::osd {

typename InterruptibleOperation::template interruptible_future<bool>
CommonClientRequest::do_recover_missing(
  Ref<PG> pg,
  const hobject_t& soid,
  const osd_reqid_t& reqid)
{
  LOG_PREFIX(CommonCLientRequest::do_recover_missing);
  DEBUGDPP(
    "reqid {} check for recovery, {}",
    *pg, reqid, soid);
  assert(pg->is_primary());
  eversion_t ver;
  auto &peering_state = pg->get_peering_state();
  auto &missing_loc = peering_state.get_missing_loc();
  bool needs_recovery_or_backfill = false;

  if (pg->is_unreadable_object(soid)) {
    DEBUGDPP(
      "reqid {}, {} is unreadable",
      *pg, reqid, soid);
    ceph_assert(missing_loc.needs_recovery(soid, &ver));
    needs_recovery_or_backfill = true;
  }

  if (pg->is_degraded_or_backfilling_object(soid)) {
    DEBUGDPP(
      "reqid {}, {} is degraded or backfilling",
      *pg, reqid, soid);
    if (missing_loc.needs_recovery(soid, &ver)) {
      needs_recovery_or_backfill = true;
    }
  }

  if (!needs_recovery_or_backfill) {
    DEBUGDPP(
      "reqid {} nothing to recover {}",
      *pg, reqid, soid);
    return seastar::make_ready_future<bool>(false);
  }

  if (pg->get_peering_state().get_missing_loc().is_unfound(soid)) {
    return seastar::make_ready_future<bool>(true);
  }
  DEBUGDPP(
    "reqid {} need to wait for recovery, {} version {}",
    *pg, reqid, soid);
  if (pg->get_recovery_backend()->is_recovering(soid)) {
    DEBUGDPP(
      "reqid {} object {} version {}, already recovering",
      *pg, reqid, soid, ver);
    return pg->get_recovery_backend()->get_recovering(
      soid).wait_for_recovered(
    ).then([] {
      return seastar::make_ready_future<bool>(false);
    });
  } else {
    DEBUGDPP(
      "reqid {} object {} version {}, starting recovery",
      *pg, reqid, soid, ver);
    auto [op, fut] =
      pg->get_shard_services().start_operation<UrgentRecovery>(
        soid, ver, pg, pg->get_shard_services(), pg->get_osdmap_epoch());
    return fut.then([] { return seastar::make_ready_future<bool>(false); });
  }
}

} // namespace crimson::osd
