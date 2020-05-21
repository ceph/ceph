// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "messages/MOSDOp.h"

#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/background_recovery.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

BackgroundRecovery::BackgroundRecovery(
  Ref<PG> pg,
  ShardServices &ss,
  epoch_t epoch_started,
  crimson::osd::scheduler::scheduler_class_t scheduler_class)
  : pg(pg), ss(ss), epoch_started(epoch_started),
    scheduler_class(scheduler_class)
{}

void BackgroundRecovery::print(std::ostream &lhs) const
{
  lhs << "BackgroundRecovery(" << pg->get_pgid() << ")";
}

void BackgroundRecovery::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
  f->open_object_section("recovery_detail");
  {
    // TODO pg->dump_recovery_state(f);
  }
  f->close_section();
}

seastar::future<> BackgroundRecovery::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  return ss.throttler.with_throttle_while(
    this, get_scheduler_params(), [this] {
      return do_recovery();
    }).handle_exception_type([ref, this](const std::system_error& err) {
      if (err.code() == std::make_error_code(std::errc::interrupted)) {
	logger().debug("{} recovery interruped: {}", *pg, err.what());
	return seastar::now();
      }
      return seastar::make_exception_future<>(err);
    });
}

seastar::future<bool> UrgentRecovery::do_recovery()
{
  if (!pg->has_reset_since(epoch_started)) {
    auto futopt = pg->get_recovery_handler()->recover_missing(soid, need);
    assert(futopt);
    return with_blocking_future(std::move(*futopt)).then([] {
      return seastar::make_ready_future<bool>(false);
    });
  }
  return seastar::make_ready_future<bool>(false);
}

void UrgentRecovery::print(std::ostream &lhs) const
{
  lhs << "UrgentRecovery(" << pg->get_pgid() << ", "
    << soid << ", v" << need << ")";
}

void UrgentRecovery::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
  f->open_object_section("recovery_detail");
  {
    f->dump_stream("oid") << soid;
    f->dump_stream("version") << need;
  }
  f->close_section();
}

PglogBasedRecovery::PglogBasedRecovery(
  Ref<PG> pg,
  ShardServices &ss,
  const epoch_t epoch_started)
  : BackgroundRecovery(
      std::move(pg),
      ss,
      epoch_started,
      crimson::osd::scheduler::scheduler_class_t::background_recovery)
{}

seastar::future<bool> PglogBasedRecovery::do_recovery()
{
  if (pg->has_reset_since(epoch_started))
    return seastar::make_ready_future<bool>(false);
  return with_blocking_future(
    pg->get_recovery_handler()->start_recovery_ops(
      crimson::common::local_conf()->osd_recovery_max_single_start));
}

BackfillRecovery::BackfillRecoveryPipeline &BackfillRecovery::bp(PG &pg)
{
  return pg.backfill_pipeline;
}

seastar::future<bool> BackfillRecovery::do_recovery()
{
  logger().debug("{}", __func__);

  if (pg->has_reset_since(epoch_started)) {
    logger().debug("{}: pg got reset since epoch_started={}",
                   __func__, epoch_started);
    return seastar::make_ready_future<bool>(false);
  }
  // TODO: limits
  return with_blocking_future(
    // process_event() of our boost::statechart machine is non-reentrant.
    // with the backfill_pipeline we protect it from a second entry from
    // the implementation of BackfillListener.
    // additionally, this stage serves to synchronize with PeeringEvent.
    handle.enter(bp(*pg).process)
  ).then([this] {
    pg->get_recovery_handler()->dispatch_backfill_event(std::move(evt));
    return seastar::make_ready_future<bool>(false);
  });
}

} // namespace crimson::osd
