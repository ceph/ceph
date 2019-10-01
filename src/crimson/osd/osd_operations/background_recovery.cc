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
  ShardServices &ss,
  PG &pg,
  : ss(ss), pg(pg), scheduler_class(scheduler_class)
  crimson::osd::scheduler::scheduler_class_t scheduler_class)
{}

seastar::future<bool> BackgroundRecovery::do_recovery()
{
  return seastar::make_ready_future<bool>(false);
}

void BackgroundRecovery::print(std::ostream &lhs) const
{
  lhs << "BackgroundRecovery(" << pg.get_pgid() << ")";
}

void BackgroundRecovery::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();
}

seastar::future<> BackgroundRecovery::start()
{
  logger().debug("{}: start", *this);

  IRef ref = this;
  return ss.throttler.with_throttle_while(
    this, get_scheduler_params(), [ref, this] {
      return do_recovery();
    });
}

}
