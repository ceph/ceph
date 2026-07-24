// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::RecoverySubRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

SET_SUBSYS(osd);

namespace crimson::osd {

RecoverySubRequest::interruptible_future<>
RecoverySubRequest::with_pg_interruptible(
  ShardServices &shard_services, Ref<PG> pgref)
{
  LOG_PREFIX(RecoverySubRequest::with_pg_interruptible);
  DEBUGI("{}: {}", "RecoverySubRequest::with_pg", *this);
  auto throttle = co_await interruptor::make_interruptible(
    shard_services.get_throttle(
      scheduler::params_t{
        std::max<int>(pgref->get_average_object_size(), 1),
        static_cast<unsigned>(pgref->get_recovery_op_priority()),
        0,
        SchedulerClass::background_recovery}));
  co_await pgref->get_recovery_backend()->handle_recovery_op(
    m, get_remote_connection());
  DEBUGI("{}: complete", *this);
  co_await interruptor::make_interruptible(handle.complete());
  // throttle destructs here -> release_throttle()
}

seastar::future<> RecoverySubRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  track_event<StartEvent>();
  IRef opref = this;
  return interruptor::with_interruption([this, pgref, &shard_services] {
    return with_pg_interruptible(shard_services, pgref);
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pgref, pgref->get_osdmap_epoch()).finally([this, opref=std::move(opref), pgref] {
    logger().debug("{}: exit", *this);
    track_event<CompletionEvent>();
    handle.exit();
  });
}

ConnectionPipeline &RecoverySubRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).peering_request_conn_pipeline;
}

PerShardPipeline &RecoverySubRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_peering_request_pipeline();
}

}
