// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "logmissing_request.h"

#include "common/Formatter.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

SET_SUBSYS(osd);

namespace crimson::osd {

LogMissingRequest::LogMissingRequest(crimson::net::ConnectionRef&& conn,
		       Ref<MOSDPGUpdateLogMissing> &&req)
  : RemoteOperation{std::move(conn)},
    req{std::move(req)}
{}

void LogMissingRequest::print(std::ostream& os) const
{
  os << "LogMissingRequest("
     << "from=" << req->from
     << " req=" << *req
     << ")";
}

void LogMissingRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("LogMissingRequest");
  f->dump_stream("req_tid") << req->get_tid();
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("entries") << req->entries;
  f->dump_stream("from") << req->from;
  f->close_section();
}

ConnectionPipeline &LogMissingRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).replicated_request_conn_pipeline;
}

PerShardPipeline &LogMissingRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

PGRepopPipeline &LogMissingRequest::repop_pipeline(PG &pg)
{
  return pg.repop_pipeline;
}

LogMissingRequest::interruptible_future<>
LogMissingRequest::with_pg_interruptible(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(LogMissingRequest::with_pg_interruptible);
  DEBUGI("{}: pg present", *this);
  co_await this->template enter_stage<interruptor>(
    repop_pipeline(*pg).process);

  co_await interruptor::make_interruptible(
  this->template with_blocking_event<
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent
  >([this, pg](auto &&trigger) {
    return pg->osdmap_gate.wait_for_map(
      std::move(trigger), req->min_epoch);
  }));

  auto throttle = co_await interruptor::make_interruptible(
    shard_services.get_throttle(
      scheduler::params_t{
        1,
        static_cast<unsigned>(req->get_priority()),
        0,
        SchedulerClass::repop}));
  co_await pg->do_update_log_missing(req, get_remote_connection());
  logger().debug("{}: complete", *this);
  co_await interruptor::make_interruptible(handle.complete());
  // throttle destructs here
}

seastar::future<> LogMissingRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(LogMissingRequest::with_pg);
  DEBUGI("{}: LogMissingRequest::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption
    ([this, pg, &shard_services] {
      return with_pg_interruptible(shard_services, pg);
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pg, pg->get_osdmap_epoch()).finally([this, ref=std::move(ref)] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

}
