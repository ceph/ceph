// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "logmissing_request_reply.h"

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

namespace crimson::osd {

LogMissingRequestReply::LogMissingRequestReply(
  crimson::net::ConnectionRef&& conn,
  Ref<MOSDPGUpdateLogMissingReply> &&req)
  : l_conn{std::move(conn)},
    req{std::move(req)}
{}

void LogMissingRequestReply::print(std::ostream& os) const
{
  os << "LogMissingRequestReply("
     << "from=" << req->from
     << " req=" << *req
     << ")";
}

void LogMissingRequestReply::dump_detail(Formatter *f) const
{
  f->open_object_section("LogMissingRequestReply");
  f->dump_stream("rep_tid") << req->get_tid();
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("from") << req->from;
  f->close_section();
}

ConnectionPipeline &LogMissingRequestReply::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).replicated_request_conn_pipeline;
}

PerShardPipeline &LogMissingRequestReply::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

ClientRequest::PGPipeline &LogMissingRequestReply::client_pp(PG &pg)
{
  return pg.request_pg_pipeline;
}

seastar::future<> LogMissingRequestReply::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  logger().debug("{}: LogMissingRequestReply::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    return pg->do_update_log_missing_reply(req
    ).then_interruptible([this] {
      logger().debug("{}: complete", *this);
      return handle.complete();
    });
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pg).finally([this, ref=std::move(ref)] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

}
