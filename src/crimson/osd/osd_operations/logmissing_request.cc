// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

namespace crimson::osd {

LogMissingRequest::LogMissingRequest(crimson::net::ConnectionRef&& conn,
		       Ref<MOSDPGUpdateLogMissing> &&req)
  : conn{std::move(conn)},
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
  return get_osd_priv(conn.get()).replicated_request_conn_pipeline;
}

RepRequest::PGPipeline &LogMissingRequest::pp(PG &pg)
{
  return pg.replicated_request_pg_pipeline;
}

seastar::future<> LogMissingRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  logger().debug("{}: LogMissingRequest::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    return pg->do_update_log_missing(std::move(req));
  }, [ref](std::exception_ptr) { return seastar::now(); }, pg);
}

}
