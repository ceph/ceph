// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ecrep_request.h"

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

ECRepRequest::ECRepRequest(crimson::net::ConnectionRef&& conn,
		       MessageRef &&req)
  : conn{std::move(conn)}//,
    //req{std::move(req)}
{}

void ECRepRequest::print(std::ostream& os) const
{
  os << "ECRepRequest("
     << ")";
}

void ECRepRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("ECRepRequest");
  f->dump_stream("req_tid") << req->get_tid();
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->close_section();
}

ConnectionPipeline &ECRepRequest::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).replicated_request_conn_pipeline;
}

ClientRequest::PGPipeline &ECRepRequest::pp(PG &pg)
{
  return pg.request_pg_pipeline;
}

seastar::future<> ECRepRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  logger().debug("{}: ECRepRequest::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    return interruptor::now();
  }, [ref](std::exception_ptr) { return seastar::now(); }, pg);
}

}

