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

void ECRepRequest::print(std::ostream& os) const
{
  os << "ECRepRequest("
     << ")";
}

void ECRepRequest::dump_detail(Formatter *f) const
{
#if 0
  f->open_object_section("ECRepRequest");
  f->dump_stream("req_tid") << req->get_tid();
  f->dump_stream("pgid") << get_pgid();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->close_section();
#endif
}

ConnectionPipeline &ECRepRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).replicated_request_conn_pipeline;
}

PerShardPipeline &ECRepRequest::get_pershard_pipeline(
  ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

// from https://en.cppreference.com/w/cpp/utility/variant/visit
// helper type for the visitor #4
template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide (not needed as of C++20)
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

seastar::future<> ECRepRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  logger().debug("{}: ECRepRequest::with_pg", *this);

  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    return interruptor::now();
  }, [ref, this](std::exception_ptr) {
    logger().debug("{}: ECRepRequest::exception handling", *this);
    return seastar::now();
  }, pg, pg->get_osdmap_epoch());
}

}

