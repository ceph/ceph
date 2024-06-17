// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_request.h"

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

RepRequest::RepRequest(crimson::net::ConnectionRef&& conn,
		       Ref<MOSDRepOp> &&req)
  : l_conn{std::move(conn)},
    req{std::move(req)}
{}

void RepRequest::print(std::ostream& os) const
{
  os << "RepRequest("
     << "from=" << req->from
     << " req=" << *req
     << ")";
}

void RepRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("RepRequest");
  f->dump_stream("reqid") << req->reqid;
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("oid") << req->poid;
  f->dump_stream("from") << req->from;
  f->close_section();
}

ConnectionPipeline &RepRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).replicated_request_conn_pipeline;
}

PerShardPipeline &RepRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

ClientRequest::PGPipeline &RepRequest::client_pp(PG &pg)
{
  return pg.request_pg_pipeline;
}

seastar::future<> RepRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(RepRequest::with_pg);
  DEBUGI("{}: RepRequest::with_pg", *this);
  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    LOG_PREFIX(RepRequest::with_pg);
    DEBUGI("{}: pg present", *this);
    return this->template enter_stage<interruptor>(client_pp(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
        PG_OSDMapGate::OSDMapBlocker::BlockingEvent
      >([this, pg](auto &&trigger) {
        return pg->osdmap_gate.wait_for_map(
          std::move(trigger), req->min_epoch);
      });
    }).then_interruptible([this, pg] (auto) {
      return pg->handle_rep_op(req);
    }).then_interruptible([this] {
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
