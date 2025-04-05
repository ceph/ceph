// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "logmissing_request.h"

#include "common/Formatter.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/replicated_backend.h"

SET_SUBSYS(osd);

namespace crimson::osd {

PGPCTRequest::PGPCTRequest(crimson::net::ConnectionRef&& conn,
		       Ref<MOSDPGPCT> &&req)
  : RemoteOperation{std::move(conn)},
    req{std::move(req)}
{}

void PGPCTRequest::print(std::ostream& os) const
{
  os << "PGPCTRequest("
     << " req=" << *req
     << ")";
}

void PGPCTRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("PGPCTRequest");
  f->dump_stream("pgid") << req->get_spg();
  f->dump_unsigned("map_epoch", req->get_map_epoch());
  f->dump_unsigned("min_epoch", req->get_min_epoch());
  f->dump_stream("pg_committed_to") << req->pg_committed_to;
  f->close_section();
}

ConnectionPipeline &PGPCTRequest::get_connection_pipeline()
{
  return get_osd_priv(
    &get_local_connection()
  ).client_request_conn_pipeline;
}

PerShardPipeline &PGPCTRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_replicated_request_pipeline();
}

PGPCTRequest::interruptible_future<> PGPCTRequest::with_pg_interruptible(
  PG &pg)
{
  LOG_PREFIX(PGPCTRequest::with_pg_interruptible);
  DEBUGDPP("{}", pg, *this);
  co_await this->template enter_stage<interruptor>(pg.repop_pipeline.process);

  {
    auto fut = this->template with_blocking_event<
      PG_OSDMapGate::OSDMapBlocker::BlockingEvent
      >([this, &pg](auto &&trigger) {
	return pg.osdmap_gate.wait_for_map(
	  std::move(trigger), req->min_epoch);
      });
    co_await interruptor::make_interruptible(std::move(fut));
  }

  // This *must* be a replicated backend, ec doesn't have pct messages
  static_cast<ReplicatedBackend&>(*(pg.backend)).do_pct(*req);
}

seastar::future<> PGPCTRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  LOG_PREFIX(PGPCTRequest::with_pg);
  DEBUGDPP("{}", *pgref, *this);

  PG &pg = *pgref;
  IRef ref = this;
  return interruptor::with_interruption([this, &pg] {
    return with_pg_interruptible(pg);
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pgref, pgref->get_osdmap_epoch()).finally(
    [FNAME, this, ref=std::move(ref), pgref=std::move(pgref)]() mutable {
      DEBUGDPP("exit", *pgref, *this);
      return handle.complete(
      ).then([ref=std::move(ref), pgref=std::move(pgref)] {});
    });
}

}
