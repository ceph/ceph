// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_request.h"

#include "common/Formatter.h"

#include "crimson/common/coroutine.h"
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

PGRepopPipeline &RepRequest::repop_pipeline(PG &pg)
{
  return pg.repop_pipeline;
}

RepRequest::interruptible_future<> RepRequest::with_pg_interruptible(
  Ref<PG> pg)
{
  LOG_PREFIX(RepRequest::with_pg_interruptible);
  DEBUGI("{}", *this);
  co_await this->template enter_stage<interruptor>(repop_pipeline(*pg).process);
  {
    /* Splitting this expression into a fut and a seperate co_await
     * works around a gcc 11 bug (observed on 11.4.1 and gcc 11.5.0)
     * which results in the pg ref captured by the lambda being
     * destructed twice.  We can probably remove these workarounds
     * once we disallow gcc 11 */
    auto fut = interruptor::make_interruptible(
      this->template with_blocking_event<
      PG_OSDMapGate::OSDMapBlocker::BlockingEvent
      >([this, pg](auto &&trigger) {
	return pg->osdmap_gate.wait_for_map(
	  std::move(trigger), req->min_epoch);
      }));
    co_await std::move(fut);
  }

  if (pg->can_discard_replica_op(*req)) {
    co_return;
  }

  auto [commit_fut, reply] = co_await pg->handle_rep_op(req);

  // Transitions from OrderedExclusive->OrderedConcurrent cannot block
  this->enter_stage_sync(repop_pipeline(*pg).wait_commit);

  co_await std::move(commit_fut);

  co_await this->template enter_stage<interruptor>(
    repop_pipeline(*pg).send_reply);

  co_await interruptor::make_interruptible(
    pg->shard_services.send_to_osd(
      req->from.osd, std::move(reply), pg->get_osdmap_epoch())
  );
}

seastar::future<> RepRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  LOG_PREFIX(RepRequest::with_pg);
  DEBUGI("{}", *this);
  IRef ref = this;
  return interruptor::with_interruption([this, pg] {
    return with_pg_interruptible(pg);
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pg, pg->get_osdmap_epoch()
  ).finally([this, pg, ref=std::move(ref)]() mutable {
    logger().debug("{}: exit", *this);
    return handle.complete(
    ).finally([ref=std::move(ref), pg=std::move(pg)] {});
  });
}

}
