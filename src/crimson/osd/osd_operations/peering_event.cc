// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "common/Formatter.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

void PeeringEvent::print(std::ostream &lhs) const
{
  lhs << "PeeringEvent("
      << "from=" << from
      << " pgid=" << pgid
      << " sent=" << evt.get_epoch_sent()
      << " requested=" << evt.get_epoch_requested()
      << " evt=" << evt.get_desc()
      << ")";
}

void PeeringEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("PeeringEvent");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  f->dump_int("sent", evt.get_epoch_sent());
  f->dump_int("requested", evt.get_epoch_requested());
  f->dump_string("evt", evt.get_desc());
  f->close_section();
}


PeeringEvent::PGPipeline &PeeringEvent::pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

seastar::future<> PeeringEvent::start()
{

  logger().debug("{}: start", *this);

  IRef ref = this;
  return get_pg().then([this](Ref<PG> pg) {
    if (!pg) {
      logger().debug("{}: pg absent, did not create", *this);
      on_pg_absent();
      handle.exit();
      return complete_rctx(pg);
    } else {
      logger().debug("{}: pg present", *this);
      return with_blocking_future(handle.enter(pp(*pg).await_map)
      ).then([this, pg] {
	return with_blocking_future(
	  pg->osdmap_gate.wait_for_map(evt.get_epoch_sent()));
      }).then([this, pg](auto) {
	return with_blocking_future(handle.enter(pp(*pg).process));
      }).then([this, pg] {
	pg->do_peering_event(evt, ctx);
	handle.exit();
	return complete_rctx(pg);
      });
    }
  }).then([this, ref=std::move(ref)] {
    logger().debug("{}: complete", *this);
  });
}

void PeeringEvent::on_pg_absent()
{
  logger().debug("{}: pg absent, dropping", *this);
}

seastar::future<> PeeringEvent::complete_rctx(Ref<PG> pg)
{
  logger().debug("{}: submitting ctx", *this);
  return shard_services.dispatch_context(
    pg->get_collection_ref(),
    std::move(ctx));
}

RemotePeeringEvent::ConnectionPipeline &RemotePeeringEvent::cp()
{
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

seastar::future<Ref<PG>> RemotePeeringEvent::get_pg() {
  return with_blocking_future(
    handle.enter(cp().await_map)
  ).then([this] {
    return with_blocking_future(
      osd.osdmap_gate.wait_for_map(evt.get_epoch_sent()));
  }).then([this](auto epoch) {
    logger().debug("{}: got map {}", *this, epoch);
    return with_blocking_future(handle.enter(cp().get_pg));
  }).then([this] {
    return with_blocking_future(
      osd.get_or_create_pg(
	pgid, evt.get_epoch_sent(), std::move(evt.create_info)));
  });
}

seastar::future<Ref<PG>> LocalPeeringEvent::get_pg() {
  return seastar::make_ready_future<Ref<PG>>(pg);
}

LocalPeeringEvent::~LocalPeeringEvent() {}

}
