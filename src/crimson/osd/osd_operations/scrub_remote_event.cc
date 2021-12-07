// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "scrub_remote_event.h"

#include <boost/smart_ptr/local_shared_ptr.hpp>

#include <seastar/core/future.hh>

#include "common/Formatter.h"
#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/scrubber/pg_scrubber.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDScrubReserve.h"
#include "msg/MessageRef.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace


namespace crimson::osd {

ScrubRemoteEvent::ScrubRemoteEvent(crimson::net::ConnectionRef conn,
				   Ref<Message> scrubop,
				   ShardServices& shard_services,
				   const pg_shard_t& from,
				   std::chrono::milliseconds delay)
    : conn{conn}
    , scrub_op{boost::static_pointer_cast<MOSDFastDispatchOp>(scrubop)}
    , shard_services{shard_services}
    , from{from}
    , pgid{scrub_op->get_spg()}
    , epoch_queued{scrub_op->get_map_epoch()}
{}

void ScrubRemoteEvent::parse_into_event(ShardServices& shardservices,
					Ref<PG> msg_pg)
{
  pg = msg_pg;
  switch (scrub_op->get_type()) {
    case MSG_OSD_SCRUB_RESERVE: {
      auto m = static_cast<MOSDScrubReserve*>(scrub_op.get());
      from = m->from;
      switch (m->type) {
	case MOSDScrubReserve::REQUEST:
	  event_fwd_func =
	    (ScrubRmtEventFwd)&PgScrubber::handle_scrub_reserve_request;
	  break;
	case MOSDScrubReserve::GRANT:
	  event_fwd_func =
	    (ScrubRmtEventFwd)&PgScrubber::handle_scrub_reserve_grant;
	  break;
	case MOSDScrubReserve::REJECT:
	  event_fwd_func =
	    (ScrubRmtEventFwd)&PgScrubber::handle_scrub_reserve_reject;
	  break;
	case MOSDScrubReserve::RELEASE:
	  event_fwd_func =
	    (ScrubRmtEventFwd)&PgScrubber::handle_scrub_reserve_release;
	  break;
      }
    } break;

    default:  // a message we don't understand
      logger().error("{} got unexpected scrub reserve message {}",
		     __func__,
		     *scrub_op);
      event_fwd_func = (ScrubRmtEventFwd)&PgScrubber::handle_unknown_req;
      break;
  }
}

void ScrubRemoteEvent::print(std::ostream& lhs) const
{
  lhs << fmt::format("{}", *this);
}

void ScrubRemoteEvent::dump_detail(Formatter* f) const
{
  /// \todo add more fields
  f->open_object_section("ScrubRemoteEvent");
  f->dump_stream("pgid") << pgid;
  f->close_section();
}

void ScrubRemoteEvent::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

seastar::future<Ref<PG>> ScrubRemoteEvent::get_pg()
{
  return seastar::make_ready_future<Ref<PG>>(pg);
}

// note: ScrubEvent & ScrubRemoteEvent are sharing the pipeline
ScrubEvent::PGPipeline& ScrubRemoteEvent::pp(PG& pg)
{
  return pg.scrub_event_pg_pipeline;
}

ScrubRemoteEvent::~ScrubRemoteEvent() = default;

ConnectionPipeline& ScrubRemoteEvent::get_connection_pipeline()
{
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

seastar::future<> ScrubRemoteEvent::with_pg(ShardServices& shard_services,
					    Ref<PG> pg)
{
  logger().debug("{}: {}", "ScrubRemoteEvent::with_pg", *this);

  track_event<StartEvent>();
  IRef opref = this;

  // create a scrub event based on the parsed request
  parse_into_event(shard_services, pg);

  return interruptor::with_interruption(
	   [this, pg]() mutable -> ScrubRemoteEvent::interruptible_future<> {
	     std::ignore = ((*(pg->get_scrubber(ScrubberPasskey{})).*
			     event_fwd_func)(conn, scrub_op, 0, from));

	     // just for now
	     return seastar::make_ready_future<>();
	   },
	   [](std::exception_ptr) { return seastar::now(); },
	   pg)
    .finally([this] { track_event<CompletionEvent>(); });
}

}  // namespace crimson::osd
