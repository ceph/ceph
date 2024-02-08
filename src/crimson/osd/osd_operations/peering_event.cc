// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include "messages/MOSDPGLog.h"

#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

SET_SUBSYS(osd);

namespace crimson::osd {

template <class T>
void PeeringEvent<T>::print(std::ostream &lhs) const
{
  lhs << "PeeringEvent("
      << "from=" << from
      << " pgid=" << pgid
      << " sent=" << evt.get_epoch_sent()
      << " requested=" << evt.get_epoch_requested()
      << " evt=" << evt.get_desc()
      << ")";
}

template <class T>
void PeeringEvent<T>::dump_detail(Formatter *f) const
{
  f->open_object_section("PeeringEvent");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  f->dump_int("sent", evt.get_epoch_sent());
  f->dump_int("requested", evt.get_epoch_requested());
  f->dump_string("evt", evt.get_desc());
  f->open_array_section("events");
  {
    std::apply([f](auto&... events) {
      (..., events.dump(f));
    }, static_cast<const T*>(this)->tracking_events);
  }
  f->close_section();
  f->close_section();
}


template <class T>
PGPeeringPipeline &PeeringEvent<T>::peering_pp(PG &pg)
{
  return pg.peering_request_pg_pipeline;
}

template <class T>
seastar::future<> PeeringEvent<T>::with_pg(
  ShardServices &shard_services, Ref<PG> pg)
{
  using interruptor = typename T::interruptor;
  LOG_PREFIX(PeeringEvent<T>::with_pg);
  if (!pg) {
    WARNI("{}: pg absent, did not create", *this);
    on_pg_absent(shard_services);
    that()->get_handle().exit();
    return complete_rctx_no_pg(shard_services);
  }
  DEBUGI("start");

  return interruptor::with_interruption([this, pg, &shard_services] {
    LOG_PREFIX(PeeringEvent<T>::with_pg);
    DEBUGI("{} {}: pg present", interruptor::get_interrupt_cond(), *this);
    return this->template enter_stage<interruptor>(peering_pp(*pg).await_map
    ).then_interruptible([this, pg] {
      return this->template with_blocking_event<
	PG_OSDMapGate::OSDMapBlocker::BlockingEvent
	>([this, pg](auto &&trigger) {
	  return pg->osdmap_gate.wait_for_map(
	    std::move(trigger), evt.get_epoch_sent());
	});
    }).then_interruptible([this, pg](auto) {
      return this->template enter_stage<interruptor>(peering_pp(*pg).process);
    }).then_interruptible([this, pg, &shard_services] {
      return pg->do_peering_event(evt, ctx
      ).then_interruptible([this] {
	return that()->get_handle().complete();
      }).then_interruptible([this, pg, &shard_services] {
	return complete_rctx(shard_services, pg);
      });
    }).then_interruptible([pg, &shard_services]()
			  -> typename T::template interruptible_future<> {
      if (!pg->get_need_up_thru()) {
	return seastar::now();
      }
      return shard_services.send_alive(pg->get_same_interval_since());
    }).then_interruptible([&shard_services] {
      return shard_services.send_pg_temp();
    });
  }, [this](std::exception_ptr ep) {
    LOG_PREFIX(PeeringEvent<T>::with_pg);
    DEBUGI("{}: interrupted with {}", *this, ep);
  }, pg).finally([this] {
    logger().debug("{}: exit", *this);
    that()->get_handle().exit();
  });
}

template <class T>
void PeeringEvent<T>::on_pg_absent(ShardServices &)
{
  using interruptor = typename T::interruptor;
  LOG_PREFIX(PeeringEvent<T>::on_pg_absent);
  DEBUGI("{}: pg absent, dropping", *this);
}

template <class T>
typename PeeringEvent<T>::template interruptible_future<>
PeeringEvent<T>::complete_rctx(ShardServices &shard_services, Ref<PG> pg)
{
  using interruptor = typename T::interruptor;
  LOG_PREFIX(PeeringEvent<T>::complete_rctx);
  DEBUGI("{}: submitting ctx", *this);
  return shard_services.dispatch_context(
    pg->get_collection_ref(),
    std::move(ctx));
}

ConnectionPipeline &RemotePeeringEvent::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).client_request_conn_pipeline;
}

PerShardPipeline &RemotePeeringEvent::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_peering_request_pipeline();
}

void RemotePeeringEvent::on_pg_absent(ShardServices &shard_services)
{
  if (auto& e = get_event().get_event();
      e.dynamic_type() == MQuery::static_type()) {
    const auto map_epoch =
      shard_services.get_map()->get_epoch();
    const auto& q = static_cast<const MQuery&>(e);
    const pg_info_t empty{spg_t{pgid.pgid, q.query.to}};
    if (q.query.type == q.query.LOG ||
	q.query.type == q.query.FULLLOG)  {
      auto m = crimson::make_message<MOSDPGLog>(q.query.from, q.query.to,
					     map_epoch, empty,
					     q.query.epoch_sent);
      ctx.send_osd_message(q.from.osd, std::move(m));
    } else {
      ctx.send_notify(q.from.osd, {q.query.from, q.query.to,
				   q.query.epoch_sent,
				   map_epoch, empty,
				   PastIntervals{}});
    }
  }
}

RemotePeeringEvent::interruptible_future<> RemotePeeringEvent::complete_rctx(
  ShardServices &shard_services,
  Ref<PG> pg)
{
  if (pg) {
    return PeeringEvent::complete_rctx(shard_services, pg);
  } else {
    return shard_services.dispatch_context_messages(std::move(ctx));
  }
}

seastar::future<> RemotePeeringEvent::complete_rctx_no_pg(
  ShardServices &shard_services)
{
  return shard_services.dispatch_context_messages(std::move(ctx));
}

seastar::future<> LocalPeeringEvent::start()
{
  LOG_PREFIX(LocalPeeringEvent::start);
  DEBUGI("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref)] {
    LOG_PREFIX(LocalPeeringEvent::start);
    DEBUGI("{}: complete", *ref);
  });
}


LocalPeeringEvent::~LocalPeeringEvent() {}

template class PeeringEvent<RemotePeeringEvent>;
template class PeeringEvent<LocalPeeringEvent>;

}
