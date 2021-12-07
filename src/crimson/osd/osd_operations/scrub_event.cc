// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "scrub_event.h"

#include <boost/smart_ptr/local_shared_ptr.hpp>

#include <seastar/core/future.hh>

#include "common/Formatter.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "crimson/osd/pg.h"
#include "messages/MOSDPGLog.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace


namespace crimson::osd {

// --------------------------- ScrubEvent ---------------------------

ScrubEvent::ScrubEvent(Ref<PG> pg,
		       ShardServices& shard_services,
		       const spg_t& pgid,
		       ScrubEventFwd func,
		       epoch_t epoch_queued,
		       Scrub::act_token_t tkn,
		       std::chrono::milliseconds delay)
    : pg{std::move(pg)}
    , event_fwd_func{func}
    , act_token{tkn}
    , shard_services{shard_services}
    , pgid{pgid}
    , epoch_queued{epoch_queued}
    , delay{delay}
{
  logger().debug("ScrubEvent: 1'st ctor {:p} delay:{}", (void*)this, delay);
}

ScrubEvent::ScrubEvent(Ref<PG> pg,
		       ShardServices& shard_services,
		       const spg_t& pgid,
		       ScrubEventFwd func,
		       epoch_t epoch_queued,
		       Scrub::act_token_t tkn)
    : ScrubEvent{std::move(pg),
		 shard_services,
		 pgid,
		 func,
		 epoch_queued,
		 tkn,
		 std::chrono::milliseconds{0}}
{
  logger().debug("ScrubEvent: 2'nd ctor {:p}", (void*)this);
}

void ScrubEvent::print(std::ostream& lhs) const
{
  lhs << fmt::format("{}", *this);
}

void ScrubEvent::dump_detail(Formatter* f) const
{
  /// \todo add more fields
  f->open_object_section("ScrubEvent");
  f->dump_stream("pgid") << pgid;
  f->close_section();
}

void ScrubEvent::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

seastar::future<Ref<PG>> ScrubEvent::get_pg()
{
  return seastar::make_ready_future<Ref<PG>>(pg);
}

ScrubEvent::PGPipeline& ScrubEvent::pp(PG& pg)
{
  return pg.scrub_event_pg_pipeline;
}

ScrubEvent::~ScrubEvent() = default;


// clang-format off
seastar::future<> ScrubEvent::start()
{
  logger().debug(
    "scrubber: ScrubEvent::start(): {}: start (delay: {}) pg:{:p}", *this,
    delay, (void*)&(*pg));

  auto maybe_delay = seastar::now();
  if (delay.count() > 0) {
    maybe_delay = seastar::sleep(delay);
  }

  track_event<StartEvent>();

  return maybe_delay.then([this] {
    return get_pg();
  }).then([this](Ref<PG> pg) {
      if (!pg) {
        logger().warn("scrubber: ScrubEvent::start(): {}: pg absent, did not create",
                        *this);
        on_pg_absent();
        handle.exit();
        return complete_rctx_no_pg();
      }

      logger().debug("scrubber: ScrubEvent::start(): {}: pg present", *this);
      return interruptor::with_interruption(
	[this, pg]() /*-> ScrubEvent::interruptible_future<>*/ {
	  return this->template enter_stage<interruptor>(pp(*pg).await_map)
	    .then_interruptible([this, pg] {
	      return this->template with_blocking_event<
		PG_OSDMapGate::OSDMapBlocker::BlockingEvent>(
		[this, pg](auto&& trigger) {
		  return pg->osdmap_gate.wait_for_map(std::move(trigger),
						      epoch_queued);
		});
	    }).then_interruptible([this, pg](auto) {
	      return this->template enter_stage<interruptor>(pp(*pg).process);
	    }).then_interruptible(
	      [this, pg]() mutable -> ScrubEvent::interruptible_future<> {
		logger().info("ScrubEvent::start() {} executing...", *this);
		if (std::holds_alternative<ScrubEvent::ScrubEventFwdImm>(
		      event_fwd_func)) {
		  (*(pg->get_scrubber(ScrubberPasskey{})).*
		   std::get<ScrubEvent::ScrubEventFwdImm>(
		     event_fwd_func))(epoch_queued, act_token);
		  return seastar::make_ready_future<>();
		} else {
		  return (*(pg->get_scrubber(ScrubberPasskey{})).*
			  std::get<ScrubEvent::ScrubEventFwdFut>(
			    event_fwd_func))(epoch_queued, act_token);
		}
	      }).then_interruptible([this, pg]() mutable {
	      logger().info("ScrubEvent::start() {} after calling fwder",
			    *this);
	      handle.exit();
	      logger().info("ScrubEvent::start() {} executing... exited",
			    *this);
	      return seastar::now();
	    }).then_interruptible([pg]() -> ScrubEvent::interruptible_future<> {
	      return seastar::now();
	    });
	},
	[this](std::exception_ptr ep) {
	  logger().debug("ScrubEvent::start(): {} interrupted with {}",
			 *this,
			 ep);
	  return seastar::now();
	},
	pg);
  }).finally([this] {
    logger().debug("ScrubEvent::start(): {} complete", *this);
  });
}
// clang-format on

}  // namespace crimson::osd
