// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_operations/snaptrim_request.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::SnapTrimRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

namespace crimson::osd {

void SnapTrimRequest::print(std::ostream &lhs) const
{
  lhs << "SnapTrimRequest("
      << "pgid=" << pg->get_pgid()
      << " snapid=" << snapid
      << ")";
}

void SnapTrimRequest::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimRequest");
  f->dump_stream("pgid") << pg->get_pgid();
  f->close_section();
}

seastar::future<> SnapTrimRequest::start()
{
  logger().debug("{}", __func__);
  return seastar::now();

  logger().debug("{}: start", *this);

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (auto delay = 0; delay) {
    maybe_delay = seastar::sleep(
      std::chrono::milliseconds(std::lround(delay * 1000)));
  }
  return maybe_delay.then([this] {
    return with_pg(pg->get_shard_services(), pg);
  }).finally([ref=std::move(ref)] {
    logger().debug("{}: complete", *ref);
  });
}

CommonPGPipeline& SnapTrimRequest::pp()
{
  return pg->client_request_pg_pipeline;
}

seastar::future<> SnapTrimRequest::with_pg(
  ShardServices &shard_services, Ref<PG> _pg)
{
  return interruptor::with_interruption([&shard_services, this] {
    return enter_stage<interruptor>(
      pp().wait_for_active
    ).then_interruptible([this] {
      return with_blocking_event<PGActivationBlocker::BlockingEvent,
                                 interruptor>([this] (auto&& trigger) {
        return pg->wait_for_active_blocker.wait(std::move(trigger));
      });
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().recover_missing);
    }).then_interruptible([this] {
      //return do_recover_missing(pg, get_target_oid());
      return seastar::now();
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().get_obc);
    }).then_interruptible([this] {
      return enter_stage<interruptor>(
        pp().process);
    }).then_interruptible([&shard_services, this] {
      std::vector<hobject_t> to_trim;
      assert(!to_trim.empty());
      for (const auto& object : to_trim) {
        logger().debug("{}: trimming {}", object);
        // TODO: start subop and add to subop blcoker
      }
      return seastar::now();
    });
  }, [this](std::exception_ptr eptr) {
    // TODO: better debug output
    logger().debug("{}: interrupted {}", *this, eptr);
  }, pg);
}

} // namespace crimson::osd
