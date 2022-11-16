// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ranges>

#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::SnapTrimEvent> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

namespace crimson::osd {

void SnapTrimEvent::SubOpBlocker::dump_detail(Formatter *f) const
{
  f->open_array_section("dependent_operations");
  {
    for (const auto &i : subops | std::views::keys) {
      f->dump_unsigned("op_id", i);
    }
  }
  f->close_section();
}

template <class... Args>
void SnapTrimEvent::SubOpBlocker::emplace_back(Args&&... args)
{
  subops.emplace_back(std::forward<Args>(args)...);
};

seastar::future<> SnapTrimEvent::SubOpBlocker::wait_completion()
{
  auto rng = subops | std::views::values;
  return seastar::when_all_succeed(
    std::begin(rng), std::end(rng)
  ).then([] (auto&&...) {
    return seastar::now();
  });
}

void SnapTrimEvent::print(std::ostream &lhs) const
{
  lhs << "SnapTrimEvent("
      << "pgid=" << pg->get_pgid()
      << " snapid=" << snapid
      << ")";
}

void SnapTrimEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimEvent");
  f->dump_stream("pgid") << pg->get_pgid();
  f->close_section();
}

seastar::future<> SnapTrimEvent::start()
{
  logger().debug("{}: {}", *this, __func__);
  return with_pg(
    pg->get_shard_services(), pg
  ).finally([ref=IRef{this}, this] {
    logger().debug("{}: complete", *ref);
    return handle.complete();
  });
}

CommonPGPipeline& SnapTrimEvent::pp()
{
  return pg->request_pg_pipeline;
}

seastar::future<> SnapTrimEvent::with_pg(
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
    }).then_interruptible([] {
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
