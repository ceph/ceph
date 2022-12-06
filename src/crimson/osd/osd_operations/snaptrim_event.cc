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

seastar::future<seastar::stop_iteration> SnapTrimEvent::start()
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

seastar::future<seastar::stop_iteration> SnapTrimEvent::with_pg(
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
      return interruptor::async([this] {
        std::vector<hobject_t> to_trim;
        using crimson::common::local_conf;
        const auto max =
          local_conf().get_val<uint64_t>("osd_pg_max_concurrent_snap_trims");
        // we need to look for at least 1 snaptrim, otherwise we'll misinterpret
        // the ENOENT below and erase snapid.
        int r = snap_mapper.get_next_objects_to_trim(
          snapid,
          max,
          &to_trim);
        if (r == -ENOENT) {
          to_trim.clear(); // paranoia
          return to_trim;
        } else if (r != 0) {
          logger().error("{}: get_next_objects_to_trim returned {}",
                         *this, cpp_strerror(r));
          ceph_abort_msg("get_next_objects_to_trim returned an invalid code");
        } else {
          assert(!to_trim.empty());
        }
        logger().debug("{}: async almost done line {}", *this, __LINE__);
        return to_trim;
      }).then_interruptible([&shard_services, this] (const auto& to_trim) {
        if (to_trim.empty()) {
          // ENOENT
          return interruptor::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        for (const auto& object : to_trim) {
          logger().debug("{}: trimming {}", *this, object);
        }
        return subop_blocker.wait_completion().then([] {
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
        });
      });
    });
  }, [this](std::exception_ptr eptr) {
    // TODO: better debug output
    logger().debug("{}: interrupted {}", *this, eptr);
    return seastar::make_ready_future<seastar::stop_iteration>(
      seastar::stop_iteration::no);
  }, pg);
}

} // namespace crimson::osd
