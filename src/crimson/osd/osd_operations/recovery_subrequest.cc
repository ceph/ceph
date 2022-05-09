#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::RecoverySubRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

namespace crimson::osd {

seastar::future<> RecoverySubRequest::start() {
  logger().debug("{}: start", *this);

  track_event<StartEvent>();
  IRef opref = this;
  using OSDMapBlockingEvent =
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent;
  return with_blocking_event<OSDMapBlockingEvent>(
    [this] (auto&& trigger) {
    return osd.osdmap_gate.wait_for_map(std::move(trigger), m->get_min_epoch());
  }).then([this] (epoch_t epoch) {
    return with_blocking_event<PGMap::PGCreationBlockingEvent>(
    [this] (auto&& trigger) {
      return osd.wait_for_pg(std::move(trigger), m->get_spg());
    });
  }).then([this, opref=std::move(opref)] (Ref<PG> pgref) {
    return interruptor::with_interruption([this, opref, pgref] {
      return seastar::do_with(std::move(pgref), std::move(opref),
	[this](auto& pgref, auto& opref) {
	return pgref->get_recovery_backend()->handle_recovery_op(m);
      });
    }, [](std::exception_ptr) { return seastar::now(); }, pgref);
  }).then([this] {
    track_event<CompletionEvent>();
  });
}

}
