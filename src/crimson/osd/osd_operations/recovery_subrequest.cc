#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/osd/osd_operations/recovery_subrequest.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

seastar::future<> RecoverySubRequest::start() {
  logger().debug("{}: start", *this);

  IRef opref = this;
  return with_blocking_future(
      osd.osdmap_gate.wait_for_map(m->get_min_epoch()))
  .then([this] (epoch_t epoch) {
    return with_blocking_future(osd.wait_for_pg(m->get_spg()));
  }).then([this, opref=std::move(opref)] (Ref<PG> pgref) {
    return interruptor::with_interruption([this, opref, pgref] {
      return seastar::do_with(std::move(pgref), std::move(opref),
	[this](auto& pgref, auto& opref) {
	return pgref->get_recovery_backend()->handle_recovery_op(m);
      });
    }, [](std::exception_ptr) { return seastar::now(); }, pgref);
  });
}

}
