// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/exception.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void OSDMapGate::OSDMapBlocker::dump_detail(Formatter *f) const
{
  f->open_object_section("OSDMapGate");
  f->dump_int("epoch", epoch);
  f->close_section();
}

blocking_future<epoch_t> OSDMapGate::wait_for_map(epoch_t epoch)
{
  if (__builtin_expect(stopping, false)) {
    return make_exception_blocking_future<epoch_t>(
	crimson::common::system_shutdown_exception());
  }
  if (current >= epoch) {
    return make_ready_blocking_future<epoch_t>(current);
  } else {
    logger().info("evt epoch is {}, i have {}, will wait", epoch, current);
    auto &blocker = waiting_peering.emplace(
      epoch, make_pair(blocker_type, epoch)).first->second;
    auto fut = blocker.promise.get_shared_future();
    if (shard_services) {
      return blocker.make_blocking_future(
	(*shard_services).get().osdmap_subscribe(current, true).then(
	  [fut=std::move(fut)]() mutable {
	    return std::move(fut);
	  }));
    } else {
      return blocker.make_blocking_future(std::move(fut));
    }
  }
}

void OSDMapGate::got_map(epoch_t epoch) {
  current = epoch;
  auto first = waiting_peering.begin();
  auto last = waiting_peering.upper_bound(epoch);
  std::for_each(first, last, [epoch](auto& blocked_requests) {
    blocked_requests.second.promise.set_value(epoch);
  });
  waiting_peering.erase(first, last);
}

seastar::future<> OSDMapGate::stop() {
  logger().info("osdmap::stop");
  stopping = true;
  auto first = waiting_peering.begin();
  auto last = waiting_peering.end();
  std::for_each(first, last, [](auto& blocked_requests) {
    blocked_requests.second.promise.set_exception(
	crimson::common::system_shutdown_exception());
  });
  return seastar::now();
}

}
