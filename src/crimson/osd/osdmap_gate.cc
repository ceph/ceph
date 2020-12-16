// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>
#include <string_view>

#include "crimson/common/exception.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/shard_services.h"
#include "common/Formatter.h"


using namespace std::literals;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

static std::array<std::string_view, static_cast<int>(OSDMapGateType::last_item)>
GATE_NAMES{"OSD"sv, "PG"sv};

template <OSDMapGateType GateTypeV>
void OSDMapGate<GateTypeV>::OSDMapBlocker::dump_detail(Formatter *f) const
{
  f->open_object_section("OSDMapGate");
  f->dump_int("epoch", epoch);
  f->close_section();
}

template <OSDMapGateType GateTypeV>
seastar::future<epoch_t> OSDMapGate<GateTypeV>::wait_for_map(
  typename OSDMapBlocker::TimedPtr& handle,
  epoch_t epoch)
{
  if (__builtin_expect(stopping, false)) {
    return seastar::make_exception_future<epoch_t>(
	crimson::common::system_shutdown_exception());
  }
  if (current >= epoch) {
    return seastar::make_ready_future<epoch_t>(current);
  } else {
    logger().info("evt epoch is {}, i have {}, will wait", epoch, current);
    auto &blocker = waiting_peering.emplace(
      epoch, make_pair(GATE_NAMES[static_cast<int>(GateTypeV)].data(), epoch)).first->second;
    auto fut = blocker.promise.get_shared_future();
    if (shard_services) {
      return blocker.make_blocking_future2(
        handle,
	(*shard_services).get().osdmap_subscribe(current, true).then(
	  [fut=std::move(fut)]() mutable {
	    return std::move(fut);
	  }));
    } else {
      return blocker.make_blocking_future2(handle, std::move(fut));
    }
  }
}

template <OSDMapGateType GateTypeV>
void OSDMapGate<GateTypeV>::got_map(epoch_t epoch) {
  current = epoch;
  auto first = waiting_peering.begin();
  auto last = waiting_peering.upper_bound(epoch);
  std::for_each(first, last, [epoch](auto& blocked_requests) {
    blocked_requests.second.promise.set_value(epoch);
  });
  waiting_peering.erase(first, last);
}

template <OSDMapGateType GateTypeV>
seastar::future<> OSDMapGate<GateTypeV>::stop() {
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

template class OSDMapGate<OSDMapGateType::PG>;
template class OSDMapGate<OSDMapGateType::OSD>;
}
