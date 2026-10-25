// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

template <OSDMapGateType OSDMapGateTypeV>
void OSDMapGate<OSDMapGateTypeV>::OSDMapBlocker::dump_detail(Formatter *f) const
{
  f->open_object_section("OSDMapGate");
  f->dump_int("epoch", epoch);
  f->close_section();
}

template <OSDMapGateType OSDMapGateTypeV>
std::pair<seastar::future<epoch_t>,
	  typename OSDMapGate<OSDMapGateTypeV>::OSDMapBlocker*>
OSDMapGate<OSDMapGateTypeV>::do_wait_for_map(
  epoch_t epoch,
  ShardServices *shard_services)
{
  if (__builtin_expect(stopping, false)) {
    return {seastar::make_exception_future<epoch_t>(
	crimson::common::system_shutdown_exception()), nullptr};
  }
  if (current >= epoch) {
    return {seastar::make_ready_future<epoch_t>(current), nullptr};
  } else {
    logger().info("evt epoch is {}, i have {}, will wait", epoch, current);
    auto &blocker = waiting_peering.emplace(
      epoch, std::make_pair(blocker_type, epoch)).first->second;
    auto fut = blocker.promise.get_shared_future();
    if (shard_services) {
      return {shard_services->osdmap_subscribe(current, true).then(
	  [fut=std::move(fut)]() mutable {
	    return std::move(fut);
	  }),
	&blocker};
    } else {
      return {std::move(fut), &blocker};
    }
  }
}

template <OSDMapGateType OSDMapGateTypeV>
seastar::future<epoch_t> OSDMapGate<OSDMapGateTypeV>::wait_for_map(
  typename OSDMapBlocker::BlockingEvent::TriggerI&& trigger,
  epoch_t epoch,
  ShardServices *shard_services)
{
  auto [fut, blocker] = do_wait_for_map(epoch, shard_services);
  if (blocker) {
    return trigger.maybe_record_blocking(std::move(fut), *blocker);
  }
  return std::move(fut);
}

template <OSDMapGateType OSDMapGateTypeV>
seastar::future<epoch_t> OSDMapGate<OSDMapGateTypeV>::wait_for_map_untracked(
  epoch_t epoch,
  ShardServices *shard_services)
{
  return std::move(do_wait_for_map(epoch, shard_services).first);
}

template <OSDMapGateType OSDMapGateTypeV>
void OSDMapGate<OSDMapGateTypeV>::got_map(epoch_t epoch) {
  if (epoch == 0) {
    return;
  }
  ceph_assert(epoch > current);
  current = epoch;
  auto first = waiting_peering.begin();
  auto last = waiting_peering.upper_bound(epoch);
  std::for_each(first, last, [epoch](auto& blocked_requests) {
    blocked_requests.second.promise.set_value(epoch);
  });
  waiting_peering.erase(first, last);
}

template <OSDMapGateType OSDMapGateTypeV>
seastar::future<> OSDMapGate<OSDMapGateTypeV>::stop() {
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

} // namespace crimson::osd
