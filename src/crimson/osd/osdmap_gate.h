// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <functional>
#include <map>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include "include/types.h"
#include "crimson/osd/osd_operation.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

enum class OSDMapGateType {
  OSD,
  PG,
};

template <OSDMapGateType OSDMapGateTypeV>
class OSDMapGate {
public:
  struct OSDMapBlocker : BlockerT<OSDMapBlocker> {
    const char * type_name;
    epoch_t epoch;

    OSDMapBlocker(std::pair<const char *, epoch_t> args)
      : type_name(args.first), epoch(args.second) {}

    OSDMapBlocker(const OSDMapBlocker &) = delete;
    OSDMapBlocker(OSDMapBlocker &&) = delete;
    OSDMapBlocker &operator=(const OSDMapBlocker &) = delete;
    OSDMapBlocker &operator=(OSDMapBlocker &&) = delete;

    seastar::shared_promise<epoch_t> promise;

    void dump_detail(Formatter *f) const final;
  };
  using Blocker = OSDMapBlocker;

private:
  // order the promises in ascending order of the waited osdmap epoch,
  // so we can access all the waiters expecting a map whose epoch is less
  // than or equal to a given epoch
  using waiting_peering_t = std::map<epoch_t,
				     OSDMapBlocker>;
  const char *blocker_type;
  waiting_peering_t waiting_peering;
  epoch_t current = 0;
  bool stopping = false;
public:
  OSDMapGate(const char *blocker_type)
    : blocker_type(blocker_type) {}

  /**
   * wait_for_map
   *
   * Wait for an osdmap whose epoch is greater or equal to given epoch.
   * If shard_services is non-null, request map if not present.
   */
  seastar::future<epoch_t>
  wait_for_map(
    typename OSDMapBlocker::BlockingEvent::TriggerI&& trigger,
    epoch_t epoch,
    ShardServices *shard_services=nullptr
  );
  void got_map(epoch_t epoch);
  seastar::future<> stop();
};

using OSD_OSDMapGate = OSDMapGate<OSDMapGateType::OSD>;
using PG_OSDMapGate = OSDMapGate<OSDMapGateType::PG>;

}
