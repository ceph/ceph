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
  last_item
};

template <OSDMapGateType GateTypeV>
class OSDMapGate {
public:
  struct OSDMapBlocker : public TimedBlocker<OSDMapBlocker> {
    const char * type_name;
    epoch_t epoch;

    OSDMapBlocker(std::pair<const char *, epoch_t> args)
      : type_name(args.first), epoch(args.second) {}

    OSDMapBlocker(const OSDMapBlocker &) = delete;
    OSDMapBlocker(OSDMapBlocker &&) = delete;
    OSDMapBlocker &operator=(const OSDMapBlocker &) = delete;
    OSDMapBlocker &operator=(OSDMapBlocker &&) = delete;

    seastar::shared_promise<epoch_t> promise;

    void dump_detail(Formatter *f) const ;
  private:
    const char *get_type_name() const {
      return type_name;
    }
  };

  // order the promises in ascending order of the waited osdmap epoch,
  // so we can access all the waiters expecting a map whose epoch is less
  // than or equal to a given epoch
  using waiting_peering_t = std::map<epoch_t,
				     OSDMapBlocker>;
  waiting_peering_t waiting_peering;
  epoch_t current = 0;
  std::optional<std::reference_wrapper<ShardServices>> shard_services;
  bool stopping = false;
public:
  OSDMapGate(
    std::optional<std::reference_wrapper<ShardServices>> shard_services)
    : shard_services(shard_services) {}

  // wait for an osdmap whose epoch is greater or equal to given epoch
  seastar::future<epoch_t> wait_for_map(typename OSDMapBlocker::TimedPtr& handle,
                                        epoch_t epoch);
  void got_map(epoch_t epoch);
  seastar::future<> stop();
};

}
