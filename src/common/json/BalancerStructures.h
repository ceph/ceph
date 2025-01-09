#pragma once

#include <string>

#include "include/types.h"

class JSONObj;

namespace ceph {
namespace messaging {
namespace balancer {
struct BalancerOffRequest {
  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct BalancerStatusRequest {
  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct BalancerStatusReply {
  bool active;
  std::string last_optimization_duration;
  std::string last_optimization_started;
  std::string mode;
  bool no_optimization_needed;
  std::string optimize_result;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};
}  // namespace balancer
}  // namespace messaging
}  // namespace ceph