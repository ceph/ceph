#pragma once

#include <optional>
#include <string>

#include "include/types.h"

class JSONObj;

namespace ceph {
namespace messaging {
namespace config {
struct ConfigSetRequest {
  std::string who;
  std::string name;
  std::string value;
  std::optional<bool> force;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};
}  // namespace config
}  // namespace messaging
}  // namespace ceph