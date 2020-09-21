#include "futurized_store.h"
#include "cyanstore/cyan_store.h"
#include "alienstore/alien_store.h"

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data,
                       const ConfigValues& values)
{
  if (type == "memstore") {
    return std::make_unique<crimson::os::CyanStore>(data);
  } else if (type == "bluestore") {
    return std::make_unique<crimson::os::AlienStore>(data, values);
  } else {
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
    return {};
  }
}

}
