#include "futurized_store.h"
#include "cyanstore/cyan_store.h"

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data)
{
  if (type == "memstore") {
    return std::make_unique<crimson::os::CyanStore>(data);
  } else {
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
    return {};
  }
}

}
