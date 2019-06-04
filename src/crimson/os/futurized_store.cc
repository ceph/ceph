#include "futurized_store.h"
#include "cyan_store.h"

namespace ceph::os {

std::unique_ptr<FuturizedStore> FuturizedStore::create(const std::string& type,
                                       const std::string& data)
{
  if (type == "memstore") {
    return std::make_unique<ceph::os::CyanStore>(data);
  }

  return nullptr;
}

}
