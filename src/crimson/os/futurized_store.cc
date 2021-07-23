#include "futurized_store.h"
#include "cyanstore/cyan_store.h"
#include "alienstore/alien_store.h"
#include "seastore/seastore.h"

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data,
                       const ConfigValues& values,
                       seastar::alien::instance& alien)
{
  if (type == "cyanstore") {
    return std::make_unique<crimson::os::CyanStore>(data);
  } else if (type == "seastore") {
    return crimson::os::seastore::make_seastore(data, values);
  } else {
    // use AlienStore as a fallback. It adapts e.g. BlueStore.
    return std::make_unique<crimson::os::AlienStore>(
      type, data, values, alien);
  }
}

}
