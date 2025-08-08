// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "futurized_store.h"
#include "cyanstore/cyan_store.h"
#ifdef WITH_BLUESTORE
#include "alienstore/alien_store.h"
#endif
#include "seastore/seastore.h"

namespace crimson::os {

std::unique_ptr<FuturizedStore>
FuturizedStore::create(const std::string& type,
                       const std::string& data,
                       const ConfigValues& values)
{
  if (type == "cyanstore") {
    using crimson::os::CyanStore;
    return std::make_unique<CyanStore>(data);
  } else if (type == "seastore") {
    return crimson::os::seastore::make_seastore(
      data);
  } else {
#ifdef WITH_BLUESTORE
    using crimson::os::AlienStore;
    // use AlienStore as a fallback. It adapts e.g. BlueStore.
    return std::make_unique<AlienStore>(type, data, values);
#else
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
    return {};
#endif
  }
}

}
