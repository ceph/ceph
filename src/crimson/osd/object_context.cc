// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/object_context.h"

#include "common/Formatter.h"
#include "crimson/common/config_proxy.h"

namespace crimson::osd {

ObjectContextRegistry::ObjectContextRegistry(crimson::common::ConfigProxy &conf)
{
  obc_lru.set_target_size(conf.get_val<uint64_t>("crimson_osd_obc_lru_size"));
  conf.add_observer(this);
}

ObjectContextRegistry::~ObjectContextRegistry()
{
  // purge the cache to avoid leaks and complains from LSan
  obc_lru.set_target_size(0UL);
}

const char** ObjectContextRegistry::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "crimson_osd_obc_lru_size",
    nullptr
  };
  return KEYS;
}

void ObjectContextRegistry::handle_conf_change(
  const crimson::common::ConfigProxy& conf,
  const std::set <std::string> &changed)
{
  obc_lru.set_target_size(conf.get_val<uint64_t>("crimson_osd_obc_lru_size"));
}


}
