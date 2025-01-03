// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_zone.h"
#include "driver/rados/config/store.h"

#include "impl.h"

namespace rgw::rados {

// period config oids
constexpr std::string_view period_config_prefix = "period_config.";
constexpr std::string_view period_config_realm_default = "default";

std::string period_config_oid(std::string_view realm_id)
{
  if (realm_id.empty()) {
    realm_id = period_config_realm_default;
  }
  return string_cat_reserve(period_config_prefix, realm_id);
}

int RadosConfigStore::read_period_config(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         std::string_view realm_id,
                                         RGWPeriodConfig& info)
{
  const auto& pool = impl->period_pool;
  const auto oid = period_config_oid(realm_id);
  return impl->read(dpp, y, pool, oid, info, nullptr);
}

int RadosConfigStore::write_period_config(const DoutPrefixProvider* dpp,
                                          optional_yield y, bool exclusive,
                                          std::string_view realm_id,
                                          const RGWPeriodConfig& info)
{
  const auto& pool = impl->period_pool;
  const auto oid = period_config_oid(realm_id);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  return impl->write(dpp, y, pool, oid, create, info, nullptr);
}

} // namespace rgw::rados
