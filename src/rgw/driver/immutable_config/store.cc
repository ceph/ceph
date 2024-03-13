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
#include "store.h"

namespace rgw::sal {

ImmutableConfigStore::ImmutableConfigStore(const RGWZoneGroup& zonegroup,
                                           const RGWZoneParams& zone,
                                           const RGWPeriodConfig& period_config)
    : zonegroup(zonegroup), zone(zone), period_config(period_config)
{
}

// Realm
int ImmutableConfigStore::write_default_realm_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y, bool exclusive,
                                                 std::string_view realm_id,
                                                 std::string_view realm_name)
{
  return -EROFS;
}

int ImmutableConfigStore::read_default_realm_id(const DoutPrefixProvider* dpp,
                                                optional_yield y,
                                                std::string& realm_id,
                                                std::string& realm_name)
{
  return -ENOENT;
}

int ImmutableConfigStore::delete_default_realm_id(const DoutPrefixProvider* dpp,
                                                  optional_yield y)
{
  return -EROFS;
}


int ImmutableConfigStore::create_realm(const DoutPrefixProvider* dpp,
                                       optional_yield y, bool exclusive,
                                       const RGWRealm& info,
                                       std::unique_ptr<RealmWriter>* writer)
{
  return -EROFS;
}

int ImmutableConfigStore::read_realm_by_id(const DoutPrefixProvider* dpp,
                                           optional_yield y,
                                           std::string_view realm_id,
                                           RGWRealm& info,
                                           std::unique_ptr<RealmWriter>* writer)
{
  return -ENOENT;
}

int ImmutableConfigStore::read_realm_by_name(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_name,
                                             RGWRealm& info,
                                             std::unique_ptr<RealmWriter>* writer)
{
  return -ENOENT;
}

int ImmutableConfigStore::read_default_realm(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             RGWRealm& info,
                                             std::unique_ptr<RealmWriter>* writer)
{
  return -ENOENT;
}

int ImmutableConfigStore::read_realm_id(const DoutPrefixProvider* dpp,
                                        optional_yield y, std::string_view realm_name,
                                        std::string& realm_id)
{
  return -ENOENT;
}

int ImmutableConfigStore::realm_notify_new_period(const DoutPrefixProvider* dpp,
                                                  optional_yield y,
                                                  const RGWPeriod& period)
{
  return -ENOTSUP;
}

int ImmutableConfigStore::list_realm_names(const DoutPrefixProvider* dpp,
                                           optional_yield y, const std::string& marker,
                                           std::span<std::string> entries,
                                           ListResult<std::string>& result)
{
  result.next.clear();
  result.entries = entries.first(0);
  return 0;
}


// Period
int ImmutableConfigStore::create_period(const DoutPrefixProvider* dpp,
                                        optional_yield y, bool exclusive,
                                        const RGWPeriod& info)
{
  return -EROFS;
}

int ImmutableConfigStore::read_period(const DoutPrefixProvider* dpp,
                                      optional_yield y, std::string_view period_id,
                                      std::optional<uint32_t> epoch, RGWPeriod& info)
{
  return -ENOENT;
}

int ImmutableConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view period_id)
{
  return -EROFS;
}

int ImmutableConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                          optional_yield y, const std::string& marker,
                                          std::span<std::string> entries,
                                          ListResult<std::string>& result)
{
  result.next.clear();
  result.entries = entries.first(0);
  return 0;
}


// ZoneGroup

class ImmutableZoneGroupWriter : public ZoneGroupWriter {
 public:
  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneGroup& info) override
  {
    return -EROFS;
  }
  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneGroup& info, std::string_view new_name) override
  {
    return -EROFS;
  }
  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    return -EROFS;
  }
};

int ImmutableConfigStore::write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                     optional_yield y, bool exclusive,
                                                     std::string_view realm_id,
                                                     std::string_view zonegroup_id)
{
  return -EROFS;
}

int ImmutableConfigStore::read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                    optional_yield y,
                                                    std::string_view realm_id,
                                                    std::string& zonegroup_id)
{
  if (!realm_id.empty()) {
    return -ENOENT;
  }
  zonegroup_id = zonegroup.id;
  return 0;
}

int ImmutableConfigStore::delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                                      optional_yield y,
                                                      std::string_view realm_id)
{
  return -EROFS;
}


int ImmutableConfigStore::create_zonegroup(const DoutPrefixProvider* dpp,
                                           optional_yield y, bool exclusive,
                                           const RGWZoneGroup& info,
                                           std::unique_ptr<ZoneGroupWriter>* writer)
{
  return -EROFS;
}

int ImmutableConfigStore::read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                               optional_yield y,
                                               std::string_view zonegroup_id,
                                               RGWZoneGroup& info,
                                               std::unique_ptr<ZoneGroupWriter>* writer)
{
  if (zonegroup_id != zonegroup.id) {
    return -ENOENT;
  }

  info = zonegroup;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneGroupWriter>();
  }
  return 0;
}
int ImmutableConfigStore::read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string_view zonegroup_name,
                                                 RGWZoneGroup& info,
                                                 std::unique_ptr<ZoneGroupWriter>* writer)
{
  if (zonegroup_name != zonegroup.name) {
    return -ENOENT;
  }

  info = zonegroup;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneGroupWriter>();
  }
  return 0;
}

int ImmutableConfigStore::read_default_zonegroup(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string_view realm_id,
                                                 RGWZoneGroup& info,
                                                 std::unique_ptr<ZoneGroupWriter>* writer)
{
  info = zonegroup;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneGroupWriter>();
  }
  return 0;
}

int ImmutableConfigStore::list_zonegroup_names(const DoutPrefixProvider* dpp,
                                               optional_yield y, const std::string& marker,
                                               std::span<std::string> entries,
                                               ListResult<std::string>& result)
{
  if (marker < zonegroup.name) {
    entries[0] = zonegroup.name;
    result.next = zonegroup.name;
    result.entries = entries.first(1);
  } else {
    result.next.clear();
    result.entries = entries.first(0);
  }
  return 0;
}

// Zone

class ImmutableZoneWriter : public ZoneWriter {
 public:
  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const RGWZoneParams& info) override
  {
    return -EROFS;
  }
  int rename(const DoutPrefixProvider* dpp, optional_yield y,
             RGWZoneParams& info, std::string_view new_name) override
  {
    return -EROFS;
  }
  int remove(const DoutPrefixProvider* dpp, optional_yield y) override
  {
    return -EROFS;
  }
};

int ImmutableConfigStore::write_default_zone_id(const DoutPrefixProvider* dpp,
                                                optional_yield y, bool exclusive,
                                                std::string_view realm_id,
                                                std::string_view zone_id)
{
  return -EROFS;
}

int ImmutableConfigStore::read_default_zone_id(const DoutPrefixProvider* dpp,
                                               optional_yield y,
                                               std::string_view realm_id,
                                               std::string& zone_id)
{
  if (realm_id.empty()) {
    return -ENOENT;
  }
  zone_id = zone.id;
  return 0;
}

int ImmutableConfigStore::delete_default_zone_id(const DoutPrefixProvider* dpp,
                                                 optional_yield y,
                                                 std::string_view realm_id)
{
  return -EROFS;
}


int ImmutableConfigStore::create_zone(const DoutPrefixProvider* dpp,
                                      optional_yield y, bool exclusive,
                                      const RGWZoneParams& info,
                                      std::unique_ptr<ZoneWriter>* writer)
{
  return -EROFS;
}

int ImmutableConfigStore::read_zone_by_id(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view zone_id,
                                          RGWZoneParams& info,
                                          std::unique_ptr<ZoneWriter>* writer)
{
  if (zone_id != zone.id) {
    return -ENOENT;
  }

  info = zone;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneWriter>();
  }
  return 0;
}

int ImmutableConfigStore::read_zone_by_name(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view zone_name,
                                            RGWZoneParams& info,
                                            std::unique_ptr<ZoneWriter>* writer)
{
  if (zone_name != zone.name) {
    return -ENOENT;
  }

  info = zone;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneWriter>();
  }
  return 0;
}

int ImmutableConfigStore::read_default_zone(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view realm_id,
                                            RGWZoneParams& info,
                                            std::unique_ptr<ZoneWriter>* writer)
{
  if (!realm_id.empty()) {
    return -ENOENT;
  }

  info = zone;

  if (writer) {
    *writer = std::make_unique<ImmutableZoneWriter>();
  }
  return 0;
}

int ImmutableConfigStore::list_zone_names(const DoutPrefixProvider* dpp,
                                          optional_yield y, const std::string& marker,
                                          std::span<std::string> entries,
                                          ListResult<std::string>& result)
{
  if (marker < zone.name) {
    entries[0] = zone.name;
    result.next = zone.name;
    result.entries = entries.first(1);
  } else {
    result.next.clear();
    result.entries = entries.first(0);
  }
  return 0;
}


// PeriodConfig
int ImmutableConfigStore::read_period_config(const DoutPrefixProvider* dpp,
                                             optional_yield y,
                                             std::string_view realm_id,
                                             RGWPeriodConfig& info)
{
  if (!realm_id.empty()) {
    return -ENOENT;
  }

  info = period_config;
  return 0;
}

int ImmutableConfigStore::write_period_config(const DoutPrefixProvider* dpp,
                                              optional_yield y, bool exclusive,
                                              std::string_view realm_id,
                                              const RGWPeriodConfig& info)
{
  return -EROFS;
}


/// ImmutableConfigStore factory function
auto create_immutable_config_store(const DoutPrefixProvider* dpp,
                                   const RGWZoneGroup& zonegroup,
                                   const RGWZoneParams& zone,
                                   const RGWPeriodConfig& period_config)
  -> std::unique_ptr<ConfigStore>
{
  return std::make_unique<ImmutableConfigStore>(zonegroup, zone, period_config);
}

} // namespace rgw::sal
