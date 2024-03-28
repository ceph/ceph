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

#pragma once

#include <list>
#include <memory>
#include <string>
#include "rgw_common.h"
#include "rgw_sal_config.h"

class DoutPrefixProvider;
class optional_yield;

namespace rgw::rados {

struct ConfigImpl;

class RadosConfigStore : public sal::ConfigStore {
 public:
  explicit RadosConfigStore(std::unique_ptr<ConfigImpl> impl);
  virtual ~RadosConfigStore() override;

  // Realm
  virtual int write_default_realm_id(const DoutPrefixProvider* dpp,
                                     optional_yield y, bool exclusive,
                                     std::string_view realm_id,
                                     std::string_view realm_name) override;
  virtual int read_default_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string& realm_id,
                                    std::string& realm_name) override;
  virtual int delete_default_realm_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) override;

  virtual int create_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, bool exclusive,
                           const RGWRealm& info,
                           std::unique_ptr<sal::RealmWriter>* writer) override;
  virtual int read_realm_by_id(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               std::string_view realm_id,
                               RGWRealm& info,
                               std::unique_ptr<sal::RealmWriter>* writer) override;
  virtual int read_realm_by_name(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_name,
                                 RGWRealm& info,
                                 std::unique_ptr<sal::RealmWriter>* writer) override;
  virtual int read_default_realm(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 RGWRealm& info,
                                 std::unique_ptr<sal::RealmWriter>* writer) override;
  virtual int read_realm_id(const DoutPrefixProvider* dpp,
                            optional_yield y, std::string_view realm_name,
                            std::string& realm_id) override;
  virtual int realm_notify_new_period(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const RGWPeriod& period) override;
  virtual int list_realm_names(const DoutPrefixProvider* dpp,
                               optional_yield y, const std::string& marker,
                               std::span<std::string> entries,
                               sal::ListResult<std::string>& result) override;

  // Period
  virtual int create_period(const DoutPrefixProvider* dpp,
                            optional_yield y, bool exclusive,
                            const RGWPeriod& info) override;
  virtual int read_period(const DoutPrefixProvider* dpp,
                          optional_yield y, std::string_view period_id,
                          std::optional<uint32_t> epoch, RGWPeriod& info) override;
  virtual int delete_period(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            std::string_view period_id) override;
  virtual int list_period_ids(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              sal::ListResult<std::string>& result) override;

  // ZoneGroup
  virtual int write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                         optional_yield y, bool exclusive,
                                         std::string_view realm_id,
                                         std::string_view zonegroup_id) override;
  virtual int read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view realm_id,
                                        std::string& zonegroup_id) override;
  virtual int delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view realm_id) override;

  virtual int create_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y, bool exclusive,
                               const RGWZoneGroup& info,
                               std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  virtual int read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view zonegroup_id,
                                   RGWZoneGroup& info,
                                   std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  virtual int read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zonegroup_name,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  virtual int read_default_zonegroup(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  virtual int list_zonegroup_names(const DoutPrefixProvider* dpp,
                                   optional_yield y, const std::string& marker,
                                   std::span<std::string> entries,
                                   sal::ListResult<std::string>& result) override;

  // Zone
  virtual int write_default_zone_id(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    std::string_view realm_id,
                                    std::string_view zone_id) override;
  virtual int read_default_zone_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view realm_id,
                                   std::string& zone_id) override;
  virtual int delete_default_zone_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id) override;

  virtual int create_zone(const DoutPrefixProvider* dpp,
                          optional_yield y, bool exclusive,
                          const RGWZoneParams& info,
                          std::unique_ptr<sal::ZoneWriter>* writer) override;
  virtual int read_zone_by_id(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              std::string_view zone_id,
                              RGWZoneParams& info,
                              std::unique_ptr<sal::ZoneWriter>* writer) override;
  virtual int read_zone_by_name(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view zone_name,
                                RGWZoneParams& info,
                                std::unique_ptr<sal::ZoneWriter>* writer) override;
  virtual int read_default_zone(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view realm_id,
                                RGWZoneParams& info,
                                std::unique_ptr<sal::ZoneWriter>* writer) override;
  virtual int list_zone_names(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              sal::ListResult<std::string>& result) override;

  // PeriodConfig
  virtual int read_period_config(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_id,
                                 RGWPeriodConfig& info) override;
  virtual int write_period_config(const DoutPrefixProvider* dpp,
                                  optional_yield y, bool exclusive,
                                  std::string_view realm_id,
                                  const RGWPeriodConfig& info) override;

 private:
  std::unique_ptr<ConfigImpl> impl;
}; // RadosConfigStore


/// RadosConfigStore factory function
auto create_config_store(const DoutPrefixProvider* dpp)
    -> std::unique_ptr<RadosConfigStore>;

} // namespace rgw::rados
