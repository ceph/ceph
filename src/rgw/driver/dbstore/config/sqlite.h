// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include "rgw_sal_config.h"

class DoutPrefixProvider;

namespace rgw::dbstore::config {

struct SQLiteImpl;

class SQLiteConfigStore : public sal::ConfigStore {
 public:
  explicit SQLiteConfigStore(std::unique_ptr<SQLiteImpl> impl);
  ~SQLiteConfigStore() override;

  int write_default_realm_id(const DoutPrefixProvider* dpp,
                             optional_yield y, bool exclusive,
                             std::string_view realm_id) override;
  int read_default_realm_id(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            std::string& realm_id) override;
  int delete_default_realm_id(const DoutPrefixProvider* dpp,
                              optional_yield y) override;

  int create_realm(const DoutPrefixProvider* dpp,
                   optional_yield y, bool exclusive,
                   const RGWRealm& info,
                   std::unique_ptr<sal::RealmWriter>* writer) override;
  int read_realm_by_id(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       std::string_view realm_id,
                       RGWRealm& info,
                       std::unique_ptr<sal::RealmWriter>* writer) override;
  int read_realm_by_name(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view realm_name,
                         RGWRealm& info,
                         std::unique_ptr<sal::RealmWriter>* writer) override;
  int read_default_realm(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         RGWRealm& info,
                         std::unique_ptr<sal::RealmWriter>* writer) override;
  int read_realm_id(const DoutPrefixProvider* dpp,
                    optional_yield y, std::string_view realm_name,
                    std::string& realm_id) override;
  int realm_notify_new_period(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              const RGWPeriod& period) override;
  int list_realm_names(const DoutPrefixProvider* dpp,
                       optional_yield y, const std::string& marker,
                       std::span<std::string> entries,
                       sal::ListResult<std::string>& result) override;

  int create_period(const DoutPrefixProvider* dpp,
                    optional_yield y, bool exclusive,
                    const RGWPeriod& info) override;
  int read_period(const DoutPrefixProvider* dpp,
                  optional_yield y, std::string_view period_id,
                  std::optional<uint32_t> epoch, RGWPeriod& info) override;
  int delete_period(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    std::string_view period_id) override;
  int list_period_ids(const DoutPrefixProvider* dpp,
                      optional_yield y, const std::string& marker,
                      std::span<std::string> entries,
                      sal::ListResult<std::string>& result) override;

  int write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                 optional_yield y, bool exclusive,
                                 std::string_view realm_id,
                                 std::string_view zonegroup_id) override;
  int read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view realm_id,
                                std::string& zonegroup_id) override;
  int delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  std::string_view realm_id) override;

  int create_zonegroup(const DoutPrefixProvider* dpp,
                       optional_yield y, bool exclusive,
                       const RGWZoneGroup& info,
                       std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  int read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           std::string_view zonegroup_id,
                           RGWZoneGroup& info,
                           std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  int read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             std::string_view zonegroup_name,
                             RGWZoneGroup& info,
                             std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  int read_default_zonegroup(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             std::string_view realm_id,
                             RGWZoneGroup& info,
                             std::unique_ptr<sal::ZoneGroupWriter>* writer) override;
  int list_zonegroup_names(const DoutPrefixProvider* dpp,
                           optional_yield y, const std::string& marker,
                           std::span<std::string> entries,
                           sal::ListResult<std::string>& result) override;

  int write_default_zone_id(const DoutPrefixProvider* dpp,
                            optional_yield y, bool exclusive,
                            std::string_view realm_id,
                            std::string_view zone_id) override;
  int read_default_zone_id(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           std::string_view realm_id,
                           std::string& zone_id) override;
  int delete_default_zone_id(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             std::string_view realm_id) override;

  int create_zone(const DoutPrefixProvider* dpp,
                  optional_yield y, bool exclusive,
                  const RGWZoneParams& info,
                  std::unique_ptr<sal::ZoneWriter>* writer) override;
  int read_zone_by_id(const DoutPrefixProvider* dpp,
                      optional_yield y,
                      std::string_view zone_id,
                      RGWZoneParams& info,
                      std::unique_ptr<sal::ZoneWriter>* writer) override;
  int read_zone_by_name(const DoutPrefixProvider* dpp,
                        optional_yield y,
                        std::string_view zone_name,
                        RGWZoneParams& info,
                        std::unique_ptr<sal::ZoneWriter>* writer) override;
  int read_default_zone(const DoutPrefixProvider* dpp,
                        optional_yield y,
                        std::string_view realm_id,
                        RGWZoneParams& info,
                        std::unique_ptr<sal::ZoneWriter>* writer) override;
  int list_zone_names(const DoutPrefixProvider* dpp,
                      optional_yield y, const std::string& marker,
                      std::span<std::string> entries,
                      sal::ListResult<std::string>& result) override;

  int read_period_config(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view realm_id,
                         RGWPeriodConfig& info) override;
  int write_period_config(const DoutPrefixProvider* dpp,
                          optional_yield y, bool exclusive,
                          std::string_view realm_id,
                          const RGWPeriodConfig& info) override;

 private:
  std::unique_ptr<SQLiteImpl> impl;
}; // SQLiteConfigStore


auto create_sqlite_store(const DoutPrefixProvider* dpp, const std::string& uri)
  -> std::unique_ptr<config::SQLiteConfigStore>;

} // namespace rgw::dbstore::config
