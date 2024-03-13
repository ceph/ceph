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

#include <memory>
#include <optional>
#include <span>
#include <string>
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class optional_yield;
struct RGWPeriod;
struct RGWPeriodConfig;
struct RGWRealm;
struct RGWZoneGroup;
struct RGWZoneParams;

namespace rgw::sal {

/// Results of a listing operation
template <typename T>
struct ListResult {
  /// The subspan of the input entries that contain results
  std::span<T> entries;
  /// The next marker to resume listing, or empty
  std::string next;
};

/// Storage abstraction for realm/zonegroup/zone configuration
class ConfigStore {
 public:
  virtual ~ConfigStore() {}

  /// @group Realm
  ///@{

  /// Set the cluster-wide default realm id and name
  virtual int write_default_realm_id(const DoutPrefixProvider* dpp,
                                     optional_yield y, bool exclusive,
                                     std::string_view realm_id,
                                     std::string_view realm_name) = 0;
  /// Read the cluster's default realm id and name
  virtual int read_default_realm_id(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string& realm_id,
                                    std::string& realm_name) = 0;
  /// Delete the cluster's default realm id
  virtual int delete_default_realm_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) = 0;

  /// Create a realm
  virtual int create_realm(const DoutPrefixProvider* dpp,
                           optional_yield y, bool exclusive,
                           const RGWRealm& info,
                           std::unique_ptr<RealmWriter>* writer) = 0;
  /// Read a realm by id
  virtual int read_realm_by_id(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               std::string_view realm_id,
                               RGWRealm& info,
                               std::unique_ptr<RealmWriter>* writer) = 0;
  /// Read a realm by name
  virtual int read_realm_by_name(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_name,
                                 RGWRealm& info,
                                 std::unique_ptr<RealmWriter>* writer) = 0;
  /// Read the cluster's default realm
  virtual int read_default_realm(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 RGWRealm& info,
                                 std::unique_ptr<RealmWriter>* writer) = 0;
  /// Look up a realm id by its name
  virtual int read_realm_id(const DoutPrefixProvider* dpp,
                            optional_yield y, std::string_view realm_name,
                            std::string& realm_id) = 0;
  /// Notify the cluster of a new period, so radosgws can reload with the new
  /// configuration
  virtual int realm_notify_new_period(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const RGWPeriod& period) = 0;
  /// List up to 'entries.size()' realm names starting from the given marker
  virtual int list_realm_names(const DoutPrefixProvider* dpp,
                               optional_yield y, const std::string& marker,
                               std::span<std::string> entries,
                               ListResult<std::string>& result) = 0;
  ///@}

  /// @group Period
  ///@{

  /// Write a period and advance its latest epoch
  virtual int create_period(const DoutPrefixProvider* dpp,
                            optional_yield y, bool exclusive,
                            const RGWPeriod& info) = 0;
  /// Read a period by id and epoch. If no epoch is given, read the latest
  virtual int read_period(const DoutPrefixProvider* dpp,
                          optional_yield y, std::string_view period_id,
                          std::optional<uint32_t> epoch, RGWPeriod& info) = 0;
  /// Delete all period epochs with the given period id
  virtual int delete_period(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            std::string_view period_id) = 0;
  /// List up to 'entries.size()' period ids starting from the given marker
  virtual int list_period_ids(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              ListResult<std::string>& result) = 0;
  ///@}

  /// @group ZoneGroup
  ///@{

  /// Set the cluster-wide default zonegroup id
  virtual int write_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                         optional_yield y, bool exclusive,
                                         std::string_view realm_id,
                                         std::string_view zonegroup_id) = 0;
  /// Read the cluster's default zonegroup id
  virtual int read_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view realm_id,
                                        std::string& zonegroup_id) = 0;
  /// Delete the cluster's default zonegroup id
  virtual int delete_default_zonegroup_id(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          std::string_view realm_id) = 0;

  /// Create a zonegroup
  virtual int create_zonegroup(const DoutPrefixProvider* dpp,
                               optional_yield y, bool exclusive,
                               const RGWZoneGroup& info,
                               std::unique_ptr<ZoneGroupWriter>* writer) = 0;
  /// Read a zonegroup by id
  virtual int read_zonegroup_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view zonegroup_id,
                                   RGWZoneGroup& info,
                                   std::unique_ptr<ZoneGroupWriter>* writer) = 0;
  /// Read a zonegroup by name
  virtual int read_zonegroup_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view zonegroup_name,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<ZoneGroupWriter>* writer) = 0;
  /// Read the cluster's default zonegroup
  virtual int read_default_zonegroup(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id,
                                     RGWZoneGroup& info,
                                     std::unique_ptr<ZoneGroupWriter>* writer) = 0;
  /// List up to 'entries.size()' zonegroup names starting from the given marker
  virtual int list_zonegroup_names(const DoutPrefixProvider* dpp,
                                   optional_yield y, const std::string& marker,
                                   std::span<std::string> entries,
                                   ListResult<std::string>& result) = 0;
  ///@}

  /// @group Zone
  ///@{

  /// Set the realm-wide default zone id
  virtual int write_default_zone_id(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    std::string_view realm_id,
                                    std::string_view zone_id) = 0;
  /// Read the realm's default zone id
  virtual int read_default_zone_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view realm_id,
                                   std::string& zone_id) = 0;
  /// Delete the realm's default zone id
  virtual int delete_default_zone_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view realm_id) = 0;

  /// Create a zone
  virtual int create_zone(const DoutPrefixProvider* dpp,
                          optional_yield y, bool exclusive,
                          const RGWZoneParams& info,
                          std::unique_ptr<ZoneWriter>* writer) = 0;
  /// Read a zone by id
  virtual int read_zone_by_id(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              std::string_view zone_id,
                              RGWZoneParams& info,
                              std::unique_ptr<ZoneWriter>* writer) = 0;
  /// Read a zone by id or name. If both are empty, try to load the
  /// cluster's default zone
  virtual int read_zone_by_name(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view zone_name,
                                RGWZoneParams& info,
                                std::unique_ptr<ZoneWriter>* writer) = 0;
  /// Read the realm's default zone
  virtual int read_default_zone(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view realm_id,
                                RGWZoneParams& info,
                                std::unique_ptr<ZoneWriter>* writer) = 0;
  /// List up to 'entries.size()' zone names starting from the given marker
  virtual int list_zone_names(const DoutPrefixProvider* dpp,
                              optional_yield y, const std::string& marker,
                              std::span<std::string> entries,
                              ListResult<std::string>& result) = 0;
  ///@}

  /// @group PeriodConfig
  ///@{

  /// Read period config object
  virtual int read_period_config(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 std::string_view realm_id,
                                 RGWPeriodConfig& info) = 0;
  /// Write period config object
  virtual int write_period_config(const DoutPrefixProvider* dpp,
                                  optional_yield y, bool exclusive,
                                  std::string_view realm_id,
                                  const RGWPeriodConfig& info) = 0;
  ///@}

}; // ConfigStore


/// A handle to manage the atomic updates of an existing realm object. This
/// is initialized on read, and any subsequent writes through this handle will
/// fail with -ECANCELED if another writer updates the object in the meantime.
class RealmWriter {
 public:
  virtual ~RealmWriter() {}

  /// Overwrite an existing realm. Must not change id or name
  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWRealm& info) = 0;
  /// Rename an existing realm. Must not change id
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWRealm& info,
                     std::string_view new_name) = 0;
  /// Delete an existing realm
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y) = 0;
};

/// A handle to manage the atomic updates of an existing zonegroup object. This
/// is initialized on read, and any subsequent writes through this handle will
/// fail with -ECANCELED if another writer updates the object in the meantime.
class ZoneGroupWriter {
 public:
  virtual ~ZoneGroupWriter() {}

  /// Overwrite an existing zonegroup. Must not change id or name
  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWZoneGroup& info) = 0;
  /// Rename an existing zonegroup. Must not change id
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWZoneGroup& info,
                     std::string_view new_name) = 0;
  /// Delete an existing zonegroup
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y) = 0;
};

/// A handle to manage the atomic updates of an existing zone object. This
/// is initialized on read, and any subsequent writes through this handle will
/// fail with -ECANCELED if another writer updates the object in the meantime.
class ZoneWriter {
 public:
  virtual ~ZoneWriter() {}

  /// Overwrite an existing zone. Must not change id or name
  virtual int write(const DoutPrefixProvider* dpp,
                    optional_yield y,
                    const RGWZoneParams& info) = 0;
  /// Rename an existing zone. Must not change id
  virtual int rename(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     RGWZoneParams& info,
                     std::string_view new_name) = 0;
  /// Delete an existing zone
  virtual int remove(const DoutPrefixProvider* dpp,
                     optional_yield y) = 0;
};

} // namespace rgw::sal
