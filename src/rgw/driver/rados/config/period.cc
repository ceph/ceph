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

#include "common/dout.h"
#include "common/errno.h"
#include "rgw_zone.h"
#include "driver/rados/config/store.h"

#include "impl.h"

namespace rgw::rados {

// period oids
constexpr std::string_view period_info_oid_prefix = "periods.";
constexpr std::string_view period_latest_epoch_info_oid = ".latest_epoch";
constexpr std::string_view period_staging_suffix = ":staging";

static std::string period_oid(std::string_view period_id, uint32_t epoch)
{
  // omit the epoch for the staging period
  if (period_id.ends_with(period_staging_suffix)) {
    return string_cat_reserve(period_info_oid_prefix, period_id);
  }
  return fmt::format("{}{}.{}", period_info_oid_prefix, period_id, epoch);
}

static std::string latest_epoch_oid(const ceph::common::ConfigProxy& conf,
                                    std::string_view period_id)
{
  return string_cat_reserve(
      period_info_oid_prefix, period_id,
      name_or_default(conf->rgw_period_latest_epoch_info_oid,
                      period_latest_epoch_info_oid));
}

static int read_latest_epoch(const DoutPrefixProvider* dpp, optional_yield y,
                             ConfigImpl* impl, std::string_view period_id,
                             uint32_t& epoch, RGWObjVersionTracker* objv)
{
  const auto& pool = impl->period_pool;
  const auto latest_oid = latest_epoch_oid(dpp->get_cct()->_conf, period_id);
  RGWPeriodLatestEpochInfo latest;
  int r = impl->read(dpp, y, pool, latest_oid, latest, objv);
  if (r >= 0) {
    epoch = latest.epoch;
  }
  return r;
}

static int write_latest_epoch(const DoutPrefixProvider* dpp, optional_yield y,
                              ConfigImpl* impl, bool exclusive,
                              std::string_view period_id, uint32_t epoch,
                              RGWObjVersionTracker* objv)
{
  const auto& pool = impl->period_pool;
  const auto latest_oid = latest_epoch_oid(dpp->get_cct()->_conf, period_id);
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  RGWPeriodLatestEpochInfo latest{epoch};
  return impl->write(dpp, y, pool, latest_oid, create, latest, objv);
}

static int delete_latest_epoch(const DoutPrefixProvider* dpp, optional_yield y,
                               ConfigImpl* impl, std::string_view period_id,
                               RGWObjVersionTracker* objv)
{
  const auto& pool = impl->period_pool;
  const auto latest_oid = latest_epoch_oid(dpp->get_cct()->_conf, period_id);
  return impl->remove(dpp, y, pool, latest_oid, objv);
}

static int update_latest_epoch(const DoutPrefixProvider* dpp, optional_yield y,
                               ConfigImpl* impl, std::string_view period_id,
                               uint32_t epoch)
{
  static constexpr int MAX_RETRIES = 20;

  for (int i = 0; i < MAX_RETRIES; i++) {
    uint32_t existing_epoch = 0;
    RGWObjVersionTracker objv;
    bool exclusive = false;

    // read existing epoch
    int r = read_latest_epoch(dpp, y, impl, period_id, existing_epoch, &objv);
    if (r == -ENOENT) {
      // use an exclusive create to set the epoch atomically
      exclusive = true;
      objv.generate_new_write_ver(dpp->get_cct());
      ldpp_dout(dpp, 20) << "creating initial latest_epoch=" << epoch
          << " for period=" << period_id << dendl;
    } else if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read latest_epoch" << dendl;
      return r;
    } else if (epoch <= existing_epoch) {
      r = -EEXIST; // fail with EEXIST if epoch is not newer
      ldpp_dout(dpp, 10) << "found existing latest_epoch " << existing_epoch
          << " >= given epoch " << epoch << ", returning r=" << r << dendl;
      return r;
    } else {
      ldpp_dout(dpp, 20) << "updating latest_epoch from " << existing_epoch
          << " -> " << epoch << " on period=" << period_id << dendl;
    }

    r = write_latest_epoch(dpp, y, impl, exclusive, period_id, epoch, &objv);
    if (r == -EEXIST) {
      continue; // exclusive create raced with another update, retry
    } else if (r == -ECANCELED) {
      continue; // write raced with a conflicting version, retry
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to write latest_epoch" << dendl;
      return r;
    }
    return 0; // return success
  }

  return -ECANCELED; // fail after max retries
}

int RadosConfigStore::create_period(const DoutPrefixProvider* dpp,
                                    optional_yield y, bool exclusive,
                                    const RGWPeriod& info)
{
  if (info.get_id().empty()) {
    ldpp_dout(dpp, 0) << "period cannot have an empty id" << dendl;
    return -EINVAL;
  }
  if (info.get_epoch() == 0) {
    ldpp_dout(dpp, 0) << "period cannot have an empty epoch" << dendl;
    return -EINVAL;
  }
  const auto& pool = impl->period_pool;
  const auto info_oid = period_oid(info.get_id(), info.get_epoch());
  const auto create = exclusive ? Create::MustNotExist : Create::MayExist;
  RGWObjVersionTracker objv;
  objv.generate_new_write_ver(dpp->get_cct());
  int r = impl->write(dpp, y, pool, info_oid, create, info, &objv);
  if (r < 0) {
    return r;
  }

  (void) update_latest_epoch(dpp, y, impl.get(), info.get_id(), info.get_epoch());
  return 0;
}

int RadosConfigStore::read_period(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  std::string_view period_id,
                                  std::optional<uint32_t> epoch,
                                  RGWPeriod& info)
{
  int r = 0;
  if (!epoch) {
    epoch = 0;
    r = read_latest_epoch(dpp, y, impl.get(), period_id, *epoch, nullptr);
    if (r < 0) {
      return r;
    }
  }

  const auto& pool = impl->period_pool;
  const auto info_oid = period_oid(period_id, *epoch);
  return impl->read(dpp, y, pool, info_oid, info, nullptr);
}

int RadosConfigStore::delete_period(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    std::string_view period_id)
{
  const auto& pool = impl->period_pool;

  // read the latest_epoch
  uint32_t latest_epoch = 0;
  RGWObjVersionTracker latest_objv;
  int r = read_latest_epoch(dpp, y, impl.get(), period_id,
                            latest_epoch, &latest_objv);
  if (r < 0 && r != -ENOENT) { // just delete epoch=0 on ENOENT
    ldpp_dout(dpp, 0) << "failed to read latest epoch for period "
        << period_id << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  for (uint32_t epoch = 0; epoch <= latest_epoch; epoch++) {
    const auto info_oid = period_oid(period_id, epoch);
    r = impl->remove(dpp, y, pool, info_oid, nullptr);
    if (r < 0 && r != -ENOENT) { // ignore ENOENT
      ldpp_dout(dpp, 0) << "failed to delete period " << info_oid
          << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return delete_latest_epoch(dpp, y, impl.get(), period_id, &latest_objv);
}

int RadosConfigStore::list_period_ids(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const std::string& marker,
                                      std::span<std::string> entries,
                                      sal::ListResult<std::string>& result)
{
  const auto& pool = impl->period_pool;
  constexpr auto prefix = [] (std::string oid) -> std::string {
      if (!oid.starts_with(period_info_oid_prefix)) {
        return {};
      }
      if (!oid.ends_with(period_latest_epoch_info_oid)) {
        return {};
      }
      // trim the prefix and suffix
      const std::size_t count = oid.size() -
          period_info_oid_prefix.size() -
          period_latest_epoch_info_oid.size();
      return oid.substr(period_info_oid_prefix.size(), count);
    };

  return impl->list(dpp, y, pool, marker, prefix, entries, result);
}

} // namespace rgw::rados
