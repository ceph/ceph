// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"

#include "services/svc_zone.h"

#define FIRST_EPOCH 1

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw_zone_defaults;

int RGWPeriod::get_zonegroup(RGWZoneGroup& zonegroup,
                             const string& zonegroup_id) const
{
  map<string, RGWZoneGroup>::const_iterator iter;
  if (!zonegroup_id.empty()) {
    iter = period_map.zonegroups.find(zonegroup_id);
  } else {
    iter = period_map.zonegroups.find("default");
  }
  if (iter != period_map.zonegroups.end()) {
    zonegroup = iter->second;
    return 0;
  }

  return -ENOENT;
}

int RGWPeriod::get_latest_epoch(const DoutPrefixProvider *dpp, epoch_t& latest_epoch, optional_yield y)
{
  RGWPeriodLatestEpochInfo info;

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  int ret = cfgstore->read_latest_epoch(dpp, y, id, info.epoch, nullptr, *this);
  if (ret < 0) {
    return ret;
  }

  latest_epoch = info.epoch;

  return 0;
}

void RGWPeriod::fork()
{
  ldout(cct, 20) << __func__ << " realm " << realm_id << " period " << id << dendl;
  predecessor_uuid = id;
  id = get_staging_id(realm_id);
  period_map.reset();
  realm_epoch++;
}

static int read_sync_status(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, rgw_meta_sync_status *sync_status)
{
  rgw::sal::RadosStore* rados_store = static_cast<rgw::sal::RadosStore*>(driver);
  // initialize a sync status manager to read the status
  RGWMetaSyncStatusManager mgr(rados_store, rados_store->svc()->async_processor);
  int r = mgr.init(dpp);
  if (r < 0) {
    return r;
  }
  r = mgr.read_sync_status(dpp, sync_status);
  mgr.stop();
  return r;
}

int RGWPeriod::update_sync_status(const DoutPrefixProvider *dpp,
                                  rgw::sal::Driver* driver, /* for now */
				  const RGWPeriod &current_period,
                                  std::ostream& error_stream,
                                  bool force_if_stale)
{
  rgw_meta_sync_status status;
  int r = read_sync_status(dpp, driver, &status);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "period failed to read sync status: "
        << cpp_strerror(-r) << dendl;
    return r;
  }

  std::vector<std::string> markers;

  const auto current_epoch = current_period.get_realm_epoch();
  if (current_epoch != status.sync_info.realm_epoch) {
    // no sync status markers for the current period
    ceph_assert(current_epoch > status.sync_info.realm_epoch);
    const int behind = current_epoch - status.sync_info.realm_epoch;
    if (!force_if_stale && current_epoch > 1) {
      error_stream << "ERROR: This zone is " << behind << " period(s) behind "
          "the current master zone in metadata sync. If this zone is promoted "
          "to master, any metadata changes during that time are likely to "
          "be lost.\n"
          "Waiting for this zone to catch up on metadata sync (see "
          "'radosgw-admin sync status') is recommended.\n"
          "To promote this zone to master anyway, add the flag "
          "--yes-i-really-mean-it." << std::endl;
      return -EINVAL;
    }
    // empty sync status markers - other zones will skip this period during
    // incremental metadata sync
    markers.resize(status.sync_info.num_shards);
  } else {
    markers.reserve(status.sync_info.num_shards);
    for (auto& i : status.sync_markers) {
      auto& marker = i.second;
      // filter out markers from other periods
      if (marker.realm_epoch != current_epoch) {
        marker.marker.clear();
      }
      markers.emplace_back(std::move(marker.marker));
    }
  }

  std::swap(sync_status, markers);
  return 0;
}

int RGWPeriod::commit(const DoutPrefixProvider *dpp,
		      rgw::sal::Driver* driver,
		      RGWRealm& realm, const RGWPeriod& current_period,
                      std::ostream& error_stream, optional_yield y,
		      bool force_if_stale)
{
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.get_id() << " period " << current_period.get_id() << dendl;
  // gateway must be in the master zone to commit
  if (driver->get_zone()->get_zonegroup().is_master_zonegroup()) {
    error_stream << "Cannot commit period on zone "
        << driver->get_zone()->get_id() << ", it must be sent to "
        "the period's master zone " << master_zone << '.' << std::endl;
    return -EINVAL;
  }
  // period predecessor must match current period
  if (predecessor_uuid != current_period.get_id()) {
    error_stream << "Period predecessor " << predecessor_uuid
        << " does not match current period " << current_period.get_id()
        << ". Use 'period pull' to get the latest period from the master, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // realm epoch must be 1 greater than current period
  if (realm_epoch != current_period.get_realm_epoch() + 1) {
    error_stream << "Period's realm epoch " << realm_epoch
        << " does not come directly after current realm epoch "
        << current_period.get_realm_epoch() << ". Use 'realm pull' to get the "
        "latest realm and period from the master zone, reapply your changes, "
        "and try again." << std::endl;
    return -EINVAL;
  }

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  // did the master zone change?
  if (master_zone != current_period.get_master_zone()) {
    // store the current metadata sync status in the period
    int r = update_sync_status(dpp, driver, current_period, error_stream, force_if_stale);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update metadata sync status: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    // create an object with a new period id
    period_map.id = id = rgw::gen_random_uuid();
    epoch = FIRST_EPOCH;

    constexpr bool exclusive = true;
    r = cfgstore->create_period(dpp, y, exclusive, *this);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create new period: " << cpp_strerror(-r) << dendl;
      return r;
    }
    // set as current period
    r = realm.set_current_period(dpp, *this, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update realm's current period: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 4) << "Promoted to master zone and committed new period "
        << id << dendl;
    return 0;
  }
  // period must be based on current epoch
  if (epoch != current_period.get_epoch()) {
    error_stream << "Period epoch " << epoch << " does not match "
        "predecessor epoch " << current_period.get_epoch()
        << ". Use 'period pull' to get the latest epoch from the master zone, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // set period as next epoch
  set_id(current_period.get_id());
  set_epoch(current_period.get_epoch() + 1);
  set_predecessor(current_period.get_predecessor());
  realm_epoch = current_period.get_realm_epoch();
  // write the period to rados
  int r = cfgstore->create_period(dpp, y, false, *this);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to store period: " << cpp_strerror(-r) << dendl;
    return r;
  }
  // set as latest epoch
  r = update_latest_epoch(dpp, epoch, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to set latest epoch: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = rgw::reflect_period(dpp, y, cfgstore.get(), *this);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to update local objects: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 4) << "Committed new epoch " << epoch
      << " for period " << id << dendl;
  return 0;
}

void RGWPeriod::generate_test_instances(list<RGWPeriod*> &o)
{
  RGWPeriod *z = new RGWPeriod;
  o.push_back(z);
  o.push_back(new RGWPeriod);
}


