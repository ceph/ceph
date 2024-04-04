// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"

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

  int ret = read_latest_epoch(dpp, info, y);
  if (ret < 0) {
    return ret;
  }

  latest_epoch = info.epoch;

  return 0;
}

int RGWPeriod::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool(cct));

  // delete the object for each period epoch
  for (epoch_t e = 1; e <= epoch; e++) {
    RGWPeriod p{get_id(), e};
    rgw_raw_obj oid{pool, p.get_period_oid()};
    auto sysobj = sysobj_svc->get_obj(oid);
    int ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
          << ": " << cpp_strerror(-ret) << dendl;
    }
  }

  // delete the .latest_epoch object
  rgw_raw_obj oid{pool, get_period_oid_prefix() + get_latest_epoch_oid()};
  auto sysobj = sysobj_svc->get_obj(oid);
  int ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
        << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWPeriod::add_zonegroup(const DoutPrefixProvider *dpp, const RGWZoneGroup& zonegroup, optional_yield y)
{
  if (zonegroup.realm_id != realm_id) {
    return 0;
  }
  int ret = period_map.update(zonegroup, cct);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: updating period map: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_info(dpp, false, y);
}

int RGWPeriod::update(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm_id << " period " << get_id() << dendl;
  list<string> zonegroups;
  int ret = zone_svc->list_zonegroups(dpp, zonegroups);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to list zonegroups: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  // clear zone short ids of removed zones. period_map.update() will add the
  // remaining zones back
  period_map.short_zone_ids.clear();

  for (auto& iter : zonegroups) {
    RGWZoneGroup zg(string(), iter);
    ret = zg.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: zg.init() failed: " << cpp_strerror(-ret) << dendl;
      continue;
    }

    if (zg.realm_id != realm_id) {
      ldpp_dout(dpp, 20) << "skipping zonegroup " << zg.get_name() << " zone realm id " << zg.realm_id << ", not on our realm " << realm_id << dendl;
      continue;
    }

    if (zg.master_zone.empty()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name() << " should have a master zone " << dendl;
      return -EINVAL;
    }

    if (zg.zones.find(zg.master_zone) == zg.zones.end()) {
      ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name()
                   << " has a non existent master zone "<< dendl;
      return -EINVAL;
    }

    if (zg.is_master_zonegroup()) {
      master_zonegroup = zg.get_id();
      master_zone = zg.master_zone;
    }

    int ret = period_map.update(zg, cct);
    if (ret < 0) {
      return ret;
    }
  }

  ret = period_config.read(dpp, sysobj_svc, realm_id, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read period config: "
        << cpp_strerror(ret) << dendl;
    return ret;
  }
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
  auto zone_svc = sysobj_svc->get_zone_svc();
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.get_id() << " period " << current_period.get_id() << dendl;
  // gateway must be in the master zone to commit
  if (master_zone != zone_svc->get_zone_params().get_id()) {
    error_stream << "Cannot commit period on zone "
        << zone_svc->get_zone_params().get_id() << ", it must be sent to "
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
    r = create(dpp, y, true);
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
    realm.notify_new_period(dpp, *this, y);
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
  int r = store_info(dpp, false, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to store period: " << cpp_strerror(-r) << dendl;
    return r;
  }
  // set as latest epoch
  r = update_latest_epoch(dpp, epoch, y);
  if (r == -EEXIST) {
    // already have this epoch (or a more recent one)
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to set latest epoch: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = reflect(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to update local objects: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 4) << "Committed new epoch " << epoch
      << " for period " << id << dendl;
  realm.notify_new_period(dpp, *this, y);
  return 0;
}

void RGWPeriod::generate_test_instances(list<RGWPeriod*> &o)
{
  RGWPeriod *z = new RGWPeriod;
  o.push_back(z);
  o.push_back(new RGWPeriod);
}


