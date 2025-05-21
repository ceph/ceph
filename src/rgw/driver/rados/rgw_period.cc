// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"

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

void RGWPeriod::generate_test_instances(list<RGWPeriod*> &o)
{
  RGWPeriod *z = new RGWPeriod;
  o.push_back(z);
  o.push_back(new RGWPeriod);
}


