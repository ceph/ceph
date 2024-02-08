// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_zone.h"
#include "svc_sys_obj.h"
#include "svc_sync_modules.h"

#include "rgw_tools.h"
#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_bucket_sync.h"

#include "common/errno.h"
#include "include/random.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw_zone_defaults;

RGWSI_Zone::RGWSI_Zone(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_Zone::init(RGWSI_SysObj *_sysobj_svc,
                      librados::Rados* rados_,
                      RGWSI_SyncModules * _sync_modules_svc,
		      RGWSI_Bucket_Sync *_bucket_sync_svc)
{
  sysobj_svc = _sysobj_svc;
  rados = rados_;
  sync_modules_svc = _sync_modules_svc;
  bucket_sync_svc = _bucket_sync_svc;

  realm = new RGWRealm();
  zonegroup = new RGWZoneGroup();
  zone_public_config = new RGWZone();
  zone_params = new RGWZoneParams();
  current_period = new RGWPeriod();
}

RGWSI_Zone::~RGWSI_Zone()
{
  delete realm;
  delete zonegroup;
  delete zone_public_config;
  delete zone_params;
  delete current_period;
}

std::shared_ptr<RGWBucketSyncPolicyHandler> RGWSI_Zone::get_sync_policy_handler(std::optional<rgw_zone_id> zone) const {
  if (!zone || *zone == zone_id()) {
    return sync_policy_handler;
  }
  auto iter = sync_policy_handlers.find(*zone);
  if (iter == sync_policy_handlers.end()) {
    return std::shared_ptr<RGWBucketSyncPolicyHandler>();
  }
  return iter->second;
}

bool RGWSI_Zone::zone_syncs_from(const RGWZone& target_zone, const RGWZone& source_zone) const
{
  return target_zone.syncs_from(source_zone.name) &&
         sync_modules_svc->get_manager()->supports_data_export(source_zone.tier_type);
}

bool RGWSI_Zone::zone_syncs_from(const RGWZone& source_zone) const
{
  auto target_zone = get_zone();
  bool found = false;

  for (auto s : data_sync_source_zones) {
    if (s->id == source_zone.id) {
      found = true;
      break;
    }
  }
  return found && target_zone.syncs_from(source_zone.name) &&
         sync_modules_svc->get_manager()->supports_data_export(source_zone.tier_type);
}

int RGWSI_Zone::search_realm_with_zone(const DoutPrefixProvider *dpp,
                                       const rgw_zone_id& zid,
                                       RGWRealm *prealm,
                                       RGWPeriod *pperiod,
                                       RGWZoneGroup *pzonegroup,
                                       bool *pfound,
                                       optional_yield y)
{
  auto& found = *pfound;

  found = false;

  list<string> realms;
  int r = list_realms(dpp, realms);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to list realms: r=" << r << dendl;
    return r;
  }

  for (auto& realm_name : realms) {
    string realm_id;
    RGWRealm realm(realm_id, realm_name);
    r = realm.init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "WARNING: can't open realm " << realm_name << ": " << cpp_strerror(-r) << " ... skipping" << dendl;
      continue;
    }

    r = realm.find_zone(dpp, zid, pperiod,
                        pzonegroup, &found, y);
    if (r < 0) {
      ldpp_dout(dpp, 20) << __func__ << "(): ERROR: realm.find_zone() returned r=" << r<< dendl;
      return r;
    }

    if (found) {
      *prealm = realm;
      ldpp_dout(dpp, 20) << __func__ << "(): found realm_id=" << realm_id << " realm_name=" << realm_name << dendl;
      return 0;
    }
  }

  return 0;
}

int RGWSI_Zone::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret = sysobj_svc->start(y, dpp);
  if (ret < 0) {
    return ret;
  }

  assert(sysobj_svc->is_started()); /* if not then there's ordering issue */

  ret = realm->init(dpp, cct, sysobj_svc, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading realm info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "realm  " << realm->get_name() << " " << realm->get_id() << dendl;
  ret = current_period->init(dpp, cct, sysobj_svc, realm->get_id(), y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading current period info: " << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = zone_params->init(dpp, cct, sysobj_svc, y);
  bool found_zone = (ret == 0);
  if (ret < 0 && ret != -ENOENT) {
    lderr(cct) << "failed reading zone info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  cur_zone_id = rgw_zone_id(zone_params->get_id());

  bool found_period_conf = false;

  /* try to find zone in period config (if we have one) */
  if (found_zone &&
      !current_period->get_id().empty()) {
    found_period_conf = current_period->find_zone(dpp,
                                    cur_zone_id,
                                    zonegroup,
                                    y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: current_period->find_zone() returned ret=" << ret << dendl;
      return ret;
    }
    if (!found_period_conf) {
      ldpp_dout(dpp, 0) << "period (" << current_period->get_id() << " does not have zone " << cur_zone_id << " configured" << dendl;
    }
  }

  RGWRealm search_realm;

  if (found_zone &&
      !found_period_conf) {
    ldpp_dout(dpp, 20) << "searching for the correct realm" << dendl;
    ret = search_realm_with_zone(dpp,
                                 cur_zone_id,
                                 realm,
                                 current_period,
                                 zonegroup,
                                 &found_period_conf,
                                 y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: search_realm_conf() failed: ret="<< ret << dendl;
      return ret;
    }
  }
  bool zg_initialized = found_period_conf;

  if (!zg_initialized) {
    /* couldn't find a proper period config, use local zonegroup */
    ret = zonegroup->init(dpp, cct, sysobj_svc, y);
    zg_initialized = (ret == 0);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "failed reading zonegroup info: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  auto& zonegroup_param = cct->_conf->rgw_zonegroup;
  bool init_from_period = found_period_conf;
  bool explicit_zg = !zonegroup_param.empty();

  if (!zg_initialized &&
      (!explicit_zg || zonegroup_param == default_zonegroup_name)) {
    /* we couldn't initialize any zonegroup,
       falling back to a non-multisite config with default zonegroup */
    ret = create_default_zg(dpp, y);
    if (ret < 0) {
      return ret;
    }
    zg_initialized = true;
  }

  if (!zg_initialized) {
    ldpp_dout(dpp, 0) << "ERROR: could not find zonegroup (" << zonegroup_param << ")" << dendl;
    return -ENOENT;
  }

  /* we have zonegroup now */

  if (explicit_zg &&
      zonegroup->get_name() != zonegroup_param) {
    ldpp_dout(dpp, 0) << "ERROR: incorrect zonegroup: " << zonegroup_param << " (got: " << zonegroup_param << ", expected: " << zonegroup->get_name() << ")" << dendl;
    return -EINVAL;
  }

  auto& zone_param = cct->_conf->rgw_zone;
  bool explicit_zone = !zone_param.empty();

  if (!found_zone) {
    if ((!explicit_zone || zone_param == default_zone_name) &&
        zonegroup->get_name() == default_zonegroup_name) {
      ret = init_default_zone(dpp, y);
      if (ret < 0 && ret != -ENOENT) {
        return ret;
      }
      cur_zone_id = zone_params->get_id();
    } else {
      ldpp_dout(dpp, 0) << "ERROR: could not find zone (" << zone_param << ")" << dendl;
      return -ENOENT;
    }
  }

  /* we have zone now */

  auto zone_iter = zonegroup->zones.find(zone_params->get_id());
  if (zone_iter == zonegroup->zones.end()) {
    /* shouldn't happen if relying on period config */
    if (!init_from_period) {
      ldpp_dout(dpp, -1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << "), switching to local zonegroup configuration" << dendl;
    init_from_period = false;
    zone_iter = zonegroup->zones.find(zone_params->get_id());
  }
  if (zone_iter == zonegroup->zones.end()) {
    ldpp_dout(dpp, -1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
    return -EINVAL;
  }
  *zone_public_config = zone_iter->second;
  ldout(cct, 20) << "zone " << zone_params->get_name() << " found"  << dendl;

  ldpp_dout(dpp, 4) << "Realm:     " << std::left << setw(20) << realm->get_name() << " (" << realm->get_id() << ")" << dendl;
  ldpp_dout(dpp, 4) << "ZoneGroup: " << std::left << setw(20) << zonegroup->get_name() << " (" << zonegroup->get_id() << ")" << dendl;
  ldpp_dout(dpp, 4) << "Zone:      " << std::left << setw(20) << zone_params->get_name() << " (" << zone_params->get_id() << ")" << dendl;

  if (init_from_period) {
    ldpp_dout(dpp, 4) << "using period configuration: " << current_period->get_id() << ":" << current_period->get_epoch() << dendl;
    ret = init_zg_from_period(dpp, y);
    if (ret < 0) {
      return ret;
    }
  } else {
    ldout(cct, 10) << "cannot find current period zonegroup using local zonegroup configuration" << dendl;
    ret = init_zg_from_local(dpp, y);
    if (ret < 0) {
      return ret;
    }
    // read period_config into current_period
    auto& period_config = current_period->get_config();
    ret = period_config.read(dpp, sysobj_svc, zonegroup->realm_id, y);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: failed to read period config: "
          << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  zone_short_id = current_period->get_map().get_zone_short_id(zone_params->get_id());

  for (auto ziter : zonegroup->zones) {
    auto zone_handler = std::make_shared<RGWBucketSyncPolicyHandler>(this, sync_modules_svc, bucket_sync_svc, ziter.second.id);
    ret = zone_handler->init(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: could not initialize zone policy handler for zone=" << ziter.second.name << dendl;
      return ret;
      }
    sync_policy_handlers[ziter.second.id] = zone_handler;
  }

  sync_policy_handler = sync_policy_handlers[zone_id()]; /* we made sure earlier that zonegroup->zones has our zone */

  set<rgw_zone_id> source_zones;
  set<rgw_zone_id> target_zones;

  sync_policy_handler->reflect(dpp, nullptr, nullptr,
                               nullptr, nullptr,
                               &source_zones,
                               &target_zones,
                               false); /* relaxed: also get all zones that we allow to sync to/from */

  ret = sync_modules_svc->start(y, dpp);
  if (ret < 0) {
    return ret;
  }

  auto sync_modules = sync_modules_svc->get_manager();
  RGWSyncModuleRef sm;
  if (!sync_modules->get_module(zone_public_config->tier_type, &sm)) {
    ldpp_dout(dpp, -1) << "ERROR: tier type not found: " << zone_public_config->tier_type << dendl;
    return -EINVAL;
  }

  writeable_zone = sm->supports_writes();
  exports_data = sm->supports_data_export();

  /* first build all zones index */
  for (auto ziter : zonegroup->zones) {
    const rgw_zone_id& id = ziter.first;
    RGWZone& z = ziter.second;
    zone_id_by_name[z.name] = id;
    zone_by_id[id] = z;
  }

  if (zone_by_id.find(zone_id()) == zone_by_id.end()) {
    ldpp_dout(dpp, 0) << "WARNING: could not find zone config in zonegroup for local zone (" << zone_id() << "), will use defaults" << dendl;
  }

  for (const auto& ziter : zonegroup->zones) {
    const rgw_zone_id& id = ziter.first;
    const RGWZone& z = ziter.second;
    if (id == zone_id()) {
      continue;
    }
    if (z.endpoints.empty()) {
      ldpp_dout(dpp, 0) << "WARNING: can't generate connection for zone " << z.id << " id " << z.name << ": no endpoints defined" << dendl;
      continue;
    }
    ldpp_dout(dpp, 20) << "generating connection object for zone " << z.name << " id " << z.id << dendl;
    RGWRESTConn *conn = new RGWRESTConn(cct, z.id, z.endpoints, zone_params->system_key, zonegroup->get_id(), zonegroup->api_name);
    zone_conn_map[id] = conn;

    bool zone_is_source = source_zones.find(z.id) != source_zones.end();
    bool zone_is_target = target_zones.find(z.id) != target_zones.end();

    if (zone_is_source || zone_is_target) {
      if (zone_is_source && sync_modules->supports_data_export(z.tier_type)) {
        data_sync_source_zones.push_back(&z);
      }
      if (zone_is_target) {
        zone_data_notify_to_map[id] = conn;
      }
    } else {
      ldpp_dout(dpp, 20) << "NOTICE: not syncing to/from zone " << z.name << " id " << z.id << dendl;
    }
  }

  ldpp_dout(dpp, 20) << "started zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << 
        ") with tier type = " << zone_public_config->tier_type << dendl;

  return 0;
}

void RGWSI_Zone::shutdown()
{
  delete rest_master_conn;

  for (auto& item : zone_conn_map) {
    auto conn = item.second;
    delete conn;
  }

  for (auto& item : zonegroup_conn_map) {
    auto conn = item.second;
    delete conn;
  }
}

int RGWSI_Zone::list_regions(const DoutPrefixProvider *dpp, list<string>& regions)
{
  RGWZoneGroup zonegroup;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zonegroup.get_pool(cct));

  return syspool.list_prefixed_objs(dpp, region_info_oid_prefix, &regions);
}

int RGWSI_Zone::list_zonegroups(const DoutPrefixProvider *dpp, list<string>& zonegroups)
{
  RGWZoneGroup zonegroup;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zonegroup.get_pool(cct));

  return syspool.list_prefixed_objs(dpp, zonegroup_names_oid_prefix, &zonegroups);
}

int RGWSI_Zone::list_zones(const DoutPrefixProvider *dpp, list<string>& zones)
{
  RGWZoneParams zoneparams;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zoneparams.get_pool(cct));

  return syspool.list_prefixed_objs(dpp, zone_names_oid_prefix, &zones);
}

int RGWSI_Zone::list_realms(const DoutPrefixProvider *dpp, list<string>& realms)
{
  RGWRealm realm(cct, sysobj_svc);
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(realm.get_pool(cct));

  return syspool.list_prefixed_objs(dpp, realm_names_oid_prefix, &realms);
}

int RGWSI_Zone::list_periods(const DoutPrefixProvider *dpp, list<string>& periods)
{
  RGWPeriod period;
  list<string> raw_periods;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(period.get_pool(cct));
  int ret = syspool.list_prefixed_objs(dpp, period.get_info_oid_prefix(), &raw_periods);
  if (ret < 0) {
    return ret;
  }
  for (const auto& oid : raw_periods) {
    size_t pos = oid.find(".");
    if (pos != std::string::npos) {
      periods.push_back(oid.substr(0, pos));
    } else {
      periods.push_back(oid);
    }
  }
  periods.sort(); // unique() only detects duplicates if they're adjacent
  periods.unique();
  return 0;
}


int RGWSI_Zone::list_periods(const DoutPrefixProvider *dpp, const string& current_period, list<string>& periods, optional_yield y)
{
  int ret = 0;
  string period_id = current_period;
  while(!period_id.empty()) {
    RGWPeriod period(period_id);
    ret = period.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      return ret;
    }
    periods.push_back(period.get_id());
    period_id = period.get_predecessor();
  }

  return ret;
}

/**
 * Add new connection to connections map
 * @param zonegroup_conn_map map which new connection will be added to
 * @param zonegroup zonegroup which new connection will connect to
 * @param new_connection pointer to new connection instance
 */
static void add_new_connection_to_map(map<string, RGWRESTConn *> &zonegroup_conn_map,
				      const RGWZoneGroup &zonegroup, RGWRESTConn *new_connection)
{
  // Delete if connection is already exists
  map<string, RGWRESTConn *>::iterator iterZoneGroup = zonegroup_conn_map.find(zonegroup.get_id());
  if (iterZoneGroup != zonegroup_conn_map.end()) {
    delete iterZoneGroup->second;
  }

  // Add new connection to connections map
  zonegroup_conn_map[zonegroup.get_id()] = new_connection;
}

int RGWSI_Zone::init_zg_from_period(const DoutPrefixProvider *dpp, optional_yield y)
{
  ldout(cct, 20) << "period zonegroup name " << zonegroup->get_name() << dendl;

  map<string, RGWZoneGroup>::const_iterator iter =
    current_period->get_map().zonegroups.find(zonegroup->get_id());

  if (iter != current_period->get_map().zonegroups.end()) {
    ldpp_dout(dpp, 20) << "using current period zonegroup " << zonegroup->get_name() << dendl;
    *zonegroup = iter->second;
    int ret = zonegroup->init(dpp, cct, sysobj_svc, y, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed init zonegroup: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  for (iter = current_period->get_map().zonegroups.begin();
       iter != current_period->get_map().zonegroups.end(); ++iter){
    const RGWZoneGroup& zg = iter->second;
    // use endpoints from the zonegroup's master zone
    auto master = zg.zones.find(zg.master_zone);
    if (master == zg.zones.end()) {
      // Check for empty zonegroup which can happen if zone was deleted before removal
      if (zg.zones.size() == 0)
        continue;
      // fix missing master zone for a single zone zonegroup
      if (zg.master_zone.empty() && zg.zones.size() == 1) {
	master = zg.zones.begin();
	ldpp_dout(dpp, 0) << "zonegroup " << zg.get_name() << " missing master_zone, setting zone " <<
	  master->second.name << " id:" << master->second.id << " as master" << dendl;
	if (zonegroup->get_id() == zg.get_id()) {
	  zonegroup->master_zone = master->second.id;
	  int ret = zonegroup->update(dpp, y);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) << "error updating zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	} else {
	  RGWZoneGroup fixed_zg(zg.get_id(),zg.get_name());
	  int ret = fixed_zg.init(dpp, cct, sysobj_svc, y);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	  fixed_zg.master_zone = master->second.id;
	  ret = fixed_zg.update(dpp, y);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	}
      } else {
	ldpp_dout(dpp, 0) << "zonegroup " << zg.get_name() << " missing zone for master_zone=" <<
	  zg.master_zone << dendl;
	return -EINVAL;
      }
    }
    const auto& endpoints = master->second.endpoints;
    add_new_connection_to_map(zonegroup_conn_map, zg, new RGWRESTConn(cct, zg.get_id(), endpoints, zone_params->system_key, zonegroup->get_id(), zg.api_name));
    if (!current_period->get_master_zonegroup().empty() &&
        zg.get_id() == current_period->get_master_zonegroup()) {
      rest_master_conn = new RGWRESTConn(cct, zg.get_id(), endpoints, zone_params->system_key, zonegroup->get_id(), zg.api_name);
    }
  }

  return 0;
}

int RGWSI_Zone::create_default_zg(const DoutPrefixProvider *dpp, optional_yield y)
{
  ldout(cct, 10) << "Creating default zonegroup " << dendl;
  int ret = zonegroup->create_default(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
      << dendl;
    return ret;
  }
  ret = zonegroup->init(dpp, cct, sysobj_svc, y);
  if (ret < 0) {
    ldout(cct, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
      << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_Zone::init_default_zone(const DoutPrefixProvider *dpp, optional_yield y)
{
  ldpp_dout(dpp, 10) << " Using default name "<< default_zone_name << dendl;
  zone_params->set_name(default_zone_name);
  int ret = zone_params->init(dpp, cct, sysobj_svc, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

int RGWSI_Zone::init_zg_from_local(const DoutPrefixProvider *dpp, optional_yield y)
{
  ldpp_dout(dpp, 20) << "zonegroup " << zonegroup->get_name() << dendl;
  if (zonegroup->is_master_zonegroup()) {
    // use endpoints from the zonegroup's master zone
    auto master = zonegroup->zones.find(zonegroup->master_zone);
    if (master == zonegroup->zones.end()) {
      // fix missing master zone for a single zone zonegroup
      if (zonegroup->master_zone.empty() && zonegroup->zones.size() == 1) {
	master = zonegroup->zones.begin();
	ldpp_dout(dpp, 0) << "zonegroup " << zonegroup->get_name() << " missing master_zone, setting zone " <<
	  master->second.name << " id:" << master->second.id << " as master" << dendl;
	zonegroup->master_zone = master->second.id;
	int ret = zonegroup->update(dpp, y);
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      } else {
	ldpp_dout(dpp, 0) << "zonegroup " << zonegroup->get_name() << " missing zone for "
          "master_zone=" << zonegroup->master_zone << dendl;
	return -EINVAL;
      }
    }
    const auto& endpoints = master->second.endpoints;
    rest_master_conn = new RGWRESTConn(cct, zonegroup->get_id(), endpoints, zone_params->system_key, zonegroup->get_id(), zonegroup->api_name);
  }

  return 0;
}

const RGWZoneParams& RGWSI_Zone::get_zone_params() const
{
  return *zone_params;
}

const RGWZone& RGWSI_Zone::get_zone() const
{
  return *zone_public_config;
}

const RGWZoneGroup& RGWSI_Zone::get_zonegroup() const
{
  return *zonegroup;
}

int RGWSI_Zone::get_zonegroup(const string& id, RGWZoneGroup& zg) const
{
  int ret = 0;
  if (id == zonegroup->get_id()) {
    zg = *zonegroup;
  } else if (!current_period->get_id().empty()) {
    ret = current_period->get_zonegroup(zg, id);
  }
  return ret;
}

const RGWRealm& RGWSI_Zone::get_realm() const
{
  return *realm;
}

const RGWPeriod& RGWSI_Zone::get_current_period() const
{
  return *current_period;
}

const string& RGWSI_Zone::get_current_period_id() const
{
  return current_period->get_id();
}

bool RGWSI_Zone::has_zonegroup_api(const std::string& api) const
{
  if (!current_period->get_id().empty()) {
    const auto& zonegroups_by_api = current_period->get_map().zonegroups_by_api;
    if (zonegroups_by_api.find(api) != zonegroups_by_api.end())
      return true;
  } else if (zonegroup->api_name == api) {
    return true;
  }
  return false;
}

bool RGWSI_Zone::zone_is_writeable()
{
  return writeable_zone && !get_zone().is_read_only();
}

uint32_t RGWSI_Zone::get_zone_short_id() const
{
  return zone_short_id;
}

const string& RGWSI_Zone::zone_name() const
{
  return get_zone_params().get_name();
}

RGWZone* RGWSI_Zone::find_zone(const rgw_zone_id& id)
{
  auto iter = zone_by_id.find(id);
  if (iter == zone_by_id.end()) {
    return nullptr;
  }
  return &(iter->second);
}

RGWRESTConn *RGWSI_Zone::get_zone_conn(const rgw_zone_id& zone_id) {
  auto citer = zone_conn_map.find(zone_id.id);
  if (citer == zone_conn_map.end()) {
    return NULL;
  }

  return citer->second;
}

RGWRESTConn *RGWSI_Zone::get_zone_conn_by_name(const string& name) {
  auto i = zone_id_by_name.find(name);
  if (i == zone_id_by_name.end()) {
    return NULL;
  }

  return get_zone_conn(i->second);
}

bool RGWSI_Zone::find_zone_id_by_name(const string& name, rgw_zone_id *id) {
  auto i = zone_id_by_name.find(name);
  if (i == zone_id_by_name.end()) {
    return false;
  }
  *id = i->second; 
  return true;
}

bool RGWSI_Zone::need_to_sync() const
{
  return !(zonegroup->master_zone.empty() ||
	   !rest_master_conn ||
	   current_period->get_id().empty());
}

bool RGWSI_Zone::need_to_log_data() const
{
  return (zone_public_config->log_data && sync_module_exports_data());
}

bool RGWSI_Zone::is_meta_master() const
{
  if (!zonegroup->is_master_zonegroup()) {
    return false;
  }

  return (zonegroup->master_zone == zone_public_config->id);
}

bool RGWSI_Zone::need_to_log_metadata() const
{
  return is_meta_master() &&
    (zonegroup->zones.size() > 1 || current_period->is_multi_zonegroups_with_zones());
}

bool RGWSI_Zone::can_reshard() const
{
  if (current_period->get_id().empty()) {
    return true; // no realm
  }
  if (zonegroup->zones.size() == 1 && current_period->is_single_zonegroup()) {
    return true; // single zone/zonegroup
  }
  // 'resharding' feature enabled in zonegroup
  return zonegroup->supports(rgw::zone_features::resharding);
}

/**
  * Check to see if the bucket metadata could be synced
  * bucket: the bucket to check
  * Returns false is the bucket is not synced
  */
bool RGWSI_Zone::is_syncing_bucket_meta(const rgw_bucket& bucket)
{

  /* no current period  */
  if (current_period->get_id().empty()) {
    return false;
  }

  /* zonegroup is not master zonegroup */
  if (!zonegroup->is_master_zonegroup()) {
    return false;
  }

  /* single zonegroup and a single zone */
  if (current_period->is_single_zonegroup() && zonegroup->zones.size() == 1) {
    return false;
  }

  /* zone is not master */
  if (zonegroup->master_zone != zone_public_config->id) {
    return false;
  }

  return true;
}


int RGWSI_Zone::select_new_bucket_location(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const string& zonegroup_id,
					   const rgw_placement_rule& request_rule,
					   rgw_placement_rule *pselected_rule_name, RGWZonePlacementInfo *rule_info,
					   optional_yield y)
{
  /* first check that zonegroup exists within current period. */
  RGWZoneGroup zonegroup;
  int ret = get_zonegroup(zonegroup_id, zonegroup);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "could not find zonegroup " << zonegroup_id << " in current period" << dendl;
    return ret;
  }

  const rgw_placement_rule *used_rule;

  /* find placement rule. Hierarchy: request rule > user default rule > zonegroup default rule */
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;

  if (!request_rule.name.empty()) {
    used_rule = &request_rule;
    titer = zonegroup.placement_targets.find(request_rule.name);
    if (titer == zonegroup.placement_targets.end()) {
      ldpp_dout(dpp, 0) << "could not find requested placement id " << request_rule 
                    << " within zonegroup " << dendl;
      return -ERR_INVALID_LOCATION_CONSTRAINT;
    }
  } else if (!user_info.default_placement.name.empty()) {
    used_rule = &user_info.default_placement;
    titer = zonegroup.placement_targets.find(user_info.default_placement.name);
    if (titer == zonegroup.placement_targets.end()) {
      ldpp_dout(dpp, 0) << "could not find user default placement id " << user_info.default_placement
                    << " within zonegroup " << dendl;
      return -ERR_INVALID_LOCATION_CONSTRAINT;
    }
  } else {
    if (zonegroup.default_placement.name.empty()) { // zonegroup default rule as fallback, it should not be empty.
      ldpp_dout(dpp, 0) << "misconfiguration, zonegroup default placement id should not be empty." << dendl;
      return -ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION;
    } else {
      used_rule = &zonegroup.default_placement;
      titer = zonegroup.placement_targets.find(zonegroup.default_placement.name);
      if (titer == zonegroup.placement_targets.end()) {
        ldpp_dout(dpp, 0) << "could not find zonegroup default placement id " << zonegroup.default_placement
                      << " within zonegroup " << dendl;
        return -ERR_INVALID_LOCATION_CONSTRAINT;
      }
    }
  }

  /* now check tag for the rule, whether user is permitted to use rule */
  const auto& target_rule = titer->second;
  if (!target_rule.user_permitted(user_info.placement_tags)) {
    ldpp_dout(dpp, 0) << "user not permitted to use placement rule " << titer->first  << dendl;
    return -EPERM;
  }

  const string *storage_class = &request_rule.storage_class;

  if (storage_class->empty()) {
    storage_class = &used_rule->storage_class;
  }

  rgw_placement_rule rule(titer->first, *storage_class);

  if (pselected_rule_name) {
    *pselected_rule_name = rule;
  }

  return select_bucket_location_by_rule(dpp, rule, rule_info, y);
}

int RGWSI_Zone::select_bucket_location_by_rule(const DoutPrefixProvider *dpp, const rgw_placement_rule& location_rule, RGWZonePlacementInfo *rule_info, optional_yield y)
{
  if (location_rule.name.empty()) {
    return -EINVAL;
  }

  /*
   * make sure that zone has this rule configured. We're
   * checking it for the local zone, because that's where this bucket object is going to
   * reside.
   */
  auto piter = zone_params->placement_pools.find(location_rule.name);
  if (piter == zone_params->placement_pools.end()) {
    /* couldn't find, means we cannot really place data for this bucket in this zone */
    ldpp_dout(dpp, 0) << "ERROR: This zone does not contain placement rule "
                  << location_rule << " present in the zonegroup!" << dendl;
    return -EINVAL;
  }

  auto storage_class = location_rule.get_storage_class();
  if (!piter->second.storage_class_exists(storage_class)) {
    ldpp_dout(dpp, 5) << "requested storage class does not exist: " << storage_class << dendl;
    return -EINVAL;
  }


  RGWZonePlacementInfo& placement_info = piter->second;

  if (rule_info) {
    *rule_info = placement_info;
  }

  return 0;
}

int RGWSI_Zone::select_bucket_placement(const DoutPrefixProvider *dpp, const RGWUserInfo& user_info, const string& zonegroup_id,
                                        const rgw_placement_rule& placement_rule,
                                        rgw_placement_rule *pselected_rule, RGWZonePlacementInfo *rule_info,
					optional_yield y)
{
  if (zone_params->placement_pools.empty()) {
    return -EINVAL; // legacy placement no longer supported
  }

  return select_new_bucket_location(dpp, user_info, zonegroup_id, placement_rule,
                                    pselected_rule, rule_info, y);
}

bool RGWSI_Zone::get_redirect_zone_endpoint(string *endpoint)
{
  if (zone_public_config->redirect_zone.empty()) {
    return false;
  }

  auto iter = zone_conn_map.find(zone_public_config->redirect_zone);
  if (iter == zone_conn_map.end()) {
    ldout(cct, 0) << "ERROR: cannot find entry for redirect zone: " << zone_public_config->redirect_zone << dendl;
    return false;
  }

  RGWRESTConn *conn = iter->second;

  int ret = conn->get_url(*endpoint);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: redirect zone, conn->get_endpoint() returned ret=" << ret << dendl;
    return false;
  }

  return true;
}

