// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_zone.h"
#include "svc_rados.h"
#include "svc_sys_obj.h"
#include "svc_sync_modules.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_rest_conn.h"
#include "rgw/rgw_bucket_sync.h"

#include "common/errno.h"
#include "include/random.h"

#define dout_subsys ceph_subsys_rgw

using namespace rgw_zone_defaults;

RGWSI_Zone::RGWSI_Zone(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_Zone::init(RGWSI_SysObj *_sysobj_svc,
                      RGWSI_RADOS * _rados_svc,
                      RGWSI_SyncModules * _sync_modules_svc,
		      RGWSI_Bucket_Sync *_bucket_sync_svc)
{
  sysobj_svc = _sysobj_svc;
  rados_svc = _rados_svc;
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

int RGWSI_Zone::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret = sysobj_svc->start(y, dpp);
  if (ret < 0) {
    return ret;
  }

  assert(sysobj_svc->is_started()); /* if not then there's ordering issue */

  ret = rados_svc->start(y, dpp);
  if (ret < 0) {
    return ret;
  }

  ret = realm->init(dpp, cct, sysobj_svc, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading realm info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldpp_dout(dpp, 20) << "realm  " << realm->get_name() << " " << realm->get_id() << dendl;
    ret = current_period->init(dpp, cct, sysobj_svc, realm->get_id(), y,
			       realm->get_name());
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "failed reading current period info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << "current period " << current_period->get_id() << dendl;  
  }

  ret = replace_region_with_zonegroup(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "failed converting region to zonegroup : ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = convert_regionmap(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "failed converting regionmap: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  bool zg_initialized = false;

  if (!current_period->get_id().empty()) {
    ret = init_zg_from_period(dpp, &zg_initialized, y);
    if (ret < 0) {
      return ret;
    }
  }

  bool creating_defaults = false;
  bool using_local = (!zg_initialized);
  if (using_local) {
    ldpp_dout(dpp, 10) << " cannot find current period zonegroup using local zonegroup" << dendl;
    ret = init_zg_from_local(dpp, &creating_defaults, y);
    if (ret < 0) {
      return ret;
    }
    // read period_config into current_period
    auto& period_config = current_period->get_config();
    ret = period_config.read(dpp, sysobj_svc, zonegroup->realm_id, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: failed to read period config: "
          << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  ldpp_dout(dpp, 10) << "Cannot find current period zone using local zone" << dendl;
  if (creating_defaults && cct->_conf->rgw_zone.empty()) {
    ldpp_dout(dpp, 10) << " Using default name "<< default_zone_name << dendl;
    zone_params->set_name(default_zone_name);
  }

  ret = zone_params->init(dpp, cct, sysobj_svc, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, -1) << "failed reading zone info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  cur_zone_id = rgw_zone_id(zone_params->get_id());

  auto zone_iter = zonegroup->zones.find(zone_params->get_id());
  if (zone_iter == zonegroup->zones.end()) {
    if (using_local) {
      ldpp_dout(dpp, -1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << "), switching to local zonegroup configuration" << dendl;
    ret = init_zg_from_local(dpp, &creating_defaults, y);
    if (ret < 0) {
      return ret;
    }
    zone_iter = zonegroup->zones.find(zone_params->get_id());
  }
  if (zone_iter != zonegroup->zones.end()) {
    *zone_public_config = zone_iter->second;
    ldpp_dout(dpp, 20) << "zone " << zone_params->get_name() << " found"  << dendl;
  } else {
    ldpp_dout(dpp, -1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
    return -EINVAL;
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
    RGWRESTConn *conn = new RGWRESTConn(cct, this, z.id, z.endpoints, zonegroup->api_name);
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
 * Replace all region configuration with zonegroup for
 * backward compatability
 * Returns 0 on success, -ERR# on failure.
 */
int RGWSI_Zone::replace_region_with_zonegroup(const DoutPrefixProvider *dpp, optional_yield y)
{
  /* copy default region */
  /* convert default region to default zonegroup */
  string default_oid = cct->_conf->rgw_default_region_info_oid;
  if (default_oid.empty()) {
    default_oid = default_region_info_oid;
  }

  RGWZoneGroup default_zonegroup;
  rgw_pool pool{default_zonegroup.get_pool(cct)};
  string oid  = "converted";
  bufferlist bl;

  RGWSysObjectCtx obj_ctx = sysobj_svc->init_obj_ctx();
  RGWSysObj sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));

  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0 && ret !=  -ENOENT) {
    ldpp_dout(dpp, 0) << __func__ << " failed to read converted: ret "<< ret << " " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldpp_dout(dpp, 20) << "System already converted " << dendl;
    return 0;
  }

  string default_region;
  ret = default_zonegroup.init(dpp, cct, sysobj_svc, y, false, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) <<  __func__ << " failed init default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret  = default_zonegroup.read_default_id(dpp, default_region, y, true);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) <<  __func__ << " failed reading old default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* convert regions to zonegroups */
  list<string> regions;
  ret = list_regions(dpp, regions);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) <<  __func__ << " failed to list regions: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT || regions.empty()) {
    RGWZoneParams zoneparams(default_zone_name);
    int ret = zoneparams.init(dpp, cct, sysobj_svc, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << __func__ << ": error initializing default zone params: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    /* update master zone */
    RGWZoneGroup default_zg(default_zonegroup_name);
    ret = default_zg.init(dpp, cct, sysobj_svc, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << __func__ << ": error in initializing default zonegroup: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    if (ret != -ENOENT && default_zg.master_zone.empty()) {
      default_zg.master_zone = zoneparams.get_id();
      return default_zg.update(dpp, y);
    }
    return 0;
  }

  string master_region;
  rgw_zone_id master_zone;
  for (list<string>::iterator iter = regions.begin(); iter != regions.end(); ++iter) {
    if (*iter != default_zonegroup_name){
      RGWZoneGroup region(*iter);
      int ret = region.init(dpp, cct, sysobj_svc, y, true, true);
      if (ret < 0) {
	  ldpp_dout(dpp, 0) <<  __func__ << " failed init region "<< *iter << ": " << cpp_strerror(-ret) << dendl;
	  return ret;
      }
      if (region.is_master_zonegroup()) {
	master_region = region.get_id();
	master_zone = region.master_zone;
      }
    }
  }

  /* create realm if there is none.
     The realm name will be the region and zone concatenated
     realm id will be mds of its name */
  if (realm->get_id().empty() && !master_region.empty() && !master_zone.empty()) {
    string new_realm_name = master_region + "." + master_zone.id;
    unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    MD5 hash;
    hash.Update((const unsigned char *)new_realm_name.c_str(), new_realm_name.length());
    hash.Final(md5);
    buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
    string new_realm_id(md5_str);
    RGWRealm new_realm(new_realm_id,new_realm_name);
    ret = new_realm.init(dpp, cct, sysobj_svc, y, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<  __func__ << " Error initing new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.create(dpp, y);
    if (ret < 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 0) <<  __func__ << " Error creating new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.set_as_default(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " Error setting realm as default: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = realm->init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " Error initing realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = current_period->init(dpp, cct, sysobj_svc, realm->get_id(), y,
			       realm->get_name());
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " Error initing current period: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
  }

  list<string>::iterator iter;
  /* create zonegroups */
  for (iter = regions.begin(); iter != regions.end(); ++iter)
  {
    ldpp_dout(dpp, 0) << __func__ << " Converting  " << *iter << dendl;
    /* check to see if we don't have already a zonegroup with this name */
    RGWZoneGroup new_zonegroup(*iter);
    ret = new_zonegroup.init(dpp, cct , sysobj_svc, y);
    if (ret == 0 && new_zonegroup.get_id() != *iter) {
      ldpp_dout(dpp, 0) << __func__ << " zonegroup  "<< *iter << " already exists id " << new_zonegroup.get_id () <<
	" skipping conversion " << dendl;
      continue;
    }
    RGWZoneGroup zonegroup(*iter);
    zonegroup.set_id(*iter);
    int ret = zonegroup.init(dpp, cct, sysobj_svc, y, true, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed init zonegroup: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    zonegroup.realm_id = realm->get_id();
    /* fix default region master zone */
    if (*iter == default_zonegroup_name && zonegroup.master_zone.empty()) {
      ldpp_dout(dpp, 0) << __func__ << " Setting default zone as master for default region" << dendl;
      zonegroup.master_zone = default_zone_name;
    }
    ret = zonegroup.update(dpp, y);
    if (ret < 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << " failed to update zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    ret = zonegroup.update_name(dpp, y);
    if (ret < 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 0) << __func__ << " failed to update_name for zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    if (zonegroup.get_name() == default_region) {
      ret = zonegroup.set_as_default(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << __func__ << " failed to set_as_default " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
          << dendl;
        return ret;
      }
    }
    for (auto iter = zonegroup.zones.begin(); iter != zonegroup.zones.end();
         ++iter) {
      ldpp_dout(dpp, 0) << __func__ << " Converting zone" << iter->first << dendl;
      RGWZoneParams zoneparams(iter->first, iter->second.name);
      zoneparams.set_id(iter->first.id);
      zoneparams.realm_id = realm->get_id();
      ret = zoneparams.init(dpp, cct, sysobj_svc, y);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 0) << __func__ << " failed to init zoneparams  " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      } else if (ret == -ENOENT) {
        ldpp_dout(dpp, 0) << __func__ << " zone is part of another cluster " << iter->first <<  " skipping " << dendl;
        continue;
      }
      zonegroup.realm_id = realm->get_id();
      ret = zoneparams.update(dpp, y);
      if (ret < 0 && ret != -EEXIST) {
        ldpp_dout(dpp, 0) << __func__ << " failed to update zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      }
      ret = zoneparams.update_name(dpp, y);
      if (ret < 0 && ret != -EEXIST) {
        ldpp_dout(dpp, 0) << __func__ << " failed to init zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }

    if (!current_period->get_id().empty()) {
      ret = current_period->add_zonegroup(dpp, zonegroup, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << __func__ << " failed to add zonegroup to current_period: " << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }
  }

  if (!current_period->get_id().empty()) {
    ret = current_period->update(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed to update new period: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = current_period->store_info(dpp, false, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed to store new period: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = current_period->reflect(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed to update local objects: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  for (auto const& iter : regions) {
    RGWZoneGroup zonegroup(iter);
    int ret = zonegroup.init(dpp, cct, sysobj_svc, y, true, true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed init zonegroup" << iter << ": ret "<< ret << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = zonegroup.delete_obj(dpp, y, true);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << __func__ << " failed to delete region " << iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
  }

  /* mark as converted */
  ret = sysobj.wop()
              .set_exclusive(true)
              .write(dpp, bl, y);
  if (ret < 0 ) {
    ldpp_dout(dpp, 0) << __func__ << " failed to mark cluster as converted: ret "<< ret << " " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  }

  return 0;
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

int RGWSI_Zone::init_zg_from_period(const DoutPrefixProvider *dpp, bool *initialized, optional_yield y)
{
  *initialized = false;

  if (current_period->get_id().empty()) {
    return 0;
  }

  int ret = zonegroup->init(dpp, cct, sysobj_svc, y);
  ldpp_dout(dpp, 20) << "period zonegroup init ret " << ret << dendl;
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading zonegroup info: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "period zonegroup name " << zonegroup->get_name() << dendl;

  map<string, RGWZoneGroup>::const_iterator iter =
    current_period->get_map().zonegroups.find(zonegroup->get_id());

  if (iter != current_period->get_map().zonegroups.end()) {
    ldpp_dout(dpp, 20) << "using current period zonegroup " << zonegroup->get_name() << dendl;
    *zonegroup = iter->second;
    ret = zonegroup->init(dpp, cct, sysobj_svc, y, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed init zonegroup: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = zone_params->init(dpp, cct, sysobj_svc, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    } if (ret ==-ENOENT && zonegroup->get_name() == default_zonegroup_name) {
      ldpp_dout(dpp, 10) << " Using default name "<< default_zone_name << dendl;
      zone_params->set_name(default_zone_name);
      ret = zone_params->init(dpp, cct, sysobj_svc, y);
      if (ret < 0 && ret != -ENOENT) {
       ldpp_dout(dpp, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
       return ret;
      }
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
	  ret = zonegroup->update(dpp, y);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) << "error updating zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	} else {
	  RGWZoneGroup fixed_zg(zg.get_id(),zg.get_name());
	  ret = fixed_zg.init(dpp, cct, sysobj_svc, y);
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
    add_new_connection_to_map(zonegroup_conn_map, zg, new RGWRESTConn(cct, this, zg.get_id(), endpoints, zg.api_name));
    if (!current_period->get_master_zonegroup().empty() &&
        zg.get_id() == current_period->get_master_zonegroup()) {
      rest_master_conn = new RGWRESTConn(cct, this, zg.get_id(), endpoints, zg.api_name);
    }
  }

  *initialized = true;

  return 0;
}

int RGWSI_Zone::init_zg_from_local(const DoutPrefixProvider *dpp, bool *creating_defaults, optional_yield y)
{
  int ret = zonegroup->init(dpp, cct, sysobj_svc, y);
  if ( (ret < 0 && ret != -ENOENT) || (ret == -ENOENT && !cct->_conf->rgw_zonegroup.empty())) {
    ldpp_dout(dpp, 0) << "failed reading zonegroup info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT) {
    *creating_defaults = true;
    ldpp_dout(dpp, 10) << "Creating default zonegroup " << dendl;
    ret = zonegroup->create_default(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    ret = zonegroup->init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
  }
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
	ret = zonegroup->update(dpp, y);
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
    rest_master_conn = new RGWRESTConn(cct, this, zonegroup->get_id(), endpoints, zonegroup->api_name);
  }

  return 0;
}

int RGWSI_Zone::convert_regionmap(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWZoneGroupMap zonegroupmap;

  string pool_name = cct->_conf->rgw_zone_root_pool;
  if (pool_name.empty()) {
    pool_name = RGW_DEFAULT_ZONE_ROOT_POOL;
  }
  string oid = region_map_oid; 

  rgw_pool pool(pool_name);
  bufferlist bl;

  RGWSysObjectCtx obj_ctx = sysobj_svc->init_obj_ctx();
  RGWSysObj sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));

  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  } else if (ret == -ENOENT) {
    return 0;
  }

  try {
    auto iter = bl.cbegin();
    decode(zonegroupmap, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding regionmap from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  
  for (map<string, RGWZoneGroup>::iterator iter = zonegroupmap.zonegroups.begin();
       iter != zonegroupmap.zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    ret = zonegroup.init(dpp, cct, sysobj_svc, y, false);
    ret = zonegroup.update(dpp, y);
    if (ret < 0 && ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "Error could not update zonegroup " << zonegroup.get_name() << ": " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    } else if (ret == -ENOENT) {
      ret = zonegroup.create(dpp, y);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "Error could not create " << zonegroup.get_name() << ": " <<
	  cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
  }

  current_period->set_user_quota(zonegroupmap.user_quota);
  current_period->set_bucket_quota(zonegroupmap.bucket_quota);

  // remove the region_map so we don't try to convert again
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error could not remove " << sysobj.get_obj()
        << " after upgrading to zonegroup map: " << cpp_strerror(ret) << dendl;
    return ret;
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

bool RGWSI_Zone::find_zone(const rgw_zone_id& id, RGWZone **zone)
{
  auto iter = zone_by_id.find(id);
  if (iter == zone_by_id.end()) {
    return false;
  }
  *zone = &(iter->second);
  return true;
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
  return zone_public_config->log_data;
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
  return current_period->get_id().empty() ||
    (zonegroup->zones.size() == 1 && current_period->is_single_zonegroup());
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
    /* we can only reach here if we're trying to set a bucket location from a bucket
     * created on a different zone, using a legacy / default pool configuration
     */
    if (rule_info) {
      return select_legacy_bucket_placement(dpp, rule_info, y);
    }

    return 0;
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
  if (!zone_params->placement_pools.empty()) {
    return select_new_bucket_location(dpp, user_info, zonegroup_id, placement_rule,
                                      pselected_rule, rule_info, y);
  }

  if (pselected_rule) {
    pselected_rule->clear();
  }

  if (rule_info) {
    return select_legacy_bucket_placement(dpp, rule_info, y);
  }

  return 0;
}

int RGWSI_Zone::select_legacy_bucket_placement(const DoutPrefixProvider *dpp, RGWZonePlacementInfo *rule_info,
					       optional_yield y)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string pool_name;
  bool write_map = false;

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.rop().read(dpp, &map_bl, y);
  if (ret < 0) {
    goto read_omap;
  }

  try {
    auto iter = map_bl.cbegin();
    decode(m, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

read_omap:
  if (m.empty()) {
    ret = sysobj.omap().get_all(dpp, &m, y);

    write_map = true;
  }

  if (ret < 0 || m.empty()) {
    vector<rgw_pool> pools;
    string s = string("default.") + default_storage_pool_suffix;
    pools.push_back(rgw_pool(s));
    vector<int> retcodes;
    bufferlist bl;
    ret = rados_svc->pool().create(dpp, pools, &retcodes);
    if (ret < 0)
      return ret;
    ret = sysobj.omap().set(dpp, s, bl, y);
    if (ret < 0)
      return ret;
    m[s] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    encode(m, new_bl);
    ret = sysobj.wop().write(dpp, new_bl, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
    }
  }

  auto miter = m.begin();
  if (m.size() > 1) {
    // choose a pool at random
    auto r = ceph::util::generate_random_number<size_t>(0, m.size() - 1);
    std::advance(miter, r);
  }
  pool_name = miter->first;

  rgw_pool pool = pool_name;

  rule_info->storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, &pool, nullptr);
  rule_info->data_extra_pool = pool_name;
  rule_info->index_pool = pool_name;
  rule_info->index_type = rgw::BucketIndexType::Normal;

  return 0;
}

int RGWSI_Zone::update_placement_map(const DoutPrefixProvider *dpp, optional_yield y)
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_raw_obj obj(zone_params->domain_root, avail_pools);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.omap().get_all(dpp, &m, y);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  encode(m, new_bl);
  ret = sysobj.wop().write(dpp, new_bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWSI_Zone::add_bucket_placement(const DoutPrefixProvider *dpp, const rgw_pool& new_pool, optional_yield y)
{
  int ret = rados_svc->pool(new_pool).lookup();
  if (ret < 0) { // DNE, or something
    return ret;
  }

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  bufferlist empty_bl;
  ret = sysobj.omap().set(dpp, new_pool.to_str(), empty_bl, y);

  // don't care about return value
  update_placement_map(dpp, y);

  return ret;
}

int RGWSI_Zone::remove_bucket_placement(const DoutPrefixProvider *dpp, const rgw_pool& old_pool, optional_yield y)
{
  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.omap().del(dpp, old_pool.to_str(), y);

  // don't care about return value
  update_placement_map(dpp, y);

  return ret;
}

int RGWSI_Zone::list_placement_set(const DoutPrefixProvider *dpp, set<rgw_pool>& names, optional_yield y)
{
  bufferlist header;
  map<string, bufferlist> m;

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  int ret = sysobj.omap().get_all(dpp, &m, y);
  if (ret < 0)
    return ret;

  names.clear();
  map<string, bufferlist>::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    names.insert(rgw_pool(miter->first));
  }

  return names.size();
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

