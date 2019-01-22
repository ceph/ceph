#include "svc_zone.h"
#include "svc_rados.h"
#include "svc_sys_obj.h"
#include "svc_sync_modules.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_rest_conn.h"

#include "common/errno.h"
#include "include/random.h"

#define dout_subsys ceph_subsys_rgw

using namespace rgw_zone_defaults;

RGWSI_Zone::RGWSI_Zone(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_Zone::init(RGWSI_SysObj *_sysobj_svc,
                      RGWSI_RADOS * _rados_svc,
                      RGWSI_SyncModules * _sync_modules_svc)
{
  sysobj_svc = _sysobj_svc;
  rados_svc = _rados_svc;
  sync_modules_svc = _sync_modules_svc;

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

bool RGWSI_Zone::zone_syncs_from(const RGWZone& target_zone, const RGWZone& source_zone) const
{
  return target_zone.syncs_from(source_zone.name) &&
         sync_modules_svc->get_manager()->supports_data_export(source_zone.tier_type);
}

int RGWSI_Zone::do_start()
{
  int ret = sysobj_svc->start();
  if (ret < 0) {
    return ret;
  }

  assert(sysobj_svc->is_started()); /* if not then there's ordering issue */

  ret = rados_svc->start();
  if (ret < 0) {
    return ret;
  }
  ret = sync_modules_svc->start();
  if (ret < 0) {
    return ret;
  }
  ret = realm->init(cct, sysobj_svc);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading realm info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldout(cct, 20) << "realm  " << realm->get_name() << " " << realm->get_id() << dendl;
    ret = current_period->init(cct, sysobj_svc, realm->get_id(), realm->get_name());
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "failed reading current period info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ldout(cct, 20) << "current period " << current_period->get_id() << dendl;  
  }

  ret = replace_region_with_zonegroup();
  if (ret < 0) {
    lderr(cct) << "failed converting region to zonegroup : ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = convert_regionmap();
  if (ret < 0) {
    lderr(cct) << "failed converting regionmap: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  bool zg_initialized = false;

  if (!current_period->get_id().empty()) {
    ret = init_zg_from_period(&zg_initialized);
    if (ret < 0) {
      return ret;
    }
  }

  bool creating_defaults = false;
  bool using_local = (!zg_initialized);
  if (using_local) {
    ldout(cct, 10) << " cannot find current period zonegroup using local zonegroup" << dendl;
    ret = init_zg_from_local(&creating_defaults);
    if (ret < 0) {
      return ret;
    }
    // read period_config into current_period
    auto& period_config = current_period->get_config();
    ret = period_config.read(sysobj_svc, zonegroup->realm_id);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: failed to read period config: "
          << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  ldout(cct, 10) << "Cannot find current period zone using local zone" << dendl;
  if (creating_defaults && cct->_conf->rgw_zone.empty()) {
    ldout(cct, 10) << " Using default name "<< default_zone_name << dendl;
    zone_params->set_name(default_zone_name);
  }

  ret = zone_params->init(cct, sysobj_svc);
  if (ret < 0 && ret != -ENOENT) {
    lderr(cct) << "failed reading zone info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  auto zone_iter = zonegroup->zones.find(zone_params->get_id());
  if (zone_iter == zonegroup->zones.end()) {
    if (using_local) {
      lderr(cct) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
      return -EINVAL;
    }
    ldout(cct, 1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << "), switching to local zonegroup configuration" << dendl;
    ret = init_zg_from_local(&creating_defaults);
    if (ret < 0) {
      return ret;
    }
    zone_iter = zonegroup->zones.find(zone_params->get_id());
  }
  if (zone_iter != zonegroup->zones.end()) {
    *zone_public_config = zone_iter->second;
    ldout(cct, 20) << "zone " << zone_params->get_name() << dendl;
  } else {
    lderr(cct) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
    return -EINVAL;
  }

  zone_short_id = current_period->get_map().get_zone_short_id(zone_params->get_id());

  RGWSyncModuleRef sm;
  if (!sync_modules_svc->get_manager()->get_module(zone_public_config->tier_type, &sm)) {
    lderr(cct) << "ERROR: tier type not found: " << zone_public_config->tier_type << dendl;
    return -EINVAL;
  }

  writeable_zone = sm->supports_writes();

  /* first build all zones index */
  for (auto ziter : zonegroup->zones) {
    const string& id = ziter.first;
    RGWZone& z = ziter.second;
    zone_id_by_name[z.name] = id;
    zone_by_id[id] = z;
  }

  if (zone_by_id.find(zone_id()) == zone_by_id.end()) {
    ldout(cct, 0) << "WARNING: could not find zone config in zonegroup for local zone (" << zone_id() << "), will use defaults" << dendl;
  }
  *zone_public_config = zone_by_id[zone_id()];
  for (auto ziter : zonegroup->zones) {
    const string& id = ziter.first;
    RGWZone& z = ziter.second;
    if (id == zone_id()) {
      continue;
    }
    if (z.endpoints.empty()) {
      ldout(cct, 0) << "WARNING: can't generate connection for zone " << z.id << " id " << z.name << ": no endpoints defined" << dendl;
      continue;
    }
    ldout(cct, 20) << "generating connection object for zone " << z.name << " id " << z.id << dendl;
    RGWRESTConn *conn = new RGWRESTConn(cct, this, z.id, z.endpoints);
    zone_conn_map[id] = conn;
    if (zone_syncs_from(*zone_public_config, z) ||
        zone_syncs_from(z, *zone_public_config)) {
      if (zone_syncs_from(*zone_public_config, z)) {
        zone_data_sync_from_map[id] = conn;
      }
      if (zone_syncs_from(z, *zone_public_config)) {
        zone_data_notify_to_map[id] = conn;
      }
    } else {
      ldout(cct, 20) << "NOTICE: not syncing to/from zone " << z.name << " id " << z.id << dendl;
    }
  }

  return 0;
}

void RGWSI_Zone::shutdown()
{
  delete rest_master_conn;

  map<string, RGWRESTConn *>::iterator iter;
  for (iter = zone_conn_map.begin(); iter != zone_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }

  for (iter = zonegroup_conn_map.begin(); iter != zonegroup_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }
}

int RGWSI_Zone::list_regions(list<string>& regions)
{
  RGWZoneGroup zonegroup;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zonegroup.get_pool(cct));

  return syspool.op().list_prefixed_objs(region_info_oid_prefix, &regions);
}

int RGWSI_Zone::list_zonegroups(list<string>& zonegroups)
{
  RGWZoneGroup zonegroup;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zonegroup.get_pool(cct));

  return syspool.op().list_prefixed_objs(zonegroup_names_oid_prefix, &zonegroups);
}

int RGWSI_Zone::list_zones(list<string>& zones)
{
  RGWZoneParams zoneparams;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(zoneparams.get_pool(cct));

  return syspool.op().list_prefixed_objs(zone_names_oid_prefix, &zones);
}

int RGWSI_Zone::list_realms(list<string>& realms)
{
  RGWRealm realm(cct, sysobj_svc);
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(realm.get_pool(cct));

  return syspool.op().list_prefixed_objs(realm_names_oid_prefix, &realms);
}

int RGWSI_Zone::list_periods(list<string>& periods)
{
  RGWPeriod period;
  list<string> raw_periods;
  RGWSI_SysObj::Pool syspool = sysobj_svc->get_pool(period.get_pool(cct));
  int ret = syspool.op().list_prefixed_objs(period.get_info_oid_prefix(), &raw_periods);
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


int RGWSI_Zone::list_periods(const string& current_period, list<string>& periods)
{
  int ret = 0;
  string period_id = current_period;
  while(!period_id.empty()) {
    RGWPeriod period(period_id);
    ret = period.init(cct, sysobj_svc);
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
int RGWSI_Zone::replace_region_with_zonegroup()
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

  int ret = sysobj.rop().read(&bl);
  if (ret < 0 && ret !=  -ENOENT) {
    ldout(cct, 0) << __func__ << " failed to read converted: ret "<< ret << " " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldout(cct, 20) << "System already converted " << dendl;
    return 0;
  }

  string default_region;
  ret = default_zonegroup.init(cct, sysobj_svc, false, true);
  if (ret < 0) {
    ldout(cct, 0) <<  __func__ << " failed init default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }    
  ret  = default_zonegroup.read_default_id(default_region, true);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) <<  __func__ << " failed reading old default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* convert regions to zonegroups */
  list<string> regions;
  ret = list_regions(regions);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) <<  __func__ << " failed to list regions: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT || regions.empty()) {
    RGWZoneParams zoneparams(default_zone_name);
    int ret = zoneparams.init(cct, sysobj_svc);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << __func__ << ": error initializing default zone params: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    /* update master zone */
    RGWZoneGroup default_zg(default_zonegroup_name);
    ret = default_zg.init(cct, sysobj_svc);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << __func__ << ": error in initializing default zonegroup: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    if (ret != -ENOENT && default_zg.master_zone.empty()) {
      default_zg.master_zone = zoneparams.get_id();
      return default_zg.update();
    }
    return 0;
  }

  string master_region, master_zone;
  for (list<string>::iterator iter = regions.begin(); iter != regions.end(); ++iter) {
    if (*iter != default_zonegroup_name){
      RGWZoneGroup region(*iter);
      int ret = region.init(cct, sysobj_svc, true, true);
      if (ret < 0) {
	  ldout(cct, 0) <<  __func__ << " failed init region "<< *iter << ": " << cpp_strerror(-ret) << dendl;
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
    string new_realm_name = master_region + "." + master_zone;
    unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    MD5 hash;
    hash.Update((const unsigned char *)new_realm_name.c_str(), new_realm_name.length());
    hash.Final(md5);
    buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
    string new_realm_id(md5_str);
    RGWRealm new_realm(new_realm_id,new_realm_name);
    ret = new_realm.init(cct, sysobj_svc, false);
    if (ret < 0) {
      ldout(cct, 0) <<  __func__ << " Error initing new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.create();
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 0) <<  __func__ << " Error creating new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.set_as_default();
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " Error setting realm as default: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = realm->init(cct, sysobj_svc);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " Error initing realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = current_period->init(cct, sysobj_svc, realm->get_id(), realm->get_name());
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " Error initing current period: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
  }

  list<string>::iterator iter;
  /* create zonegroups */
  for (iter = regions.begin(); iter != regions.end(); ++iter)
  {
    ldout(cct, 0) << __func__ << " Converting  " << *iter << dendl;
    /* check to see if we don't have already a zonegroup with this name */
    RGWZoneGroup new_zonegroup(*iter);
    ret = new_zonegroup.init(cct , sysobj_svc);
    if (ret == 0 && new_zonegroup.get_id() != *iter) {
      ldout(cct, 0) << __func__ << " zonegroup  "<< *iter << " already exists id " << new_zonegroup.get_id () <<
	" skipping conversion " << dendl;
      continue;
    }
    RGWZoneGroup zonegroup(*iter);
    zonegroup.set_id(*iter);
    int ret = zonegroup.init(cct, sysobj_svc, true, true);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " failed init zonegroup: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    zonegroup.realm_id = realm->get_id();
    /* fix default region master zone */
    if (*iter == default_zonegroup_name && zonegroup.master_zone.empty()) {
      ldout(cct, 0) << __func__ << " Setting default zone as master for default region" << dendl;
      zonegroup.master_zone = default_zone_name;
    }
    ret = zonegroup.update();
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 0) << __func__ << " failed to update zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    ret = zonegroup.update_name();
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 0) << __func__ << " failed to update_name for zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    if (zonegroup.get_name() == default_region) {
      ret = zonegroup.set_as_default();
      if (ret < 0) {
        ldout(cct, 0) << __func__ << " failed to set_as_default " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
          << dendl;
        return ret;
      }
    }
    for (map<string, RGWZone>::const_iterator iter = zonegroup.zones.begin(); iter != zonegroup.zones.end();
         ++iter) {
      ldout(cct, 0) << __func__ << " Converting zone" << iter->first << dendl;
      RGWZoneParams zoneparams(iter->first, iter->first);
      zoneparams.set_id(iter->first);
      zoneparams.realm_id = realm->get_id();
      ret = zoneparams.init(cct, sysobj_svc);
      if (ret < 0 && ret != -ENOENT) {
        ldout(cct, 0) << __func__ << " failed to init zoneparams  " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      } else if (ret == -ENOENT) {
        ldout(cct, 0) << __func__ << " zone is part of another cluster " << iter->first <<  " skipping " << dendl;
        continue;
      }
      zonegroup.realm_id = realm->get_id();
      ret = zoneparams.update();
      if (ret < 0 && ret != -EEXIST) {
        ldout(cct, 0) << __func__ << " failed to update zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      }
      ret = zoneparams.update_name();
      if (ret < 0 && ret != -EEXIST) {
        ldout(cct, 0) << __func__ << " failed to init zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }

    if (!current_period->get_id().empty()) {
      ret = current_period->add_zonegroup(zonegroup);
      if (ret < 0) {
        ldout(cct, 0) << __func__ << " failed to add zonegroup to current_period: " << cpp_strerror(-ret) << dendl;
        return ret;
      }
    }
  }

  if (!current_period->get_id().empty()) {
    ret = current_period->update();
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " failed to update new period: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = current_period->store_info(false);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " failed to store new period: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = current_period->reflect();
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " failed to update local objects: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  for (auto const& iter : regions) {
    RGWZoneGroup zonegroup(iter);
    int ret = zonegroup.init(cct, sysobj_svc, true, true);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " failed init zonegroup" << iter << ": ret "<< ret << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = zonegroup.delete_obj(true);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << __func__ << " failed to delete region " << iter << ": ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
  }

  /* mark as converted */
  ret = sysobj.wop()
              .set_exclusive(true)
              .write(bl);
  if (ret < 0 ) {
    ldout(cct, 0) << __func__ << " failed to mark cluster as converted: ret "<< ret << " " << cpp_strerror(-ret)
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

int RGWSI_Zone::init_zg_from_period(bool *initialized)
{
  *initialized = false;

  if (current_period->get_id().empty()) {
    return 0;
  }

  int ret = zonegroup->init(cct, sysobj_svc);
  ldout(cct, 20) << "period zonegroup init ret " << ret << dendl;
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    ldout(cct, 0) << "failed reading zonegroup info: " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ldout(cct, 20) << "period zonegroup name " << zonegroup->get_name() << dendl;

  map<string, RGWZoneGroup>::const_iterator iter =
    current_period->get_map().zonegroups.find(zonegroup->get_id());

  if (iter != current_period->get_map().zonegroups.end()) {
    ldout(cct, 20) << "using current period zonegroup " << zonegroup->get_name() << dendl;
    *zonegroup = iter->second;
    ret = zonegroup->init(cct, sysobj_svc, false);
    if (ret < 0) {
      ldout(cct, 0) << "failed init zonegroup: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = zone_params->init(cct, sysobj_svc);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    } if (ret ==-ENOENT && zonegroup->get_name() == default_zonegroup_name) {
      ldout(cct, 10) << " Using default name "<< default_zone_name << dendl;
      zone_params->set_name(default_zone_name);
      ret = zone_params->init(cct, sysobj_svc);
      if (ret < 0 && ret != -ENOENT) {
       ldout(cct, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
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
	ldout(cct, 0) << "zonegroup " << zg.get_name() << " missing master_zone, setting zone " <<
	  master->second.name << " id:" << master->second.id << " as master" << dendl;
	if (zonegroup->get_id() == zg.get_id()) {
	  zonegroup->master_zone = master->second.id;
	  ret = zonegroup->update();
	  if (ret < 0) {
	    ldout(cct, 0) << "error updating zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	} else {
	  RGWZoneGroup fixed_zg(zg.get_id(),zg.get_name());
	  ret = fixed_zg.init(cct, sysobj_svc);
	  if (ret < 0) {
	    ldout(cct, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	  fixed_zg.master_zone = master->second.id;
	  ret = fixed_zg.update();
	  if (ret < 0) {
	    ldout(cct, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	}
      } else {
	ldout(cct, 0) << "zonegroup " << zg.get_name() << " missing zone for master_zone=" <<
	  zg.master_zone << dendl;
	return -EINVAL;
      }
    }
    const auto& endpoints = master->second.endpoints;
    add_new_connection_to_map(zonegroup_conn_map, zg, new RGWRESTConn(cct, this, zg.get_id(), endpoints));
    if (!current_period->get_master_zonegroup().empty() &&
        zg.get_id() == current_period->get_master_zonegroup()) {
      rest_master_conn = new RGWRESTConn(cct, this, zg.get_id(), endpoints);
    }
  }

  *initialized = true;

  return 0;
}

int RGWSI_Zone::init_zg_from_local(bool *creating_defaults)
{
  int ret = zonegroup->init(cct, sysobj_svc);
  if ( (ret < 0 && ret != -ENOENT) || (ret == -ENOENT && !cct->_conf->rgw_zonegroup.empty())) {
    ldout(cct, 0) << "failed reading zonegroup info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT) {
    *creating_defaults = true;
    ldout(cct, 10) << "Creating default zonegroup " << dendl;
    ret = zonegroup->create_default();
    if (ret < 0) {
      ldout(cct, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    ret = zonegroup->init(cct, sysobj_svc);
    if (ret < 0) {
      ldout(cct, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
  }
  ldout(cct, 20) << "zonegroup " << zonegroup->get_name() << dendl;
  if (zonegroup->is_master_zonegroup()) {
    // use endpoints from the zonegroup's master zone
    auto master = zonegroup->zones.find(zonegroup->master_zone);
    if (master == zonegroup->zones.end()) {
      // fix missing master zone for a single zone zonegroup
      if (zonegroup->master_zone.empty() && zonegroup->zones.size() == 1) {
	master = zonegroup->zones.begin();
	ldout(cct, 0) << "zonegroup " << zonegroup->get_name() << " missing master_zone, setting zone " <<
	  master->second.name << " id:" << master->second.id << " as master" << dendl;
	zonegroup->master_zone = master->second.id;
	ret = zonegroup->update();
	if (ret < 0) {
	  ldout(cct, 0) << "error initializing zonegroup : " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      } else {
	ldout(cct, 0) << "zonegroup " << zonegroup->get_name() << " missing zone for "
          "master_zone=" << zonegroup->master_zone << dendl;
	return -EINVAL;
      }
    }
    const auto& endpoints = master->second.endpoints;
    rest_master_conn = new RGWRESTConn(cct, this, zonegroup->get_id(), endpoints);
  }

  return 0;
}

int RGWSI_Zone::convert_regionmap()
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

  int ret = sysobj.rop().read(&bl);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  } else if (ret == -ENOENT) {
    return 0;
  }

  try {
    auto iter = bl.cbegin();
    decode(zonegroupmap, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "error decoding regionmap from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  
  for (map<string, RGWZoneGroup>::iterator iter = zonegroupmap.zonegroups.begin();
       iter != zonegroupmap.zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    ret = zonegroup.init(cct, sysobj_svc, false);
    ret = zonegroup.update();
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "Error could not update zonegroup " << zonegroup.get_name() << ": " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    } else if (ret == -ENOENT) {
      ret = zonegroup.create();
      if (ret < 0) {
	ldout(cct, 0) << "Error could not create " << zonegroup.get_name() << ": " <<
	  cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
  }

  current_period->set_user_quota(zonegroupmap.user_quota);
  current_period->set_bucket_quota(zonegroupmap.bucket_quota);

  // remove the region_map so we don't try to convert again
  ret = sysobj.wop().remove();
  if (ret < 0) {
    ldout(cct, 0) << "Error could not remove " << sysobj.get_obj()
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

const string& RGWSI_Zone::get_current_period_id()
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

const string& RGWSI_Zone::zone_name()
{
  return get_zone_params().get_name();
}
const string& RGWSI_Zone::zone_id()
{
  return get_zone_params().get_id();
}

bool RGWSI_Zone::find_zone_by_id(const string& id, RGWZone **zone)
{
  auto iter = zone_by_id.find(id);
  if (iter == zone_by_id.end()) {
    return false;
  }
  *zone = &(iter->second);
  return true;
}

RGWRESTConn *RGWSI_Zone::get_zone_conn_by_id(const string& id) {
  auto citer = zone_conn_map.find(id);
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

  return get_zone_conn_by_id(i->second);
}

bool RGWSI_Zone::find_zone_id_by_name(const string& name, string *id) {
  auto i = zone_id_by_name.find(name);
  if (i == zone_id_by_name.end()) {
    return false;
  }
  *id = i->second; 
  return true;
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
  if (zonegroup->master_zone.compare(zone_public_config->id) != 0) {
    return false;
  }

  return true;
}


int RGWSI_Zone::select_new_bucket_location(const RGWUserInfo& user_info, const string& zonegroup_id,
                                         const rgw_placement_rule& request_rule,
                                         rgw_placement_rule *pselected_rule_name, RGWZonePlacementInfo *rule_info)
{
  /* first check that zonegroup exists within current period. */
  RGWZoneGroup zonegroup;
  int ret = get_zonegroup(zonegroup_id, zonegroup);
  if (ret < 0) {
    ldout(cct, 0) << "could not find zonegroup " << zonegroup_id << " in current period" << dendl;
    return ret;
  }

  const rgw_placement_rule *used_rule;

  /* find placement rule. Hierarchy: request rule > user default rule > zonegroup default rule */
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;

  if (!request_rule.name.empty()) {
    used_rule = &request_rule;
    titer = zonegroup.placement_targets.find(request_rule.name);
    if (titer == zonegroup.placement_targets.end()) {
      ldout(cct, 0) << "could not find requested placement id " << request_rule 
                    << " within zonegroup " << dendl;
      return -ERR_INVALID_LOCATION_CONSTRAINT;
    }
  } else if (!user_info.default_placement.name.empty()) {
    used_rule = &user_info.default_placement;
    titer = zonegroup.placement_targets.find(user_info.default_placement.name);
    if (titer == zonegroup.placement_targets.end()) {
      ldout(cct, 0) << "could not find user default placement id " << user_info.default_placement
                    << " within zonegroup " << dendl;
      return -ERR_INVALID_LOCATION_CONSTRAINT;
    }
  } else {
    if (zonegroup.default_placement.name.empty()) { // zonegroup default rule as fallback, it should not be empty.
      ldout(cct, 0) << "misconfiguration, zonegroup default placement id should not be empty." << dendl;
      return -ERR_ZONEGROUP_DEFAULT_PLACEMENT_MISCONFIGURATION;
    } else {
      used_rule = &zonegroup.default_placement;
      titer = zonegroup.placement_targets.find(zonegroup.default_placement.name);
      if (titer == zonegroup.placement_targets.end()) {
        ldout(cct, 0) << "could not find zonegroup default placement id " << zonegroup.default_placement
                      << " within zonegroup " << dendl;
        return -ERR_INVALID_LOCATION_CONSTRAINT;
      }
    }
  }

  /* now check tag for the rule, whether user is permitted to use rule */
  const auto& target_rule = titer->second;
  if (!target_rule.user_permitted(user_info.placement_tags)) {
    ldout(cct, 0) << "user not permitted to use placement rule " << titer->first  << dendl;
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

  return select_bucket_location_by_rule(rule, rule_info);
}

int RGWSI_Zone::select_bucket_location_by_rule(const rgw_placement_rule& location_rule, RGWZonePlacementInfo *rule_info)
{
  if (location_rule.name.empty()) {
    /* we can only reach here if we're trying to set a bucket location from a bucket
     * created on a different zone, using a legacy / default pool configuration
     */
    if (rule_info) {
      return select_legacy_bucket_placement(rule_info);
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
    ldout(cct, 0) << "ERROR: This zone does not contain placement rule "
                  << location_rule << " present in the zonegroup!" << dendl;
    return -EINVAL;
  }

  auto storage_class = location_rule.get_storage_class();
  if (!piter->second.storage_class_exists(storage_class)) {
    ldout(cct, 5) << "requested storage class does not exist: " << storage_class << dendl;
    return -EINVAL;
  }


  RGWZonePlacementInfo& placement_info = piter->second;

  if (rule_info) {
    *rule_info = placement_info;
  }

  return 0;
}

int RGWSI_Zone::select_bucket_placement(const RGWUserInfo& user_info, const string& zonegroup_id,
                                        const rgw_placement_rule& placement_rule,
                                        rgw_placement_rule *pselected_rule, RGWZonePlacementInfo *rule_info)
{
  if (!zone_params->placement_pools.empty()) {
    return select_new_bucket_location(user_info, zonegroup_id, placement_rule,
                                      pselected_rule, rule_info);
  }

  if (pselected_rule) {
    pselected_rule->clear();
  }

  if (rule_info) {
    return select_legacy_bucket_placement(rule_info);
  }

  return 0;
}

int RGWSI_Zone::select_legacy_bucket_placement(RGWZonePlacementInfo *rule_info)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string pool_name;
  bool write_map = false;

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.rop().read(&map_bl);
  if (ret < 0) {
    goto read_omap;
  }

  try {
    auto iter = map_bl.cbegin();
    decode(m, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

read_omap:
  if (m.empty()) {
    ret = sysobj.omap().get_all(&m);

    write_map = true;
  }

  if (ret < 0 || m.empty()) {
    vector<rgw_pool> pools;
    string s = string("default.") + default_storage_pool_suffix;
    pools.push_back(rgw_pool(s));
    vector<int> retcodes;
    bufferlist bl;
    ret = rados_svc->pool().create(pools, &retcodes);
    if (ret < 0)
      return ret;
    ret = sysobj.omap().set(s, bl);
    if (ret < 0)
      return ret;
    m[s] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    encode(m, new_bl);
    ret = sysobj.wop().write(new_bl);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
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
  rule_info->index_type = RGWBIType_Normal;

  return 0;
}

int RGWSI_Zone::update_placement_map()
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_raw_obj obj(zone_params->domain_root, avail_pools);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.omap().get_all(&m);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  encode(m, new_bl);
  ret = sysobj.wop().write(new_bl);
  if (ret < 0) {
    ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWSI_Zone::add_bucket_placement(const rgw_pool& new_pool)
{
  int ret = rados_svc->pool(new_pool).lookup();
  if (ret < 0) { // DNE, or something
    return ret;
  }

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  bufferlist empty_bl;
  ret = sysobj.omap().set(new_pool.to_str(), empty_bl);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWSI_Zone::remove_bucket_placement(const rgw_pool& old_pool)
{
  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);

  int ret = sysobj.omap().del(old_pool.to_str());

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWSI_Zone::list_placement_set(set<rgw_pool>& names)
{
  bufferlist header;
  map<string, bufferlist> m;

  rgw_raw_obj obj(zone_params->domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  int ret = sysobj.omap().get_all(&m);
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

