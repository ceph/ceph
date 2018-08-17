#include "svc_zone.h"
#include "svc_rados.h"
#include "svc_sys_obj.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_rest_conn.h"

#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

static string zone_names_oid_prefix = "zone_names.";
static string region_info_oid_prefix = "region_info.";
static string realm_names_oid_prefix = "realms_names.";
static string default_region_info_oid = "default.region";
static string region_map_oid = "region_map";
const string default_zonegroup_name = "default";
const string default_zone_name = "default";
static string zonegroup_names_oid_prefix = "zonegroups_names.";
static string RGW_DEFAULT_ZONE_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_ZONEGROUP_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_REALM_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_PERIOD_ROOT_POOL = "rgw.root";

int RGWS_Zone::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_Zone(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_Zone::get_deps()
{
  RGWServiceInstance::dependency dep1 = { .name = "rados",
                                          .conf = "{}" };
  RGWServiceInstance::dependency dep2 = { .name = "sys_obj",
                                          .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["rados_dep"] = dep1;
  deps["sys_obj_dep"] = dep2;
  return deps;
}

int RGWSI_Zone::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  rados_svc = static_pointer_cast<RGWSI_RADOS>(dep_refs["rados_dep"]);
  assert(rados_svc);

  sysobj_svc = static_pointer_cast<RGWSI_SysObj>(dep_refs["sys_obj_dep"]);
  assert(sysobj_svc);

  realm = make_shared<RGWRealm>();
  zonegroup = make_shared<RGWZoneGroup>();
  zone_public_config = make_shared<RGWZone>();
  zone_params = make_shared<RGWZoneParams>();
  current_period = make_shared<RGWPeriod>();

  return 0;
}

int RGWSI_Zone::init()
{
  int ret = realm->init(cct, sysobj_svc.get());
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading realm info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldout(cct, 20) << "realm  " << realm->get_name() << " " << realm->get_id() << dendl;
    ret = current_period->init(cct, sysobj_svc.get(), realm->get_id(), realm->get_name());
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
    ret = period_config.read(sysobj_svc.get(), zonegroup->realm_id);
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

  ret = zone_params->init(cct, sysobj_svc.get());
  if (ret < 0 && ret != -ENOENT) {
    lderr(cct) << "failed reading zone info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  map<string, RGWZone>::iterator zone_iter = get_zonegroup().zones.find(zone_params->get_id());
  if (zone_iter == get_zonegroup().zones.end()) {
    if (using_local) {
      lderr(cct) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
      return -EINVAL;
    }
    ldout(cct, 1) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << "), switching to local zonegroup configuration" << dendl;
    ret = init_zg_from_local(&creating_defaults);
    if (ret < 0) {
      return ret;
    }
    zone_iter = get_zonegroup().zones.find(zone_params->get_id());
  }
  if (zone_iter != get_zonegroup().zones.end()) {
    *zone_public_config = zone_iter->second;
    ldout(cct, 20) << "zone " << zone_params->get_name() << dendl;
  } else {
    lderr(cct) << "Cannot find zone id=" << zone_params->get_id() << " (name=" << zone_params->get_name() << ")" << dendl;
    return -EINVAL;
  }

  zone_short_id = current_period->get_map().get_zone_short_id(zone_params->get_id());

  writeable_zone = (zone_public_config->tier_type.empty() || zone_public_config->tier_type == "rgw");

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

  return list_raw_prefixed_objs(zonegroup.get_pool(cct), region_info_oid_prefix, regions);
}

int RGWSI_Zone::list_zonegroups(list<string>& zonegroups)
{
  RGWZoneGroup zonegroup;

  return list_raw_prefixed_objs(zonegroup.get_pool(cct), zonegroup_names_oid_prefix, zonegroups);
}

int RGWSI_Zone::list_zones(list<string>& zones)
{
  RGWZoneParams zoneparams;

  return list_raw_prefixed_objs(zoneparams.get_pool(cct), zone_names_oid_prefix, zones);
}

int RGWSI_Zone::list_realms(list<string>& realms)
{
  RGWRealm realm(cct, sysobj_svc.get());
  return list_raw_prefixed_objs(realm.get_pool(cct), realm_names_oid_prefix, realms);
}

int RGWSI_Zone::list_periods(list<string>& periods)
{
  RGWPeriod period;
  list<string> raw_periods;
  int ret = list_raw_prefixed_objs(period.get_pool(cct), period.get_info_oid_prefix(), raw_periods);
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
    ret = period.init(cct, sysobj_svc.get());
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
  RGWSysObj::Read rop(sysobj);

  int ret = rop.read(&bl);
  if (ret < 0 && ret !=  -ENOENT) {
    ldout(cct, 0) << __func__ << " failed to read converted: ret "<< ret << " " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldout(cct, 20) << "System already converted " << dendl;
    return 0;
  }

  string default_region;
  ret = default_zonegroup.init(cct, sysobj_svc.get(), false, true);
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
    int ret = zoneparams.init(cct, sysobj_svc.get());
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << __func__ << ": error initializing default zone params: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    /* update master zone */
    RGWZoneGroup default_zg(default_zonegroup_name);
    ret = default_zg.init(cct, sysobj_svc.get());
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
      int ret = region.init(cct, sysobj_svc.get(), true, true);
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
    ret = new_realm.init(cct, sysobj_svc.get(), false);
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
    ret = realm.init(cct, this);
    if (ret < 0) {
      ldout(cct, 0) << __func__ << " Error initing realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = current_period->init(cct, this, realm->get_id(), realm->get_name());
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
    ret = new_zonegroup.init(cct , sysobj_svc.get());
    if (ret == 0 && new_zonegroup.get_id() != *iter) {
      ldout(cct, 0) << __func__ << " zonegroup  "<< *iter << " already exists id " << new_zonegroup.get_id () <<
	" skipping conversion " << dendl;
      continue;
    }
    RGWZoneGroup zonegroup(*iter);
    zonegroup.set_id(*iter);
    int ret = zonegroup.init(cct, this, true, true);
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
      ret = zoneparams.init(cct, sysobj_svc.get());
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
    int ret = zonegroup.init(cct, this, true, true);
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
  ret = rgw_put_system_obj(this, pool, oid, bl,
			   true, NULL, real_time(), NULL);
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

  int ret = zonegroup->init(cct, sysobj_svc.get());
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
    ret = zonegroup->init(cct, sysobj_svc.get(), false);
    if (ret < 0) {
      ldout(cct, 0) << "failed init zonegroup: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ret = zone_params->init(cct, sysobj_svc.get());
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    } if (ret ==-ENOENT && zonegroup->get_name() == default_zonegroup_name) {
      ldout(cct, 10) << " Using default name "<< default_zone_name << dendl;
      zone_params->set_name(default_zone_name);
      ret = zone_params->init(cct, sysobj_svc.get());
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
	  ret = fixed_zg.init(cct, sysobj_svc.get());
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
  int ret = zonegroup->init(cct, sysobj_svc.get());
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
    ret = zonegroup->init(cct, sysobj_svc.get());
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
  RGWSysObj::Read rop(sysobj);

  int ret = rop.read(&bl);
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
    ret = zonegroup.init(cct, sysobj_svc.get(), false);
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
  rgw_raw_obj obj(pool, oid);
  ret = delete_system_obj(obj);
  if (ret < 0) {
    ldout(cct, 0) << "Error could not remove " << obj
        << " after upgrading to zonegroup map: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  return 0;
}

RGWZoneParams& RGWSI_Zone::get_zone_params()
{
  return *zone_params;
}

RGWZone& RGWSI_Zone::get_zone()
{
  return *zone_public_config;
}

RGWZoneGroup& RGWSI_Zone::get_zonegroup()
{
  return *zonegroup;
}

int RGWSI_Zone::get_zonegroup(const string& id, RGWZoneGroup& zonegroup)
{
  int ret = 0;
  if (id == get_zonegroup().get_id()) {
    zonegroup = get_zonegroup();
  } else if (!current_period->get_id().empty()) {
    ret = current_period->get_zonegroup(zonegroup, id);
  }
  return ret;
}

RGWRealm& RGWSI_Zone::get_realm()
{
  return *realm;
}

RGWPeriod& RGWSI_Zone::get_current_period()
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


void RGWSI_Zone::canonicalize_raw_obj(rgw_raw_obj *obj)
{
  if (obj->oid.empty()) {
    obj->oid = obj->pool.to_str();
    obj->pool = zone_params->domain_root;
  }
}
