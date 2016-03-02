
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "common/ceph_json.h"
#include "common/utf8.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"
#include "common/Finisher.h"

#include "rgw_rados.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_metadata.h"
#include "rgw_bucket.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/statelog/cls_statelog_client.h"
#include "cls/timeindex/cls_timeindex_client.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/user/cls_user_client.h"

#include "rgw_tools.h"

#include "rgw_coroutine.h"

#include "common/Clock.h"

#include "include/rados/librados.hpp"
using namespace librados;

#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>
#include "auth/Crypto.h" // get_random_bytes()

#include "rgw_log.h"

#include "rgw_gc.h"
#include "rgw_object_expirer_core.h"
#include "rgw_sync.h"
#include "rgw_data_sync.h"
#include "rgw_realm_watcher.h"

#define dout_subsys ceph_subsys_rgw

#define MAX_BUCKET_INDEX_SHARDS_PRIME 7877

using namespace std;

static RGWCache<RGWRados> cached_rados_provider;
static RGWRados rados_provider;

static string notify_oid_prefix = "notify";
static string *notify_oids = NULL;
static string shadow_ns = "shadow";
static string dir_oid_prefix = ".dir.";
static string default_storage_pool_suffix = "rgw.buckets.data";
static string default_bucket_index_pool_suffix = "rgw.buckets.index";
static string default_storage_extra_pool_suffix = "rgw.buckets.non-ec";
static string avail_pools = ".pools.avail";

static string zone_info_oid_prefix = "zone_info.";
static string zone_names_oid_prefix = "zone_names.";
static string region_info_oid_prefix = "region_info.";
static string zone_group_info_oid_prefix = "zonegroup_info.";
static string realm_names_oid_prefix = "realms_names.";
static string realm_info_oid_prefix = "realms.";
static string default_region_info_oid = "default.region";
static string default_zone_group_info_oid = "default.zonegroup";
static string period_info_oid_prefix = "periods.";
static string period_latest_epoch_info_oid = ".latest_epoch";
static string region_map_oid = "region_map";
static string zonegroup_map_oid = "zonegroup_map";
static string log_lock_name = "rgw_log_lock";
static string default_realm_info_oid = "default.realm";
const string default_zonegroup_name = "default";
const string default_zone_name = "default";
static string zonegroup_names_oid_prefix = "zonegroups_names.";
static RGWObjCategory main_category = RGW_OBJ_CATEGORY_MAIN;
#define RGW_USAGE_OBJ_PREFIX "usage."
#define FIRST_EPOCH 1
static string RGW_DEFAULT_ZONE_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_ZONEGROUP_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_REALM_ROOT_POOL = "rgw.root";
static string RGW_DEFAULT_PERIOD_ROOT_POOL = "rgw.root";

#define RGW_STATELOG_OBJ_PREFIX "statelog."

#define dout_subsys ceph_subsys_rgw

struct bucket_info_entry {
  RGWBucketInfo info;
  time_t mtime;
  map<string, bufferlist> attrs;
};

static RGWChainedCacheImpl<bucket_info_entry> binfo_cache;

void RGWDefaultZoneGroupInfo::dump(Formatter *f) const {
  encode_json("default_zonegroup", default_zonegroup, f);
}

void RGWDefaultZoneGroupInfo::decode_json(JSONObj *obj) {

  JSONDecoder::decode_json("default_zonegroup", default_zonegroup, obj);
  /* backward compatability with region */
  if (default_zonegroup.empty()) {
    JSONDecoder::decode_json("default_region", default_zonegroup, obj);
  }
}

const string& RGWZoneGroup::get_pool_name(CephContext *cct_)
{
  if (cct_->_conf->rgw_zonegroup_root_pool.empty()) {
    return RGW_DEFAULT_ZONEGROUP_ROOT_POOL;
  }

  return cct_->_conf->rgw_zonegroup_root_pool;
}

int RGWZoneGroup::create_default(bool old_format)
{
  name = default_zonegroup_name;
  is_master = true;

  RGWZoneGroupPlacementTarget placement_target;
  placement_target.name = "default-placement";
  placement_targets[placement_target.name] = placement_target;
  default_placement = "default-placement";

  RGWZoneParams zone_params(default_zone_name);

  int r = zone_params.init(cct, store, false);
  if (r < 0) {
    ldout(cct, 0) << "create_default: error initializing zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_params.create_default();
  if (r < 0 && r != -EEXIST) {
    ldout(cct, 0) << "create_default: error in create_default  zone params: " << cpp_strerror(-r) << dendl;
    return r;
  } else if (r == -EEXIST) {
    ldout(cct, 10) << "zone_params::create_default() returned -EEXIST, we raced with another default zone_params creation" << dendl;
    zone_params.clear_id();
    r = zone_params.init(cct, store);
    if (r < 0) {
      ldout(cct, 0) << "create_default: error in init existing zone params: " << cpp_strerror(-r) << dendl;
      return r;
    }
    ldout(cct, 20) << "zone_params::create_default() " << zone_params.get_name() << " id " << zone_params.get_id()
		   << dendl;
  }
  
  RGWZone& default_zone = zones[zone_params.get_id()];
  default_zone.name = zone_params.get_name();
  default_zone.id = zone_params.get_id();
  master_zone = default_zone.id;
  
  r = create();
  if (r < 0 && r != -EEXIST) {
    ldout(cct, 0) << "error storing zone group info: " << cpp_strerror(-r) << dendl;
    return r;
  }

  if (r == -EEXIST) {
    ldout(cct, 10) << "create_default() returned -EEXIST, we raced with another zonegroup creation" << dendl;
    id.clear();
    r = init(cct, store);
    if (r < 0) {
      return r;
    }
  }

  if (old_format) {
    name = id;
  }

  post_process_params();

  return 0;
}

const string RGWZoneGroup::get_default_oid(bool old_region_format)
{
  if (old_region_format) {
    if (cct->_conf->rgw_default_region_info_oid.empty()) {
      return default_region_info_oid;
    }
    return cct->_conf->rgw_default_region_info_oid;
  }

  string default_oid = cct->_conf->rgw_default_zonegroup_info_oid;

  if (cct->_conf->rgw_default_zonegroup_info_oid.empty()) {
    default_oid = default_zone_group_info_oid;
  }

  default_oid += "." + realm_id;

  return default_oid;
}

const string& RGWZoneGroup::get_info_oid_prefix(bool old_region_format)
{
  if (old_region_format) {
    return region_info_oid_prefix;
  }
  return zone_group_info_oid_prefix;
}

const string& RGWZoneGroup::get_names_oid_prefix()
{
  return zonegroup_names_oid_prefix;
}

const string& RGWZoneGroup::get_predefined_name(CephContext *cct) {
  return cct->_conf->rgw_zonegroup;
}

int RGWZoneGroup::equals(const string& other_zonegroup) const
{
  if (is_master && other_zonegroup.empty())
    return true;

  return (id  == other_zonegroup);
}

int RGWZoneGroup::add_zone(const RGWZoneParams& zone_params, bool *is_master, bool *read_only, const list<string>& endpoints)
{
  if (is_master && *is_master) {
    if (!master_zone.empty() && master_zone != zone_params.get_id()) {
      ldout(cct, 0) << "NOTICE: overriding master zone: " << master_zone  << dendl;
    }
    master_zone = zone_params.get_id();
  } else if (master_zone == zone_params.get_id()) {
    master_zone ="";
  }

  RGWZone& zone = zones[zone_params.get_id()];
  zone.name = zone_params.get_name();
  zone.id = zone_params.get_id();
  if (!endpoints.empty()) {
    zone.endpoints = endpoints;
  }
  if (read_only) {
    zone.read_only = *read_only;
  }

  post_process_params();

  return update();
}


int RGWZoneGroup::rename_zone(const RGWZoneParams& zone_params)
{ 
  RGWZone& zone = zones[zone_params.get_id()];
  zone.name = zone_params.get_name();
  
  return update();
}

void RGWZoneGroup::post_process_params()
{
  bool log_data = zones.size() > 1;

  if (master_zone.empty()) {
    map<string, RGWZone>::iterator iter = zones.begin();
    if (iter != zones.end()) {
      master_zone = iter->first;
    }
  }
  
  for (map<string, RGWZone>::iterator iter = zones.begin(); iter != zones.end(); ++iter) {
    RGWZone& zone = iter->second;
    zone.log_data = log_data;
    zone.log_meta = (is_master && zone.id == master_zone);

    RGWZoneParams zone_params(zone.id, zone.name);
    int ret = zone_params.init(cct, store);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not read zone params for zone id=" << zone.id << " name=" << zone.name << dendl;
      continue;
    }

    for (map<string, RGWZonePlacementInfo>::iterator iter = zone_params.placement_pools.begin(); 
         iter != zone_params.placement_pools.end(); ++iter) {
      const string& placement_name = iter->first;
      if (placement_targets.find(placement_name) == placement_targets.end()) {
        RGWZoneGroupPlacementTarget placement_target;
        placement_target.name = placement_name;
        placement_targets[placement_name] = placement_target;
      }
    }
  }

  if (default_placement.empty() && !placement_targets.empty()) {
    default_placement = placement_targets.begin()->first;
  }
}

int RGWZoneGroup::remove_zone(const RGWZoneParams& zone_params)
{
  map<string, RGWZone>::iterator iter = zones.find(zone_params.get_id());

  if (iter == zones.end()) {
    ldout(cct, 0) << "zone " << zone_params.get_name() << " " << zone_params.get_id() << " doesn't exists "<< dendl;
    return -ENOENT;
  }

  zones.erase(iter);

  post_process_params();

  return update();
}

int RGWZoneGroup::read_default_id(string& default_id, bool old_format)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(cct, store);
    if (ret < 0) {
      ldout(cct, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -ENOENT;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::read_default_id(default_id, old_format);
}

int RGWZoneGroup::set_as_default()
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(cct, store);
    if (ret < 0) {
      ldout(cct, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -EINVAL;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::set_as_default();
}

int RGWSystemMetaObj::init(CephContext *_cct, RGWRados *_store, bool setup_obj, bool old_format)
{
  cct = _cct;
  store = _store;

  if (!setup_obj)
    return 0;

  if (old_format && id.empty()) {
    id = name;
  }

  if (id.empty()) {
    int r;
    if (name.empty()) {
      name = get_predefined_name(cct);
    }
    if (name.empty()) {
      r = use_default(old_format);
      if (r < 0) {
	return r;
      }
    } else if (!old_format) {
      r = read_id(name, id);
      if (r < 0) {
	ldout(cct, 0) << "error in read_id for id " << id << " : " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
  }

  return read_info(id, old_format);
}

int RGWSystemMetaObj::read_default(RGWDefaultSystemMetaObjInfo& default_info, const string& oid)
{
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(default_info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::read_default_id(string& default_id, bool old_format)
{
  RGWDefaultSystemMetaObjInfo default_info;

  int ret = read_default(default_info, get_default_oid(old_format));
  if (ret < 0) {
    return ret;
  }

  default_id = default_info.default_id;

  return 0;
}

int RGWSystemMetaObj::use_default(bool old_format)
{
  return read_default_id(id, old_format);
}

int RGWSystemMetaObj::set_as_default()
{
  string pool_name = get_pool_name(cct);
  string oid  = get_default_oid();

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = id;

  ::encode(default_info, bl);

  int ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSystemMetaObj::read_id(const string& obj_name, string& object_id)
{
  string pool_name = get_pool_name(cct);
  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  string oid = get_names_oid_prefix() + obj_name;

  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  object_id = nameToId.obj_id;
  return 0;
}

int RGWSystemMetaObj::delete_obj(bool old_format)
{
  string pool_name = get_pool_name(cct);
  rgw_bucket pool(pool_name.c_str());

  /* check to see if obj is the default */
  RGWDefaultSystemMetaObjInfo default_info;
  int ret = read_default(default_info, get_default_oid(old_format));
  if (ret < 0 && ret != -ENOENT)
    return ret;
  if (default_info.default_id == id || (old_format && default_info.default_id == name)) {
    string oid = get_default_oid(old_format);
    rgw_obj default_named_obj(pool, oid);
    ret = store->delete_system_obj(default_named_obj);
    if (ret < 0) {
      ldout(cct, 0) << "Error delete default obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  if (!old_format) {
    string oid  = get_names_oid_prefix() + name;
    rgw_obj object_name(pool, oid);
    ret = store->delete_system_obj(object_name);
    if (ret < 0) {
      ldout(cct, 0) << "Error delete obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  string oid = get_info_oid_prefix(old_format);
  if (old_format) {
    oid += name;
  } else {
    oid += id;
  }

  rgw_obj object_id(pool, oid);
  ret = store->delete_system_obj(object_id);
  if (ret < 0) {
    ldout(cct, 0) << "Error delete object id " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWSystemMetaObj::store_name(bool exclusive)
{
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());
  string oid = get_names_oid_prefix() + name;

  RGWNameToId nameToId;
  nameToId.obj_id = id;

  bufferlist bl;
  ::encode(nameToId, bl);
  return  rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), exclusive, NULL, 0, NULL);
}

int RGWSystemMetaObj::rename(const string& new_name)
{
  string new_id;
  int ret = read_id(new_name, new_id);
  if (!ret) {
    return -EEXIST;
  }
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "Error read_id " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  string old_name = name;
  name = new_name;
  ret = update();
  if (ret < 0) {
    ldout(cct, 0) << "Error storing new obj info " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = store_name(true);
  if (ret < 0) {
    ldout(cct, 0) << "Error storing new name " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  /* delete old name */
  string pool_name = get_pool_name(cct);
  rgw_bucket pool(pool_name.c_str());
  string oid = get_names_oid_prefix() + old_name;
  rgw_obj old_name_obj(pool, oid);
  ret = store->delete_system_obj(old_name_obj);
  if (ret < 0) {
    ldout(cct, 0) << "Error delete old obj name  " << old_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return ret;
}

int RGWSystemMetaObj::read_info(const string& obj_id, bool old_format)
{
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  string oid = get_info_oid_prefix(old_format) + obj_id;

  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "failed reading obj info from " << pool << ":" << oid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::read()
{
  int ret = read_id(name, id);
  if (ret < 0) {
    return ret;
  }

  return read_info(id);
}

int RGWSystemMetaObj::create(bool exclusive)
{
  int ret;
  
  /* check to see the name is not used */
  ret = read_id(name, id);
  if (exclusive && ret == 0) {
    ldout(cct, 10) << "ERROR: name " << name << " already in use for obj id " << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading obj id  " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (id.empty()) {
    /* create unique id */
    uuid_d new_uuid;
    char uuid_str[37];
    new_uuid.generate_random();
    new_uuid.print(uuid_str);
    id = uuid_str;
  }

  ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing info for " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_name(exclusive);
}

int RGWSystemMetaObj::store_info(bool exclusive)
{
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());

  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  ::encode(*this, bl);
  return  rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), exclusive, NULL, 0, NULL);
}

int RGWSystemMetaObj::write(bool exclusive)
{
  int ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 20) << __func__ << "(): store_info() returned ret=" << ret << dendl;
    return ret;
  }
  ret = store_name(exclusive);
  if (ret < 0) {
    ldout(cct, 20) << __func__ << "(): store_name() returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}


const string& RGWRealm::get_predefined_name(CephContext *cct) {
  return cct->_conf->rgw_realm;
}

int RGWRealm::create(bool exclusive)
{
  list<string> realms;
  int ret = store->list_realms(realms);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "ERROR: listing realms, ret=" << ret << dendl;
    return ret;
  }

  bool first_realm = realms.empty();

  ret = RGWSystemMetaObj::create(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR creating new realm object " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  // create the control object for watch/notify
  ret = create_control();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR creating control for new realm " << name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  RGWPeriod period;
  if (current_period.empty()) {
    /* create new period for the realm */
    ret = period.init(cct, store, id, name, false);
    if (ret < 0 ) {
      return ret;
    }
    ret = period.create(true);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: creating new period for realm " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  } else {
    period = RGWPeriod(current_period, 0);
    int ret = period.init(cct, store, id, name);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to init period " << current_period << dendl;
      return ret;
    }
  }
  ret = set_current_period(period);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed set current period " << current_period << dendl;
    return ret;
  }

  if (first_realm) { /* this is racy, but it's fine */
    ret = set_as_default();
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: failed to set realm as default realm, ret=" << ret << dendl;
    }
  }

  return 0;
}

int RGWRealm::delete_obj()
{
  int ret = RGWSystemMetaObj::delete_obj();
  if (ret < 0) {
    return ret;
  }
  return delete_control();
}

int RGWRealm::create_control()
{
  auto pool_name = get_pool_name(cct);
  auto pool = rgw_bucket{pool_name.c_str()};
  auto oid = get_control_oid();
  return rgw_put_system_obj(store, pool, oid, nullptr, 0, true,
                            nullptr, 0, nullptr);
}

int RGWRealm::delete_control()
{
  auto pool_name = get_pool_name(cct);
  auto pool = rgw_bucket{pool_name.c_str()};
  auto obj = rgw_obj{pool, get_control_oid()};
  return store->delete_system_obj(obj);
}

const string& RGWRealm::get_pool_name(CephContext *cct)
{
  if (cct->_conf->rgw_realm_root_pool.empty()) {
    return RGW_DEFAULT_REALM_ROOT_POOL;
  }
  return cct->_conf->rgw_realm_root_pool;
}

const string RGWRealm::get_default_oid(bool old_format)
{
  if (cct->_conf->rgw_default_realm_info_oid.empty()) {
    return default_realm_info_oid;
  }
  return cct->_conf->rgw_default_realm_info_oid;
}

const string& RGWRealm::get_names_oid_prefix()
{
  return realm_names_oid_prefix;
}

const string& RGWRealm::get_info_oid_prefix(bool old_format)
{
  return realm_info_oid_prefix;
}

int RGWRealm::set_current_period(RGWPeriod& period)
{
  // update realm epoch to match the period's
  if (epoch > period.get_realm_epoch()) {
    ldout(cct, 0) << "ERROR: set_current_period with old realm epoch "
        << period.get_realm_epoch() << ", current epoch=" << epoch << dendl;
    return -EINVAL;
  }
  if (epoch == period.get_realm_epoch() && current_period != period.get_id()) {
    ldout(cct, 0) << "ERROR: set_current_period with same realm epoch "
        << period.get_realm_epoch() << ", but different period id "
        << period.get_id() << " != " << current_period << dendl;
    return -EINVAL;
  }

  epoch = period.get_realm_epoch();
  current_period = period.get_id();

  int ret = update();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: period update: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = period.reflect();
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: period.reflect(): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

string RGWRealm::get_control_oid()
{
  return get_info_oid_prefix() + id + ".control";
}

int RGWRealm::notify_zone(bufferlist& bl)
{
  // open a context on the realm's pool
  auto pool = get_pool_name(cct);
  librados::IoCtx ctx;
  int r = store->get_rados_handle()->ioctx_create(pool.c_str(), ctx);
  if (r < 0) {
    ldout(cct, 0) << "Failed to open pool " << pool << dendl;
    return r;
  }
  // send a notify on the realm object
  r = ctx.notify2(get_control_oid(), bl, 0, nullptr);
  if (r < 0) {
    ldout(cct, 0) << "Realm notify failed with " << r << dendl;
    return r;
  }
  return 0;
}

int RGWRealm::notify_new_period(const RGWPeriod& period)
{
  bufferlist bl;
  // push the period to dependent zonegroups/zones
  ::encode(RGWRealmNotify::ZonesNeedPeriod, bl);
  ::encode(period, bl);
  // reload the gateway with the new period
  ::encode(RGWRealmNotify::Reload, bl);

  return notify_zone(bl);
}

int RGWPeriod::init(CephContext *_cct, RGWRados *_store, const string& period_realm_id,
		    const string& period_realm_name, bool setup_obj)
{
  cct = _cct;
  store = _store;
  realm_id = period_realm_id;
  realm_name = period_realm_name;

  if (!setup_obj)
    return 0;

  return init(_cct, _store, setup_obj);
}


int RGWPeriod::init(CephContext *_cct, RGWRados *_store, bool setup_obj)
{
  cct = _cct;
  store = _store;

  if (!setup_obj)
    return 0;

  if (id.empty()) {
    RGWRealm realm(realm_id, realm_name);
    int ret = realm.init(cct, store);
    if (ret < 0) {
      ldout(cct, 0) << "RGWPeriod::init failed to init realm " << realm_name  << " id " << realm_id << " : " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }
    id = realm.get_current_period();
    realm_id = realm.get_id();
  }

  if (!epoch) {
    int ret = use_latest_epoch();
    if (ret < 0) {
      ldout(cct, 0) << "failed to use_latest_epoch period id " << id << " realm " << realm_name  << " id " << realm_id
	   << " : " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return read_info();
}


int RGWPeriod::get_zonegroup(RGWZoneGroup& zonegroup, const string& zonegroup_id) {
  map<string, RGWZoneGroup>::const_iterator iter = period_map.zonegroups.find(zonegroup_id);
  if (iter != period_map.zonegroups.end()) {
    zonegroup = iter->second;
    return 0;
  }

  return -ENOENT;
}

bool RGWPeriod::is_single_zonegroup(CephContext *cct, RGWRados *store)
{
  return (period_map.zonegroups.size() == 1);
}

const string& RGWPeriod::get_latest_epoch_oid()
{
  if (cct->_conf->rgw_period_latest_epoch_info_oid.empty()) {
    return period_latest_epoch_info_oid;
  }
  return cct->_conf->rgw_period_latest_epoch_info_oid;
}

const string& RGWPeriod::get_info_oid_prefix()
{
  return period_info_oid_prefix;
}

const string RGWPeriod::get_period_oid_prefix()
{
  return get_info_oid_prefix() + id;
}

const string RGWPeriod::get_period_oid()
{
  std::ostringstream oss;
  oss << get_period_oid_prefix();
  // skip the epoch for the staging period
  if (id != get_staging_id(realm_id))
    oss << "." << epoch;
  return oss.str();
}

int RGWPeriod::read_latest_epoch(RGWPeriodLatestEpochInfo& info)
{
  string pool_name = get_pool_name(cct);
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;
  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "error read_lastest_epoch " << pool << ":" << oid << dendl;
    return ret;
  }
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::get_latest_epoch(epoch_t& latest_epoch)
{
  RGWPeriodLatestEpochInfo info;

  int ret = read_latest_epoch(info);
  if (ret < 0) {
    return ret;
  }

  latest_epoch = info.epoch;

  return 0;
}

int RGWPeriod::use_latest_epoch()
{
  RGWPeriodLatestEpochInfo info;
  int ret = read_latest_epoch(info);
  if (ret < 0) {
    return ret;
  }

  epoch = info.epoch;

  return 0;
}

int RGWPeriod::set_latest_epoch(epoch_t epoch, bool exclusive)
{
  string pool_name = get_pool_name(cct);
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  RGWPeriodLatestEpochInfo info;
  info.epoch = epoch;

  ::encode(info, bl);

  return rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(),
                            exclusive, NULL, 0, NULL);
}

int RGWPeriod::delete_obj()
{
  string pool_name = get_pool_name(cct);
  rgw_bucket pool(pool_name.c_str());

  rgw_obj object_id(pool, get_period_oid());
  int ret = store->delete_system_obj(object_id);
  if (ret < 0) {
    ldout(cct, 0) << "Error delete object id " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWPeriod::read_info()
{
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  RGWObjectCtx obj_ctx(store);
  int ret = rgw_get_system_obj(store, obj_ctx, pool, get_period_oid(), bl, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "failed reading obj info from " << pool << ":" << get_period_oid() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode obj from " << pool << ":" << get_period_oid() << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::create(bool exclusive)
{
  int ret;
  
  /* create unique id */
  uuid_d new_uuid;
  char uuid_str[37];
  new_uuid.generate_random();
  new_uuid.print(uuid_str);
  id = uuid_str;

  epoch = FIRST_EPOCH;

  period_map.id = id;
  
  ret = store_info(exclusive);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR:  storing info for " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  ret = set_latest_epoch(epoch);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: setting latest epoch " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWPeriod::store_info(bool exclusive)
{
  epoch_t latest_epoch = FIRST_EPOCH - 1;
  int ret = get_latest_epoch(latest_epoch);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "ERROR: RGWPeriod::get_latest_epoch() returned " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  string pool_name = get_pool_name(cct);

  rgw_bucket pool(pool_name.c_str());

  string oid = get_period_oid();
  bufferlist bl;
  ::encode(*this, bl);
  ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), exclusive, NULL, 0, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: rgw_put_system_obj(" << pool << ":" << oid << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (latest_epoch < epoch) {
    ret = set_latest_epoch(epoch);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: RGWPeriod::set_latest_epoch() returned " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  return 0;
}

const string& RGWPeriod::get_pool_name(CephContext *cct)
{
  if (cct->_conf->rgw_period_root_pool.empty()) {
    return RGW_DEFAULT_PERIOD_ROOT_POOL;
  }
  return cct->_conf->rgw_period_root_pool;
}

int RGWPeriod::use_next_epoch()
{
  epoch_t latest_epoch;
  int ret = get_latest_epoch(latest_epoch);
  if (ret < 0) {
    return ret;
  }
  epoch = latest_epoch + 1;
  ret = read_info();
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret == -ENOENT) {
    ret = create();
    if (ret < 0) {
      ldout(cct, 0) << "Error creating new epoch " << epoch << dendl;
      return ret;
    }
  }
  return 0;
}

int RGWPeriod::add_zonegroup(const RGWZoneGroup& zonegroup)
{
  if (zonegroup.realm_id != realm_id) {
    return 0;
  }
  int ret = period_map.update(zonegroup);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: updating period map: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return store_info(false);
}

int RGWPeriod::update()
{
  ldout(cct, 20) << __func__ << " realm " << realm_id << " period " << get_id() << dendl;
  list<string> zonegroups;
  int ret = store->list_zonegroups(zonegroups);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to list zonegroups: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  for (auto& iter : zonegroups) {
    RGWZoneGroup zg(string(), iter);
    ret = zg.init(cct, store);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: zg.init() failed: " << cpp_strerror(-ret) << dendl;
      continue;
    }

    if (zg.realm_id != realm_id) {
      ldout(cct, 20) << "skipping zonegroup " << zg.get_name() << " zone realm id " << zg.realm_id << ", not on our realm " << realm_id << dendl;
      continue;
    }
    
    if (zg.is_master_zonegroup()) {
      master_zonegroup = zg.get_id();
      master_zone = zg.master_zone;
    }

    int ret = period_map.update(zg);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: updating period map: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return 0;
}

int RGWPeriod::reflect()
{
  for (auto& iter : period_map.zonegroups) {
    RGWZoneGroup& zg = iter.second;
    zg.reinit_instance(cct, store);
    int r = zg.write(false);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to store zonegroup info for zonegroup=" << iter.first << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
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

void RGWPeriod::update(const RGWZoneGroupMap& map)
{
  ldout(cct, 20) << __func__ << " realm " << realm_id << " period " << id << dendl;
  for (std::map<string, RGWZoneGroup>::const_iterator iter = map.zonegroups.begin();
       iter != map.zonegroups.end(); iter++) {
    period_map.zonegroups_by_api[iter->second.api_name] = iter->second;
    period_map.zonegroups[iter->second.get_name()] = iter->second;
  }

  period_config.bucket_quota = map.bucket_quota;
  period_config.user_quota = map.user_quota;
  period_map.master_zonegroup = map.master_zonegroup;
}

int RGWPeriod::update_sync_status()
{
  // must be new period's master zone to write sync status
  if (master_zone != store->get_zone_params().get_id()) {
    ldout(cct, 0) << "my zone " << store->get_zone_params().get_id()
        << " is not period's master zone " << master_zone << dendl;
    return -EINVAL;
  }

  auto mdlog = store->meta_mgr->get_log(get_id());
  const auto num_shards = cct->_conf->rgw_md_log_max_shards;

  std::vector<std::string> markers;
  markers.reserve(num_shards);

  // gather the markers for each shard
  // TODO: use coroutines to read them in parallel
  for (int i = 0; i < num_shards; i++) {
    RGWMetadataLogInfo info;
    int r = mdlog->get_info(i, &info);
    if (r < 0) {
      ldout(cct, 0) << "period failed to get metadata log info for shard " << i
          << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    ldout(cct, 15) << "got shard " << i << " marker " << info.marker << dendl;
    markers.emplace_back(std::move(info.marker));
  }

  std::swap(sync_status, markers);
  return 0;
}

int RGWPeriod::commit(RGWRealm& realm, const RGWPeriod& current_period)
{
  ldout(cct, 20) << __func__ << " realm " << realm.get_id() << " period " << current_period.get_id() << dendl;
  // gateway must be in the master zone to commit
  if (master_zone != store->get_zone_params().get_id()) {
    ldout(cct, 0) << "period commit on zone " << store->get_zone_params().get_id()
        << ", not period's master zone " << master_zone << dendl;
    return -EINVAL;
  }
  // period predecessor must match current period
  if (predecessor_uuid != current_period.get_id()) {
    ldout(cct, 0) << "period predecessor " << predecessor_uuid
        << " does not match current period " << current_period.get_id()
        << dendl;
    return -EINVAL;
  }
  // realm epoch must be 1 greater than current period
  if (realm_epoch != current_period.get_realm_epoch() + 1) {
    ldout(cct, 0) << "period's realm epoch " << realm_epoch
        << " does not come directly after current realm epoch "
        << current_period.get_realm_epoch() << dendl;
    return -EINVAL;
  }
  // did the master zone change?
  if (master_zone != current_period.get_master_zone()) {
    // store the current metadata sync status in the period
    int r = update_sync_status();
    if (r < 0) {
      ldout(cct, 0) << "failed to update metadata sync status: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    // create an object with a new period id
    r = create(true);
    if (r < 0) {
      ldout(cct, 0) << "failed to create new period: " << cpp_strerror(-r) << dendl;
      return r;
    }
    // set as current period
    r = realm.set_current_period(*this);
    if (r < 0) {
      ldout(cct, 0) << "failed to update realm's current period: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    ldout(cct, 4) << "Promoted to master zone and committed new period "
        << id << dendl;
    realm.notify_new_period(*this);
    return 0;
  }
  // period must be based on current epoch
  if (epoch != current_period.get_epoch()) {
    ldout(cct, 0) << "period epoch " << epoch << " does not match "
        "predecessor epoch " << current_period.get_epoch() << dendl;
    return -EINVAL;
  }
  // set period as next epoch
  set_id(current_period.get_id());
  set_epoch(current_period.get_epoch() + 1);
  set_predecessor(current_period.get_predecessor());
  realm_epoch = current_period.get_realm_epoch();
  // write the period to rados
  int r = store_info(false);
  if (r < 0) {
    ldout(cct, 0) << "failed to store period: " << cpp_strerror(-r) << dendl;
    return r;
  }
  // set as latest epoch
  r = set_latest_epoch(epoch);
  if (r < 0) {
    ldout(cct, 0) << "failed to set latest epoch: " << cpp_strerror(-r) << dendl;
    return r;
  }
  r = reflect();
  if (r < 0) {
    ldout(cct, 0) << "failed to update local objects: " << cpp_strerror(-r) << dendl;
    return r;
  }
  ldout(cct, 4) << "Committed new epoch " << epoch
      << " for period " << id << dendl;
  realm.notify_new_period(*this);
  return 0;
}

int RGWZoneParams::create_default(bool old_format)
{
  name = default_zone_name;

  int r = create();
  if (r < 0) {
    return r;
  }

  if (old_format) {
    name = id;
  }

  return r;
}


int get_zones_pool_names_set(CephContext* cct,
			     RGWRados* store,
			     const list<string>& zones,
			     const string& my_zone_id,
			     set<string>& pool_names)
{
  for(auto const& iter : zones) {
    RGWZoneParams zone(iter);
    int r = zone.init(cct, store);
    if (r < 0) {
      ldout(cct, 0) << "Error: init zone " << iter << ":" << cpp_strerror(-r) << dendl;
      return r;
    }
    if (zone.get_id() != my_zone_id) {
      pool_names.insert(zone.domain_root.name);
      pool_names.insert(zone.metadata_heap.name);
      pool_names.insert(zone.control_pool.name);
      pool_names.insert(zone.gc_pool.name);
      pool_names.insert(zone.log_pool.name);
      pool_names.insert(zone.intent_log_pool.name);
      pool_names.insert(zone.usage_log_pool.name);
      pool_names.insert(zone.user_keys_pool.name);
      pool_names.insert(zone.user_email_pool.name);
      pool_names.insert(zone.user_swift_pool.name);
      pool_names.insert(zone.user_uid_pool.name);
      for(auto& iter : zone.placement_pools) {
	pool_names.insert(iter.second.index_pool);
	pool_names.insert(iter.second.data_pool);
	pool_names.insert(iter.second.data_extra_pool);
      }
    }
  }
  return 0;
}

string fix_zone_pool_name(set<string> pool_names,
			  const string& default_prefix,
			  const string& default_suffix,
			  const string& suggested_name)
{
  string prefix = default_prefix;
  string suffix = default_suffix;

  if (!suggested_name.empty()) {
    prefix = suggested_name.substr(0,suggested_name.find("."));
    suffix = suggested_name.substr(prefix.length(), suggested_name.length() - 1);
  }

  string name = prefix + suffix;
  
  if (pool_names.find(name) == pool_names.end()) {
    return name;
  } else {
    while(true) {
      name =  prefix + "_" + std::to_string(std::rand()) + suffix;
      if (pool_names.find(name) == pool_names.end()) {
	return name;
      }
    }
  }  
}

int RGWZoneParams::fix_pool_names()
{

  list<string> zones;
  int r = store->list_zones(zones);
  if (r < 0) {
    ldout(cct, 10) << "WARNING: store->list_zones() returned r=" << r << dendl;
  }

  set<string> pool_names;
  r = get_zones_pool_names_set(cct, store, zones, id, pool_names);
  if (r < 0) {
    ldout(cct, 0) << "Error: get_zones_pool_names" << r << dendl;
    return r;
  }

  domain_root = fix_zone_pool_name(pool_names, name, ".rgw.data.root", domain_root.name);
  metadata_heap = fix_zone_pool_name(pool_names, name, ".rgw.meta", metadata_heap.name);
  control_pool = fix_zone_pool_name(pool_names, name, ".rgw.control", control_pool.name);
  gc_pool = fix_zone_pool_name(pool_names, name ,".rgw.gc", gc_pool.name);
  log_pool = fix_zone_pool_name(pool_names, name, ".rgw.log", log_pool.name);
  intent_log_pool = fix_zone_pool_name(pool_names, name, ".rgw.intent-log", intent_log_pool.name);
  usage_log_pool = fix_zone_pool_name(pool_names, name, ".rgw.usage", usage_log_pool.name);
  user_keys_pool = fix_zone_pool_name(pool_names, name, ".rgw.users.keys", user_keys_pool.name);
  user_email_pool = fix_zone_pool_name(pool_names, name, ".rgw.users.email", user_email_pool.name);
  user_swift_pool = fix_zone_pool_name(pool_names, name, ".rgw.users.swift", user_swift_pool.name);
  user_uid_pool = fix_zone_pool_name(pool_names, name, ".rgw.users.uid", user_uid_pool.name);

  for(auto& iter : placement_pools) {
    iter.second.index_pool = fix_zone_pool_name(pool_names, name, "." + default_bucket_index_pool_suffix,
						iter.second.index_pool);
    iter.second.data_pool = fix_zone_pool_name(pool_names, name, "." + default_storage_pool_suffix,
					       iter.second.data_pool);
    iter.second.data_extra_pool= fix_zone_pool_name(pool_names, name, "." + default_storage_extra_pool_suffix,
						    iter.second.data_extra_pool);
  }

  return 0;
}

int RGWZoneParams::create(bool exclusive)
{
  list<string> zones;
  int r = store->list_zones(zones);
  if (r < 0) {
    ldout(cct, 10) << "WARNING: store->list_zones() returned r=" << r << dendl;
  }

  /* check for old pools config */
  rgw_obj obj(domain_root, avail_pools);
  r = store->raw_obj_stat(obj, NULL, NULL, NULL, NULL, NULL, NULL);
  if (r < 0) {
    ldout(store->ctx(), 10) << "couldn't find old data placement pools config, setting up new ones for the zone" << dendl;
    /* a new system, let's set new placement info */
    RGWZonePlacementInfo default_placement;
    default_placement.index_pool = name + "." + default_bucket_index_pool_suffix;
    default_placement.data_pool =  name + "." + default_storage_pool_suffix;
    default_placement.data_extra_pool = name + "." + default_storage_extra_pool_suffix;
    placement_pools["default-placement"] = default_placement;
  }

  r = fix_pool_names();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: fix_pool_names returned r=" << r << dendl;
    return r;
  }

  r = RGWSystemMetaObj::create(exclusive);
  if (r < 0) {
    ldout(cct, 0) << "RGWZoneParams::create(): error creating default zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  if (zones.empty()) { /* first zone? maybe, it's a racy check */
    r = set_as_default();
    if (r < 0) {
      ldout(cct, 10) << "WARNING: failed to set zone as default, r=" << r << dendl;
    }
  }

  return 0;
}

const string& RGWZoneParams::get_pool_name(CephContext *cct)
{
  if (cct->_conf->rgw_zone_root_pool.empty()) {
    return RGW_DEFAULT_ZONE_ROOT_POOL;
  }

  return cct->_conf->rgw_zone_root_pool;
}

const string RGWZoneParams::get_default_oid(bool old_format)
{
  if (old_format) {
    return cct->_conf->rgw_default_zone_info_oid;
  }

  return cct->_conf->rgw_default_zone_info_oid + "." + realm_id;
}

const string& RGWZoneParams::get_names_oid_prefix()
{
  return zone_names_oid_prefix;
}

const string& RGWZoneParams::get_info_oid_prefix(bool old_format)
{
  return zone_info_oid_prefix;
}

const string& RGWZoneParams::get_predefined_name(CephContext *cct) {
  return cct->_conf->rgw_zone;
}

int RGWZoneParams::init(CephContext *cct, RGWRados *store, bool setup_obj, bool old_format)
{
  if (name.empty()) {
    name = cct->_conf->rgw_zone;
  }

  int ret = RGWSystemMetaObj::init(cct, store, setup_obj, old_format);
  if (ret < 0) {
    return ret;
  }

  return ret;
}

int RGWZoneParams::read_default_id(string& default_id, bool old_format)
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(cct, store);
    if (ret < 0) {
      ldout(cct, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -ENOENT;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::read_default_id(default_id, old_format);
}


int RGWZoneParams::set_as_default()
{
  if (realm_id.empty()) {
    /* try using default realm */
    RGWRealm realm;
    int ret = realm.init(cct, store);
    if (ret < 0) {
      ldout(cct, 10) << "could not read realm id: " << cpp_strerror(-ret) << dendl;
      return -EINVAL;
    }
    realm_id = realm.get_id();
  }

  return RGWSystemMetaObj::set_as_default();
}

void RGWPeriodMap::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  ::encode(id, bl);
  ::encode(zonegroups, bl);
  ::encode(master_zonegroup, bl);
  ::encode(short_zone_ids, bl);
  ENCODE_FINISH(bl);
}

void RGWPeriodMap::decode(bufferlist::iterator& bl) {
  DECODE_START(2, bl);
  ::decode(id, bl);
  ::decode(zonegroups, bl);
  ::decode(master_zonegroup, bl);
  if (struct_v >= 2) {
    ::decode(short_zone_ids, bl);
  }
  DECODE_FINISH(bl);

  zonegroups_by_api.clear();
  for (map<string, RGWZoneGroup>::iterator iter = zonegroups.begin();
       iter != zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
    if (zonegroup.is_master) {
      master_zonegroup = zonegroup.get_id();
    }
  }
}

int RGWPeriodMap::update(const RGWZoneGroup& zonegroup)
{
  if (zonegroup.is_master && (!master_zonegroup.empty() && zonegroup.get_id() != master_zonegroup)) {
    return -EINVAL;
  }
  map<string, RGWZoneGroup>::iterator iter = zonegroups.find(zonegroup.get_id());
  if (iter != zonegroups.end()) {
    RGWZoneGroup& old_zonegroup = iter->second;
    if (!old_zonegroup.api_name.empty()) {
      zonegroups_by_api.erase(old_zonegroup.api_name);
    }
  }
  zonegroups[zonegroup.get_id()] = zonegroup;

  if (!zonegroup.api_name.empty()) {
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
  }

  if (zonegroup.is_master) {
    master_zonegroup = zonegroup.get_id();
  } else if (master_zonegroup == zonegroup.get_id()) {
    master_zonegroup = "";
  }

  for (auto iter : zonegroups) {
    for (auto i : iter.second.zones) {
      string& zone_id = i.second.id;
      if (short_zone_ids.find(zone_id) == short_zone_ids.end()) {
        uint32_t short_id;

        unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
        MD5 hash;
        hash.Update((const byte *)zone_id.c_str(), zone_id.size());
        hash.Final(md5);
        memcpy((char *)&short_id, md5, sizeof(short_id));

        if (short_id == 0) {
          ++short_id;
        }

        short_zone_ids[i.second.id] = short_id;
      }
    }
  }

  return 0;
}

uint32_t RGWPeriodMap::get_zone_short_id(const string& zone_id) const
{
  auto i = short_zone_ids.find(zone_id);
  if (i == short_zone_ids.end()) {
    return 0;
  }
  return i->second;
}

int RGWZoneGroupMap::read(CephContext *cct, RGWRados *store)
{

  RGWPeriod period;
  int ret = period.init(cct, store);
  if (ret < 0) {
    cerr << "failed to read current period info: " << cpp_strerror(ret);
    return ret;
  }
	
  bucket_quota = period.get_config().bucket_quota;
  user_quota = period.get_config().user_quota;
  zonegroups = period.get_map().zonegroups;
  zonegroups_by_api = period.get_map().zonegroups_by_api;
  master_zonegroup = period.get_map().master_zonegroup;

  return 0;
}

void RGWRegionMap::encode(bufferlist& bl) const {
  ENCODE_START( 3, 1, bl);
  ::encode(regions, bl);
  ::encode(master_region, bl);
  ::encode(bucket_quota, bl);
  ::encode(user_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWRegionMap::decode(bufferlist::iterator& bl) {
  DECODE_START(3, bl);
  ::decode(regions, bl);
  ::decode(master_region, bl);
  if (struct_v >= 2)
    ::decode(bucket_quota, bl);
  if (struct_v >= 3)
    ::decode(user_quota, bl);
  DECODE_FINISH(bl);
}

void RGWZoneGroupMap::encode(bufferlist& bl) const {
  ENCODE_START( 3, 1, bl);
  ::encode(zonegroups, bl);
  ::encode(master_zonegroup, bl);
  ::encode(bucket_quota, bl);
  ::encode(user_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWZoneGroupMap::decode(bufferlist::iterator& bl) {
  DECODE_START(3, bl);
  ::decode(zonegroups, bl);
  ::decode(master_zonegroup, bl);
  if (struct_v >= 2)
    ::decode(bucket_quota, bl);
  if (struct_v >= 3)
    ::decode(user_quota, bl);
  DECODE_FINISH(bl);

  zonegroups_by_api.clear();
  for (map<string, RGWZoneGroup>::iterator iter = zonegroups.begin();
       iter != zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
    if (zonegroup.is_master) {
      master_zonegroup = zonegroup.get_name();
    }
  }
}

void RGWObjVersionTracker::prepare_op_for_read(ObjectReadOperation *op)
{
  obj_version *check_objv = version_for_check();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  cls_version_read(*op, &read_version);
}

void RGWObjVersionTracker::prepare_op_for_write(ObjectWriteOperation *op)
{
  obj_version *check_objv = version_for_check();
  obj_version *modify_version = version_for_write();

  if (check_objv) {
    cls_version_check(*op, *check_objv, VER_COND_EQ);
  }

  if (modify_version) {
    cls_version_set(*op, *modify_version);
  } else {
    cls_version_inc(*op);
  }
}

void RGWObjManifest::obj_iterator::operator++()
{
  if (manifest->explicit_objs) {
    ++explicit_iter;

    if (explicit_iter == manifest->objs.end()) {
      ofs = manifest->obj_size;
      return;
    }

    update_explicit_pos();

    update_location();
    return;
  }

  uint64_t obj_size = manifest->get_obj_size();
  uint64_t head_size = manifest->get_head_size();

  if (ofs == obj_size) {
    return;
  }

  if (manifest->rules.empty()) {
    return;
  }

  /* are we still pointing at the head? */
  if (ofs < head_size) {
    rule_iter = manifest->rules.begin();
    RGWObjManifestRule *rule = &rule_iter->second;
    ofs = MIN(head_size, obj_size);
    stripe_ofs = ofs;
    cur_stripe = 1;
    stripe_size = MIN(obj_size - ofs, rule->stripe_max_size);
    if (rule->part_size > 0) {
      stripe_size = MIN(stripe_size, rule->part_size);
    }
    update_location();
    return;
  }

  RGWObjManifestRule *rule = &rule_iter->second;

  stripe_ofs += rule->stripe_max_size;
  cur_stripe++;
  dout(20) << "RGWObjManifest::operator++(): rule->part_size=" << rule->part_size << " rules.size()=" << manifest->rules.size() << dendl;

  if (rule->part_size > 0) {
    /* multi part, multi stripes object */

    dout(20) << "RGWObjManifest::operator++(): stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;

    if (stripe_ofs >= part_ofs + rule->part_size) {
      /* moved to the next part */
      cur_stripe = 0;
      part_ofs += rule->part_size;
      stripe_ofs = part_ofs;

      bool last_rule = (next_rule_iter == manifest->rules.end());
      /* move to the next rule? */
      if (!last_rule && stripe_ofs >= next_rule_iter->second.start_ofs) {
        rule_iter = next_rule_iter;
        last_rule = (next_rule_iter == manifest->rules.end());
        if (!last_rule) {
          ++next_rule_iter;
        }
        cur_part_id = rule_iter->second.start_part_num;
      } else {
        cur_part_id++;
      }

      rule = &rule_iter->second;
    }

    stripe_size = MIN(rule->part_size - (stripe_ofs - part_ofs), rule->stripe_max_size);
  }

  cur_override_prefix = rule->override_prefix;

  ofs = stripe_ofs;
  if (ofs > obj_size) {
    ofs = obj_size;
    stripe_ofs = ofs;
    stripe_size = 0;
  }

  dout(20) << "RGWObjManifest::operator++(): result: ofs=" << ofs << " stripe_ofs=" << stripe_ofs << " part_ofs=" << part_ofs << " rule->part_size=" << rule->part_size << dendl;
  update_location();
}

int RGWObjManifest::generator::create_begin(CephContext *cct, RGWObjManifest *_m, rgw_bucket& _b, rgw_obj& _h)
{
  manifest = _m;

  bucket = _b;
  manifest->set_tail_bucket(_b);
  manifest->set_head(_h);
  last_ofs = 0;

  if (manifest->get_prefix().empty()) {
    char buf[33];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);

    string oid_prefix = ".";
    oid_prefix.append(buf);
    oid_prefix.append("_");

    manifest->set_prefix(oid_prefix);
  }

  bool found = manifest->get_rule(0, &rule);
  if (!found) {
    derr << "ERROR: manifest->get_rule() could not find rule" << dendl;
    return -EIO;
  }

  uint64_t head_size = manifest->get_head_size();

  if (head_size > 0) {
    cur_stripe_size = head_size;
  } else {
    cur_stripe_size = rule.stripe_max_size;
  }
  
  cur_part_id = rule.start_part_num;

  manifest->get_implicit_location(cur_part_id, cur_stripe, 0, NULL, &cur_obj);

  manifest->update_iterators();

  return 0;
}

int RGWObjManifest::generator::create_next(uint64_t ofs)
{
  if (ofs < last_ofs) /* only going forward */
    return -EINVAL;

  uint64_t max_head_size = manifest->get_max_head_size();

  if (ofs <= max_head_size) {
    manifest->set_head_size(ofs);
  }

  if (ofs >= max_head_size) {
    manifest->set_head_size(max_head_size);
    cur_stripe = (ofs - max_head_size) / rule.stripe_max_size;
    cur_stripe_size =  rule.stripe_max_size;

    if (cur_part_id == 0 && max_head_size > 0) {
      cur_stripe++;
    }
  }

  last_ofs = ofs;
  manifest->set_obj_size(ofs);

  manifest->get_implicit_location(cur_part_id, cur_stripe, ofs, NULL, &cur_obj);

  manifest->update_iterators();

  return 0;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_begin()
{
  return begin_iter;
}

const RGWObjManifest::obj_iterator& RGWObjManifest::obj_end()
{
  return end_iter;
}

RGWObjManifest::obj_iterator RGWObjManifest::obj_find(uint64_t ofs)
{
  if (ofs > obj_size) {
    ofs = obj_size;
  }
  RGWObjManifest::obj_iterator iter(this);
  iter.seek(ofs);
  return iter;
}

int RGWObjManifest::append(RGWObjManifest& m)
{
  if (explicit_objs || m.explicit_objs) {
    return append_explicit(m);
  }

  if (rules.empty()) {
    *this = m;
    return 0;
  }

  string override_prefix;

  if (prefix.empty()) {
    prefix = m.prefix;
  }

  if (prefix != m.prefix) {
    override_prefix = m.prefix;
  }

  map<uint64_t, RGWObjManifestRule>::iterator miter = m.rules.begin();
  if (miter == m.rules.end()) {
    return append_explicit(m);
  }

  for (; miter != m.rules.end(); ++miter) {
    map<uint64_t, RGWObjManifestRule>::reverse_iterator last_rule = rules.rbegin();

    RGWObjManifestRule& rule = last_rule->second;

    if (rule.part_size == 0) {
      rule.part_size = obj_size - rule.start_ofs;
    }

    RGWObjManifestRule& next_rule = miter->second;
    if (!next_rule.part_size) {
      next_rule.part_size = m.obj_size - next_rule.start_ofs;
    }

    string rule_prefix = prefix;
    if (!rule.override_prefix.empty()) {
      rule_prefix = rule.override_prefix;
    }

    string next_rule_prefix = m.prefix;
    if (!next_rule.override_prefix.empty()) {
      next_rule_prefix = next_rule.override_prefix;
    }

    if (rule.part_size != next_rule.part_size ||
        rule.stripe_max_size != next_rule.stripe_max_size ||
        rule_prefix != next_rule_prefix) {
      if (next_rule_prefix != prefix) {
        append_rules(m, miter, &next_rule_prefix);
      } else {
        append_rules(m, miter, NULL);
      }
      break;
    }

    uint64_t expected_part_num = rule.start_part_num + 1;
    if (rule.part_size > 0) {
      expected_part_num = rule.start_part_num + (obj_size + next_rule.start_ofs - rule.start_ofs) / rule.part_size;
    }

    if (expected_part_num != next_rule.start_part_num) {
      append_rules(m, miter, NULL);
      break;
    }
  }

  set_obj_size(obj_size + m.obj_size);

  return 0;
}

void RGWObjManifest::append_rules(RGWObjManifest& m, map<uint64_t, RGWObjManifestRule>::iterator& miter,
                                  string *override_prefix)
{
  for (; miter != m.rules.end(); ++miter) {
    RGWObjManifestRule rule = miter->second;
    rule.start_ofs += obj_size;
    if (override_prefix)
      rule.override_prefix = *override_prefix;
    rules[rule.start_ofs] = rule;
  }
}

void RGWObjManifest::convert_to_explicit()
{
  if (explicit_objs) {
    return;
  }
  obj_iterator iter = obj_begin();

  while (iter != obj_end()) {
    RGWObjManifestPart& part = objs[iter.get_stripe_ofs()];
    part.loc = iter.get_location();
    part.loc_ofs = 0;

    uint64_t ofs = iter.get_stripe_ofs();
    ++iter;
    uint64_t next_ofs = iter.get_stripe_ofs();

    part.size = next_ofs - ofs;
  }

  explicit_objs = true;
  rules.clear();
  prefix.clear();
}

int RGWObjManifest::append_explicit(RGWObjManifest& m)
{
  if (!explicit_objs) {
    convert_to_explicit();
  }
  if (!m.explicit_objs) {
    m.convert_to_explicit();
  }
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;

  return 0;
}

bool RGWObjManifest::get_rule(uint64_t ofs, RGWObjManifestRule *rule)
{
  if (rules.empty()) {
    return false;
  }

  map<uint64_t, RGWObjManifestRule>::iterator iter = rules.upper_bound(ofs);
  if (iter != rules.begin()) {
    --iter;
  }

  *rule = iter->second;

  return true;
}

void RGWObjVersionTracker::generate_new_write_ver(CephContext *cct)
{
  write_version.ver = 1;
#define TAG_LEN 24

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

int RGWPutObjProcessor::complete(string& etag, time_t *mtime, time_t set_mtime,
                                 map<string, bufferlist>& attrs, time_t delete_at,
                                 const char *if_match, const char * if_nomatch)
{
  int r = do_complete(etag, mtime, set_mtime, attrs, delete_at, if_match, if_nomatch);
  if (r < 0)
    return r;

  is_complete = !canceled;
  return 0;
}

CephContext *RGWPutObjProcessor::ctx()
{
  return store->ctx();
}

RGWPutObjProcessor_Aio::~RGWPutObjProcessor_Aio()
{
  drain_pending();

  if (is_complete)
    return;

  list<rgw_obj>::iterator iter;
  bool is_multipart_obj = false;
  rgw_obj multipart_obj;

  /** 
   * We should delete the object in the "multipart" namespace to avoid race condition. 
   * Such race condition is caused by the fact that the multipart object is the gatekeeper of a multipart 
   * upload, when it is deleted, a second upload would start with the same suffix("2/"), therefore, objects
   * written by the second upload may be deleted by the first upload.
   * details is describled on #11749
   */ 
  for (iter = written_objs.begin(); iter != written_objs.end(); ++iter) {
    rgw_obj &obj = *iter;
    if (RGW_OBJ_NS_MULTIPART == obj.ns) {
      ldout(store->ctx(), 5) << "NOTE: we should not process the multipart object (" << obj << ") here" << dendl;
      multipart_obj = *iter;
      is_multipart_obj = true;
      continue;
    }

    int r = store->delete_obj(obj_ctx, bucket_info, obj, 0, 0);
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 5) << "WARNING: failed to remove obj (" << obj << "), leaked" << dendl;
    }
  }

  if (true == is_multipart_obj) {
    ldout(store->ctx(), 5) << "NOTE: we are going to process the multipart obj (" << multipart_obj << dendl;
    int r = store->delete_obj(obj_ctx, bucket_info, multipart_obj, 0, 0);
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 0) << "WARNING: failed to remove obj (" << multipart_obj << "), leaked" << dendl;
    }
  }
}

int RGWPutObjProcessor_Aio::handle_obj_data(rgw_obj& obj, bufferlist& bl, off_t ofs, off_t abs_ofs, void **phandle, bool exclusive)
{
  if ((uint64_t)abs_ofs + bl.length() > obj_len)
    obj_len = abs_ofs + bl.length();

  if (!(obj == last_written_obj)) {
    add_written_obj(obj);
    last_written_obj = obj;
  }

  // For the first call pass -1 as the offset to
  // do a write_full.
  int r = store->aio_put_obj_data(NULL, obj,
                                     bl,
                                     ((ofs != 0) ? ofs : -1),
                                     exclusive, phandle);

  return r;
}

struct put_obj_aio_info RGWPutObjProcessor_Aio::pop_pending()
{
  struct put_obj_aio_info info;
  info = pending.front();
  pending.pop_front();
  return info;
}

int RGWPutObjProcessor_Aio::wait_pending_front()
{
  if (pending.empty()) {
    return 0;
  }
  struct put_obj_aio_info info = pop_pending();
  int ret = store->aio_wait(info.handle);
  return ret;
}

bool RGWPutObjProcessor_Aio::pending_has_completed()
{
  if (pending.empty())
    return false;

  struct put_obj_aio_info& info = pending.front();
  return store->aio_completed(info.handle);
}

int RGWPutObjProcessor_Aio::drain_pending()
{
  int ret = 0;
  while (!pending.empty()) {
    int r = wait_pending_front();
    if (r < 0)
      ret = r;
  }
  return ret;
}

int RGWPutObjProcessor_Aio::throttle_data(void *handle, bool need_to_wait)
{
  bool _wait = need_to_wait;

  if (handle) {
    struct put_obj_aio_info info;
    info.handle = handle;
    pending.push_back(info);
  }
  size_t orig_size = pending.size();

  /* first drain complete IOs */
  while (pending_has_completed()) {
    int r = wait_pending_front();
    if (r < 0)
      return r;

    _wait = false;
  }

  /* resize window in case messages are draining too fast */
  if (orig_size - pending.size() >= max_chunks) {
    max_chunks++;
  }

  /* now throttle. Note that need_to_wait should only affect the first IO operation */
  if (pending.size() > max_chunks || _wait) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWPutObjProcessor_Atomic::write_data(bufferlist& bl, off_t ofs, void **phandle, bool exclusive)
{
  if (ofs >= next_part_ofs) {
    int r = prepare_next_part(ofs);
    if (r < 0) {
      return r;
    }
  }

  return RGWPutObjProcessor_Aio::handle_obj_data(cur_obj, bl, ofs - cur_part_ofs, ofs, phandle, exclusive);
}

int RGWPutObjProcessor_Atomic::handle_data(bufferlist& bl, off_t ofs, MD5 *hash, void **phandle, bool *again)
{
  *again = false;

  *phandle = NULL;
  if (extra_data_len) {
    size_t extra_len = bl.length();
    if (extra_len > extra_data_len)
      extra_len = extra_data_len;

    bufferlist extra;
    bl.splice(0, extra_len, &extra);
    extra_data_bl.append(extra);

    extra_data_len -= extra_len;
    if (bl.length() == 0) {
      return 0;
    }
  }

  uint64_t max_write_size = MIN(max_chunk_size, (uint64_t)next_part_ofs - data_ofs);

  pending_data_bl.claim_append(bl);
  if (pending_data_bl.length() < max_write_size)
    return 0;

  pending_data_bl.splice(0, max_write_size, &bl);

  /* do we have enough data pending accumulated that needs to be written? */
  *again = (pending_data_bl.length() >= max_chunk_size);

  if (!data_ofs && !immutable_head()) {
    first_chunk.claim(bl);
    obj_len = (uint64_t)first_chunk.length();
    if (hash) {
      hash->Update((const byte *)first_chunk.c_str(), obj_len);
    }
    int r = prepare_next_part(obj_len);
    if (r < 0) {
      return r;
    }
    data_ofs = obj_len;
    return 0;
  }
  off_t write_ofs = data_ofs;
  data_ofs = write_ofs + bl.length();
  bool exclusive = (!write_ofs && immutable_head()); /* immutable head object, need to verify nothing exists there
                                                        we could be racing with another upload, to the same
                                                        object and cleanup can be messy */
  int ret = write_data(bl, write_ofs, phandle, exclusive);
  if (ret >= 0) { /* we might return, need to clear bl as it was already sent */
    if (hash) {
      hash->Update((const byte *)bl.c_str(), bl.length());
    }
    bl.clear();
  }
  return ret;
}

void RGWPutObjProcessor_Atomic::complete_hash(MD5 *hash)
{
  hash->Update((const byte *)pending_data_bl.c_str(), pending_data_bl.length());
}


int RGWPutObjProcessor_Atomic::prepare_init(RGWRados *store, string *oid_rand)
{
  RGWPutObjProcessor::prepare(store, oid_rand);

  int r = store->get_max_chunk_size(bucket, &max_chunk_size);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWPutObjProcessor_Atomic::prepare(RGWRados *store, string *oid_rand)
{
  int r = prepare_init(store, oid_rand);
  if (r < 0) {
    return r;
  }
  head_obj.init(bucket, obj_str);

  if (!version_id.empty()) {
    head_obj.set_instance(version_id);
  } else if (versioned_object) {
    store->gen_rand_obj_instance_name(&head_obj);
  }

  manifest.set_trivial_rule(max_chunk_size, store->ctx()->_conf->rgw_obj_stripe_size);

  r = manifest_gen.create_begin(store->ctx(), &manifest, bucket, head_obj);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWPutObjProcessor_Atomic::prepare_next_part(off_t ofs) {

  int ret = manifest_gen.create_next(ofs);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: manifest_gen.create_next() returned ret=" << ret << dendl;
    return ret;
  }
  cur_part_ofs = ofs;
  next_part_ofs = ofs + manifest_gen.cur_stripe_max_size();
  cur_obj = manifest_gen.get_cur_obj();

  return 0;
}

int RGWPutObjProcessor_Atomic::complete_parts()
{
  if (obj_len > (uint64_t)cur_part_ofs) {
    return prepare_next_part(obj_len);
  }
  return 0;
}

int RGWPutObjProcessor_Atomic::complete_writing_data()
{
  if (!data_ofs && !immutable_head()) {
    /* only claim if pending_data_bl() is not empty. This is needed because we might be called twice
     * (e.g., when a retry due to race happens). So a second call to first_chunk.claim() would
     * clobber first_chunk
     */
    if (pending_data_bl.length() > 0) {
      first_chunk.claim(pending_data_bl);
    }
    obj_len = (uint64_t)first_chunk.length();
  }
  if (pending_data_bl.length()) {
    void *handle;
    int r = write_data(pending_data_bl, data_ofs, &handle, false);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: write_data() returned " << r << dendl;
      return r;
    }
    r = throttle_data(handle, false);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: throttle_data() returned " << r << dendl;
      return r;
    }
  }
  int r = complete_parts();
  if (r < 0) {
    return r;
  }

  r = drain_pending();
  if (r < 0)
    return r;

  return 0;
}

int RGWPutObjProcessor_Atomic::do_complete(string& etag, time_t *mtime, time_t set_mtime,
                                           map<string, bufferlist>& attrs, time_t delete_at,
                                           const char *if_match,
                                           const char *if_nomatch) {
  int r = complete_writing_data();
  if (r < 0)
    return r;

  obj_ctx.set_atomic(head_obj);

  RGWRados::Object op_target(store, bucket_info, obj_ctx, head_obj);

  /* some object types shouldn't be versioned, e.g., multipart parts */
  op_target.set_versioning_disabled(!versioned_object);

  RGWRados::Object::Write obj_op(&op_target);

  obj_op.meta.data = &first_chunk;
  obj_op.meta.manifest = &manifest;
  obj_op.meta.ptag = &unique_tag; /* use req_id as operation tag */
  obj_op.meta.if_match = if_match;
  obj_op.meta.if_nomatch = if_nomatch;
  obj_op.meta.mtime = mtime;
  obj_op.meta.set_mtime = set_mtime;
  obj_op.meta.owner = bucket_info.owner;
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.olh_epoch = olh_epoch;
  obj_op.meta.delete_at = delete_at;

  r = obj_op.write_meta(obj_len, attrs);
  if (r < 0) {
    return r;
  }

  canceled = obj_op.meta.canceled;

  return 0;
}

int RGWRados::watch(const string& oid, uint64_t *watch_handle, librados::WatchCtx2 *ctx) {
  int r = control_pool_ctx.watch2(oid, watch_handle, ctx);
  if (r < 0)
    return r;
  return 0;
}

int RGWRados::unwatch(uint64_t watch_handle)
{
  int r = control_pool_ctx.unwatch2(watch_handle);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->unwatch2() returned r=" << r << dendl;
    return r;
  }
  r = rados[0]->watch_flush();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->watch_flush() returned r=" << r << dendl;
    return r;
  }
  return 0;
}

void RGWRados::add_watcher(int i)
{
  ldout(cct, 20) << "add_watcher() i=" << i << dendl;
  Mutex::Locker l(watchers_lock);
  watchers_set.insert(i);
  if (watchers_set.size() ==  (size_t)num_watchers) {
    ldout(cct, 2) << "all " << num_watchers << " watchers are set, enabling cache" << dendl;
    set_cache_enabled(true);
  }
}

void RGWRados::remove_watcher(int i)
{
  ldout(cct, 20) << "remove_watcher() i=" << i << dendl;
  Mutex::Locker l(watchers_lock);
  size_t orig_size = watchers_set.size();
  watchers_set.erase(i);
  if (orig_size == (size_t)num_watchers &&
      watchers_set.size() < orig_size) { /* actually removed */
    ldout(cct, 2) << "removed watcher, disabling cache" << dendl;
    set_cache_enabled(false);
  }
}

class RGWWatcher : public librados::WatchCtx2 {
  RGWRados *rados;
  int index;
  string oid;
  uint64_t watch_handle;

  class C_ReinitWatch : public Context {
    RGWWatcher *watcher;
    public:
      explicit C_ReinitWatch(RGWWatcher *_watcher) : watcher(_watcher) {}
      void finish(int r) {
        watcher->reinit();
      }
  };
public:
  RGWWatcher(RGWRados *r, int i, const string& o) : rados(r), index(i), oid(o), watch_handle(0) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist& bl) {
    ldout(rados->ctx(), 10) << "RGWWatcher::handle_notify() "
			    << " notify_id " << notify_id
			    << " cookie " << cookie
			    << " notifier " << notifier_id
			    << " bl.length()=" << bl.length() << dendl;
    rados->watch_cb(notify_id, cookie, notifier_id, bl);

    bufferlist reply_bl; // empty reply payload
    rados->control_pool_ctx.notify_ack(oid, notify_id, cookie, reply_bl);
  }
  void handle_error(uint64_t cookie, int err) {
    lderr(rados->ctx()) << "RGWWatcher::handle_error cookie " << cookie
			<< " err " << cpp_strerror(err) << dendl;
    rados->remove_watcher(index);
    rados->schedule_context(new C_ReinitWatch(this));
  }

  void reinit() {
    int ret = unregister_watch();
    if (ret < 0) {
      ldout(rados->ctx(), 0) << "ERROR: unregister_watch() returned ret=" << ret << dendl;
      return;
    }
    ret = register_watch();
    if (ret < 0) {
      ldout(rados->ctx(), 0) << "ERROR: register_watch() returned ret=" << ret << dendl;
      return;
    }
  }

  int unregister_watch() {
    int r = rados->unwatch(watch_handle);
    if (r < 0) {
      return r;
    }
    rados->remove_watcher(index);
    return 0;
  }

  int register_watch() {
    int r = rados->watch(oid, &watch_handle, this);
    if (r < 0) {
      return r;
    }
    rados->add_watcher(index);
    return 0;
  }
};

RGWObjState *RGWObjectCtx::get_state(rgw_obj& obj) {
  RGWObjState *result;
  map<rgw_obj, RGWObjState>::iterator iter;
  lock.get_read();
  if (!obj.get_object().empty()) {
    iter = objs_state.find(obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[obj];
      lock.unlock();
    }
    return result;
  } else {
    rgw_obj new_obj(store->get_zone_params().domain_root, obj.bucket.name);
    iter = objs_state.find(new_obj);
    if (iter != objs_state.end()) {
      result = &iter->second;
      lock.unlock();
    } else {
      lock.unlock();
      lock.get_write();
      result = &objs_state[new_obj];
      lock.unlock();
    }
    return result;
  }
}

void RGWObjectCtx::invalidate(rgw_obj& obj)
{
  RWLock::WLocker wl(lock);
  objs_state.erase(obj);
}

void RGWObjectCtx::set_atomic(rgw_obj& obj) {
  RWLock::WLocker wl(lock);
  if (!obj.get_object().empty()) {
    objs_state[obj].is_atomic = true;
  } else {
    rgw_obj new_obj(store->get_zone_params().domain_root, obj.bucket.name);
    objs_state[new_obj].is_atomic = true;
  }
}

void RGWObjectCtx::set_prefetch_data(rgw_obj& obj) {
  RWLock::WLocker wl(lock);
  if (!obj.get_object().empty()) {
    objs_state[obj].prefetch_data = true;
  } else {
    rgw_obj new_obj(store->get_zone_params().domain_root, obj.bucket.name);
    objs_state[new_obj].prefetch_data = true;
  }
}

class RGWMetaNotifierManager : public RGWCoroutinesManager {
  RGWRados *store;
  RGWHTTPManager http_manager;

public:
  RGWMetaNotifierManager(RGWRados *_store) : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()), store(_store),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.set_threaded();
  }

  int notify_all(map<string, RGWRESTConn *>& conn_map, set<int>& shards) {
    rgw_http_param_pair pairs[] = { { "type", "metadata" },
                                    { "notify", NULL },
                                    { NULL, NULL } };

    list<RGWCoroutinesStack *> stacks;
    for (map<string, RGWRESTConn *>::iterator iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWPostRESTResourceCR<set<int>, int>(store->ctx(), conn, &http_manager, "/admin/log", pairs, shards, NULL));

      stacks.push_back(stack);
    }
    return run(stacks);
  }
};

class RGWDataNotifierManager : public RGWCoroutinesManager {
  RGWRados *store;
  RGWHTTPManager http_manager;

public:
  RGWDataNotifierManager(RGWRados *_store) : RGWCoroutinesManager(_store->ctx(), _store->get_cr_registry()), store(_store),
                                             http_manager(store->ctx(), completion_mgr) {
    http_manager.set_threaded();
  }

  int notify_all(map<string, RGWRESTConn *>& conn_map, map<int, set<string> >& shards) {
    rgw_http_param_pair pairs[] = { { "type", "data" },
                                    { "notify", NULL },
                                    { "source-zone", store->get_zone_params().get_id().c_str() },
                                    { NULL, NULL } };

    list<RGWCoroutinesStack *> stacks;
    for (map<string, RGWRESTConn *>::iterator iter = conn_map.begin(); iter != conn_map.end(); ++iter) {
      RGWRESTConn *conn = iter->second;
      RGWCoroutinesStack *stack = new RGWCoroutinesStack(store->ctx(), this);
      stack->call(new RGWPostRESTResourceCR<map<int, set<string> >, int>(store->ctx(), conn, &http_manager, "/admin/log", pairs, shards, NULL));

      stacks.push_back(stack);
    }
    return run(stacks);
  }
};

class RGWRadosThread {
  class Worker : public Thread {
    CephContext *cct;
    RGWRadosThread *processor;
    Mutex lock;
    Cond cond;

  public:
    Worker(CephContext *_cct, RGWRadosThread *_p) : cct(_cct), processor(_p), lock("RGWRadosThread::Worker") {}
    void *entry();
    void stop() {
      Mutex::Locker l(lock);
      cond.Signal();
    }
  };

  Worker *worker;

protected:
  CephContext *cct;
  RGWRados *store;

  atomic_t down_flag;

  virtual uint64_t interval_msec() = 0;
  virtual void stop_process() {}
public:
  RGWRadosThread(RGWRados *_store) : worker(NULL), cct(_store->ctx()), store(_store) {}
  virtual ~RGWRadosThread() {
    stop();
  }

  virtual int init() { return 0; }
  virtual int process() = 0;

  bool going_down() { return down_flag.read() != 0; }
  void start();
  void stop();
};

void RGWRadosThread::start()
{
  worker = new Worker(cct, this);
  worker->create("radosgw");
}

void RGWRadosThread::stop()
{
  down_flag.set(1);
  stop_process();
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void *RGWRadosThread::Worker::entry() {
  uint64_t msec = processor->interval_msec();
  utime_t interval = utime_t(msec / 1000, (msec % 1000) * 1000000);

  do {
    utime_t start = ceph_clock_now(cct);
    int r = processor->process();
    if (r < 0) {
      dout(0) << "ERROR: processor->process() returned error r=" << r << dendl;
    }

    if (processor->going_down())
      break;

    utime_t end = ceph_clock_now(cct);
    end -= start;

    uint64_t cur_msec = processor->interval_msec();
    if (cur_msec != msec) { /* was it reconfigured? */
      msec = cur_msec;
      interval = utime_t(msec / 1000, (msec % 1000) * 1000000);
    }

    if (cur_msec > 0) {
      if (interval <= end)
        continue; // next round

      utime_t wait_time = interval;
      wait_time -= end;

      lock.Lock();
      cond.WaitInterval(cct, lock, wait_time);
      lock.Unlock();
    } else {
      lock.Lock();
      cond.Wait(lock);
      lock.Unlock();
    }
  } while (!processor->going_down());

  return NULL;
}

class RGWMetaNotifier : public RGWRadosThread {
  RGWMetaNotifierManager notify_mgr;
  RGWMetadataLog *const log;

  uint64_t interval_msec() {
    return cct->_conf->rgw_md_notify_interval_msec;
  }
public:
  RGWMetaNotifier(RGWRados *_store, RGWMetadataLog* log)
    : RGWRadosThread(_store), notify_mgr(_store), log(log) {}

  int process();
};

int RGWMetaNotifier::process()
{
  set<int> shards;

  log->read_clear_modified(shards);

  if (shards.empty()) {
    return 0;
  }

  for (set<int>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
    ldout(cct, 20) << __func__ << "(): notifying mdlog change, shard_id=" << *iter << dendl;
  }

  notify_mgr.notify_all(store->zonegroup_conn_map, shards);

  return 0;
}

class RGWDataNotifier : public RGWRadosThread {
  RGWDataNotifierManager notify_mgr;

  uint64_t interval_msec() {
    return cct->_conf->rgw_md_notify_interval_msec;
  }
public:
  RGWDataNotifier(RGWRados *_store) : RGWRadosThread(_store), notify_mgr(_store) {}

  int process();
};

int RGWDataNotifier::process()
{
  if (!store->data_log) {
    return 0;
  }

  map<int, set<string> > shards;

  store->data_log->read_clear_modified(shards);

  if (shards.empty()) {
    return 0;
  }

  for (map<int, set<string> >::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
    ldout(cct, 20) << __func__ << "(): notifying datalog change, shard_id=" << iter->first << ": " << iter->second << dendl;
  }

  notify_mgr.notify_all(store->zone_conn_map, shards);

  return 0;
}

class RGWSyncProcessorThread : public RGWRadosThread {
public:
  RGWSyncProcessorThread(RGWRados *_store) : RGWRadosThread(_store) {}
  virtual ~RGWSyncProcessorThread() {}
  virtual int init() = 0 ;
  virtual int process() = 0;
};

class RGWMetaSyncProcessorThread : public RGWSyncProcessorThread
{
  RGWMetaSyncStatusManager sync;

  uint64_t interval_msec() {
    return 0; /* no interval associated, it'll run once until stopped */
  }
  void stop_process() {
    sync.stop();
  }
public:
  RGWMetaSyncProcessorThread(RGWRados *_store, RGWAsyncRadosProcessor *async_rados)
    : RGWSyncProcessorThread(_store), sync(_store, async_rados) {}

  void wakeup_sync_shards(set<int>& shard_ids) {
    for (set<int>::iterator iter = shard_ids.begin(); iter != shard_ids.end(); ++iter) {
      sync.wakeup(*iter);
    }
  }

  int init() {
    int ret = sync.init();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: sync.init() returned " << ret << dendl;
      return ret;
    }
    return 0;
  }

  int process() {
    sync.run();
    return 0;
  }
};

class RGWDataSyncProcessorThread : public RGWSyncProcessorThread
{
  RGWDataSyncStatusManager sync;
  bool initialized;

  uint64_t interval_msec() {
    if (initialized) {
      return 0; /* no interval associated, it'll run once until stopped */
    } else {
#define DATA_SYNC_INIT_WAIT_SEC 20
      return DATA_SYNC_INIT_WAIT_SEC * 1000;
    }
  }
  void stop_process() {
    sync.stop();
  }
public:
  RGWDataSyncProcessorThread(RGWRados *_store, RGWAsyncRadosProcessor *async_rados,
                             const string& _source_zone)
    : RGWSyncProcessorThread(_store), sync(_store, async_rados, _source_zone),
      initialized(false) {}

  void wakeup_sync_shards(map<int, set<string> >& shard_ids) {
    for (map<int, set<string> >::iterator iter = shard_ids.begin(); iter != shard_ids.end(); ++iter) {
      sync.wakeup(iter->first, iter->second);
    }
  }


  int init() {
    return 0;
  }

  int process() {
    while (!going_down()) {
      int ret = sync.init();
      if (ret >= 0) {
        initialized = true;
        break;
      }
      /* we'll be back! */
      return 0;
    }
    sync.run();
    return 0;
  }
};

void RGWRados::wakeup_meta_sync_shards(set<int>& shard_ids)
{
  Mutex::Locker l(meta_sync_thread_lock);
  if (meta_sync_processor_thread) {
    meta_sync_processor_thread->wakeup_sync_shards(shard_ids);
  }
}

void RGWRados::wakeup_data_sync_shards(const string& source_zone, map<int, set<string> >& shard_ids)
{
  ldout(ctx(), 20) << __func__ << ": source_zone=" << source_zone << ", shard_ids=" << shard_ids << dendl;
  Mutex::Locker l(data_sync_thread_lock);
  map<string, RGWDataSyncProcessorThread *>::iterator iter = data_sync_processor_threads.find(source_zone);
  if (iter == data_sync_processor_threads.end()) {
    ldout(ctx(), 10) << __func__ << ": couldn't find sync thread for zone " << source_zone << ", skipping async data sync processing" << dendl;
    return;
  }

  RGWDataSyncProcessorThread *thread = iter->second;
  assert(thread);
  thread->wakeup_sync_shards(shard_ids);
}

int RGWRados::get_required_alignment(rgw_bucket& bucket, uint64_t *alignment)
{
  IoCtx ioctx;
  int r = open_bucket_data_ctx(bucket, ioctx);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: open_bucket_data_ctx() returned " << r << dendl;
    return r;
  }

  bool requires;
  r = ioctx.pool_requires_alignment2(&requires);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: ioctx.pool_requires_alignment2() returned " 
      << r << dendl;
    return r;
  }

  if (!requires) {
    *alignment = 0;
    return 0;
  }

  uint64_t align;
  r = ioctx.pool_required_alignment2(&align);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: ioctx.pool_required_alignment2() returned " 
      << r << dendl;
    return r;
  }
  *alignment = align;
  return 0;
}

int RGWRados::get_max_chunk_size(rgw_bucket& bucket, uint64_t *max_chunk_size)
{
  uint64_t alignment;
  int r = get_required_alignment(bucket, &alignment);
  if (r < 0) {
    return r;
  }

  uint64_t config_chunk_size = cct->_conf->rgw_max_chunk_size;

  if (alignment == 0) {
    *max_chunk_size = config_chunk_size;
    return 0;
  }

  if (config_chunk_size <= alignment) {
    *max_chunk_size = alignment;
    return 0;
  }

  *max_chunk_size = config_chunk_size - (config_chunk_size % alignment);

  return 0;
}

void RGWRados::finalize()
{
  if (run_sync_thread) {
    Mutex::Locker l(meta_sync_thread_lock);
    meta_sync_processor_thread->stop();

    Mutex::Locker dl(data_sync_thread_lock);
    for (auto iter : data_sync_processor_threads) {
      RGWDataSyncProcessorThread *thread = iter.second;
      thread->stop();
    }
  }
  if (async_rados) {
    async_rados->stop();
  }
  if (run_sync_thread) {
    delete meta_sync_processor_thread;
    meta_sync_processor_thread = NULL;
    Mutex::Locker dl(data_sync_thread_lock);
    for (auto iter : data_sync_processor_threads) {
      RGWDataSyncProcessorThread *thread = iter.second;
      delete thread;
    }
    data_sync_processor_threads.clear();
  }
  if (finisher) {
    finisher->stop();
  }
  if (need_watch_notify()) {
    finalize_watch();
  }
  if (finisher) {
    /* delete finisher only after cleaning up watches, as watch error path might call
     * into finisher. We stop finisher before finalizing watch to make sure we don't
     * actually handle any racing work
     */
    delete finisher;
  }
  if (meta_notifier) {
    meta_notifier->stop();
    delete meta_notifier;
  }
  if (data_notifier) {
    data_notifier->stop();
    delete data_notifier;
  }
  delete meta_mgr;
  delete data_log;
  if (async_rados) {
    delete async_rados;
  }
  if (use_gc_thread) {
    gc->stop_processor();
    obj_expirer->stop_processor();
  }
  delete gc;
  gc = NULL;

  delete obj_expirer;
  obj_expirer = NULL;

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
  RGWQuotaHandler::free_handler(quota_handler);
  if (cr_registry) {
    cr_registry->put();
  }
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_rados()
{
  int ret = 0;

  num_rados_handles = cct->_conf->rgw_num_rados_handles;

  rados = new librados::Rados *[num_rados_handles];
  if (!rados) {
    ret = -ENOMEM;
    return ret;
  }

  for (uint32_t i=0; i < num_rados_handles; i++) {

    rados[i] = new Rados();
    if (!rados[i]) {
      ret = -ENOMEM;
      goto fail;
    }

    ret = rados[i]->init_with_context(cct);
    if (ret < 0) {
      goto fail;
    }

    ret = rados[i]->connect();
    if (ret < 0) {
      goto fail;
    }
  }

  cr_registry = new RGWCoroutinesManagerRegistry(cct);
  ret =  cr_registry->hook_to_admin_command("cr dump");
  if (ret < 0) {
    goto fail;
  }

  meta_mgr = new RGWMetadataManager(cct, this);
  data_log = new RGWDataChangesLog(cct, this);

  return ret;

fail:
  for (uint32_t i=0; i < num_rados_handles; i++) {
    if (rados[i]) {
      delete rados[i];
      rados[i] = NULL;
    }
  }
  num_rados_handles = 0;
  if (rados) {
    delete[] rados;
    rados = NULL;
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

int RGWRados::convert_regionmap()
{
  RGWZoneGroupMap zonegroupmap;

  string pool_name = cct->_conf->rgw_zone_root_pool;
  if (pool_name.empty()) {
    pool_name = RGW_DEFAULT_ZONE_ROOT_POOL;
  }
  string oid = region_map_oid; 

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;
  RGWObjectCtx obj_ctx(this);
  int ret = rgw_get_system_obj(this, obj_ctx, pool, oid, bl, NULL, NULL);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  } else if (ret == -ENOENT) {
    return 0;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(zonegroupmap, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "error decoding regionmap from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  
  for (map<string, RGWZoneGroup>::iterator iter = zonegroupmap.zonegroups.begin();
       iter != zonegroupmap.zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    ret = zonegroup.init(cct, this, false);
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

  current_period.set_user_quota(zonegroupmap.user_quota);
  current_period.set_bucket_quota(zonegroupmap.bucket_quota);

  return 0;
}

/** 
 * Replace all region configuration with zonegroup for
 * backward compatability
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::replace_region_with_zonegroup()
{
  if (!cct->_conf->rgw_region.empty() && cct->_conf->rgw_zonegroup.empty()) {
    int ret = cct->_conf->set_val("rgw_zonegroup", cct->_conf->rgw_region, true, false);
    if (ret < 0) {
      ldout(cct, 0) << "failed to set rgw_zonegroup to " << cct->_conf->rgw_region << dendl;
      return ret;
    }
  }

  /* copy default region */
  /* convert default region to default zonegroup */
  string default_oid = cct->_conf->rgw_default_region_info_oid;
  if (default_oid.empty()) {
    default_oid = default_region_info_oid;
  }

  string default_region;
  RGWZoneGroup default_zonegroup;
  int ret = default_zonegroup.init(cct, this, false, true);
  if (ret < 0) {
    ldout(cct, 0) << "failed init default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }    
  ret  = default_zonegroup.read_default_id(default_region, true);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading old default region: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* convert regions to zonegroups */
  list<string> regions;
  ret = list_regions(regions);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed to list regions: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT || regions.empty()) {
    return 0;
  }

  string master_region, master_zone;
  for (list<string>::iterator iter = regions.begin(); iter != regions.end(); ++iter) {
    if (*iter != default_zonegroup_name){
      RGWZoneGroup region(*iter);
      int ret = region.init(cct, this, true, true);
      if (ret < 0) {
	  ldout(cct, 0) << "failed init region "<< *iter << ": " << cpp_strerror(-ret) << dendl;
	  return ret;
      }
      if (region.is_master) {
	master_region = region.get_id();
	master_zone = region.master_zone;
      }
    }
  }

  /* create realm if there is none.
     The realm name will be the region and zone concatenated
     realm id will be mds of its name */
  if (realm.get_id().empty() && !master_region.empty() && !master_zone.empty()) {
    string new_realm_name = master_region + "." + master_zone;
    unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
    MD5 hash;
    hash.Update((const byte *)new_realm_name.c_str(), new_realm_name.length());
    hash.Final(md5);
    buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
    string new_realm_id(md5_str);
    RGWRealm new_realm(new_realm_id,new_realm_name);
    ret = new_realm.init(cct, this, false);
    if (ret < 0) {
      ldout(cct, 0) << "Error initing new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.create();
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 0) << "Error creating new realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = new_realm.set_as_default();
    if (ret < 0) {
      ldout(cct, 0) << "Error setting realm as default: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = realm.init(cct, this);
    if (ret < 0) {
      ldout(cct, 0) << "Error initing realm: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
    ret = current_period.init(cct, this, realm.get_id(), realm.get_name());
    if (ret < 0) {
      ldout(cct, 0) << "Error initing current period: " << cpp_strerror(-ret)  << dendl;
      return ret;
    }
  }

  list<string>::iterator iter;
  /* create zonegroups */
  for (iter = regions.begin(); iter != regions.end(); ++iter)
  {
    /* read region info default has no data */
    if (*iter != default_zonegroup_name){
      RGWZoneGroup zonegroup(*iter);
      int ret = zonegroup.init(cct, this, true, true);
      if (ret < 0) {
	ldout(cct, 0) << "failed init zonegroup: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
	return ret;
      }
      zonegroup.realm_id = realm.get_id();
      ret = zonegroup.update();
      if (ret < 0 && ret != -EEXIST) {
	ldout(cct, 0) << "failed to update zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
		   << dendl;
	return ret;
      }
      ret = zonegroup.update_name();
      if (ret < 0 && ret != -EEXIST) {
	ldout(cct, 0) << "failed to update_name for zonegroup " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
		   << dendl;
	return ret;
      }
      if (zonegroup.get_name() == default_region) {
	ret = zonegroup.set_as_default();
	if (ret < 0) {
	  ldout(cct, 0) << "failed to set_as_default " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
		     << dendl;
	  return ret;
	}
      }
      for (map<string, RGWZone>::const_iterator iter = zonegroup.zones.begin(); iter != zonegroup.zones.end();
	   iter ++) {
	RGWZoneParams zoneparams(iter->first, iter->first);
	ret = zoneparams.init(cct, this);
	if (ret < 0) {
	  ldout(cct, 0) << "failed to init zoneparams  " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
	zonegroup.realm_id = realm.get_id();
	ret = zoneparams.update();
	if (ret < 0 && ret != -EEXIST) {
	  ldout(cct, 0) << "failed to update zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
	ret = zoneparams.update_name();
	if (ret < 0 && ret != -EEXIST) {
	  ldout(cct, 0) << "failed to init zoneparams " << iter->first <<  ": " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      }

      if (!current_period.get_id().empty()) {
	ret = current_period.add_zonegroup(zonegroup);
	if (ret < 0) {
	  ldout(cct, 0) << "failed to add zonegroup to current_period: " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
	ret = current_period.update();
	if (ret < 0) {
	  ldout(cct, 0) << "failed to update current_period: " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      }

      ret = zonegroup.delete_obj(true);
      if (ret < 0 && ret != -ENOENT) {
	ldout(cct, 0) << "failed to delete region " << *iter << ": ret "<< ret << " " << cpp_strerror(-ret)
		   << dendl;
	return ret;
      }
    }
  }

  return 0;
}

int RGWRados::init_zg_from_period(bool *initialized)
{
  *initialized = false;

  if (current_period.get_id().empty()) {
    return 0;
  }

  int ret = zonegroup.init(cct, this);
  ldout(cct, 20) << "period zonegroup init ret " << ret << dendl;
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    ldout(cct, 0) << "failed reading zonegroup info: " << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ldout(cct, 20) << "period zonegroup name " << zonegroup.get_name() << dendl;

  map<string, RGWZoneGroup>::const_iterator iter =
    current_period.get_map().zonegroups.find(zonegroup.get_id());

  if (iter != current_period.get_map().zonegroups.end()) {
    ldout(cct, 20) << "using current period zonegroup " << zonegroup.get_name() << dendl;
    zonegroup = iter->second;
    ret = zone_params.init(cct, this);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "failed reading zone params info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  for (iter = current_period.get_map().zonegroups.begin();
       iter != current_period.get_map().zonegroups.end(); ++iter){
    const RGWZoneGroup& zg = iter->second;
    add_new_connection_to_map(zonegroup_conn_map, zg, new RGWRESTConn(cct, this, zg.get_id(), zg.endpoints));
    if (!current_period.get_master_zonegroup().empty() &&
        zg.get_id() == current_period.get_master_zonegroup()) {
      rest_master_conn = new RGWRESTConn(cct, this, zg.get_id(), zg.endpoints);
    }
  }

  *initialized = true;

  return 0;
}

int RGWRados::init_zg_from_local(bool *creating_defaults)
{
  int ret = zonegroup.init(cct, this);
  if ( (ret < 0 && ret != -ENOENT) || (ret == -ENOENT && !cct->_conf->rgw_zonegroup.empty())) {
    ldout(cct, 0) << "failed reading zonegroup info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret == -ENOENT) {
    *creating_defaults = true;
    ldout(cct, 10) << "Creating default zonegroup " << dendl;
    ret = zonegroup.create_default();
    if (ret < 0) {
      ldout(cct, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
    ret = zonegroup.init(cct, this);
    if (ret < 0) {
      ldout(cct, 0) << "failure in zonegroup create_default: ret "<< ret << " " << cpp_strerror(-ret)
        << dendl;
      return ret;
    }
  }
  ldout(cct, 20) << "zonegroup " << zonegroup.get_name() << dendl;
  if (zonegroup.is_master) {
    rest_master_conn = new RGWRESTConn(cct, this, zonegroup.get_id(), zonegroup.endpoints);
  }

  return 0;
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_complete()
{
  int ret = realm.init(cct, this);
  if (ret < 0 && ret != -ENOENT) {
    ldout(cct, 0) << "failed reading realm info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  } else if (ret != -ENOENT) {
    ldout(cct, 20) << "realm  " << realm.get_name() << " " << realm.get_id() << dendl;
    ret = current_period.init(cct, this, realm.get_id(), realm.get_name());
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "failed reading current period info: " << " " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ldout(cct, 20) << "current period " << current_period.get_id() << dendl;  
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

  if (!current_period.get_id().empty()) {
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
  }

  ldout(cct, 10) << "Cannot find current period zone using local zone" << dendl;
  if (creating_defaults && cct->_conf->rgw_zone.empty()) {
    ldout(cct, 10) << " Using default name "<< default_zone_name << dendl;
    zone_params.set_name(default_zone_name);
  }

  ret = zone_params.init(cct, this);
  if (ret < 0 && ret != -ENOENT) {
    lderr(cct) << "failed reading zone info: ret "<< ret << " " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  map<string, RGWZone>::iterator zone_iter = get_zonegroup().zones.find(zone_params.get_id());
  if (zone_iter == get_zonegroup().zones.end()) {
    if (using_local) {
      lderr(cct) << "Cannot find zone id=" << zone_params.get_id() << " (name=" << zone_params.get_name() << ")" << dendl;
      return -EINVAL;
    }
    ldout(cct, 0) << "Cannot find zone id=" << zone_params.get_id() << " (name=" << zone_params.get_name() << "), switching to local zonegroup configuration" << dendl;
    ret = init_zg_from_local(&creating_defaults);
    if (ret < 0) {
      return ret;
    }
    zone_iter = get_zonegroup().zones.find(zone_params.get_id());
  }
  if (zone_iter != get_zonegroup().zones.end()) {
    zone_public_config = zone_iter->second;
    ldout(cct, 20) << "zone " << zone_params.get_name() << dendl;
  } else {
    lderr(cct) << "Cannot find zone id=" << zone_params.get_id() << " (name=" << zone_params.get_name() << ")" << dendl;
    return -EINVAL;
  }

  zone_short_id = current_period.get_map().get_zone_short_id(zone_params.get_id());

  init_unique_trans_id_deps();

  finisher = new Finisher(cct);
  finisher->start();

  period_puller.reset(new RGWPeriodPuller(this));
  period_history.reset(new RGWPeriodHistory(cct, period_puller.get(),
                                            current_period));

  if (need_watch_notify()) {
    ret = init_watch();
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to initialize watch: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  map<string, RGWZone>::iterator ziter;
  for (ziter = get_zonegroup().zones.begin(); ziter != get_zonegroup().zones.end(); ++ziter) {
    const string& id = ziter->first;
    RGWZone& z = ziter->second;
    zone_id_by_name[z.name] = id;
    zone_name_by_id[id] = z.name;
    if (id != zone_id()) {
      if (!z.endpoints.empty()) {
        ldout(cct, 20) << "generating connection object for zone " << z.name << " id " << z.id << dendl;
        RGWRESTConn *conn = new RGWRESTConn(cct, this, z.id, z.endpoints);
        zone_conn_map[id] = conn;
      } else {
        ldout(cct, 0) << "WARNING: can't generate connection for zone " << z.id << " id " << z.name << ": no endpoints defined" << dendl;
      }
    } else {
      zone_public_config = z;
    }
  }

  ret = open_root_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_gc_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_objexp_pool_ctx();
  if (ret < 0)
    return ret;

  pools_initialized = true;

  gc = new RGWGC();
  gc->initialize(cct, this);

  obj_expirer = new RGWObjectExpirer(this);

  if (use_gc_thread) {
    gc->start_processor();
    obj_expirer->start_processor();
  }

  /* not point of running sync thread if there is a single zone or
     we don't have a master zone configured or there is no rest_master_conn */
  if (get_zonegroup().zones.size() < 2 || get_zonegroup().master_zone.empty() || !rest_master_conn) {
    run_sync_thread = false;
  }

  async_rados = new RGWAsyncRadosProcessor(this, cct->_conf->rgw_num_async_rados_threads);
  async_rados->start();

  ret = meta_mgr->init(current_period.get_id());
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to initialize metadata log: "
        << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (is_meta_master()) {
    auto md_log = meta_mgr->get_log(current_period.get_id());
    meta_notifier = new RGWMetaNotifier(this, md_log);
    meta_notifier->start();
  }

  if (run_sync_thread) {
    Mutex::Locker l(meta_sync_thread_lock);
    meta_sync_processor_thread = new RGWMetaSyncProcessorThread(this, async_rados);
    ret = meta_sync_processor_thread->init();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to initialize" << dendl;
      return ret;
    }
    meta_sync_processor_thread->start();

    Mutex::Locker dl(data_sync_thread_lock);
    for (map<string, RGWRESTConn *>::iterator iter = zone_conn_map.begin(); iter != zone_conn_map.end(); ++iter) {
      ldout(cct, 5) << "starting data sync thread for zone " << iter->first << dendl;
      RGWDataSyncProcessorThread *thread = new RGWDataSyncProcessorThread(this, async_rados, iter->first);
      ret = thread->init();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: failed to initialize" << dendl;
        return ret;
      }
      thread->start();
      data_sync_processor_threads[iter->first] = thread;
    }
  }
  data_notifier = new RGWDataNotifier(this);
  data_notifier->start();

  quota_handler = RGWQuotaHandler::generate_handler(this, quota_threads);

  bucket_index_max_shards = (cct->_conf->rgw_override_bucket_index_max_shards ? cct->_conf->rgw_override_bucket_index_max_shards :
                             get_zone().bucket_index_max_shards);
  if (bucket_index_max_shards > MAX_BUCKET_INDEX_SHARDS_PRIME) {
    bucket_index_max_shards = MAX_BUCKET_INDEX_SHARDS_PRIME;
    ldout(cct, 1) << __func__ << " bucket index max shards is too large, reset to value: "
      << MAX_BUCKET_INDEX_SHARDS_PRIME << dendl;
  }
  ldout(cct, 20) << __func__ << " bucket index max shards: " << bucket_index_max_shards << dendl;

  binfo_cache.init(this);

  return ret;
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize()
{
  int ret;

  ret = init_rados();
  if (ret < 0)
    return ret;

  ret = init_complete();

  return ret;
}

void RGWRados::finalize_watch()
{
  for (int i = 0; i < num_watchers; i++) {
    RGWWatcher *watcher = watchers[i];
    watcher->unregister_watch();
    delete watcher;
  }

  delete[] notify_oids;
  delete[] watchers;
}

void RGWRados::schedule_context(Context *c) {
  finisher->queue(c);
}

int RGWRados::list_raw_prefixed_objs(const string& pool_name, const string& prefix, list<string>& result)
{
  rgw_bucket pool(pool_name.c_str());
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<string> oids;
    int r = list_raw_objects(pool, prefix, 1000,
			     ctx, oids, &is_truncated);
    if (r < 0) {
      return r;
    }
    list<string>::iterator iter;
    for (iter = oids.begin(); iter != oids.end(); ++iter) {
      string& val = *iter;
      if (val.size() > prefix.size())
        result.push_back(val.substr(prefix.size()));
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::list_regions(list<string>& regions)
{
  RGWZoneGroup zonegroup;

  return list_raw_prefixed_objs(zonegroup.get_pool_name(cct), region_info_oid_prefix, regions);
}

int RGWRados::list_zonegroups(list<string>& zonegroups)
{
  RGWZoneGroup zonegroup;

  return list_raw_prefixed_objs(zonegroup.get_pool_name(cct), zonegroup_names_oid_prefix, zonegroups);
}

int RGWRados::list_zones(list<string>& zones)
{
  RGWZoneParams zoneparams;
  string pool_name = zoneparams.get_pool_name(cct);

  return list_raw_prefixed_objs(pool_name, zone_names_oid_prefix, zones);
}

int RGWRados::list_realms(list<string>& realms)
{
  RGWRealm realm(cct, this);
  string pool_name = realm.get_pool_name(cct);
  return list_raw_prefixed_objs(pool_name, realm_names_oid_prefix, realms);
}

int RGWRados::list_periods(list<string>& periods)
{
  RGWPeriod period;
  list<string> raw_periods;
  int ret = list_raw_prefixed_objs(period.get_pool_name(cct), period.get_info_oid_prefix(), raw_periods);
  if (ret < 0) {
    return ret;
  }
  for(list<string>::iterator iter = raw_periods.begin(); iter != raw_periods.end(); iter++) {
    size_t pos = iter->find(".");
    if ( pos != std::string::npos) {
      periods.push_back(iter->substr(0, pos));
    } else {
      periods.push_back(*iter);
    }
  }
  periods.unique();
  return 0;
}


int RGWRados::list_periods(const string& current_period, list<string>& periods)
{
  int ret = 0;
  string period_id = current_period;
  while(!period_id.empty()) {
    RGWPeriod period(period_id);
    ret = period.init(cct, this);
    if (ret < 0) {
      return ret;
    }
    periods.push_back(period.get_id());
    period_id = period.get_predecessor();
  }
  
  return ret;
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx()
{
  const string& pool = get_zone_params().domain_root.name;
  const char *pool_str = pool.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(pool_str, root_pool_ctx);
  if (r == -ENOENT) {
    r = rad->pool_create(pool_str);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rad->ioctx_create(pool_str, root_pool_ctx);
  }

  return r;
}

int RGWRados::open_gc_pool_ctx()
{
  const char *gc_pool = get_zone_params().gc_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(gc_pool, gc_pool_ctx);
  if (r == -ENOENT) {
    r = rad->pool_create(gc_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rad->ioctx_create(gc_pool, gc_pool_ctx);
  }

  return r;
}

int RGWRados::open_objexp_pool_ctx()
{
  const char * const pool_name = get_zone_params().log_pool.name.c_str();
  librados::Rados * const rad = get_rados_handle();
  int r = rad->ioctx_create(pool_name, objexp_pool_ctx);
  if (r == -ENOENT) {
    r = rad->pool_create(pool_name);
    if (r == -EEXIST) {
      r = 0;
    } else if (r < 0) {
      return r;
    }

    r = rad->ioctx_create(pool_name, objexp_pool_ctx);
  }

  return r;
}

int RGWRados::init_watch()
{
  const char *control_pool = get_zone_params().control_pool.name.c_str();

  librados::Rados *rad = rados[0];
  int r = rad->ioctx_create(control_pool, control_pool_ctx);

  if (r == -ENOENT) {
    r = rad->pool_create(control_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rad->ioctx_create(control_pool, control_pool_ctx);
    if (r < 0)
      return r;
  }

  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  notify_oids = new string[num_watchers];
  watchers = new RGWWatcher *[num_watchers];

  for (int i=0; i < num_watchers; i++) {
    string& notify_oid = notify_oids[i];
    notify_oid = notify_oid_prefix;
    if (!compat_oid) {
      char buf[16];
      snprintf(buf, sizeof(buf), ".%d", i);
      notify_oid.append(buf);
    }
    r = control_pool_ctx.create(notify_oid, false);
    if (r < 0 && r != -EEXIST)
      return r;

    RGWWatcher *watcher = new RGWWatcher(this, i, notify_oid);
    watchers[i] = watcher;

    r = watcher->register_watch();
    if (r < 0)
      return r;
  }

  watch_initialized = true;

  set_cache_enabled(true);

  return 0;
}

void RGWRados::pick_control_oid(const string& key, string& notify_oid)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  char buf[16];
  snprintf(buf, sizeof(buf), ".%d", i);

  notify_oid = notify_oid_prefix;
  notify_oid.append(buf);
}

int RGWRados::open_pool_ctx(const string& pool, librados::IoCtx&  io_ctx)
{
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(pool.c_str(), io_ctx);
  if (r != -ENOENT)
    return r;

  if (!pools_initialized)
    return r;

  r = rad->pool_create(pool.c_str());
  if (r < 0 && r != -EEXIST)
    return r;

  r = rad->ioctx_create(pool.c_str(), io_ctx);

  return r;
}

int RGWRados::open_bucket_data_ctx(rgw_bucket& bucket, librados::IoCtx& data_ctx)
{
  int r = open_pool_ctx(bucket.data_pool, data_ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::open_bucket_data_extra_ctx(rgw_bucket& bucket, librados::IoCtx& data_ctx)
{
  string& pool = (!bucket.data_extra_pool.empty() ? bucket.data_extra_pool : bucket.data_pool);
  int r = open_pool_ctx(pool, data_ctx);
  if (r < 0)
    return r;

  return 0;
}

void RGWRados::build_bucket_index_marker(const string& shard_id_str, const string& shard_marker,
      string *marker) {
  if (marker) {
    *marker = shard_id_str;
    marker->append(BucketIndexShardsManager::KEY_VALUE_SEPARATOR);
    marker->append(shard_marker);
  }
}

int RGWRados::open_bucket_index_ctx(rgw_bucket& bucket, librados::IoCtx& index_ctx)
{
  int r = open_pool_ctx(bucket.index_pool, index_ctx);
  if (r < 0)
    return r;

  return 0;
}

/**
 * set up a bucket listing.
 * handle is filled in.
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_init(RGWAccessHandle *handle)
{
  librados::NObjectIterator *state = new librados::NObjectIterator(root_pool_ctx.nobjects_begin());
  *handle = (RGWAccessHandle)state;
  return 0;
}

/** 
 * get the next bucket in the listing.
 * obj is filled in,
 * handle is updated.
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_next(RGWObjEnt& obj, RGWAccessHandle *handle)
{
  librados::NObjectIterator *state = (librados::NObjectIterator *)*handle;

  do {
    if (*state == root_pool_ctx.nobjects_end()) {
      delete state;
      return -ENOENT;
    }

    obj.key.set((*state)->get_oid());
    (*state)++;
  } while (obj.key.name[0] == '.'); /* skip all entries starting with '.' */

  return 0;
}


/**** logs ****/

struct log_list_state {
  string prefix;
  librados::IoCtx io_ctx;
  librados::NObjectIterator obit;
};

int RGWRados::log_list_init(const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->prefix = prefix;
  state->obit = state->io_ctx.nobjects_begin();
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_list_next(RGWAccessHandle handle, string *name)
{
  log_list_state *state = static_cast<log_list_state *>(handle);
  while (true) {
    if (state->obit == state->io_ctx.nobjects_end()) {
      delete state;
      return -ENOENT;
    }
    if (state->prefix.length() &&
	state->obit->get_oid().find(state->prefix) != 0) {
      state->obit++;
      continue;
    }
    *name = state->obit->get_oid();
    state->obit++;
    break;
  }
  return 0;
}

int RGWRados::log_remove(const string& name)
{
  librados::IoCtx io_ctx;
  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;
  return io_ctx.remove(name);
}

struct log_show_state {
  librados::IoCtx io_ctx;
  bufferlist bl;
  bufferlist::iterator p;
  string name;
  uint64_t pos;
  bool eof;
  log_show_state() : pos(0), eof(false) {}
};

int RGWRados::log_show_init(const string& name, RGWAccessHandle *handle)
{
  log_show_state *state = new log_show_state;
  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->name = name;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_show_next(RGWAccessHandle handle, rgw_log_entry *entry)
{
  log_show_state *state = static_cast<log_show_state *>(handle);
  off_t off = state->p.get_off();

  ldout(cct, 10) << "log_show_next pos " << state->pos << " bl " << state->bl.length()
	   << " off " << off
	   << " eof " << (int)state->eof
	   << dendl;
  // read some?
  unsigned chunk = 1024*1024;
  if ((state->bl.length() - off) < chunk/2 && !state->eof) {
    bufferlist more;
    int r = state->io_ctx.read(state->name, more, chunk, state->pos);
    if (r < 0)
      return r;
    state->pos += r;
    bufferlist old;
    try {
      old.substr_of(state->bl, off, state->bl.length() - off);
    } catch (buffer::error& err) {
      return -EINVAL;
    }
    state->bl.clear();
    state->bl.claim(old);
    state->bl.claim_append(more);
    state->p = state->bl.begin();
    if ((unsigned)r < chunk)
      state->eof = true;
    ldout(cct, 10) << " read " << r << dendl;
  }

  if (state->p.end())
    return 0;  // end of file
  try {
    ::decode(*entry, state->p);
  }
  catch (const buffer::error &e) {
    return -EINVAL;
  }
  return 1;
}

/**
 * usage_log_hash: get usage log key hash, based on name and index
 *
 * Get the usage object name. Since a user may have more than 1
 * object holding that info (multiple shards), we use index to
 * specify that shard number. Once index exceeds max shards it
 * wraps.
 * If name is not being set, results for all users will be returned
 * and index will wrap only after total shards number.
 *
 * @param cct [in] ceph context
 * @param name [in] user name
 * @param hash [out] hash value
 * @param index [in] shard index number 
 */
static void usage_log_hash(CephContext *cct, const string& name, string& hash, uint32_t index)
{
  uint32_t val = index;

  if (!name.empty()) {
    int max_user_shards = max(cct->_conf->rgw_usage_max_user_shards, 1);
    val %= max_user_shards;
    val += ceph_str_hash_linux(name.c_str(), name.size());
  }
  char buf[16];
  int max_shards = max(cct->_conf->rgw_usage_max_shards, 1);
  snprintf(buf, sizeof(buf), RGW_USAGE_OBJ_PREFIX "%u", (unsigned)(val % max_shards));
  hash = buf;
}

int RGWRados::log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
  uint32_t index = 0;

  map<string, rgw_usage_log_info> log_objs;

  string hash;
  string last_user;

  /* restructure usage map, zone by object hash */
  map<rgw_user_bucket, RGWUsageBatch>::iterator iter;
  for (iter = usage_info.begin(); iter != usage_info.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    RGWUsageBatch& info = iter->second;

    if (ub.user.empty()) {
      ldout(cct, 0) << "WARNING: RGWRados::log_usage(): user name empty (bucket=" << ub.bucket << "), skipping" << dendl;
      continue;
    }

    if (ub.user != last_user) {
      /* index *should* be random, but why waste extra cycles
         in most cases max user shards is not going to exceed 1,
         so just incrementing it */
      usage_log_hash(cct, ub.user, hash, index++);
    }
    last_user = ub.user;
    vector<rgw_usage_log_entry>& v = log_objs[hash].entries;

    map<utime_t, rgw_usage_log_entry>::iterator miter;
    for (miter = info.m.begin(); miter != info.m.end(); ++miter) {
      v.push_back(miter->second);
    }
  }

  map<string, rgw_usage_log_info>::iterator liter;

  for (liter = log_objs.begin(); liter != log_objs.end(); ++liter) {
    int r = cls_obj_usage_log_add(liter->first, liter->second);
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWRados::read_usage(const rgw_user& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                         bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  uint32_t num = max_entries;
  string hash, first_hash;
  string user_str = user.to_str();
  usage_log_hash(cct, user_str, first_hash, 0);

  if (usage_iter.index) {
    usage_log_hash(cct, user_str, hash, usage_iter.index);
  } else {
    hash = first_hash;
  }

  usage.clear();

  do {
    map<rgw_user_bucket, rgw_usage_log_entry> ret_usage;
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;

    int ret =  cls_obj_usage_log_read(hash, user_str, start_epoch, end_epoch, num,
                                    usage_iter.read_iter, ret_usage, is_truncated);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

    num -= ret_usage.size();

    for (iter = ret_usage.begin(); iter != ret_usage.end(); ++iter) {
      usage[iter->first].aggregate(iter->second);
    }

next:
    if (!*is_truncated) {
      usage_iter.read_iter.clear();
      usage_log_hash(cct, user_str, hash, ++usage_iter.index);
    }
  } while (num && !*is_truncated && hash != first_hash);
  return 0;
}

int RGWRados::trim_usage(rgw_user& user, uint64_t start_epoch, uint64_t end_epoch)
{
  uint32_t index = 0;
  string hash, first_hash;
  string user_str = user.to_str();
  usage_log_hash(cct, user_str, first_hash, index);

  hash = first_hash;

  do {
    int ret =  cls_obj_usage_log_trim(hash, user_str, start_epoch, end_epoch);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

next:
    usage_log_hash(cct, user_str, hash, ++index);
  } while (hash != first_hash);

  return 0;
}

#define MAX_SHARDS_PRIME 7877

int RGWRados::key_to_shard_id(const string& key, int max_shards)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size()) % MAX_SHARDS_PRIME;
  return val % max_shards;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards, const string& key, string& name, int *shard_id)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  char buf[16];
  if (shard_id) {
    *shard_id = val % max_shards;
  }
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  val ^= ceph_str_hash_linux(section.c_str(), section.size());
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void RGWRados::shard_name(const string& prefix, unsigned shard_id, string& name)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", shard_id);
  name = prefix + buf;

}

void RGWRados::time_log_prepare_entry(cls_log_entry& entry, const utime_t& ut, const string& section, const string& key, bufferlist& bl)
{
  cls_log_add_prepare_entry(entry, ut, section, key, bl);
}

int RGWRados::time_log_add_init(librados::IoCtx& io_ctx)
{
  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rad->ioctx_create(log_pool, io_ctx);
  }
  if (r < 0)
    return r;

  return 0;

}

int RGWRados::time_log_add(const string& oid, const utime_t& ut, const string& section, const string& key, bufferlist& bl)
{
  librados::IoCtx io_ctx;

  int r = time_log_add_init(io_ctx);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  cls_log_add(op, ut, section, key, bl);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::time_log_add(const string& oid, list<cls_log_entry>& entries,
			   librados::AioCompletion *completion, bool monotonic_inc)
{
  librados::IoCtx io_ctx;

  int r = time_log_add_init(io_ctx);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;
  cls_log_add(op, entries, monotonic_inc);

  if (!completion) {
    r = io_ctx.operate(oid, &op);
  } else {
    r = io_ctx.aio_operate(oid, completion, &op);
  }
  return r;
}

int RGWRados::time_log_list(const string& oid, utime_t& start_time, utime_t& end_time,
                            int max_entries, list<cls_log_entry>& entries,
			    const string& marker,
			    string *out_marker,
			    bool *truncated)
{
  librados::IoCtx io_ctx;

  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;
  librados::ObjectReadOperation op;

  cls_log_list(op, start_time, end_time, marker, max_entries, entries,
	       out_marker, truncated);

  bufferlist obl;

  int ret = io_ctx.operate(oid, &op, &obl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_info(const string& oid, cls_log_header *header)
{
  librados::IoCtx io_ctx;

  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;
  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  bufferlist obl;

  int ret = io_ctx.operate(oid, &op, &obl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_info_async(librados::IoCtx& io_ctx, const string& oid, cls_log_header *header, librados::AioCompletion *completion)
{
  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;

  librados::ObjectReadOperation op;

  cls_log_info(op, header);

  int ret = io_ctx.aio_operate(oid, completion, &op, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::time_log_trim(const string& oid, const utime_t& start_time, const utime_t& end_time,
			    const string& from_marker, const string& to_marker)
{
  librados::IoCtx io_ctx;

  const char *log_pool = get_zone_params().log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;

  return cls_log_trim(io_ctx, oid, start_time, end_time, from_marker, to_marker);
}

string RGWRados::objexp_hint_get_shardname(int shard_num)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%010u", shard_num);

  string objname("obj_delete_at_hint.");
  return objname + buf;
}

#define MAX_OBJEXP_SHARDS_PRIME 7877

int RGWRados::objexp_key_shard(const rgw_obj_key& key)
{
  string obj_key = key.name + key.instance;
  int num_shards = cct->_conf->rgw_objexp_hints_num_shards;
  uint32_t sid = ceph_str_hash_linux(obj_key.c_str(), obj_key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  sid = sid2 % MAX_OBJEXP_SHARDS_PRIME % num_shards;
  return sid % num_shards;
}

static string objexp_hint_get_keyext(const string& tenant_name,
                                     const string& bucket_name,
                                     const string& bucket_id,
                                     const rgw_obj_key& obj_key)
{
  return tenant_name + (tenant_name.empty() ? "" : ":") + bucket_name + ":" + bucket_id +
      ":" + obj_key.name + ":" + obj_key.instance;
}

int RGWRados::objexp_hint_add(const utime_t& delete_at,
                              const string& tenant_name,
                              const string& bucket_name,
                              const string& bucket_id,
                              const rgw_obj_key& obj_key)
{
  const string keyext = objexp_hint_get_keyext(tenant_name, bucket_name,
          bucket_id, obj_key);
  objexp_hint_entry he = {
      .tenant = tenant_name,
      .bucket_name = bucket_name,
      .bucket_id = bucket_id,
      .obj_key = obj_key,
      .exp_time = delete_at };
  bufferlist hebl;
  ::encode(he, hebl);
  ObjectWriteOperation op;
  cls_timeindex_add(op, delete_at, keyext, hebl);

  string shard_name = objexp_hint_get_shardname(objexp_key_shard(obj_key));
  return objexp_pool_ctx.operate(shard_name, &op);
}

void  RGWRados::objexp_get_shard(int shard_num,
                                 string& shard)                       /* out */
{
  shard = objexp_hint_get_shardname(shard_num);
}

int RGWRados::objexp_hint_list(const string& oid,
                               const utime_t& start_time,
                               const utime_t& end_time,
                               const int max_entries,
                               const string& marker,
                               list<cls_timeindex_entry>& entries, /* out */
                               string *out_marker,                 /* out */
                               bool *truncated)                    /* out */
{
  librados::ObjectReadOperation op;
  cls_timeindex_list(op, start_time, end_time, marker, max_entries, entries,
	       out_marker, truncated);

  bufferlist obl;
  int ret = objexp_pool_ctx.operate(oid, &op, &obl);

  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  if ((ret == -ENOENT) && truncated) {
    *truncated = false;
  }

  return 0;
}

int RGWRados::objexp_hint_parse(cls_timeindex_entry &ti_entry,  /* in */
                                objexp_hint_entry& hint_entry)  /* out */
{
  try {
    bufferlist::iterator iter = ti_entry.value.begin();
    ::decode(hint_entry, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

  return 0;
}

int RGWRados::objexp_hint_trim(const string& oid,
                               const utime_t& start_time,
                               const utime_t& end_time,
                               const string& from_marker,
                               const string& to_marker)
{
  int ret = cls_timeindex_trim(objexp_pool_ctx, oid, start_time, end_time,
          from_marker, to_marker);
  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  return 0;
}

int RGWRados::lock_exclusive(rgw_bucket& pool, const string& oid, utime_t& duration, 
                             string& zone_id, string& owner_id) {
  librados::IoCtx io_ctx;

  const char *pool_name = pool.name.c_str();
  
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(pool_name, io_ctx);
  if (r < 0)
    return r;
  
  rados::cls::lock::Lock l(log_lock_name);
  l.set_duration(duration);
  l.set_cookie(owner_id);
  l.set_tag(zone_id);
  l.set_renew(true);
  
  return l.lock_exclusive(&io_ctx, oid);
}

int RGWRados::unlock(rgw_bucket& pool, const string& oid, string& zone_id, string& owner_id) {
  librados::IoCtx io_ctx;

  const char *pool_name = pool.name.c_str();

  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(pool_name, io_ctx);
  if (r < 0)
    return r;
  
  rados::cls::lock::Lock l(log_lock_name);
  l.set_tag(zone_id);
  l.set_cookie(owner_id);
  
  return l.unlock(&io_ctx, oid);
}

int RGWRados::decode_policy(bufferlist& bl, ACLOwner *owner)
{
  bufferlist::iterator i = bl.begin();
  RGWAccessControlPolicy policy(cct);
  try {
    policy.decode_owner(i);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  *owner = policy.get_owner();
  return 0;
}

int rgw_policy_from_attrset(CephContext *cct, map<string, bufferlist>& attrset, RGWAccessControlPolicy *policy)
{
  map<string, bufferlist>::iterator aiter = attrset.find(RGW_ATTR_ACL);
  if (aiter == attrset.end())
    return -EIO;

  bufferlist& bl = aiter->second;
  bufferlist::iterator iter = bl.begin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    ldout(cct, 15) << __func__ << " Read AccessControlPolicy";
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}


/** 
 * get listing of the objects in a bucket.
 * bucket: bucket to list contents of
 * max: maximum number of results to return
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * end_marker: if filled in, end the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed
 *     here.
 */
int RGWRados::Bucket::List::list_objects(int max, vector<RGWObjEnt> *result,
                                         map<string, bool> *common_prefixes,
                                         bool *is_truncated)
{
  RGWRados *store = target->get_store();
  CephContext *cct = store->ctx();
  rgw_bucket& bucket = target->get_bucket();
  int shard_id = target->get_shard_id();

  int count = 0;
  bool truncated = true;

  result->clear();

  rgw_obj marker_obj, end_marker_obj, prefix_obj;
  marker_obj.set_instance(params.marker.instance);
  marker_obj.set_ns(params.ns);
  marker_obj.set_obj(params.marker.name);
  rgw_obj_key cur_marker;
  marker_obj.get_index_key(&cur_marker);

  end_marker_obj.set_instance(params.end_marker.instance);
  end_marker_obj.set_ns(params.ns);
  end_marker_obj.set_obj(params.end_marker.name);
  rgw_obj_key cur_end_marker;
  if (params.ns.empty()) { /* no support for end marker for namespaced objects */
    end_marker_obj.get_index_key(&cur_end_marker);
  }
  const bool cur_end_marker_valid = !cur_end_marker.empty();

  prefix_obj.set_ns(params.ns);
  prefix_obj.set_obj(params.prefix);
  string cur_prefix = prefix_obj.get_index_key_name();

  string bigger_than_delim;

  if (!params.delim.empty()) {
    unsigned long val = decode_utf8((unsigned char *)params.delim.c_str(), params.delim.size());
    char buf[params.delim.size() + 16];
    int r = encode_utf8(val + 1, (unsigned char *)buf);
    if (r < 0) {
      ldout(cct,0) << "ERROR: encode_utf8() failed" << dendl;
      return -EINVAL;
    }
    buf[r] = '\0';

    bigger_than_delim = buf;
  }

  string skip_after_delim;

  /* if marker points at a common prefix, fast forward it into its upperbound string */
  if (!params.delim.empty()) {
    int delim_pos = cur_marker.name.find(params.delim, params.prefix.size());
    if (delim_pos >= 0) {
      string s = cur_marker.name.substr(0, delim_pos);
      s.append(bigger_than_delim);
      cur_marker.set(s);
    }
  }

  while (truncated && count <= max) {
    if (skip_after_delim > cur_marker.name) {
      cur_marker.set(skip_after_delim);
      ldout(cct, 20) << "setting cur_marker=" << cur_marker.name << "[" << cur_marker.instance << "]" << dendl;
    }
    std::map<string, RGWObjEnt> ent_map;
    int r = store->cls_bucket_list(bucket, shard_id, cur_marker, cur_prefix, max + 1 - count, params.list_versions, ent_map,
                            &truncated, &cur_marker);
    if (r < 0)
      return r;

    std::map<string, RGWObjEnt>::iterator eiter;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      rgw_obj_key obj = eiter->second.key;
      RGWObjEnt& entry = eiter->second;
      rgw_obj_key key = obj;
      string instance;
      string ns;

      bool valid = rgw_obj::parse_raw_oid(obj.name, &obj.name, &instance, &ns);
      if (!valid) {
        ldout(cct, 0) << "ERROR: could not parse object name: " << obj.name << dendl;
        continue;
      }
      bool check_ns = (ns == params.ns);
      if (!params.list_versions && !entry.is_visible()) {
        continue;
      }

      if (params.enforce_ns && !check_ns) {
        if (!params.ns.empty()) {
          /* we've iterated past the namespace we're searching -- done now */
          truncated = false;
          goto done;
        }

        /* we're not looking at the namespace this object is in, next! */
        continue;
      }

      if (cur_end_marker_valid && cur_end_marker <= obj) {
        truncated = false;
        goto done;
      }

      if (count < max) {
        params.marker = obj;
        next_marker = obj;
      }

      if (params.filter && !params.filter->filter(obj.name, key.name))
        continue;

      if (params.prefix.size() &&  (obj.name.compare(0, params.prefix.size(), params.prefix) != 0))
        continue;

      if (!params.delim.empty()) {
        int delim_pos = obj.name.find(params.delim, params.prefix.size());

        if (delim_pos >= 0) {
          string prefix_key = obj.name.substr(0, delim_pos + 1);

          if (common_prefixes &&
              common_prefixes->find(prefix_key) == common_prefixes->end()) {
            if (count >= max) {
              truncated = true;
              goto done;
            }
            next_marker = prefix_key;
            (*common_prefixes)[prefix_key] = true;

            skip_after_delim = obj.name.substr(0, delim_pos);
            skip_after_delim.append(bigger_than_delim);

            ldout(cct, 20) << "skip_after_delim=" << skip_after_delim << dendl;

            count++;
          }

          continue;
        }
      }

      if (count >= max) {
        truncated = true;
        goto done;
      }

      RGWObjEnt ent = eiter->second;
      ent.key = obj;
      ent.ns = ns;
      result->push_back(ent);
      count++;
    }

    // Either the back-end telling us truncated, or we don't consume all
    // items returned per the amount caller request
    truncated = (truncated || eiter != ent_map.end());
  }

done:
  if (is_truncated)
    *is_truncated = truncated;

  return 0;
}

/**
 * create a rados pool, associated meta info
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_pool(rgw_bucket& bucket) 
{
  int ret = 0;

  string pool = bucket.index_pool;

  librados::Rados *rad = get_rados_handle();
  ret = rad->pool_create(pool.c_str(), 0);
  if (ret == -EEXIST)
    ret = 0;
  if (ret < 0)
    return ret;

  if (bucket.data_pool != pool) {
    ret = rad->pool_create(bucket.data_pool.c_str(), 0);
    if (ret == -EEXIST)
      ret = 0;
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWRados::init_bucket_index(rgw_bucket& bucket, int num_shards)
{
  librados::IoCtx index_ctx; // context for new bucket

  int r = open_bucket_index_ctx(bucket, index_ctx);
  if (r < 0)
    return r;

  string dir_oid =  dir_oid_prefix;
  dir_oid.append(bucket.marker);

  map<int, string> bucket_objs;
  get_bucket_index_objects(dir_oid, num_shards, bucket_objs);

  return CLSRGWIssueBucketIndexInit(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}

/**
 * create a bucket with name bucket and the given list of attrs
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& zonegroup_id,
                            const string& placement_rule,
                            const string& swift_ver_location,
			    map<std::string, bufferlist>& attrs,
                            RGWBucketInfo& info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            time_t creation_time,
                            rgw_bucket *pmaster_bucket,
			    bool exclusive)
{
#define MAX_CREATE_RETRIES 20 /* need to bound retries */
  string selected_placement_rule_name;
  RGWZonePlacementInfo rule_info;

  for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
    int ret = 0;
    ret = select_bucket_placement(owner, zonegroup_id, placement_rule,
                                  bucket.tenant, bucket.name, bucket,
                                  &selected_placement_rule_name, &rule_info);
    if (ret < 0)
      return ret;
    bufferlist bl;
    uint32_t nop = 0;
    ::encode(nop, bl);

    const string& pool = get_zone_params().domain_root.name;
    const char *pool_str = pool.c_str();
    librados::IoCtx id_io_ctx;
    librados::Rados *rad = get_rados_handle();
    int r = rad->ioctx_create(pool_str, id_io_ctx);
    if (r < 0)
      return r;

    if (!pmaster_bucket) {
      uint64_t iid = instance_id();
      uint64_t bid = next_bucket_id();
      char buf[get_zone_params().get_id().size() + 48];
      snprintf(buf, sizeof(buf), "%s.%llu.%llu", get_zone_params().get_id().c_str(), (long long)iid, (long long)bid);
      bucket.marker = buf;
      bucket.bucket_id = bucket.marker;
    } else {
      bucket.marker = pmaster_bucket->marker;
      bucket.bucket_id = pmaster_bucket->bucket_id;
    }

    r = init_bucket_index(bucket, bucket_index_max_shards);
    if (r < 0)
      return r;

    RGWObjVersionTracker& objv_tracker = info.objv_tracker;

    if (pobjv) {
      objv_tracker.write_version = *pobjv;
    } else {
      objv_tracker.generate_new_write_ver(cct);
    }

    info.bucket = bucket;
    info.owner = owner.user_id;
    info.zonegroup = zonegroup_id;
    info.placement_rule = selected_placement_rule_name;
    info.index_type = rule_info.index_type;
    info.swift_ver_location = swift_ver_location;
    info.swift_versioning = (!swift_ver_location.empty());
    info.num_shards = bucket_index_max_shards;
    info.bucket_index_shard_hash_type = RGWBucketInfo::MOD;
    info.requester_pays = false;
    if (!creation_time)
      time(&info.creation_time);
    else
      info.creation_time = creation_time;
    ret = put_linked_bucket_info(info, exclusive, 0, pep_objv, &attrs, true);
    if (ret == -EEXIST) {
       /* we need to reread the info and return it, caller will have a use for it */
      RGWObjVersionTracker instance_ver = info.objv_tracker;
      info.objv_tracker.clear();
      RGWObjectCtx obj_ctx(this);
      r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, NULL);
      if (r < 0) {
        if (r == -ENOENT) {
          continue;
        }
        ldout(cct, 0) << "get_bucket_info returned " << r << dendl;
        return r;
      }

      /* only remove it if it's a different bucket instance */
      if (info.bucket.bucket_id != bucket.bucket_id) {
        /* remove bucket index */
        librados::IoCtx index_ctx; // context for new bucket
        map<int, string> bucket_objs;
        int r = open_bucket_index(bucket, index_ctx, bucket_objs);
        if (r < 0)
          return r;

        /* remove bucket meta instance */
        string entry;
        get_bucket_instance_entry(bucket, entry);
        r = rgw_bucket_instance_remove_entry(this, entry, &instance_ver);
        if (r < 0)
          return r;

        map<int, string>::const_iterator biter;
        for (biter = bucket_objs.begin(); biter != bucket_objs.end(); ++biter) {
          // Do best effort removal
          index_ctx.remove(biter->second);
        }
      }
      /* ret == -ENOENT here */
    }
    return ret;
  }

  /* this is highly unlikely */
  ldout(cct, 0) << "ERROR: could not create bucket, continuously raced with bucket creation and removal" << dendl;
  return -ENOENT;
}

int RGWRados::select_new_bucket_location(RGWUserInfo& user_info, const string& zonegroup_id, const string& request_rule,
                                         const string& tenant_name, const string& bucket_name, rgw_bucket& bucket, string *pselected_rule_name,
                                         RGWZonePlacementInfo *rule_info)

{
  /* first check that rule exists within the specific zonegroup */
  RGWZoneGroup zonegroup;
  int ret = get_zonegroup(zonegroup_id, zonegroup);
  if (ret < 0) {
    ldout(cct, 0) << "could not find zonegroup " << zonegroup_id << " in current period" << dendl;
    return ret;
  }

  /* now check that tag exists within zonegroup */
  /* find placement rule. Hierarchy: request rule > user default rule > zonegroup default rule */
  string rule = request_rule;
  if (rule.empty()) {
    rule = user_info.default_placement;
    if (rule.empty())
      rule = zonegroup.default_placement;
  }

  if (rule.empty()) {
    ldout(cct, 0) << "misconfiguration, should not have an empty placement rule name" << dendl;
    return -EIO;
  }

  map<string, RGWZoneGroupPlacementTarget>::iterator titer = zonegroup.placement_targets.find(rule);
  if (titer == zonegroup.placement_targets.end()) {
    ldout(cct, 0) << "could not find placement rule " << rule << " within zonegroup " << dendl;
    return -EINVAL;
  }

  /* now check tag for the rule, whether user is permitted to use rule */
  RGWZoneGroupPlacementTarget& target_rule = titer->second;
  if (!target_rule.user_permitted(user_info.placement_tags)) {
    ldout(cct, 0) << "user not permitted to use placement rule" << dendl;
    return -EPERM;
  }

  if (pselected_rule_name)
    *pselected_rule_name = rule;

  return set_bucket_location_by_rule(rule, tenant_name, bucket_name, bucket, rule_info);
}

int RGWRados::set_bucket_location_by_rule(const string& location_rule, const string& tenant_name, const string& bucket_name, rgw_bucket& bucket,
                                         RGWZonePlacementInfo *rule_info)
{
  bucket.tenant = tenant_name;
  bucket.name = bucket_name;

  if (location_rule.empty()) {
    /* we can only reach here if we're trying to set a bucket location from a bucket
     * created on a different zone, using a legacy / default pool configuration
     */
    return select_legacy_bucket_placement(tenant_name, bucket_name, bucket, rule_info);
  }

  /*
   * make sure that zone has this rule configured. We're
   * checking it for the local zone, because that's where this bucket object is going to
   * reside.
   */
  map<string, RGWZonePlacementInfo>::iterator piter = get_zone_params().placement_pools.find(location_rule);
  if (piter == get_zone_params().placement_pools.end()) {
    /* couldn't find, means we cannot really place data for this bucket in this zone */
    if (get_zonegroup().equals(zonegroup_id)) {
      /* that's a configuration error, zone should have that rule, as we're within the requested
       * zonegroup */
      return -EINVAL;
    } else {
      /* oh, well, data is not going to be placed here, bucket object is just a placeholder */
      return 0;
    }
  }

  RGWZonePlacementInfo& placement_info = piter->second;

  bucket.data_pool = placement_info.data_pool;
  bucket.data_extra_pool = placement_info.data_extra_pool;
  bucket.index_pool = placement_info.index_pool;

  if (rule_info) {
    *rule_info = placement_info;
  }

  return 0;
}

int RGWRados::select_bucket_placement(RGWUserInfo& user_info, const string& zonegroup_id, const string& placement_rule,
                                      const string& tenant_name, const string& bucket_name, rgw_bucket& bucket,
                                      string *pselected_rule_name, RGWZonePlacementInfo *rule_info)
{
  if (!get_zone_params().placement_pools.empty()) {
    return select_new_bucket_location(user_info, zonegroup_id, placement_rule,
                                      tenant_name, bucket_name, bucket, pselected_rule_name, rule_info);
  }

  if (pselected_rule_name) {
    pselected_rule_name->clear();
  }

  return select_legacy_bucket_placement(tenant_name, bucket_name, bucket, rule_info);
}

int RGWRados::select_legacy_bucket_placement(const string& tenant_name, const string& bucket_name, rgw_bucket& bucket,
                                             RGWZonePlacementInfo *rule_info)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string pool_name;
  bool write_map = false;

  rgw_obj obj(get_zone_params().domain_root, avail_pools);

  RGWObjectCtx obj_ctx(this);
  int ret = rgw_get_system_obj(this, obj_ctx, get_zone_params().domain_root, avail_pools, map_bl, NULL, NULL);
  if (ret < 0) {
    goto read_omap;
  }

  try {
    bufferlist::iterator iter = map_bl.begin();
    ::decode(m, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

read_omap:
  if (m.empty()) {
    bufferlist header;
    ret = omap_get_all(obj, header, m);

    write_map = true;
  }

  if (ret < 0 || m.empty()) {
    vector<string> names;
    string s = string("default.") + default_storage_pool_suffix;
    names.push_back(s);
    vector<int> retcodes;
    bufferlist bl;
    ret = create_pools(names, retcodes);
    if (ret < 0)
      return ret;
    ret = omap_set(obj, s, bl);
    if (ret < 0)
      return ret;
    m[s] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    ::encode(m, new_bl);
    ret = put_system_obj_data(NULL, obj, new_bl, -1, false);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
    }
  }

  map<string, bufferlist>::iterator miter;
  if (m.size() > 1) {
    vector<string> v;
    for (miter = m.begin(); miter != m.end(); ++miter) {
      v.push_back(miter->first);
    }

    uint32_t r;
    ret = get_random_bytes((char *)&r, sizeof(r));
    if (ret < 0)
      return ret;

    int i = r % v.size();
    pool_name = v[i];
  } else {
    miter = m.begin();
    pool_name = miter->first;
  }
  bucket.data_pool = pool_name;
  bucket.index_pool = pool_name;

  rule_info->data_pool = pool_name;
  rule_info->data_extra_pool = pool_name;
  rule_info->index_pool = pool_name;
  rule_info->index_type = RGWBIType_Normal;

  return 0;
}

int RGWRados::update_placement_map()
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_obj obj(get_zone_params().domain_root, avail_pools);
  int ret = omap_get_all(obj, header, m);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  ::encode(m, new_bl);
  ret = put_system_obj_data(NULL, obj, new_bl, -1, false);
  if (ret < 0) {
    ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWRados::add_bucket_placement(std::string& new_pool)
{
  librados::Rados *rad = get_rados_handle();
  int ret = rad->pool_lookup(new_pool.c_str());
  if (ret < 0) // DNE, or something
    return ret;

  rgw_obj obj(get_zone_params().domain_root, avail_pools);
  bufferlist empty_bl;
  ret = omap_set(obj, new_pool, empty_bl);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::remove_bucket_placement(std::string& old_pool)
{
  rgw_obj obj(get_zone_params().domain_root, avail_pools);
  int ret = omap_del(obj, old_pool);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::list_placement_set(set<string>& names)
{
  bufferlist header;
  map<string, bufferlist> m;

  rgw_obj obj(get_zone_params().domain_root, avail_pools);
  int ret = omap_get_all(obj, header, m);
  if (ret < 0)
    return ret;

  names.clear();
  map<string, bufferlist>::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    names.insert(miter->first);
  }

  return names.size();
}

int RGWRados::create_pools(vector<string>& names, vector<int>& retcodes)
{
  vector<string>::iterator iter;
  vector<librados::PoolAsyncCompletion *> completions;
  vector<int> rets;

  librados::Rados *rad = get_rados_handle();
  for (iter = names.begin(); iter != names.end(); ++iter) {
    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    string& name = *iter;
    int ret = rad->pool_create_async(name.c_str(), c);
    rets.push_back(ret);
  }

  vector<int>::iterator riter;
  vector<librados::PoolAsyncCompletion *>::iterator citer;

  assert(rets.size() == completions.size());
  for (riter = rets.begin(), citer = completions.begin(); riter != rets.end(); ++riter, ++citer) {
    int r = *riter;
    PoolAsyncCompletion *c = *citer;
    if (r == 0) {
      c->wait();
      r = c->get_return_value();
      if (r < 0) {
        ldout(cct, 0) << "WARNING: async pool_create returned " << r << dendl;
      }
    }
    c->release();
    retcodes.push_back(r);
  }
  return 0;
}


int RGWRados::get_obj_ioctx(const rgw_obj& obj, librados::IoCtx *ioctx)
{
  rgw_bucket bucket;
  string oid, key;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

  int r;

  if (!obj.is_in_extra_data()) {
    r = open_bucket_data_ctx(bucket, *ioctx);
  } else {
    r = open_bucket_data_extra_ctx(bucket, *ioctx);
  }
  if (r < 0)
    return r;

  ioctx->locator_set_key(key);

  return 0;
}

int RGWRados::get_obj_ref(const rgw_obj& obj, rgw_rados_ref *ref, rgw_bucket *bucket)
{
  get_obj_bucket_and_oid_loc(obj, *bucket, ref->oid, ref->key);

  int r;

  if (!obj.is_in_extra_data()) {
    r = open_bucket_data_ctx(*bucket, ref->ioctx);
  } else {
    r = open_bucket_data_extra_ctx(*bucket, ref->ioctx);
  }
  if (r < 0)
    return r;

  ref->ioctx.locator_set_key(ref->key);

  return 0;
}

int RGWRados::get_system_obj_ref(const rgw_obj& obj, rgw_rados_ref *ref, rgw_bucket *bucket)
{
  get_obj_bucket_and_oid_loc(obj, *bucket, ref->oid, ref->key);

  int r;

  if (ref->oid.empty()) {
    ref->oid = bucket->name;
    *bucket = get_zone_params().domain_root;
  }
  r = open_pool_ctx(bucket->name, ref->ioctx);
  if (r < 0)
    return r;

  ref->ioctx.locator_set_key(ref->key);

  return 0;
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_head_obj_locator(rgw_bucket& bucket, bool copy_obj, bool remove_bad, rgw_obj_key& key)
{
  string oid;
  string locator;

  rgw_obj obj(bucket, key);

  get_obj_bucket_and_oid_loc(obj, bucket, oid, locator);

  if (locator.empty()) {
    ldout(cct, 20) << "object does not have a locator, nothing to fix" << dendl;
    return 0;
  }

  librados::IoCtx ioctx;

  int ret = get_obj_ioctx(obj, &ioctx);
  if (ret < 0) {
    cerr << "ERROR: get_obj_ioctx() returned ret=" << ret << std::endl;
    return ret;
  }
  ioctx.locator_set_key(string()); /* override locator for this object, use empty locator */

  uint64_t size;
  time_t mtime;
  bufferlist data;

  map<string, bufferlist> attrs;
  librados::ObjectReadOperation op;
  op.getxattrs(&attrs, NULL);
  op.stat(&size, &mtime, NULL);
#define HEAD_SIZE 512 * 1024
  op.read(0, HEAD_SIZE, &data, NULL);

  ret = ioctx.operate(oid, &op, NULL);
  if (ret < 0) {
    lderr(cct) << "ERROR: ioctx.operate(oid=" << oid << ") returned ret=" << ret << dendl;
    return ret;
  }

  if (size > HEAD_SIZE) {
    lderr(cct) << "ERROR: returned object size (" << size << ") > HEAD_SIZE (" << HEAD_SIZE << ")" << dendl;
    return -EIO;
  }

  if (size != data.length()) {
    lderr(cct) << "ERROR: returned object size (" << size << ") != data.length() (" << data.length() << ")" << dendl;
    return -EIO;
  }

  if (copy_obj) {
    librados::ObjectWriteOperation wop;

    wop.mtime(&mtime);

    map<string, bufferlist>::iterator iter;
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      wop.setxattr(iter->first.c_str(), iter->second);
    }

    wop.write(0, data);

    ioctx.locator_set_key(locator);
    ioctx.operate(oid, &wop);
  }

  if (remove_bad) {
    ioctx.locator_set_key(string());

    ret = ioctx.remove(oid);
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to remove original bad object" << dendl;
      return ret;
    }
  }

  return 0;
}

int RGWRados::move_rados_obj(librados::IoCtx& src_ioctx,
			     const string& src_oid, const string& src_locator,
		             librados::IoCtx& dst_ioctx,
			     const string& dst_oid, const string& dst_locator)
{

#define COPY_BUF_SIZE (4 * 1024 * 1024)
  bool done = false;
  uint64_t chunk_size = COPY_BUF_SIZE;
  uint64_t ofs = 0;
  int ret = 0;
  time_t mtime = 0;
  uint64_t size;

  if (src_oid == dst_oid && src_locator == dst_locator) {
    return 0;
  }

  src_ioctx.locator_set_key(src_locator);
  dst_ioctx.locator_set_key(dst_locator);

  do {
    bufferlist data;
    ObjectReadOperation rop;
    ObjectWriteOperation wop;

    if (ofs == 0) {
      rop.stat(&size, &mtime, NULL);
    }
    rop.read(ofs, chunk_size, &data, NULL);
    ret = src_ioctx.operate(src_oid, &rop, NULL);
    if (ret < 0) {
      goto done_err;
    }

    if (data.length() == 0) {
      break;
    }

    if (ofs == 0) {
      wop.create(true); /* make it exclusive */
      wop.mtime(&mtime);
    }
    wop.write(ofs, data);
    ret = dst_ioctx.operate(dst_oid, &wop);
    ofs += data.length();
    done = data.length() != chunk_size;
  } while (!done);

  if (ofs != size) {
    lderr(cct) << "ERROR: " << __func__ << ": copying " << src_oid << " -> " << dst_oid
               << ": expected " << size << " bytes to copy, ended up with " << ofs << dendl;
    ret = -EIO;
    goto done_err;
  }

  src_ioctx.remove(src_oid);

  return 0;

done_err:
  lderr(cct) << "ERROR: failed to copy " << src_oid << " -> " << dst_oid << dendl;
  return ret;
}

/*
 * fixes an issue where head objects were supposed to have a locator created, but ended
 * up without one
 */
int RGWRados::fix_tail_obj_locator(rgw_bucket& bucket, rgw_obj_key& key, bool fix, bool *need_fix)
{
  rgw_obj obj(bucket, key);

  if (need_fix) {
    *need_fix = false;
  }

  rgw_rados_ref ref;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  RGWObjState *astate = NULL;
  RGWObjectCtx rctx(this);
  r = get_obj_state(&rctx, obj, &astate, NULL);
  if (r < 0)
    return r;

  if (astate->has_manifest) {
    RGWObjManifest::obj_iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      rgw_obj loc = miter.get_location();
      string oid;
      string locator;

      if (loc.ns.empty()) {
	/* continue, we're only interested in tail objects */
	continue;
      }

      get_obj_bucket_and_oid_loc(loc, bucket, oid, locator);
      ref.ioctx.locator_set_key(locator);

      ldout(cct, 20) << __func__ << ": key=" << key << " oid=" << oid << " locator=" << locator << dendl;

      r = ref.ioctx.stat(oid, NULL, NULL);
      if (r != -ENOENT) {
	continue;
      }

      string bad_loc;
      prepend_bucket_marker(bucket, loc.get_orig_obj(), bad_loc);

      /* create a new ioctx with the bad locator */
      librados::IoCtx src_ioctx;
      src_ioctx.dup(ref.ioctx);
      src_ioctx.locator_set_key(bad_loc);

      r = src_ioctx.stat(oid, NULL, NULL);
      if (r != 0) {
	/* cannot find a broken part */
	continue;
      }
      ldout(cct, 20) << __func__ << ": found bad object part: " << loc << dendl;
      if (need_fix) {
        *need_fix = true;
      }
      if (fix) {
        r = move_rados_obj(src_ioctx, oid, bad_loc, ref.ioctx, oid, locator);
        if (r < 0) {
          lderr(cct) << "ERROR: copy_rados_obj() on oid=" << oid << " returned r=" << r << dendl;
        }
      }
    }
  }

  return 0;
}

int RGWRados::BucketShard::init(rgw_bucket& _bucket, rgw_obj& obj)
{
  bucket = _bucket;

  int ret = store->open_bucket_index_shard(bucket, index_ctx, obj.get_hash_object(), &bucket_obj, &shard_id);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: open_bucket_index_shard() returned ret=" << ret << dendl;
    return ret;
  }
  ldout(store->ctx(), 20) << " bucket index object: " << bucket_obj << dendl;

  return 0;
}


int RGWRados::swift_versioning_copy(RGWBucketInfo& bucket_info, RGWRados::Object *source, RGWObjState *state,
                                    rgw_user& user)
{
  if (!bucket_info.has_swift_versioning() || bucket_info.swift_ver_location.empty()) {
    return 0;
  }

  if (!state->exists) {
    return 0;
  }

  string client_id;
  string op_id;

  rgw_obj& obj = source->get_obj();
  const string& src_name = obj.get_object();
  char buf[src_name.size() + 32];
  snprintf(buf, sizeof(buf), "%03d%s%lld.%06d", (int)src_name.size(), src_name.c_str(), (long long)state->mtime, 0);

  RGWBucketInfo dest_bucket_info;

  int r = get_bucket_info(source->get_ctx(), bucket_info.bucket.tenant, bucket_info.swift_ver_location, dest_bucket_info, NULL, NULL);
  if (r < 0) {
    ldout(cct, 10) << "failed to read dest bucket info: r=" << r << dendl;
    return r;
  }

  if (dest_bucket_info.owner != bucket_info.owner) {
    return -EPERM;
  }

  rgw_obj dest_obj(dest_bucket_info.bucket, buf);

  string no_zone;

  r = copy_obj(source->get_ctx(),
               user,
               client_id,
               op_id,
               NULL, /* req_info *info */
               no_zone,
               dest_obj,
               obj,
               dest_bucket_info,
               bucket_info,
               NULL, /* time_t *src_mtime */
               NULL, /* time_t *mtime */
               NULL, /* const time_t *mod_ptr */
               NULL, /* const time_t *unmod_ptr */
               NULL, /* const char *if_match */
               NULL, /* const char *if_nomatch */
               RGWRados::ATTRSMOD_NONE,
               true, /* bool copy_if_newer */
               state->attrset,
               RGW_OBJ_CATEGORY_MAIN,
               0, /* uint64_t olh_epoch */
               0, /* time_t delete_at */
               NULL, /* string *version_id */
               NULL, /* string *ptag */
               NULL, /* string *petag */
               NULL, /* struct rgw_err *err */
               NULL, /* void (*progress_cb)(off_t, void *) */
               NULL); /* void *progress_data */
  if (r == -ECANCELED || r == -ENOENT) { /* has already been overwritten, meaning another rgw process already copied it out */
    return 0;
  }

  return r;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * size: the amount of data to write (data must be this long)
 * mtime: if non-NULL, writes the given mtime to the bucket storage
 * attrs: all the given attrs are written to bucket storage for the given object
 * exclusive: create object exclusively
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::Object::Write::write_meta(uint64_t size,
                  map<string, bufferlist>& attrs)
{
  rgw_bucket bucket;
  rgw_rados_ref ref;
  RGWRados *store = target->get_store();

  ObjectWriteOperation op;

  RGWObjState *state;
  int r = target->get_state(&state, false);
  if (r < 0)
    return r;

  rgw_obj& obj = target->get_obj();

  if (obj.get_object().empty()) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): cannot write object with empty name" << dendl;
    return -EIO;
  }

  r = store->get_obj_ref(obj, &ref, &bucket);
  if (r < 0)
    return r;

  bool is_olh = state->is_olh;

  bool reset_obj = (meta.flags & PUT_OBJ_CREATE) != 0;
  r = target->prepare_atomic_modification(op, reset_obj, meta.ptag, meta.if_match, meta.if_nomatch, false);
  if (r < 0)
    return r;

  utime_t ut;
  if (meta.set_mtime) {
    ut = utime_t(meta.set_mtime, 0);
  } else {
    ut = ceph_clock_now(0);
    meta.set_mtime = ut.sec();
  }

  if (state->is_olh) {
    op.setxattr(RGW_ATTR_OLH_ID_TAG, state->olh_tag);
  }
  op.mtime(&meta.set_mtime);

  if (meta.data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    op.write_full(*meta.data);
  }

  string etag;
  string content_type;
  bufferlist acl_bl;

  map<string, bufferlist>::iterator iter;
  if (meta.rmattrs) {
    for (iter = meta.rmattrs->begin(); iter != meta.rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  if (meta.manifest) {
    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    ::encode(*meta.manifest, bl);
    op.setxattr(RGW_ATTR_MANIFEST, bl);
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = bl.c_str();
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = bl.c_str();
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }
  if (attrs.find(RGW_ATTR_PG_VER) == attrs.end()) {
    cls_rgw_obj_store_pg_ver(op, RGW_ATTR_PG_VER);
  }

  if (attrs.find(RGW_ATTR_SOURCE_ZONE) == attrs.end()) {
    bufferlist bl;
    ::encode(store->get_zone_short_id(), bl);
    op.setxattr(RGW_ATTR_SOURCE_ZONE, bl);
  }

  if (!op.size())
    return 0;

  string index_tag;
  uint64_t epoch;
  int64_t poolid;

  bool orig_exists = state->exists;
  uint64_t orig_size = state->size;

  bool versioned_target = (meta.olh_epoch > 0 || !obj.get_instance().empty());

  index_tag = state->write_tag;

  bool versioned_op = (target->versioning_enabled() || is_olh || versioned_target);

  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(store, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj, state);

  if (versioned_op) {
    index_op.set_bilog_flags(RGW_BILOG_FLAG_VERSIONED_OP);
  }

  r = store->swift_versioning_copy(bucket_info, target, state, meta.owner);
  if (r < 0) {
    goto done_cancel;
  }

  r = index_op.prepare(CLS_RGW_OP_ADD);
  if (r < 0)
    return r;

  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0) { /* we can expect to get -ECANCELED if object was replaced under,
                or -ENOENT if was removed, or -EEXIST if it did not exist
                before and now it does */
    goto done_cancel;
  }

  epoch = ref.ioctx.get_last_version();
  poolid = ref.ioctx.get_id();

  r = target->complete_atomic_modification();
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: complete_atomic_modification returned r=" << r << dendl;
  }

  r = index_op.complete(poolid, epoch, size,
                        ut, etag, content_type, &acl_bl,
                        meta.category, meta.remove_objs);
  if (r < 0)
    goto done_cancel;

  if (meta.mtime) {
    *meta.mtime = meta.set_mtime;
  }

  /* note that index_op was using state so we couldn't invalidate it earlier */
  target->invalidate_state();
  state = NULL;

  if (versioned_op) {
    r = store->set_olh(target->get_ctx(), target->get_bucket_info(), obj, false, NULL, meta.olh_epoch, 0);
    if (r < 0) {
      return r;
    }
  }

  if (meta.delete_at > 0) {
    rgw_obj_key obj_key;
    obj.get_index_key(&obj_key);

    r = store->objexp_hint_add(utime_t(meta.delete_at, 0),
            bucket.tenant, bucket.name, bucket.bucket_id, obj_key);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: objexp_hint_add() returned r=" << r << ", object will not get removed" << dendl;
      /* ignoring error, nothing we can do at this point */
    }
  }
  meta.canceled = false;

  /* update quota cache */
  store->quota_handler->update_stats(meta.owner, bucket, (orig_exists ? 0 : 1), size, orig_size);

  return 0;

done_cancel:
  int ret = index_op.cancel();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: index_op.cancel()() returned ret=" << ret << dendl;
  }

  meta.canceled = true;

  /* we lost in a race. There are a few options:
   * - existing object was rewritten (ECANCELED)
   * - non existing object was created (EEXIST)
   * - object was removed (ENOENT)
   * should treat it as a success
   */
  if (meta.if_match == NULL && meta.if_nomatch == NULL) {
    if (r == -ECANCELED || r == -ENOENT || r == -EEXIST) {
      r = 0;
    }
  } else {
    if (meta.if_match != NULL) {
      // only overwrite existing object
      if (strcmp(meta.if_match, "*") == 0) {
        if (r == -ENOENT) {
          r = -ERR_PRECONDITION_FAILED;
        } else if (r == -ECANCELED) {
          r = 0;
        }
      }
    }

    if (meta.if_nomatch != NULL) {
      // only create a new object
      if (strcmp(meta.if_nomatch, "*") == 0) {
        if (r == -EEXIST) {
          r = -ERR_PRECONDITION_FAILED;
        } else if (r == -ENOENT) {
          r = 0;
        }
      }
    }
  }

  return r;
}

/** Write/overwrite a system object. */
int RGWRados::put_system_obj_impl(rgw_obj& obj, uint64_t size, time_t *mtime,
              map<std::string, bufferlist>& attrs, int flags,
              bufferlist& data,
              RGWObjVersionTracker *objv_tracker,
              time_t set_mtime /* 0 for don't set */)
{
  rgw_bucket bucket;
  rgw_rados_ref ref;
  int r = get_system_obj_ref(obj, &ref, &bucket);
  if (r < 0)
    return r;

  ObjectWriteOperation op;

  if (flags & PUT_OBJ_EXCL) {
    if (!(flags & PUT_OBJ_CREATE))
	return -EINVAL;
    op.create(true); // exclusive create
  } else {
    op.remove();
    op.set_op_flags2(LIBRADOS_OP_FLAG_FAILOK);
    op.create(false);
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  utime_t ut;
  if (set_mtime) {
    ut = utime_t(set_mtime, 0);
  } else {
    ut = ceph_clock_now(0);
    set_mtime = ut.sec();
  }

  op.mtime(&set_mtime);
  op.write_full(data);

  bufferlist acl_bl;

  for (map<string, bufferlist>::iterator iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);
  }

  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0) {
    return r;
  }

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  if (mtime) {
    *mtime = set_mtime;
  }

  return 0;
}

int RGWRados::put_system_obj_data(void *ctx, rgw_obj& obj, bufferlist& bl,
			       off_t ofs, bool exclusive)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_system_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;

  if (exclusive)
    op.create(true);

  if (ofs == -1) {
    op.write_full(bl);
  } else {
    op.write(ofs, bl);
  }
  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * offset: the offet to write to in the object
 *         If this is -1, we will overwrite the whole object.
 * size: the amount of data to write (data must be this long)
 * attrs: all the given attrs are written to bucket storage for the given object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_data(void *ctx, rgw_obj& obj,
			   const char *data, off_t ofs, size_t len, bool exclusive)
{
  void *handle;
  bufferlist bl;
  bl.append(data, len);
  int r = aio_put_obj_data(ctx, obj, bl, ofs, exclusive, &handle);
  if (r < 0)
    return r;
  return aio_wait(handle);
}

int RGWRados::aio_put_obj_data(void *ctx, rgw_obj& obj, bufferlist& bl,
			       off_t ofs, bool exclusive,
                               void **handle)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  *handle = c;
  
  ObjectWriteOperation op;

  if (exclusive)
    op.create(true);

  if (ofs == -1) {
    op.write_full(bl);
  } else {
    op.write(ofs, bl);
  }
  r = ref.ioctx.aio_operate(ref.oid, c, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::aio_wait(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  c->wait_for_complete();
  int ret = c->get_return_value();
  c->release();
  return ret;
}

bool RGWRados::aio_completed(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  return c->is_complete();
}

class RGWRadosPutObj : public RGWGetDataCB
{
  rgw_obj obj;
  RGWPutObjProcessor_Atomic *processor;
  RGWOpStateSingleOp *opstate;
  void (*progress_cb)(off_t, void *);
  void *progress_data;
public:
  RGWRadosPutObj(RGWPutObjProcessor_Atomic *p, RGWOpStateSingleOp *_ops,
                 void (*_progress_cb)(off_t, void *), void *_progress_data) : processor(p), opstate(_ops),
                                                                       progress_cb(_progress_cb),
                                                                       progress_data(_progress_data) {}
  int handle_data(bufferlist& bl, off_t ofs, off_t len) {
    if (progress_cb) {
      progress_cb(ofs, progress_data);
    }

    bool again;

    bool need_opstate = true;

    do {
      void *handle;
      int ret = processor->handle_data(bl, ofs, NULL, &handle, &again);
      if (ret < 0)
        return ret;

      if (need_opstate && opstate) {
        /* need to update opstate repository with new state. This is ratelimited, so we're not
         * really doing it every time
         */
        ret = opstate->renew_state();
        if (ret < 0) {
          ldout(processor->ctx(), 0) << "ERROR: RGWRadosPutObj::handle_data(): failed to renew op state ret=" << ret << dendl;
          int r = processor->throttle_data(handle, false);
          if (r < 0) {
            ldout(processor->ctx(), 0) << "ERROR: RGWRadosPutObj::handle_data(): processor->throttle_data() returned " << r << dendl;
          }
          /* could not renew state! might have been marked as cancelled */
          return ret;
        }

        need_opstate = false;
      }

      ret = processor->throttle_data(handle, false);
      if (ret < 0)
        return ret;
    } while (again);

    return 0;
  }

  void set_extra_data_len(uint64_t len) {
    RGWGetDataCB::set_extra_data_len(len);
    processor->set_extra_data_len(len);
  }

  int complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs, time_t delete_at) {
    return processor->complete(etag, mtime, set_mtime, attrs, delete_at);
  }

  bool is_canceled() {
    return processor->is_canceled();
  }
};

/*
 * prepare attrset depending on attrs_mod.
 */
static void set_copy_attrs(map<string, bufferlist>& src_attrs,
                           map<string, bufferlist>& attrs,
                           RGWRados::AttrsMod attrs_mod)
{
  switch (attrs_mod) {
  case RGWRados::ATTRSMOD_NONE:
    attrs = src_attrs;
    break;
  case RGWRados::ATTRSMOD_REPLACE:
    if (!attrs[RGW_ATTR_ETAG].length()) {
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];
    }
    break;
  case RGWRados::ATTRSMOD_MERGE:
    for (map<string, bufferlist>::iterator it = src_attrs.begin(); it != src_attrs.end(); ++it) {
      if (attrs.find(it->first) == attrs.end()) {
       attrs[it->first] = it->second;
      }
    }
    break;
  }
}

int RGWRados::rewrite_obj(RGWBucketInfo& dest_bucket_info, rgw_obj& obj)
{
  map<string, bufferlist> attrset;
  int64_t ofs = 0;
  int64_t end = -1;

  time_t mtime;
  uint64_t total_len;
  uint64_t obj_size;
  RGWObjectCtx rctx(this);

  RGWRados::Object op_target(this, dest_bucket_info, rctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrset;
  read_op.params.lastmod = &mtime;
  read_op.params.read_size = &total_len;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare(&ofs, &end);
  if (ret < 0)
    return ret;

  attrset.erase(RGW_ATTR_ID_TAG);

  uint64_t max_chunk_size;

  ret = get_max_chunk_size(obj.bucket, &max_chunk_size);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to get max_chunk_size() for bucket " << obj.bucket << dendl;
    return ret;
  }

  return copy_obj_data(rctx, dest_bucket_info, read_op, end, obj, obj, max_chunk_size, NULL, mtime, attrset, RGW_OBJ_CATEGORY_MAIN, 0, 0, NULL, NULL, NULL, NULL);
}

struct obj_time_weight {
  time_t mtime;
  uint32_t zone_short_id;
  uint64_t pg_ver;

  obj_time_weight() : mtime(0), zone_short_id(0), pg_ver(0) {}

  bool operator<(const obj_time_weight& rhs) {
    if (mtime > rhs.mtime) {
      return false;
    }
    if (mtime < rhs.mtime) {
      return true;
    }
    if (zone_short_id != rhs.zone_short_id) {
      return (zone_short_id < rhs.zone_short_id);
    }
    return (pg_ver < rhs.pg_ver);
  }

  void init(const time_t& _mtime, uint32_t _short_id, uint64_t _pg_ver) {
    mtime = _mtime;
    zone_short_id = _short_id;
    pg_ver = _pg_ver;
  }

  void init(RGWObjState *state) {
    mtime = state->mtime;
    zone_short_id = state->zone_short_id;
    pg_ver = state->pg_ver;
  }
};

inline ostream& operator<<(ostream& out, const obj_time_weight &o) {
  out << o.mtime;

  if (o.zone_short_id != 0 || o.pg_ver != 0) {
    out << "[zid=" << o.zone_short_id << ", pgv=" << o.pg_ver << "]";
  }

  return out;
}

int RGWRados::fetch_remote_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               const string& client_id,
               const string& op_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               time_t *src_mtime,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       time_t delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               struct rgw_err *err,
               void (*progress_cb)(off_t, void *),
               void *progress_data)
{
  /* source is in a different zonegroup, copy from there */

  RGWRESTStreamReadRequest *in_stream_req;
  string tag;
  map<string, bufferlist> src_attrs;
  int i;
  append_rand_alpha(cct, tag, tag, 32);
  obj_time_weight set_mtime_weight;

  RGWPutObjProcessor_Atomic processor(obj_ctx,
                                      dest_bucket_info, dest_obj.bucket, dest_obj.get_orig_obj(),
                                      cct->_conf->rgw_obj_stripe_size, tag, dest_bucket_info.versioning_enabled());
  const string& instance = dest_obj.get_instance();
  if (instance != "null") {
    processor.set_version_id(dest_obj.get_instance());
  }
  processor.set_olh_epoch(olh_epoch);
  int ret = processor.prepare(this, NULL);
  if (ret < 0) {
    return ret;
  }

  RGWRESTConn *conn;
  if (source_zone.empty()) {
    if (dest_bucket_info.zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = rest_master_conn;
    } else {
      map<string, RGWRESTConn *>::iterator iter = zonegroup_conn_map.find(src_bucket_info.zonegroup);
      if (iter == zonegroup_conn_map.end()) {
        ldout(cct, 0) << "could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    map<string, RGWRESTConn *>::iterator iter = zone_conn_map.find(source_zone);
    if (iter == zone_conn_map.end()) {
      ldout(cct, 0) << "could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  string obj_name = dest_obj.bucket.name + "/" + dest_obj.get_object();

  RGWOpStateSingleOp opstate(this, client_id, op_id, obj_name);

  ret = opstate.set_state(RGWOpState::OPSTATE_IN_PROGRESS);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
    return ret;
  }
  RGWRadosPutObj cb(&processor, &opstate, progress_cb, progress_data);
  string etag;
  map<string, string> req_headers;
  time_t set_mtime;

  RGWObjState *dest_state = NULL;

  const time_t *pmod = mod_ptr;

  obj_time_weight dest_mtime_weight;

  if (copy_if_newer) {
    /* need to get mtime for destination */
    ret = get_obj_state(&obj_ctx, dest_obj, &dest_state, NULL);
    if (ret < 0)
      return ret;

    if (dest_state->exists) {
      dest_mtime_weight.init(dest_state);
      pmod = &dest_mtime_weight.mtime;
    }
  }

 
  ret = conn->get_obj(user_id, info, src_obj, pmod, unmod_ptr,
                      dest_mtime_weight.zone_short_id, dest_mtime_weight.pg_ver,
                      true, &cb, &in_stream_req);
  if (ret < 0) {
    goto set_err_state;
  }

  ret = conn->complete_request(in_stream_req, etag, &set_mtime, req_headers);
  if (ret < 0) {
    goto set_err_state;
  }

  { /* opening scope so that we can do goto, sorry */
    bufferlist& extra_data_bl = processor.get_extra_data();
    if (extra_data_bl.length()) {
      JSONParser jp;
      if (!jp.parse(extra_data_bl.c_str(), extra_data_bl.length())) {
        ldout(cct, 0) << "failed to parse response extra data. len=" << extra_data_bl.length() << " data=" << extra_data_bl.c_str() << dendl;
        goto set_err_state;
      }

      JSONDecoder::decode_json("attrs", src_attrs, &jp);

      src_attrs.erase(RGW_ATTR_MANIFEST); // not interested in original object layout
      if (source_zone.empty()) { /* need to preserve expiration if copy in the same zonegroup */
        src_attrs.erase(RGW_ATTR_DELETE_AT);
      } else {
	map<string, bufferlist>::iterator iter = src_attrs.find(RGW_ATTR_DELETE_AT);
	if (iter != src_attrs.end()) {
	  try {
	    utime_t da;
	    ::decode(da, iter->second);
	    delete_at = (time_t)da.sec();
	  } catch (buffer::error& err) {
	    ldout(cct, 0) << "ERROR: failed to decode delete_at field in intra zone copy" << dendl;
	  }
	}
      }
    }
  }

  if (src_mtime) {
    *src_mtime = set_mtime;
  }

  if (petag) {
    map<string, bufferlist>::iterator iter = src_attrs.find(RGW_ATTR_ETAG);
    if (iter != src_attrs.end()) {
      bufferlist& etagbl = iter->second;
      *petag = string(etagbl.c_str(), etagbl.length());
    }
  }

  if (source_zone.empty()) {
    set_copy_attrs(src_attrs, attrs, attrs_mod);
  } else {
    attrs = src_attrs;
  }

  if (copy_if_newer) {
    uint64_t pg_ver = 0;
    auto i = attrs.find(RGW_ATTR_PG_VER);
    if (i != attrs.end() && i->second.length() > 0) {
      bufferlist::iterator iter = i->second.begin();
      try {
        ::decode(pg_ver, iter);
      } catch (buffer::error& err) {
        ldout(ctx(), 0) << "ERROR: failed to decode pg ver attribute, ignoring" << dendl;
        /* non critical error */
      }
    }
    set_mtime_weight.init(set_mtime, get_zone_short_id(), pg_ver);
  }

#define MAX_COMPLETE_RETRY 100
  for (i = 0; i < MAX_COMPLETE_RETRY; i++) {
    ret = cb.complete(etag, mtime, set_mtime, attrs, delete_at);
    if (ret < 0) {
      goto set_err_state;
    }
    if (copy_if_newer && cb.is_canceled()) {
      ldout(cct, 20) << "raced with another write of obj: " << dest_obj << dendl;
      obj_ctx.invalidate(dest_obj); /* object was overwritten */
      ret = get_obj_state(&obj_ctx, dest_obj, &dest_state, NULL);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << ": get_err_state() returned ret=" << ret << dendl;
        goto set_err_state;
      }
      dest_mtime_weight.init(dest_state);
      if (!dest_state->exists ||
        dest_mtime_weight < set_mtime_weight) {
        ldout(cct, 20) << "retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
        continue;
      } else {
        ldout(cct, 20) << "not retrying writing object mtime=" << set_mtime << " dest_state->mtime=" << dest_state->mtime << " dest_state->exists=" << dest_state->exists << dendl;
      }
    }
    break;
  }

  if (i == MAX_COMPLETE_RETRY) {
    ldout(cct, 0) << "ERROR: retried object completion too many times, something is wrong!" << dendl;
    ret = -EIO;
    goto set_err_state;
  }

  ret = opstate.set_state(RGWOpState::OPSTATE_COMPLETE);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
  }

  return 0;
set_err_state:
  RGWOpState::OpState state = RGWOpState::OPSTATE_ERROR;
  if (copy_if_newer && ret == -ERR_NOT_MODIFIED) {
    state = RGWOpState::OPSTATE_COMPLETE;
    ret = 0;
  }
  int r = opstate.set_state(state);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to set opstate r=" << ret << dendl;
  }
  return ret;
}


int RGWRados::copy_obj_to_remote_dest(RGWObjState *astate,
                                      map<string, bufferlist>& src_attrs,
                                      RGWRados::Object::Read& read_op,
                                      const rgw_user& user_id,
                                      rgw_obj& dest_obj,
                                      time_t *mtime)
{
  string etag;

  RGWRESTStreamWriteRequest *out_stream_req;

  int ret = rest_master_conn->put_obj_init(user_id, dest_obj, astate->size, src_attrs, &out_stream_req);
  if (ret < 0) {
    delete out_stream_req;
    return ret;
  }

  ret = read_op.iterate(0, astate->size - 1, out_stream_req->get_out_cb());
  if (ret < 0)
    return ret;

  ret = rest_master_conn->complete_request(out_stream_req, etag, mtime);
  if (ret < 0)
    return ret;

  return 0;
}

/**
 * Copy an object.
 * dest_obj: the object to copy into
 * src_obj: the object to copy from
 * attrs: usage depends on attrs_mod parameter
 * attrs_mod: the modification mode of the attrs, may have the following values:
 *            ATTRSMOD_NONE - the attributes of the source object will be
 *                            copied without modifications, attrs parameter is ignored;
 *            ATTRSMOD_REPLACE - new object will have the attributes provided by attrs
 *                               parameter, source object attributes are not copied;
 *            ATTRSMOD_MERGE - any conflicting meta keys on the source object's attributes
 *                             are overwritten by values contained in attrs parameter.
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(RGWObjectCtx& obj_ctx,
               const rgw_user& user_id,
               const string& client_id,
               const string& op_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               time_t *src_mtime,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               AttrsMod attrs_mod,
               bool copy_if_newer,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       time_t delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               struct rgw_err *err,
               void (*progress_cb)(off_t, void *),
               void *progress_data)
{
  int ret;
  uint64_t total_len, obj_size;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  bool remote_src;
  bool remote_dest;

  append_rand_alpha(cct, dest_obj.get_object(), shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  remote_dest = !get_zonegroup().equals(dest_bucket_info.zonegroup);
  remote_src = !get_zonegroup().equals(src_bucket_info.zonegroup);

  if (remote_src && remote_dest) {
    ldout(cct, 0) << "ERROR: can't copy object when both src and dest buckets are remote" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.get_object() << " => " << dest_obj.bucket << ":" << dest_obj.get_object() << dendl;

  if (remote_src || !source_zone.empty()) {
    return fetch_remote_obj(obj_ctx, user_id, client_id, op_id, info, source_zone,
               dest_obj, src_obj, dest_bucket_info, src_bucket_info, src_mtime, mtime, mod_ptr,
               unmod_ptr, if_match, if_nomatch, attrs_mod, copy_if_newer, attrs, category,
               olh_epoch, delete_at, version_id, ptag, petag, err, progress_cb, progress_data);
  }

  map<string, bufferlist> src_attrs;
  int64_t ofs = 0;
  int64_t end = -1;
  RGWRados::Object src_op_target(this, src_bucket_info, obj_ctx, src_obj);
  RGWRados::Object::Read read_op(&src_op_target);

  read_op.conds.mod_ptr = mod_ptr;
  read_op.conds.unmod_ptr = unmod_ptr;
  read_op.conds.if_match = if_match;
  read_op.conds.if_nomatch = if_nomatch;
  read_op.params.attrs = &src_attrs;
  read_op.params.lastmod = src_mtime;
  read_op.params.read_size = &total_len;
  read_op.params.obj_size = &obj_size;
  read_op.params.perr = err;

  ret = read_op.prepare(&ofs, &end);
  if (ret < 0) {
    return ret;
  }

  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  src_attrs.erase(RGW_ATTR_DELETE_AT);

  set_copy_attrs(src_attrs, attrs, attrs_mod);
  attrs.erase(RGW_ATTR_ID_TAG);
  attrs.erase(RGW_ATTR_PG_VER);
  attrs.erase(RGW_ATTR_SOURCE_ZONE);

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;
  ret = get_obj_state(&obj_ctx, src_obj, &astate, NULL);
  if (ret < 0) {
    return ret;
  }

  vector<rgw_obj> ref_objs;

  if (remote_dest) {
    /* dest is in a different zonegroup, copy it there */
    return copy_obj_to_remote_dest(astate, attrs, read_op, user_id, dest_obj, mtime);
  }
  uint64_t max_chunk_size;

  ret = get_max_chunk_size(dest_obj.bucket, &max_chunk_size);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to get max_chunk_size() for bucket " << dest_obj.bucket << dendl;
    return ret;
  }

  bool copy_data = !astate->has_manifest || (src_obj.bucket.data_pool != dest_obj.bucket.data_pool);
  bool copy_first = false;
  if (astate->has_manifest) {
    if (!astate->manifest.has_tail()) {
      copy_data = true;
    } else {
      uint64_t head_size = astate->manifest.get_head_size();

      if (head_size > 0) {
        if (head_size > max_chunk_size) {
          copy_data = true;
        } else {
          copy_first = true;
        }
      }
    }
  }

  if (petag) {
    map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
    if (iter != attrs.end()) {
      bufferlist& etagbl = iter->second;
      *petag = string(etagbl.c_str(), etagbl.length());
    }
  }

  if (copy_data) { /* refcounting tail wouldn't work here, just copy the data */
    return copy_obj_data(obj_ctx, dest_bucket_info, read_op, end, dest_obj, src_obj,
                         max_chunk_size, mtime, 0, attrs, category, olh_epoch, delete_at,
                         version_id, ptag, petag, err);
  }

  RGWObjManifest::obj_iterator miter = astate->manifest.obj_begin();

  if (copy_first) { // we need to copy first chunk, not increase refcount
    ++miter;
  }

  rgw_rados_ref ref;
  rgw_bucket bucket;
  ret = get_obj_ref(miter.get_location(), &ref, &bucket);
  if (ret < 0) {
    return ret;
  }

  bool versioned_dest = dest_bucket_info.versioning_enabled();

  if (version_id && !version_id->empty()) {
    versioned_dest = true;
    dest_obj.set_instance(*version_id);
  } else if (versioned_dest) {
    gen_rand_obj_instance_name(&dest_obj);
  }

  bufferlist first_chunk;

  bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest; 
  ldout(cct, 0) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;

  RGWRados::Object dest_op_target(this, dest_bucket_info, obj_ctx, dest_obj);
  RGWRados::Object::Write write_op(&dest_op_target);

  string tag;

  if (ptag) {
    tag = *ptag;
  }

  if (tag.empty()) {
    append_rand_alpha(cct, tag, tag, 32);
  }

  if (!copy_itself) {
    manifest = astate->manifest;
    rgw_bucket& tail_bucket = manifest.get_tail_bucket();
    if (tail_bucket.name.empty()) {
      manifest.set_tail_bucket(src_obj.bucket);
    }
    string oid, key;
    for (; miter != astate->manifest.obj_end(); ++miter) {
      ObjectWriteOperation op;
      cls_refcount_get(op, tag, true);
      const rgw_obj& loc = miter.get_location();
      get_obj_bucket_and_oid_loc(loc, bucket, oid, key);
      ref.ioctx.locator_set_key(key);

      ret = ref.ioctx.operate(oid, &op);
      if (ret < 0) {
        goto done_ret;
      }

      ref_objs.push_back(loc);
    }

    pmanifest = &manifest;
  } else {
    pmanifest = &astate->manifest;
    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = read_op.read(0, max_chunk_size, first_chunk);
    if (ret < 0) {
      goto done_ret;
    }

    pmanifest->set_head(dest_obj);
    pmanifest->set_head_size(first_chunk.length());
  }

  write_op.meta.data = &first_chunk;
  write_op.meta.manifest = pmanifest;
  write_op.meta.ptag = &tag;
  write_op.meta.owner = dest_bucket_info.owner;
  write_op.meta.mtime = mtime;
  write_op.meta.flags = PUT_OBJ_CREATE;
  write_op.meta.category = category;
  write_op.meta.olh_epoch = olh_epoch;
  write_op.meta.delete_at = delete_at;

  ret = write_op.write_meta(end + 1, attrs);
  if (ret < 0) {
    goto done_ret;
  }

  return 0;

done_ret:
  if (!copy_itself) {
    vector<rgw_obj>::iterator riter;

    string oid, key;

    /* rollback reference */
    for (riter = ref_objs.begin(); riter != ref_objs.end(); ++riter) {
      ObjectWriteOperation op;
      cls_refcount_put(op, tag, true);

      get_obj_bucket_and_oid_loc(*riter, bucket, oid, key);
      ref.ioctx.locator_set_key(key);

      int r = ref.ioctx.operate(oid, &op);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << *riter << dendl;
      }
    }
  }
  return ret;
}


int RGWRados::copy_obj_data(RGWObjectCtx& obj_ctx,
               RGWBucketInfo& dest_bucket_info,
	       RGWRados::Object::Read& read_op, off_t end,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               uint64_t max_chunk_size,
	       time_t *mtime,
	       time_t set_mtime,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               uint64_t olh_epoch,
	       time_t delete_at,
               string *version_id,
               string *ptag,
               string *petag,
               struct rgw_err *err)
{
  bufferlist first_chunk;
  RGWObjManifest manifest;

  string tag;
  append_rand_alpha(cct, tag, tag, 32);

  RGWPutObjProcessor_Atomic processor(obj_ctx,
                                      dest_bucket_info, dest_obj.bucket, dest_obj.get_object(),
                                      cct->_conf->rgw_obj_stripe_size, tag, dest_bucket_info.versioning_enabled());
  if (version_id) {
    processor.set_version_id(*version_id);
  }
  processor.set_olh_epoch(olh_epoch);
  int ret = processor.prepare(this, NULL);
  if (ret < 0)
    return ret;

  off_t ofs = 0;

  do {
    bufferlist bl;
    ret = read_op.read(ofs, end, bl);

    uint64_t read_len = ret;
    bool again;

    do {
      void *handle;

      ret = processor.handle_data(bl, ofs, NULL, &handle, &again);
      if (ret < 0) {
        return ret;
      }
      ret = processor.throttle_data(handle, false);
      if (ret < 0)
        return ret;
    } while (again);

    ofs += read_len;
  } while (ofs <= end);

  string etag;
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_ETAG);
  if (iter != attrs.end()) {
    bufferlist& bl = iter->second;
    etag = string(bl.c_str(), bl.length());
    if (petag) {
      *petag = etag;
    }
  }

  ret = processor.complete(etag, mtime, set_mtime, attrs, delete_at);

  return ret;
}

bool RGWRados::is_meta_master()
{
  if (!get_zonegroup().is_master) {
    return false;
  }

  return (get_zonegroup().master_zone == zone_public_config.id);
}

/**
  * Check to see if the bucket metadata could be synced
  * bucket: the bucket to check
  * Returns false is the bucket is not synced
  */
bool RGWRados::is_syncing_bucket_meta(rgw_bucket& bucket)
{
  /* zonegroup is not master zonegroup */
  if (!get_zonegroup().is_master) {
    return false;
  }

  /* single zonegroup and a single zone */
  if (current_period.is_single_zonegroup(cct, this) && get_zonegroup().zones.size() == 1) {
    return false;
  }

  /* zone is not master */
  if (get_zonegroup().master_zone.compare(zone_public_config.id) != 0) {
    return false;
  }

  return true;
}
  
/**
 * Delete a bucket.
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(rgw_bucket& bucket, RGWObjVersionTracker& objv_tracker)
{
  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  std::map<string, RGWObjEnt> ent_map;
  rgw_obj_key marker;
  string prefix;
  bool is_truncated;

  do {
#define NUM_ENTRIES 1000
    r = cls_bucket_list(bucket, RGW_NO_SHARD, marker, prefix, NUM_ENTRIES, true, ent_map,
                        &is_truncated, &marker);
    if (r < 0)
      return r;

    string ns;
    std::map<string, RGWObjEnt>::iterator eiter;
    rgw_obj_key obj;
    string instance;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      obj = eiter->second.key;

      if (rgw_obj::translate_raw_obj_to_obj_in_ns(obj.name, instance, ns))
        return -ENOTEMPTY;
    }
  } while (is_truncated);

  r = rgw_bucket_delete_bucket_obj(this, bucket.tenant, bucket.name, objv_tracker);
  if (r < 0)
    return r;

  /* if the bucket is not synced we can remove the meta file */
  if (!is_syncing_bucket_meta(bucket)) {
    RGWObjVersionTracker objv_tracker;
    string entry;
    get_bucket_instance_entry(bucket, entry);
    r= rgw_bucket_instance_remove_entry(this, entry, &objv_tracker);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}


int RGWRados::set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner)
{
  RGWBucketInfo info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(this);
  int r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, &attrs);
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  info.owner = owner.get_id();

  r = put_bucket_instance_info(info, false, 0, &attrs);
  if (r < 0) {
    ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << dendl;
    return r;
  }

  return 0;
}


int RGWRados::set_buckets_enabled(vector<rgw_bucket>& buckets, bool enabled)
{
  int ret = 0;

  vector<rgw_bucket>::iterator iter;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    rgw_bucket& bucket = *iter;
    if (enabled)
      ldout(cct, 20) << "enabling bucket name=" << bucket.name << dendl;
    else
      ldout(cct, 20) << "disabling bucket name=" << bucket.name << dendl;

    RGWBucketInfo info;
    map<string, bufferlist> attrs;
    RGWObjectCtx obj_ctx(this);
    int r = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
    if (enabled) {
      info.flags &= ~BUCKET_SUSPENDED;
    } else {
      info.flags |= BUCKET_SUSPENDED;
    }

    r = put_bucket_instance_info(info, false, 0, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
  }
  return ret;
}

int RGWRados::bucket_suspended(rgw_bucket& bucket, bool *suspended)
{
  RGWBucketInfo bucket_info;
  RGWObjectCtx obj_ctx(this);
  int ret = get_bucket_info(obj_ctx, bucket.tenant, bucket.name, bucket_info, NULL);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::Object::complete_atomic_modification()
{
  if (!state->has_manifest || state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  store->update_gc_chain(obj, state->manifest, &chain);

  string tag = (state->obj_tag.c_str() ? state->obj_tag.c_str() : "");
  int ret = store->gc->send_chain(chain, tag, false);  // do it async

  return ret;
}

void RGWRados::update_gc_chain(rgw_obj& head_obj, RGWObjManifest& manifest, cls_rgw_obj_chain *chain)
{
  RGWObjManifest::obj_iterator iter;
  for (iter = manifest.obj_begin(); iter != manifest.obj_end(); ++iter) {
    const rgw_obj& mobj = iter.get_location();
    if (mobj == head_obj)
      continue;
    string oid, loc;
    rgw_bucket bucket;
    get_obj_bucket_and_oid_loc(mobj, bucket, oid, loc);
    cls_rgw_obj_key key(oid);
    chain->push_obj(bucket.data_pool, key, loc);
  }
}

int RGWRados::send_chain_to_gc(cls_rgw_obj_chain& chain, const string& tag, bool sync)
{
  return gc->send_chain(chain, tag, sync);
}

int RGWRados::open_bucket_index(rgw_bucket& bucket, librados::IoCtx& index_ctx, string& bucket_oid)
{
  int r = open_bucket_index_ctx(bucket, index_ctx);
  if (r < 0)
    return r;

  if (bucket.marker.empty()) {
    ldout(cct, 0) << "ERROR: empty marker for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid = dir_oid_prefix;
  bucket_oid.append(bucket.marker);

  return 0;
}

int RGWRados::open_bucket_index_base(rgw_bucket& bucket, librados::IoCtx& index_ctx,
    string& bucket_oid_base) {
  int r = open_bucket_index_ctx(bucket, index_ctx);
  if (r < 0)
    return r;

  if (bucket.marker.empty()) {
    ldout(cct, 0) << "ERROR: empty marker for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid_base = dir_oid_prefix;
  bucket_oid_base.append(bucket.marker);

  return 0;

}

int RGWRados::open_bucket_index(rgw_bucket& bucket, librados::IoCtx& index_ctx,
    map<int, string>& bucket_objs, int shard_id, map<int, string> *bucket_instance_ids) {
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket, index_ctx, bucket_oid_base);
  if (ret < 0)
    return ret;

  RGWObjectCtx obj_ctx(this);

  // Get the bucket info
  RGWBucketInfo binfo;
  ret = get_bucket_instance_info(obj_ctx, bucket, binfo, NULL, NULL);
  if (ret < 0)
    return ret;

  get_bucket_index_objects(bucket_oid_base, binfo.num_shards, bucket_objs, shard_id);
  if (bucket_instance_ids) {
    get_bucket_instance_ids(binfo, shard_id, bucket_instance_ids);
  }
  return 0;
}

template<typename T>
int RGWRados::open_bucket_index(rgw_bucket& bucket, librados::IoCtx& index_ctx,
                                map<int, string>& oids, map<int, T>& bucket_objs,
                                int shard_id, map<int, string> *bucket_instance_ids)
{
  int ret = open_bucket_index(bucket, index_ctx, oids, shard_id, bucket_instance_ids);
  if (ret < 0)
    return ret;

  map<int, string>::const_iterator iter = oids.begin();
  for (; iter != oids.end(); ++iter) {
    bucket_objs[iter->first] = T();
  }
  return 0;
}

int RGWRados::open_bucket_index_shard(rgw_bucket& bucket, librados::IoCtx& index_ctx,
    const string& obj_key, string *bucket_obj, int *shard_id)
{
  string bucket_oid_base;
  int ret = open_bucket_index_base(bucket, index_ctx, bucket_oid_base);
  if (ret < 0)
    return ret;

  RGWObjectCtx obj_ctx(this);

  // Get the bucket info
  RGWBucketInfo binfo;
  ret = get_bucket_instance_info(obj_ctx, bucket, binfo, NULL, NULL);
  if (ret < 0)
    return ret;

  ret = get_bucket_index_object(bucket_oid_base, obj_key, binfo.num_shards,
        (RGWBucketInfo::BIShardsHashType)binfo.bucket_index_shard_hash_type, bucket_obj, shard_id);
  if (ret < 0) {
    ldout(cct, 10) << "get_bucket_index_object() returned ret=" << ret << dendl;
  }
  return 0;
}

static void accumulate_raw_stats(rgw_bucket_dir_header& header, map<RGWObjCategory, RGWStorageStats>& stats)
{
  map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.begin();
  for (; iter != header.stats.end(); ++iter) {
    RGWObjCategory category = (RGWObjCategory)iter->first;
    RGWStorageStats& s = stats[category];
    struct rgw_bucket_category_stats& header_stats = iter->second;
    s.category = (RGWObjCategory)iter->first;
    s.num_kb += ((header_stats.total_size + 1023) / 1024);
    s.num_kb_rounded += ((header_stats.total_size_rounded + 1023) / 1024);
    s.num_objects += header_stats.num_entries;
  }
}

int RGWRados::bucket_check_index(rgw_bucket& bucket,
				 map<RGWObjCategory, RGWStorageStats> *existing_stats,
				 map<RGWObjCategory, RGWStorageStats> *calculated_stats)
{
  librados::IoCtx index_ctx;
  // key - bucket index object id
  // value - bucket index check OP returned result with the given bucket index object (shard)
  map<int, string> oids;
  map<int, struct rgw_cls_check_index_ret> bucket_objs_ret;
  int ret = open_bucket_index(bucket, index_ctx, oids, bucket_objs_ret);
  if (ret < 0)
    return ret;

  ret = CLSRGWIssueBucketCheck(index_ctx, oids, bucket_objs_ret, cct->_conf->rgw_bucket_index_max_aio)();
  if (ret < 0)
    return ret;

  // Aggregate results (from different shards if there is any)
  map<int, struct rgw_cls_check_index_ret>::iterator iter;
  for (iter = bucket_objs_ret.begin(); iter != bucket_objs_ret.end(); ++iter) {
    accumulate_raw_stats(iter->second.existing_header, *existing_stats);
    accumulate_raw_stats(iter->second.calculated_header, *calculated_stats);
  }

  return 0;
}

int RGWRados::bucket_rebuild_index(rgw_bucket& bucket)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket, index_ctx, bucket_objs);
  if (r < 0)
    return r;

  return CLSRGWIssueBucketRebuild(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio)();
}


int RGWRados::defer_gc(void *ctx, rgw_obj& obj)
{
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);
  if (!rctx)
    return 0;

  RGWObjState *state = NULL;

  int r = get_obj_state(rctx, obj, &state, NULL);
  if (r < 0)
    return r;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << obj << " is not atomic, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  if (state->obj_tag.length() == 0) {// check for backward compatibility
    ldout(cct, 20) << "state->obj_tag is empty, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  string tag = state->obj_tag.c_str();

  ldout(cct, 0) << "defer chain tag=" << tag << dendl;

  return gc->defer_chain(tag, false);
}

void RGWRados::remove_rgw_head_obj(ObjectWriteOperation& op)
{
  list<string> prefixes;
  prefixes.push_back(RGW_ATTR_OLH_PREFIX);
  cls_rgw_remove_obj(op, prefixes);
}

void RGWRados::cls_obj_check_prefix_exist(ObjectOperation& op, const string& prefix, bool fail_if_exist)
{
  cls_rgw_obj_check_attrs_prefix(op, prefix, fail_if_exist);
}

void RGWRados::cls_obj_check_mtime(ObjectOperation& op, const utime_t& mtime, RGWCheckMTimeType type)
{
  cls_rgw_obj_check_mtime(op, mtime, type);
}


/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::Object::Delete::delete_obj()
{
  RGWRados *store = target->get_store();
  rgw_obj& src_obj = target->get_obj();
  const string& instance = src_obj.get_instance();
  rgw_obj obj = src_obj;

  if (instance == "null") {
    obj.clear_instance();
  }

  bool explicit_marker_version = (!params.marker_version_id.empty());

  if (params.versioning_status & BUCKET_VERSIONED || explicit_marker_version) {
    if (instance.empty() || explicit_marker_version) {
      rgw_obj marker = obj;

      if (!params.marker_version_id.empty()) {
        if (params.marker_version_id != "null") {
          marker.set_instance(params.marker_version_id);
        }
      } else if ((params.versioning_status & BUCKET_VERSIONS_SUSPENDED) == 0) {
        store->gen_rand_obj_instance_name(&marker);
      }

      result.version_id = marker.get_instance();
      result.delete_marker = true;

      struct rgw_bucket_dir_entry_meta meta;

      meta.owner = params.obj_owner.get_id().to_str();
      meta.owner_display_name = params.obj_owner.get_display_name();

      if (params.mtime == 0) {
        meta.mtime = ceph_clock_now(store->ctx());
      } else {
        meta.mtime = utime_t(params.mtime, 0);
      }

      int r = store->set_olh(target->get_ctx(), target->get_bucket_info(), marker, true, &meta, params.olh_epoch, params.unmod_since);
      if (r < 0) {
        return r;
      }
    } else {
      rgw_bucket_dir_entry dirent;

      int r = store->bi_get_instance(obj, &dirent);
      if (r < 0) {
        return r;
      }
      result.delete_marker = dirent.is_delete_marker();
      r = store->unlink_obj_instance(target->get_ctx(), target->get_bucket_info(), obj, params.olh_epoch);
      if (r < 0) {
        return r;
      }
      result.version_id = instance;
    }

    BucketShard *bs;
    int r = target->get_bucket_shard(&bs);
    if (r < 0) {
      ldout(store->ctx(), 5) << "failed to get BucketShard object: r=" << r << dendl;
      return r;
    }

    r = store->data_log->add_entry(bs->bucket, bs->shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
      return r;
    }

    return 0;
  }

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = store->get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  RGWObjState *state;
  r = target->get_state(&state, false);
  if (r < 0)
    return r;

  ObjectWriteOperation op;

  if (params.unmod_since > 0) {
    time_t ctime = state->mtime;

    ldout(store->ctx(), 10) << "If-UnModified-Since: " << params.unmod_since << " Last-Modified: " << ctime << dendl;
    if (ctime > params.unmod_since) {
      return -ERR_PRECONDITION_FAILED;
    }

    /* only delete object if mtime is less than or equal to params.unmod_since */
    store->cls_obj_check_mtime(op, utime_t(params.unmod_since, 0), CLS_RGW_CHECK_TIME_MTIME_LE);
  }
  uint64_t obj_size = state->size;

  if (!params.expiration_time.is_zero()) {
    bufferlist bl;
    utime_t delete_at;

    if (state->get_attr(RGW_ATTR_DELETE_AT, bl)) {
      try {
        bufferlist::iterator iter = bl.begin();
        ::decode(delete_at, iter);
      } catch (buffer::error& err) {
        ldout(store->ctx(), 0) << "ERROR: couldn't decode RGW_ATTR_DELETE_AT" << dendl;
	return -EIO;
      }

      if (params.expiration_time != delete_at) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  r = target->prepare_atomic_modification(op, false, NULL, NULL, NULL, true);
  if (r < 0)
    return r;

  bool ret_not_existed = (!state->exists);

  RGWBucketInfo& bucket_info = target->get_bucket_info();

  RGWRados::Bucket bop(store, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj, state);

  index_op.set_bilog_flags(params.bilog_flags);

  r = store->swift_versioning_copy(bucket_info, target, state, params.bucket_owner);
  if (r < 0) {
    return r;
  }

  r = index_op.prepare(CLS_RGW_OP_DEL);
  if (r < 0)
    return r;

  store->remove_rgw_head_obj(op);
  r = ref.ioctx.operate(ref.oid, &op);
  bool need_invalidate = false;
  if (r == -ECANCELED) {
    /* raced with another operation, we can regard it as removed */
    need_invalidate = true;
    r = 0;
  }
  bool removed = (r >= 0);

  int64_t poolid = ref.ioctx.get_id();
  if (r >= 0) {
    r = index_op.complete_del(poolid, ref.ioctx.get_last_version(), params.remove_objs);
  } else {
    int ret = index_op.cancel();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: index_op.cancel() returned ret=" << ret << dendl;
    }
  }
  if (removed) {
    int ret = target->complete_atomic_modification();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: complete_atomic_modification returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  }

  if (need_invalidate) {
    target->invalidate_state();
  }

  if (r < 0)
    return r;

  if (ret_not_existed)
    return -ENOENT;

  /* update quota cache */
  store->quota_handler->update_stats(params.bucket_owner, bucket, -1, 0, obj_size);

  return 0;
}

int RGWRados::delete_obj(RGWObjectCtx& obj_ctx,
                         RGWBucketInfo& bucket_info,
                         rgw_obj& obj,
                         int versioning_status,
                         uint16_t bilog_flags,
                         const utime_t& expiration_time)
{
  RGWRados::Object del_target(this, bucket_info, obj_ctx, obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = versioning_status;
  del_op.params.bilog_flags = bilog_flags;
  del_op.params.expiration_time = expiration_time;

  return del_op.delete_obj();
}

int RGWRados::delete_system_obj(rgw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectWriteOperation op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  op.remove();
  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::delete_obj_index(rgw_obj& obj)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

  RGWObjectCtx obj_ctx(this);

  RGWBucketInfo bucket_info;
  int ret = get_bucket_instance_info(obj_ctx, bucket, bucket_info, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "() get_bucket_instance_info(bucket=" << bucket << ") returned ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj, NULL);

  int r = index_op.complete_del(-1 /* pool */, 0, NULL);

  return r;
}

static void generate_fake_tag(CephContext *cct, map<string, bufferlist>& attrset, RGWObjManifest& manifest, bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  RGWObjManifest::obj_iterator mi = manifest.obj_begin();
  if (mi != manifest.obj_end()) {
    if (manifest.has_tail()) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi.get_location().get_object();
    tag.append("_");
  }

  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;
  hash.Update((const byte *)manifest_bl.c_str(), manifest_bl.length());

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
  if (iter != attrset.end()) {
    bufferlist& bl = iter->second;
    hash.Update((const byte *)bl.c_str(), bl.length());
  }

  hash.Final(md5);
  buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
  tag.append(md5_str);

  ldout(cct, 10) << "generate_fake_tag new tag=" << tag << dendl;

  tag_bl.append(tag.c_str(), tag.size() + 1);
}

static bool is_olh(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_INFO);
  return (iter != attrs.end());
}

static bool has_olh_tag(map<string, bufferlist>& attrs)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_OLH_ID_TAG);
  return (iter != attrs.end());
}

int RGWRados::get_olh_target_state(RGWObjectCtx& obj_ctx, rgw_obj& obj, RGWObjState *olh_state,
                                   RGWObjState **target_state)
{
  assert(olh_state->is_olh);

  rgw_obj target;
  int r = RGWRados::follow_olh(obj_ctx, olh_state, obj, &target); /* might return -EAGAIN */
  if (r < 0) {
    return r;
  }
  r = get_obj_state(&obj_ctx, target, target_state, false);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWRados::get_system_obj_state_impl(RGWObjectCtx *rctx, rgw_obj& obj, RGWObjState **state, RGWObjVersionTracker *objv_tracker)
{
  RGWObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    return 0;
  }

  s->obj = obj;

  int r = raw_obj_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), objv_tracker);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = 0;
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_system_obj_state: setting s->obj_tag to " << string(s->obj_tag.c_str(), s->obj_tag.length()) << dendl;
  else
    ldout(cct, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;

  return 0;
}

int RGWRados::get_system_obj_state(RGWObjectCtx *rctx, rgw_obj& obj, RGWObjState **state, RGWObjVersionTracker *objv_tracker)
{
  int ret;

  do {
    ret = get_system_obj_state_impl(rctx, obj, state, objv_tracker);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWRados::get_obj_state_impl(RGWObjectCtx *rctx, rgw_obj& obj, RGWObjState **state, bool follow_olh)
{
  bool need_follow_olh = follow_olh && !obj.have_instance();

  RGWObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    if (s->is_olh && need_follow_olh) {
      return get_olh_target_state(*rctx, obj, s, state);
    }
    return 0;
  }

  s->obj = obj;

  int r = RGWRados::raw_obj_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), NULL);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = 0;
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;

  map<string, bufferlist>::iterator iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ);
  if (iter != s->attrset.end()) {
    bufferlist bl = iter->second;
    bufferlist::iterator it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (manifest_bl.length()) {
    bufferlist::iterator miter = manifest_bl.begin();
    try {
      ::decode(s->manifest, miter);
      s->has_manifest = true;
      s->size = s->manifest.get_obj_size();
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldout(cct, 10) << "manifest: total_size = " << s->manifest.get_obj_size() << dendl;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20) && s->manifest.has_explicit_objs()) {
      RGWObjManifest::obj_iterator mi;
      for (mi = s->manifest.obj_begin(); mi != s->manifest.obj_end(); ++mi) {
        ldout(cct, 20) << "manifest: ofs=" << mi.get_ofs() << " loc=" << mi.get_location() << dendl;
      }
    }

    if (!s->obj_tag.length()) {
      /*
       * Uh oh, something's wrong, object with manifest should have tag. Let's
       * create one out of the manifest, would be unique
       */
      generate_fake_tag(cct, s->attrset, s->manifest, manifest_bl, s->obj_tag);
      s->fake_tag = true;
    }
  }
  map<string, bufferlist>::iterator aiter = s->attrset.find(RGW_ATTR_PG_VER);
  if (aiter != s->attrset.end()) {
    bufferlist& pg_ver_bl = aiter->second;
    if (pg_ver_bl.length()) {
      bufferlist::iterator pgbl = pg_ver_bl.begin();
      try {
        ::decode(s->pg_ver, pgbl);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: couldn't decode pg ver attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  aiter = s->attrset.find(RGW_ATTR_SOURCE_ZONE);
  if (aiter != s->attrset.end()) {
    bufferlist& zone_short_id_bl = aiter->second;
    if (zone_short_id_bl.length()) {
      bufferlist::iterator zbl = zone_short_id_bl.begin();
      try {
        ::decode(s->zone_short_id, zbl);
      } catch (buffer::error& err) {
        ldout(cct, 0) << "ERROR: couldn't decode zone short id attr for object " << s->obj << ", non-critical error, ignoring" << dendl;
      }
    }
  }
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_obj_state: setting s->obj_tag to " << string(s->obj_tag.c_str(), s->obj_tag.length()) << dendl;
  else
    ldout(cct, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;

  /* an object might not be olh yet, but could have olh id tag, so we should set it anyway if
   * it exist, and not only if is_olh() returns true
   */
  iter = s->attrset.find(RGW_ATTR_OLH_ID_TAG);
  if (iter != s->attrset.end()) {
    s->olh_tag = iter->second;
  }

  if (is_olh(s->attrset)) {
    s->is_olh = true;

    ldout(cct, 20) << __func__ << ": setting s->olh_tag to " << string(s->olh_tag.c_str(), s->olh_tag.length()) << dendl;

    if (need_follow_olh) {
      return get_olh_target_state(*rctx, obj, s, state);
    }
  }

  return 0;
}

int RGWRados::get_obj_state(RGWObjectCtx *rctx, rgw_obj& obj, RGWObjState **state, bool follow_olh)
{
  int ret;

  do {
    ret = get_obj_state_impl(rctx, obj, state, follow_olh);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWRados::Object::Read::get_attr(const char *name, bufferlist& dest)
{
  RGWObjState *state;
  int r = source->get_state(&state, true);
  if (r < 0)
    return r;
  if (!state->exists)
    return -ENOENT;
  if (!state->get_attr(name, dest))
    return -ENODATA;

  return 0;
}


int RGWRados::Object::Stat::stat_async()
{
  RGWObjectCtx& ctx = source->get_ctx();
  rgw_obj& obj = source->get_obj();
  RGWRados *store = source->get_store();

  RGWObjState *s = ctx.get_state(obj); /* calling this one directly because otherwise a sync request will be sent */
  result.obj = obj;
  if (s->has_attrs) {
    state.ret = 0;
    result.size = s->size;
    result.mtime = s->mtime;
    result.attrs = s->attrset;
    result.has_manifest = s->has_manifest;
    result.manifest = s->manifest;
    return 0;
  }

  string oid;
  string loc;
  rgw_bucket bucket;
  get_obj_bucket_and_oid_loc(obj, bucket, oid, loc);

  int r = store->get_obj_ioctx(obj, &state.io_ctx);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  op.stat(&result.size, &result.mtime, NULL);
  op.getxattrs(&result.attrs, NULL);
  state.completion = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  state.io_ctx.locator_set_key(loc);
  r = state.io_ctx.aio_operate(oid, state.completion, &op, NULL);
  if (r < 0) {
    ldout(store->ctx(), 5) << __func__ << ": ERROR: aio_operate() returned ret=" << r << dendl;
    return r;
  }

  return 0;
}


int RGWRados::Object::Stat::wait()
{
  if (!state.completion) {
    return state.ret;
  }

  state.completion->wait_for_complete();
  state.ret = state.completion->get_return_value();
  state.completion->release();

  if (state.ret != 0) {
    return state.ret;
  }

  return finish();
}

int RGWRados::Object::Stat::finish()
{
  map<string, bufferlist>::iterator iter = result.attrs.find(RGW_ATTR_MANIFEST);
  if (iter != result.attrs.end()) {
    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    try {
      ::decode(result.manifest, biter);
    } catch (buffer::error& err) {
      RGWRados *store = source->get_store();
      ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode manifest"  << dendl;
      return -EIO;
    }
    result.has_manifest = true;
  }

  return 0;
}

/**
 * Get the attributes for an object.
 * bucket: name of the bucket holding the object.
 * obj: name of the object
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::system_obj_get_attr(rgw_obj& obj, const char *name, bufferlist& dest)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_system_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  ObjectReadOperation op;

  int rval;
  op.getxattr(name, &dest, &rval);
  
  r = ref.ioctx.operate(ref.oid, &op, NULL);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::append_atomic_test(RGWObjectCtx *rctx, rgw_obj& obj,
                            ObjectOperation& op, RGWObjState **pstate)
{
  if (!rctx)
    return 0;

  int r = get_obj_state(rctx, obj, pstate, NULL);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << obj << " is not atomic, not appending atomic test" << dendl;
    return 0;
  }

  if (state->obj_tag.length() > 0 && !state->fake_tag) {// check for backward compatibility
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
  } else {
    ldout(cct, 20) << "state->obj_tag is empty, not appending atomic test" << dendl;
  }
  return 0;
}

int RGWRados::Object::get_state(RGWObjState **pstate, bool follow_olh)
{
  return store->get_obj_state(&ctx, obj, pstate, follow_olh);
}

void RGWRados::Object::invalidate_state()
{
  ctx.invalidate(obj);
}

int RGWRados::Object::prepare_atomic_modification(ObjectWriteOperation& op, bool reset_obj, const string *ptag,
                                                  const char *if_match, const char *if_nomatch, bool removal_op)
{
  int r = get_state(&state, false);
  if (r < 0)
    return r;

  bool need_guard = (state->has_manifest || (state->obj_tag.length() != 0) ||
                     if_match != NULL || if_nomatch != NULL) &&
                     (!state->fake_tag);

  if (!state->is_atomic) {
    ldout(store->ctx(), 20) << "prepare_atomic_modification: state is not atomic. state=" << (void *)state << dendl;

    if (reset_obj) {
      op.create(false);
      store->remove_rgw_head_obj(op); // we're not dropping reference here, actually removing object
    }

    return 0;
  }

  if (need_guard) {
    /* first verify that the object wasn't replaced under */
    if (if_nomatch == NULL || strcmp(if_nomatch, "*") != 0) {
      op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag); 
      // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
    }

    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        // test the object is existing
        if (!state->exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        bufferlist bl;
        if (!state->get_attr(RGW_ATTR_ETAG, bl) ||
            strncmp(if_match, bl.c_str(), bl.length()) != 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }

    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        // test the object is NOT existing
        if (state->exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else {
        bufferlist bl;
        if (!state->get_attr(RGW_ATTR_ETAG, bl) ||
            strncmp(if_nomatch, bl.c_str(), bl.length()) == 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  if (reset_obj) {
    if (state->exists) {
      op.create(false);
      store->remove_rgw_head_obj(op);
    } else {
      op.create(true);
    }
  }

  if (removal_op) {
    /* the object is being removed, no need to update its tag */
    return 0;
  }

  if (ptag) {
    state->write_tag = *ptag;
  } else {
    append_rand_alpha(store->ctx(), state->write_tag, state->write_tag, 32);
  }
  bufferlist bl;
  bl.append(state->write_tag.c_str(), state->write_tag.size() + 1);

  ldout(store->ctx(), 10) << "setting object write_tag=" << state->write_tag << dendl;

  op.setxattr(RGW_ATTR_ID_TAG, bl);

  return 0;
}

int RGWRados::system_obj_set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl,
				  RGWObjVersionTracker *objv_tracker)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return system_obj_set_attrs(ctx, obj, attrs, NULL, objv_tracker);
}

int RGWRados::system_obj_set_attrs(void *ctx, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs,
                        RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_system_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  ObjectWriteOperation op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);
  }

  if (!op.size())
    return 0;

  bufferlist bl;

  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return set_attrs(ctx, obj, attrs, NULL);
}

int RGWRados::set_attrs(void *ctx, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, obj, op, &state);
  if (r < 0)
    return r;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_DELETE_AT) == 0) {
      utime_t ts;
      try {
        ::decode(ts, bl);

        rgw_obj_key obj_key;
        obj.get_index_key(&obj_key);

        objexp_hint_add(ts, bucket.tenant, bucket.name, bucket.bucket_id, obj_key);
      } catch (buffer::error& err) {
	ldout(cct, 0) << "ERROR: failed to decode " RGW_ATTR_DELETE_AT << " attr" << dendl;
      }
    }
  }

  if (!op.size())
    return 0;

  RGWObjectCtx obj_ctx(this);

  RGWBucketInfo bucket_info;
  int ret = get_bucket_instance_info(obj_ctx, bucket, bucket_info, NULL, NULL);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "() get_bucket_instance_info(bucket=" << bucket << ") returned ret=" << ret << dendl;
    return ret;
  }

  bufferlist bl;
  RGWRados::Bucket bop(this, bucket_info);
  RGWRados::Bucket::UpdateIndex index_op(&bop, obj, state);

  if (state) {
    string tag;
    append_rand_alpha(cct, tag, tag, 32);
    state->write_tag = tag;
    r = index_op.prepare(CLS_RGW_OP_ADD);

    if (r < 0)
      return r;

    bl.append(tag.c_str(), tag.size() + 1);

    op.setxattr(RGW_ATTR_ID_TAG,  bl);
  }

  r = ref.ioctx.operate(ref.oid, &op);
  if (state) {
    if (r >= 0) {
      bufferlist acl_bl = attrs[RGW_ATTR_ACL];
      bufferlist etag_bl = attrs[RGW_ATTR_ETAG];
      bufferlist content_type_bl = attrs[RGW_ATTR_CONTENT_TYPE];
      string etag(etag_bl.c_str(), etag_bl.length());
      string content_type(content_type_bl.c_str(), content_type_bl.length());
      uint64_t epoch = ref.ioctx.get_last_version();
      int64_t poolid = ref.ioctx.get_id();
      utime_t mtime = ceph_clock_now(cct);
      r = index_op.complete(poolid, epoch, state->size,
                            mtime, etag, content_type, &acl_bl,
                            RGW_OBJ_CATEGORY_MAIN, NULL);
    } else {
      int ret = index_op.cancel();
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: comlete_update_index_cancel() returned r=" << r << dendl;
      }
    }
  }
  if (r < 0)
    return r;

  if (state) {
    state->obj_tag.swap(bl);
    if (rmattrs) {
      for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
        state->attrset.erase(iter->first);
      }
    }
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      state->attrset[iter->first] = iter->second;
    }
  }

  return 0;
}

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * obj: name/key of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * get_data: if true, the object's data/value will be read out, otherwise not
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *          (if get_data==true) length of read data,
 *          (if get_data==false) length of the object
 */
// P3 XXX get_data is not seen used anywhere.
int RGWRados::Object::Read::prepare(int64_t *pofs, int64_t *pend)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();

  bufferlist etag;

  off_t ofs = 0;
  off_t end = -1;

  map<string, bufferlist>::iterator iter;

  RGWObjState *astate;
  int r = source->get_state(&astate, true);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  state.obj = astate->obj;

  r = store->get_obj_ioctx(state.obj, &state.io_ctx);
  if (r < 0) {
    return r;
  }

  if (params.attrs) {
    *params.attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      for (iter = params.attrs->begin(); iter != params.attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  /* Convert all times go GMT to make them compatible */
  if (conds.mod_ptr || conds.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);

    obj_time_weight dest_weight;

    if (conds.mod_ptr) {
      dest_weight.init(*conds.mod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldout(cct, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (conds.unmod_ptr) {
      dest_weight.init(*conds.unmod_ptr, conds.mod_zone_id, conds.mod_pg_ver);
      ldout(cct, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  if (conds.if_match || conds.if_nomatch) {
    r = get_attr(RGW_ATTR_ETAG, etag);
    if (r < 0)
      return r;

    if (conds.if_match) {
      string if_match_str = rgw_string_unquote(conds.if_match);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(etag.c_str()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }

    if (conds.if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(conds.if_nomatch);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(etag.c_str()) == 0) {
        return -ERR_NOT_MODIFIED;
      }
    }
  }

  if (pofs)
    ofs = *pofs;
  if (pend)
    end = *pend;

  if (ofs < 0) {
    ofs += astate->size;
    if (ofs < 0)
      ofs = 0;
    end = astate->size - 1;
  } else if (end < 0) {
    end = astate->size - 1;
  }

  if (astate->size > 0) {
    if (ofs >= (off_t)astate->size) {
      return -ERANGE;
    }
    if (end >= (off_t)astate->size) {
      end = astate->size - 1;
    }
  }

  if (pofs)
    *pofs = ofs;
  if (pend)
    *pend = end;
  if (params.read_size)
    *params.read_size = (ofs <= end ? end + 1 - ofs : 0);
  if (params.obj_size)
    *params.obj_size = astate->size;
  if (params.lastmod)
    *params.lastmod = astate->mtime;

  return 0;
}

int RGWRados::SystemObject::get_state(RGWObjState **pstate, RGWObjVersionTracker *objv_tracker)
{
  return store->get_system_obj_state(&ctx, obj, pstate, objv_tracker);
}

int RGWRados::stat_system_obj(RGWObjectCtx& obj_ctx,
                              RGWRados::SystemObject::Read::GetObjState& state,
                              rgw_obj& obj,
                              map<string, bufferlist> *attrs,
                              time_t *lastmod,
                              uint64_t *obj_size,
                              RGWObjVersionTracker *objv_tracker)
{
  RGWObjState *astate = NULL;

  int r = get_system_obj_state(&obj_ctx, obj, &astate, objv_tracker);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      map<string, bufferlist>::iterator iter;
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return 0;
}

int RGWRados::SystemObject::Read::stat(RGWObjVersionTracker *objv_tracker)
{
  RGWRados *store = source->get_store();
  rgw_obj& obj = source->get_obj();

  return store->stat_system_obj(source->get_ctx(), state, obj, stat_params.attrs,
                                stat_params.lastmod, stat_params.obj_size, objv_tracker);
}

int RGWRados::Bucket::UpdateIndex::prepare(RGWModifyOp op)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;
  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }

  ret = store->data_log->add_entry(bs->bucket, bs->shard_id);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
    return ret;
  }

  if (obj_state && obj_state->write_tag.length()) {
    optag = string(obj_state->write_tag.c_str(), obj_state->write_tag.length());
  } else {
    if (optag.empty()) {
      append_rand_alpha(store->ctx(), optag, optag, 32);
    }
  }
  ret = store->cls_obj_prepare_op(*bs, op, optag, obj, bilog_flags);

  return ret;
}

int RGWRados::Bucket::UpdateIndex::complete(int64_t poolid, uint64_t epoch, uint64_t size,
                                    utime_t& ut, string& etag, string& content_type, bufferlist *acl_bl, RGWObjCategory category,
                                    list<rgw_obj_key> *remove_objs)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;
  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }

  RGWObjEnt ent;
  obj.get_index_key(&ent.key);
  ent.size = size;
  ent.mtime = ut;
  ent.etag = etag;
  ACLOwner owner;
  if (acl_bl && acl_bl->length()) {
    int ret = store->decode_policy(*acl_bl, &owner);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.owner = owner.get_id();
  ent.owner_display_name = owner.get_display_name();
  ent.content_type = content_type;

  ret = store->cls_obj_complete_add(*bs, optag, poolid, epoch, ent, category, remove_objs, bilog_flags);

  return ret;
}

int RGWRados::Bucket::UpdateIndex::complete_del(int64_t poolid, uint64_t epoch,
                                                list<rgw_obj_key> *remove_objs)
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;
  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }
  return store->cls_obj_complete_del(*bs, optag, poolid, epoch, obj, remove_objs, bilog_flags);
}


int RGWRados::Bucket::UpdateIndex::cancel()
{
  if (blind) {
    return 0;
  }
  RGWRados *store = target->get_store();
  BucketShard *bs;
  int ret = get_bucket_shard(&bs);
  if (ret < 0) {
    ldout(store->ctx(), 5) << "failed to get BucketShard object: ret=" << ret << dendl;
    return ret;
  }
  return store->cls_obj_complete_cancel(*bs, optag, obj, bilog_flags);
}

int RGWRados::Object::Read::read(int64_t ofs, int64_t end, bufferlist& bl)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();

  rgw_bucket bucket;
  std::string oid, key;
  rgw_obj read_obj = state.obj;
  uint64_t read_ofs = ofs;
  uint64_t len, read_len;
  bool reading_from_head = true;
  ObjectReadOperation op;

  bool merge_bl = false;
  bufferlist *pbl = &bl;
  bufferlist read_bl;
  uint64_t max_chunk_size;


  get_obj_bucket_and_oid_loc(state.obj, bucket, oid, key);

  RGWObjState *astate;
  int r = source->get_state(&astate, true);
  if (r < 0)
    return r;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest && astate->manifest.has_tail()) {
    /* now get the relevant object part */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    uint64_t stripe_ofs = iter.get_stripe_ofs();
    read_obj = iter.get_location();
    len = min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
    read_ofs = iter.location_ofs() + (ofs - stripe_ofs);
    reading_from_head = (read_obj == state.obj);

    if (!reading_from_head) {
      get_obj_bucket_and_oid_loc(read_obj, bucket, oid, key);
    }
  }

  r = store->get_max_chunk_size(bucket, &max_chunk_size);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed to get max_chunk_size() for bucket " << bucket << dendl;
    return r;
  }

  if (len > max_chunk_size)
    len = max_chunk_size;


  state.io_ctx.locator_set_key(key);

  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the atomic test */
    r = store->append_atomic_test(&source->get_ctx(), read_obj, op, &astate);
    if (r < 0)
      return r;

    if (astate && astate->prefetch_data) {
      if (!ofs && astate->data.length() >= len) {
        bl = astate->data;
        return bl.length();
      }

      if (ofs < astate->data.length()) {
        unsigned copy_len = min((uint64_t)astate->data.length() - ofs, len);
        astate->data.copy(ofs, copy_len, bl);
        read_len -= copy_len;
        read_ofs += copy_len;
        if (!read_len)
	  return bl.length();

        merge_bl = true;
        pbl = &read_bl;
      }
    }
  }

  ldout(cct, 20) << "rados->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
  op.read(read_ofs, read_len, pbl, NULL);

  r = state.io_ctx.operate(oid, &op, NULL);
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (r < 0) {
    return r;
  }

  if (merge_bl) {
    bl.append(read_bl);
  }

  return bl.length();
}

int RGWRados::SystemObject::Read::GetObjState::get_ioctx(RGWRados *store, rgw_obj& obj, librados::IoCtx **ioctx)
{
  if (!has_ioctx) {
    int r = store->get_obj_ioctx(obj, &io_ctx);
    if (r < 0) {
      return r;
    }
    has_ioctx = true;
  }
  *ioctx = &io_ctx;
  return 0;

}

int RGWRados::get_system_obj(RGWObjectCtx& obj_ctx, RGWRados::SystemObject::Read::GetObjState& read_state,
                             RGWObjVersionTracker *objv_tracker, rgw_obj& obj,
                             bufferlist& bl, off_t ofs, off_t end,
                             map<string, bufferlist> *attrs,
                             rgw_cache_entry_info *cache_info)
{
  rgw_bucket bucket;
  std::string oid, key;
  uint64_t len;
  ObjectReadOperation op;

  RGWObjState *astate = NULL;

  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

  int r = get_system_obj_state(&obj_ctx, obj, &astate, NULL);
  if (r < 0)
    return r;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }

  ldout(cct, 20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len, &bl, NULL);

  if (attrs) {
    op.getxattrs(attrs, NULL);
  }

  librados::IoCtx *io_ctx;
  r = read_state.get_ioctx(this, obj, &io_ctx);
  if (r < 0) {
    ldout(cct, 20) << "get_ioctx() on obj=" << obj << " returned " << r << dendl;
    return r;
  }
  r = io_ctx->operate(oid, &op, NULL);
  if (r < 0) {
    ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;
    return r;
  }
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  uint64_t op_ver = io_ctx->get_last_version();

  if (read_state.last_ver > 0 &&
      read_state.last_ver != op_ver) {
    ldout(cct, 5) << "raced with an object write, abort" << dendl;
    return -ECANCELED;
  }

  read_state.last_ver = op_ver;

  return bl.length();
}

int RGWRados::SystemObject::Read::read(int64_t ofs, int64_t end, bufferlist& bl, RGWObjVersionTracker *objv_tracker)
{
  RGWRados *store = source->get_store();
  rgw_obj& obj = source->get_obj();

  return store->get_system_obj(source->get_ctx(), state, objv_tracker, obj, bl, ofs, end, read_params.attrs, read_params.cache_info);
}

int RGWRados::SystemObject::Read::get_attr(const char *name, bufferlist& dest)
{
  RGWRados *store = source->get_store();
  rgw_obj& obj = source->get_obj();

  return store->system_obj_get_attr(obj, name, dest);
}

struct get_obj_data;

struct get_obj_aio_data {
  struct get_obj_data *op_data;
  off_t ofs;
  off_t len;
};

struct get_obj_io {
  off_t len;
  bufferlist bl;
};

static void _get_obj_aio_completion_cb(completion_t cb, void *arg);

struct get_obj_data : public RefCountedObject {
  CephContext *cct;
  RGWRados *rados;
  RGWObjectCtx *ctx;
  IoCtx io_ctx;
  map<off_t, get_obj_io> io_map;
  map<off_t, librados::AioCompletion *> completion_map;
  uint64_t total_read;
  Mutex lock;
  Mutex data_lock;
  list<get_obj_aio_data> aio_data;
  RGWGetDataCB *client_cb;
  atomic_t cancelled;
  atomic_t err_code;
  Throttle throttle;
  list<bufferlist> read_list;

  explicit get_obj_data(CephContext *_cct)
    : cct(_cct),
      rados(NULL), ctx(NULL),
      total_read(0), lock("get_obj_data"), data_lock("get_obj_data::data_lock"),
      client_cb(NULL),
      throttle(cct, "get_obj_data", cct->_conf->rgw_get_obj_window_size, false) {}
  virtual ~get_obj_data() { } 
  void set_cancelled(int r) {
    cancelled.set(1);
    err_code.set(r);
  }

  bool is_cancelled() {
    return cancelled.read() == 1;
  }

  int get_err_code() {
    return err_code.read();
  }

  int wait_next_io(bool *done) {
    lock.Lock();
    map<off_t, librados::AioCompletion *>::iterator iter = completion_map.begin();
    if (iter == completion_map.end()) {
      *done = true;
      lock.Unlock();
      return 0;
    }
    off_t cur_ofs = iter->first;
    librados::AioCompletion *c = iter->second;
    lock.Unlock();

    c->wait_for_complete_and_cb();
    int r = c->get_return_value();

    lock.Lock();
    completion_map.erase(cur_ofs);

    if (completion_map.empty()) {
      *done = true;
    }
    lock.Unlock();

    c->release();
    
    return r;
  }

  void add_io(off_t ofs, off_t len, bufferlist **pbl, AioCompletion **pc) {
    Mutex::Locker l(lock);

    get_obj_io& io = io_map[ofs];
    *pbl = &io.bl;

    struct get_obj_aio_data aio;
    aio.ofs = ofs;
    aio.len = len;
    aio.op_data = this;

    aio_data.push_back(aio);

    struct get_obj_aio_data *paio_data =  &aio_data.back(); /* last element */

    librados::AioCompletion *c = librados::Rados::aio_create_completion((void *)paio_data, _get_obj_aio_completion_cb, NULL);
    completion_map[ofs] = c;

    *pc = c;

    /* we have a reference per IO, plus one reference for the calling function.
     * reference is dropped for each callback, plus when we're done iterating
     * over the parts */
    get();
  }

  void cancel_io(off_t ofs) {
    ldout(cct, 20) << "get_obj_data::cancel_io() ofs=" << ofs << dendl;
    lock.Lock();
    map<off_t, AioCompletion *>::iterator iter = completion_map.find(ofs);
    if (iter != completion_map.end()) {
      AioCompletion *c = iter->second;
      c->release();
      completion_map.erase(ofs);
      io_map.erase(ofs);
    }
    lock.Unlock();

    /* we don't drop a reference here -- e.g., not calling d->put(), because we still
     * need IoCtx to live, as io callback may still be called
     */
  }

  void cancel_all_io() {
    ldout(cct, 20) << "get_obj_data::cancel_all_io()" << dendl;
    Mutex::Locker l(lock);
    for (map<off_t, librados::AioCompletion *>::iterator iter = completion_map.begin();
         iter != completion_map.end(); ++iter) {
      librados::AioCompletion  *c = iter->second;
      c->release();
    }
  }

  int get_complete_ios(off_t ofs, list<bufferlist>& bl_list) {
    Mutex::Locker l(lock);

    map<off_t, get_obj_io>::iterator liter = io_map.begin();

    if (liter == io_map.end() ||
        liter->first != ofs) {
      return 0;
    }

    map<off_t, librados::AioCompletion *>::iterator aiter;
    aiter = completion_map.find(ofs);
    if (aiter == completion_map.end()) {
    /* completion map does not hold this io, it was cancelled */
      return 0;
    }

    AioCompletion *completion = aiter->second;
    int r = completion->get_return_value();
    if (r < 0)
      return r;

    for (; aiter != completion_map.end(); ++aiter) {
      completion = aiter->second;
      if (!completion->is_complete()) {
        /* reached a request that is not yet complete, stop */
        break;
      }

      r = completion->get_return_value();
      if (r < 0) {
        set_cancelled(r); /* mark it as cancelled, so that we don't continue processing next operations */
        return r;
      }

      total_read += r;

      map<off_t, get_obj_io>::iterator old_liter = liter++;
      bl_list.push_back(old_liter->second.bl);
      io_map.erase(old_liter);
    }

    return 0;
  }
};

static int _get_obj_iterate_cb(rgw_obj& obj, off_t obj_ofs, off_t read_ofs, off_t len, bool is_head_obj, RGWObjState *astate, void *arg)
{
  struct get_obj_data *d = (struct get_obj_data *)arg;

  return d->rados->get_obj_iterate_cb(d->ctx, astate, obj, obj_ofs, read_ofs, len, is_head_obj, arg);
}

static void _get_obj_aio_completion_cb(completion_t cb, void *arg)
{
  struct get_obj_aio_data *aio_data = (struct get_obj_aio_data *)arg;
  struct get_obj_data *d = aio_data->op_data;

  d->rados->get_obj_aio_completion_cb(cb, arg);
}


void RGWRados::get_obj_aio_completion_cb(completion_t c, void *arg)
{
  struct get_obj_aio_data *aio_data = (struct get_obj_aio_data *)arg;
  struct get_obj_data *d = aio_data->op_data;
  off_t ofs = aio_data->ofs;
  off_t len = aio_data->len;

  list<bufferlist> bl_list;
  list<bufferlist>::iterator iter;
  int r;

  ldout(cct, 20) << "get_obj_aio_completion_cb: io completion ofs=" << ofs << " len=" << len << dendl;
  d->throttle.put(len);

  r = rados_aio_get_return_value(c);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: got unexpected error when trying to read object: " << r << dendl;
    d->set_cancelled(r);
    goto done;
  }

  if (d->is_cancelled()) {
    goto done;
  }

  d->data_lock.Lock();

  r = d->get_complete_ios(ofs, bl_list);
  if (r < 0) {
    goto done_unlock;
  }

  d->read_list.splice(d->read_list.end(), bl_list);

done_unlock:
  d->data_lock.Unlock();
done:
  d->put();
  return;
}

int RGWRados::flush_read_list(struct get_obj_data *d)
{
  d->data_lock.Lock();
  list<bufferlist> l;
  l.swap(d->read_list);
  d->get();
  d->read_list.clear();

  d->data_lock.Unlock();

  int r = 0;

  list<bufferlist>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    bufferlist& bl = *iter;
    r = d->client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      dout(0) << "ERROR: flush_read_list(): d->client_c->handle_data() returned " << r << dendl;
      break;
    }
  }

  d->data_lock.Lock();
  d->put();
  if (r < 0) {
    d->set_cancelled(r);
  }
  d->data_lock.Unlock();
  return r;
}

int RGWRados::get_obj_iterate_cb(RGWObjectCtx *ctx, RGWObjState *astate,
		         rgw_obj& obj,
			 off_t obj_ofs,
                         off_t read_ofs, off_t len,
                         bool is_head_obj, void *arg)
{
  RGWObjectCtx *rctx = static_cast<RGWObjectCtx *>(ctx);
  ObjectReadOperation op;
  struct get_obj_data *d = (struct get_obj_data *)arg;
  string oid, key;
  rgw_bucket bucket;
  bufferlist *pbl;
  AioCompletion *c;

  int r;

  if (is_head_obj) {
    /* only when reading from the head object do we need to do the atomic test */
    r = append_atomic_test(rctx, obj, op, &astate);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      d->data_lock.Lock();
      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      d->data_lock.Unlock();
      if (r < 0)
        return r;

      d->lock.Lock();
      d->total_read += chunk_len;
      d->lock.Unlock();
	
      len -= chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
	  return 0;
    }
  }

  get_obj_bucket_and_oid_loc(obj, bucket, oid, key);

  d->throttle.get(len);
  if (d->is_cancelled()) {
    return d->get_err_code();
  }

  /* add io after we check that we're not cancelled, otherwise we're going to have trouble
   * cleaning up
   */
  d->add_io(obj_ofs, len, &pbl, &c);

  ldout(cct, 20) << "rados->get_obj_iterate_cb oid=" << oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, pbl, NULL);

  librados::IoCtx io_ctx(d->io_ctx);
  io_ctx.locator_set_key(key);

  r = io_ctx.aio_operate(oid, c, &op, NULL);
  ldout(cct, 20) << "rados->aio_operate r=" << r << " bl.length=" << pbl->length() << dendl;
  if (r < 0)
    goto done_err;

  // Flush data to client if there is any
  r = flush_read_list(d);
  if (r < 0)
    return r;

  return 0;

done_err:
  ldout(cct, 20) << "cancelling io r=" << r << " obj_ofs=" << obj_ofs << dendl;
  d->set_cancelled(r);
  d->cancel_io(obj_ofs);

  return r;
}

int RGWRados::Object::Read::iterate(int64_t ofs, int64_t end, RGWGetDataCB *cb)
{
  RGWRados *store = source->get_store();
  CephContext *cct = store->ctx();

  struct get_obj_data *data = new get_obj_data(cct);
  bool done = false;

  RGWObjectCtx& obj_ctx = source->get_ctx();

  data->rados = store;
  data->io_ctx.dup(state.io_ctx);
  data->client_cb = cb;

  int r = store->iterate_obj(obj_ctx, state.obj, ofs, end, cct->_conf->rgw_get_obj_max_req_size, _get_obj_iterate_cb, (void *)data);
  if (r < 0) {
    data->cancel_all_io();
    goto done;
  }

  while (!done) {
    r = data->wait_next_io(&done);
    if (r < 0) {
      dout(10) << "get_obj_iterate() r=" << r << ", canceling all io" << dendl;
      data->cancel_all_io();
      break;
    }
    r = store->flush_read_list(data);
    if (r < 0) {
      dout(10) << "get_obj_iterate() r=" << r << ", canceling all io" << dendl;
      data->cancel_all_io();
      break;
    }
  }

done:
  data->put();
  return r;
}

int RGWRados::iterate_obj(RGWObjectCtx& obj_ctx, rgw_obj& obj,
                          off_t ofs, off_t end,
			  uint64_t max_chunk_size,
			  int (*iterate_obj_cb)(rgw_obj&, off_t, off_t, off_t, bool, RGWObjState *, void *),
	                  void *arg)
{
  rgw_bucket bucket;
  rgw_obj read_obj = obj;
  uint64_t read_ofs = ofs;
  uint64_t len;
  bool reading_from_head = true;
  RGWObjState *astate = NULL;

  int r = get_obj_state(&obj_ctx, obj, &astate, NULL);
  if (r < 0) {
    return r;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest) {
    /* now get the relevant object stripe */
    RGWObjManifest::obj_iterator iter = astate->manifest.obj_find(ofs);

    RGWObjManifest::obj_iterator obj_end = astate->manifest.obj_end();

    for (; iter != obj_end && ofs <= end; ++iter) {
      off_t stripe_ofs = iter.get_stripe_ofs();
      off_t next_stripe_ofs = stripe_ofs + iter.get_stripe_size();

      while (ofs < next_stripe_ofs && ofs <= end) {
        read_obj = iter.get_location();
        uint64_t read_len = min(len, iter.get_stripe_size() - (ofs - stripe_ofs));
        read_ofs = iter.location_ofs() + (ofs - stripe_ofs);

        if (read_len > max_chunk_size) {
          read_len = max_chunk_size;
        }

        reading_from_head = (read_obj == obj);
        r = iterate_obj_cb(read_obj, ofs, read_ofs, read_len, reading_from_head, astate, arg);
	if (r < 0) {
	  return r;
        }

	len -= read_len;
        ofs += read_len;
      }
    }
  } else {
    while (ofs <= end) {
      uint64_t read_len = min(len, max_chunk_size);

      r = iterate_obj_cb(obj, ofs, ofs, read_len, reading_from_head, astate, arg);
      if (r < 0) {
	return r;
      }

      len -= read_len;
      ofs += read_len;
    }
  }

  return 0;
}

int RGWRados::obj_operate(rgw_obj& obj, ObjectWriteOperation *op)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  return ref.ioctx.operate(ref.oid, op);
}

int RGWRados::obj_operate(rgw_obj& obj, ObjectReadOperation *op)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  bufferlist outbl;

  return ref.ioctx.operate(ref.oid, op, &outbl);
}

int RGWRados::olh_init_modification_impl(RGWObjState& state, rgw_obj& olh_obj, string *op_tag)
{
  ObjectWriteOperation op;

  assert(olh_obj.get_instance().empty());

  bool has_tag = (state.exists && has_olh_tag(state.attrset));

  if (!state.exists) {
    op.create(true);
  } else {
    op.assert_exists();
  }

  /*
   * 3 possible cases: olh object doesn't exist, it exists as an olh, it exists as a regular object.
   * If it exists as a regular object we'll need to transform it into an olh. We'll do it in two
   * steps, first change its tag and set the olh pending attrs. Once write is done we'll need to
   * truncate it, remove extra attrs, and send it to the garbage collection. The bucket index olh
   * log will reflect that.
   *
   * Need to generate separate olh and obj tags, as olh can be colocated with object data. obj_tag
   * is used for object data instance, olh_tag for olh instance.
   */
  if (has_tag) {
    /* guard against racing writes */
    bucket_index_guard_olh_op(state, op);
  }

  if (!has_tag) {
    /* obj tag */
    string obj_tag;
    int ret = gen_rand_alphanumeric_lower(cct, &obj_tag, 32);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: gen_rand_alphanumeric_lower() returned ret=" << ret << dendl;
      return ret;
    }
    bufferlist bl;
    bl.append(obj_tag.c_str(), obj_tag.size());
    op.setxattr(RGW_ATTR_ID_TAG, bl);

    state.attrset[RGW_ATTR_ID_TAG] = bl;
    state.obj_tag = bl;

    /* olh tag */
    string olh_tag;
    ret = gen_rand_alphanumeric_lower(cct, &olh_tag, 32);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: gen_rand_alphanumeric_lower() returned ret=" << ret << dendl;
      return ret;
    }
    bufferlist olh_bl;
    olh_bl.append(olh_tag.c_str(), olh_tag.size());
    op.setxattr(RGW_ATTR_OLH_ID_TAG, olh_bl);

    state.attrset[RGW_ATTR_OLH_ID_TAG] = bl;
    state.olh_tag = olh_bl;
    state.is_olh = true;

    bufferlist verbl;
    op.setxattr(RGW_ATTR_OLH_VER, verbl);
  }

  bufferlist bl;
  RGWOLHPendingInfo pending_info;
  pending_info.time = ceph_clock_now(cct);
  ::encode(pending_info, bl);

#define OLH_PENDING_TAG_LEN 32
  /* tag will start with current time epoch, this so that entries are sorted by time */
  char buf[32];
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)pending_info.time.sec());
  *op_tag = buf;

  string s;
  int ret = gen_rand_alphanumeric_lower(cct, &s, OLH_PENDING_TAG_LEN - op_tag->size());
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: gen_rand_alphanumeric_lower() returned ret=" << ret << dendl;
    return ret;
  }
  op_tag->append(s);

  string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
  attr_name.append(*op_tag);

  op.setxattr(attr_name.c_str(), bl);

  ret = obj_operate(olh_obj, &op);
  if (ret < 0) {
    return ret;
  }

  state.exists = true;
  state.attrset[attr_name] = bl;

  return 0;
}

int RGWRados::olh_init_modification(RGWObjState& state, rgw_obj& obj, string *op_tag)
{
  int ret;

  ret = olh_init_modification_impl(state, obj, op_tag);
  if (ret == -EEXIST) {
    ret = -ECANCELED;
  }

  return ret;
}

int RGWRados::bucket_index_link_olh(RGWObjState& olh_state, rgw_obj& obj_instance, bool delete_marker,
                                    const string& op_tag,
                                    struct rgw_bucket_dir_entry_meta *meta,
                                    uint64_t olh_epoch,
                                    time_t unmod_since)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj_instance, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret = bs.init(bucket, obj_instance);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  cls_rgw_obj_key key(obj_instance.get_index_key_name(), obj_instance.get_instance());
  ret = cls_rgw_bucket_link_olh(bs.index_ctx, bs.bucket_obj, key, olh_state.olh_tag, delete_marker, op_tag, meta, olh_epoch,
                                unmod_since,
                                get_zone().log_data);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

void RGWRados::bucket_index_guard_olh_op(RGWObjState& olh_state, ObjectOperation& op)
{
  ldout(cct, 20) << __func__ << "(): olh_state.olh_tag=" << string(olh_state.olh_tag.c_str(), olh_state.olh_tag.length()) << dendl;
  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_state.olh_tag);
}

int RGWRados::bucket_index_unlink_instance(rgw_obj& obj_instance, const string& op_tag, uint64_t olh_epoch)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj_instance, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret = bs.init(bucket, obj_instance);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  cls_rgw_obj_key key(obj_instance.get_index_key_name(), obj_instance.get_instance());
  ret = cls_rgw_bucket_unlink_instance(bs.index_ctx, bs.bucket_obj, key, op_tag, olh_epoch, get_zone().log_data);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRados::bucket_index_read_olh_log(RGWObjState& state, rgw_obj& obj_instance, uint64_t ver_marker,
                                        map<uint64_t, vector<rgw_bucket_olh_log_entry> > *log,
                                        bool *is_truncated)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj_instance, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret = bs.init(bucket, obj_instance);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.get_index_key_name(), string());

  ObjectReadOperation op;

  ret = cls_rgw_get_olh_log(bs.index_ctx, bs.bucket_obj, op, key, ver_marker, olh_tag, log, is_truncated);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bucket_index_trim_olh_log(RGWObjState& state, rgw_obj& obj_instance, uint64_t ver)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj_instance, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret = bs.init(bucket, obj_instance);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.get_index_key_name(), string());

  ObjectWriteOperation op;

  cls_rgw_trim_olh_log(op, key, ver, olh_tag);

  ret = bs.index_ctx.operate(bs.bucket_obj, &op);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bucket_index_clear_olh(RGWObjState& state, rgw_obj& obj_instance)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj_instance, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  BucketShard bs(this);
  int ret = bs.init(bucket, obj_instance);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  string olh_tag(state.olh_tag.c_str(), state.olh_tag.length());

  cls_rgw_obj_key key(obj_instance.get_index_key_name(), string());

  ret = cls_rgw_clear_olh(bs.index_ctx, bs.bucket_obj, key, olh_tag);
  if (ret < 0) {
    ldout(cct, 5) << "cls_rgw_clear_olh() returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::apply_olh_log(RGWObjectCtx& obj_ctx, RGWObjState& state, RGWBucketInfo& bucket_info, rgw_obj& obj,
                            bufferlist& olh_tag, map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log,
                            uint64_t *plast_ver)
{
  if (log.empty()) {
    return 0;
  }

  librados::ObjectWriteOperation op;

  uint64_t last_ver = log.rbegin()->first;
  *plast_ver = last_ver;

  map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator iter = log.begin();

  op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_tag);
  op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_GT, last_ver);

  bool need_to_link = false;
  cls_rgw_obj_key key;
  bool delete_marker = false;
  list<cls_rgw_obj_key> remove_instances;
  bool need_to_remove = false;

  for (iter = log.begin(); iter != log.end(); ++iter) {
    vector<rgw_bucket_olh_log_entry>::iterator viter = iter->second.begin();
    for (; viter != iter->second.end(); ++viter) {
      rgw_bucket_olh_log_entry& entry = *viter;

      ldout(cct, 20) << "olh_log_entry: op=" << (int)entry.op
                     << " key=" << entry.key.name << "[" << entry.key.instance << "] "
                     << (entry.delete_marker ? "(delete)" : "") << dendl;
      switch (entry.op) {
      case CLS_RGW_OLH_OP_REMOVE_INSTANCE:
        remove_instances.push_back(entry.key);
        break;
      case CLS_RGW_OLH_OP_LINK_OLH:
        need_to_link = true;
        need_to_remove = false;
        key = entry.key;
        delete_marker = entry.delete_marker;
        break;
      case CLS_RGW_OLH_OP_UNLINK_OLH:
        need_to_remove = true;
        need_to_link = false;
        break;
      default:
        ldout(cct, 0) << "ERROR: apply_olh_log: invalid op: " << (int)entry.op << dendl;
        return -EIO;
      }
      string attr_name = RGW_ATTR_OLH_PENDING_PREFIX;
      attr_name.append(entry.op_tag);
      op.rmxattr(attr_name.c_str());
    }
  }

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  if (need_to_link) {
    rgw_obj target(bucket, key);
    RGWOLHInfo info;
    info.target = target;
    info.removed = delete_marker;
    bufferlist bl;
    ::encode(info, bl);
    op.setxattr(RGW_ATTR_OLH_INFO, bl);
  }

  /* first remove object instances */
  for (list<cls_rgw_obj_key>::iterator liter = remove_instances.begin();
       liter != remove_instances.end(); ++liter) {
    cls_rgw_obj_key& key = *liter;
    rgw_obj obj_instance(bucket, key);
    int ret = delete_obj(obj_ctx, bucket_info, obj_instance, 0, RGW_BILOG_FLAG_VERSIONED_OP);
    if (ret < 0 && ret != -ENOENT) {
      ldout(cct, 0) << "ERROR: delete_obj() returned " << ret << " obj_instance=" << obj_instance << dendl;
      return ret;
    }
  }

  /* update olh object */
  r = ref.ioctx.operate(ref.oid, &op);
  if (r == -ECANCELED) {
    r = 0;
  }
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not apply olh update, r=" << r << dendl;
    return r;
  }

  r = bucket_index_trim_olh_log(state, obj, last_ver);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not trim olh log, r=" << r << dendl;
    return r;
  }

  if (need_to_remove) {
    ObjectWriteOperation rm_op;

    rm_op.cmpxattr(RGW_ATTR_OLH_ID_TAG, CEPH_OSD_CMPXATTR_OP_EQ, olh_tag);
    rm_op.cmpxattr(RGW_ATTR_OLH_VER, CEPH_OSD_CMPXATTR_OP_GT, last_ver);
    cls_obj_check_prefix_exist(rm_op, RGW_ATTR_OLH_PENDING_PREFIX, true); /* fail if found one of these, pending modification */
    rm_op.remove();

    r = ref.ioctx.operate(ref.oid, &rm_op);
    if (r == -ECANCELED) {
      return 0; /* someone else won this race */
    } else {
      /* 
       * only clear if was successful, otherwise we might clobber pending operations on this object
       */
      r = bucket_index_clear_olh(state, obj);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: could not clear bucket index olh entries r=" << r << dendl;
        return r;
      }
    }
  }

  return 0;
}

/*
 * read olh log and apply it
 */
int RGWRados::update_olh(RGWObjectCtx& obj_ctx, RGWObjState *state, RGWBucketInfo& bucket_info, rgw_obj& obj)
{
  map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
  bool is_truncated;
  uint64_t ver_marker = 0;

  do {
    int ret = bucket_index_read_olh_log(*state, obj, ver_marker, &log, &is_truncated);
    if (ret < 0) {
      return ret;
    }
    ret = apply_olh_log(obj_ctx, *state, bucket_info, obj, state->olh_tag, log, &ver_marker);
    if (ret < 0) {
      return ret;
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::set_olh(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, rgw_obj& target_obj, bool delete_marker, rgw_bucket_dir_entry_meta *meta,
                      uint64_t olh_epoch, time_t unmod_since)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.clear_instance();

  RGWObjState *state = NULL;

  int ret = 0;
  int i;

#define MAX_ECANCELED_RETRY 100
  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(&obj_ctx, olh_obj, &state, false); /* don't follow olh */
    if (ret < 0) {
      return ret;
    }

    ret = olh_init_modification(*state, olh_obj, &op_tag);
    if (ret < 0) {
      ldout(cct, 20) << "olh_init_modification() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    ret = bucket_index_link_olh(*state, target_obj, delete_marker, op_tag, meta, olh_epoch, unmod_since);
    if (ret < 0) {
      ldout(cct, 20) << "bucket_index_link_olh() target_obj=" << target_obj << " delete_marker=" << (int)delete_marker << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldout(cct, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  ret = update_olh(obj_ctx, state, bucket_info, olh_obj);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    ret = 0;
  }
  if (ret < 0) {
    ldout(cct, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWRados::unlink_obj_instance(RGWObjectCtx& obj_ctx, RGWBucketInfo& bucket_info, rgw_obj& target_obj,
                                  uint64_t olh_epoch)
{
  string op_tag;

  rgw_obj olh_obj = target_obj;
  olh_obj.clear_instance();

  RGWObjState *state = NULL;

  int ret = 0;
  int i;

  for (i = 0; i < MAX_ECANCELED_RETRY; i++) {
    if (ret == -ECANCELED) {
      obj_ctx.invalidate(olh_obj);
    }

    ret = get_obj_state(&obj_ctx, olh_obj, &state, false); /* don't follow olh */
    if (ret < 0)
      return ret;

    ret = olh_init_modification(*state, olh_obj, &op_tag);
    if (ret < 0) {
      ldout(cct, 20) << "olh_init_modification() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }

    ret = bucket_index_unlink_instance(target_obj, op_tag, olh_epoch);
    if (ret < 0) {
      ldout(cct, 20) << "bucket_index_link_olh() target_obj=" << target_obj << " returned " << ret << dendl;
      if (ret == -ECANCELED) {
        continue;
      }
      return ret;
    }
    break;
  }

  if (i == MAX_ECANCELED_RETRY) {
    ldout(cct, 0) << "ERROR: exceeded max ECANCELED retries, aborting (EIO)" << dendl;
    return -EIO;
  }

  ret = update_olh(obj_ctx, state, bucket_info, olh_obj);
  if (ret == -ECANCELED) { /* already did what we needed, no need to retry, raced with another user */
    return 0;
  }
  if (ret < 0) {
    ldout(cct, 20) << "update_olh() target_obj=" << target_obj << " returned " << ret << dendl;
    return ret;
  }

  return 0;
}

void RGWRados::gen_rand_obj_instance_name(rgw_obj *target_obj)
{
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(cct, buf, OBJ_INSTANCE_LEN); /* don't want it to get url escaped,
                                                                      no underscore for instance name due to the way we encode the raw keys */

  target_obj->set_instance(buf);
}

static void filter_attrset(map<string, bufferlist>& unfiltered_attrset, const string& check_prefix,
                           map<string, bufferlist> *attrset)
{
  attrset->clear();
  map<string, bufferlist>::iterator iter;
  for (iter = unfiltered_attrset.lower_bound(check_prefix);
       iter != unfiltered_attrset.end(); ++iter) {
    if (!str_startswith(iter->first, check_prefix))
      break;
    (*attrset)[iter->first] = iter->second;
  }
}

int RGWRados::get_olh(rgw_obj& obj, RGWOLHInfo *olh)
{
  map<string, bufferlist> unfiltered_attrset;

  ObjectReadOperation op;
  op.getxattrs(&unfiltered_attrset, NULL);

  bufferlist outbl;
  int r = obj_operate(obj, &op);

  if (r < 0) {
    return r;
  }
  map<string, bufferlist> attrset;

  filter_attrset(unfiltered_attrset, RGW_ATTR_OLH_PREFIX, &attrset);

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_OLH_INFO);
  if (iter == attrset.end()) { /* not an olh */
    return -EINVAL;
  }

  try {
    bufferlist::iterator biter = iter->second.begin();
    ::decode(*olh, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode olh info" << dendl;
    return -EIO;
  }

  return 0;
}

void RGWRados::check_pending_olh_entries(map<string, bufferlist>& pending_entries, 
                                         map<string, bufferlist> *rm_pending_entries)
{
  map<string, bufferlist>::iterator iter = pending_entries.begin();

  utime_t now = ceph_clock_now(cct);

  while (iter != pending_entries.end()) {
    bufferlist::iterator biter = iter->second.begin();
    RGWOLHPendingInfo pending_info;
    try {
      ::decode(pending_info, biter);
    } catch (buffer::error& err) {
      /* skipping bad entry, we could remove it but it might hide a bug */
      ldout(cct, 0) << "ERROR: failed to decode pending entry " << iter->first << dendl;
      ++iter;
      continue;
    }

    map<string, bufferlist>::iterator cur_iter = iter;
    ++iter;
    if (now - pending_info.time >= cct->_conf->rgw_olh_pending_timeout_sec) {
      (*rm_pending_entries)[cur_iter->first] = cur_iter->second;
      pending_entries.erase(cur_iter);
    } else {
      /* entries names are sorted by time (rounded to a second) */
      break;
    }
  }
}

int RGWRados::remove_olh_pending_entries(RGWObjState& state, rgw_obj& olh_obj, map<string, bufferlist>& pending_attrs)
{
  ObjectWriteOperation op;

  bucket_index_guard_olh_op(state, op);

  for (map<string, bufferlist>::iterator iter = pending_attrs.begin(); iter != pending_attrs.end(); ++iter) {
    op.rmxattr(iter->first.c_str());
  }

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(olh_obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  /* update olh object */
  r = ref.ioctx.operate(ref.oid, &op);
  if (r == -ENOENT || r == -ECANCELED) {
    /* raced with some other change, shouldn't sweat about it */
    r = 0;
  }
  if (r < 0) {
    ldout(cct, 0) << "ERROR: could not apply olh update, r=" << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::follow_olh(RGWObjectCtx& obj_ctx, RGWObjState *state, rgw_obj& olh_obj, rgw_obj *target)
{
  map<string, bufferlist> pending_entries;
  filter_attrset(state->attrset, RGW_ATTR_OLH_PENDING_PREFIX, &pending_entries);

  map<string, bufferlist> rm_pending_entries;
  check_pending_olh_entries(pending_entries, &rm_pending_entries);

  if (!rm_pending_entries.empty()) {
    int ret = remove_olh_pending_entries(*state, olh_obj, rm_pending_entries);
    if (ret < 0) {
      ldout(cct, 20) << "ERROR: rm_pending_entries returned ret=" << ret << dendl;
      return ret;
    }
  }
  if (!pending_entries.empty()) {
    ldout(cct, 20) << __func__ << "(): found pending entries, need to update_olh() on bucket=" << olh_obj.bucket << dendl;

    /* we could save a few cpu cycles if we drilled through all the layers and passed the bucket
     * info in, but this is supposed to be a rare call and it doesn't seem compelling enough
     * for cluttering everything
     */
    RGWBucketInfo bucket_info;
    int ret = get_bucket_instance_info(obj_ctx, olh_obj.bucket, bucket_info, NULL, NULL);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: get_bucket_instance_info() returned " << ret << " olh_obj.bucket=" << olh_obj.bucket << dendl;
    }
    ret = update_olh(obj_ctx, state, bucket_info, olh_obj);
    if (ret < 0) {
      return ret;
    }
  }

  map<string, bufferlist>::iterator iter = state->attrset.find(RGW_ATTR_OLH_INFO);
  assert(iter != state->attrset.end());
  RGWOLHInfo olh;
  try {
    bufferlist::iterator biter = iter->second.begin();
    ::decode(olh, biter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode olh info" << dendl;
    return -EIO;
  }

  if (olh.removed) {
    return -ENOENT;
  }

  *target = olh.target;

  return 0;
}

int RGWRados::raw_obj_stat(rgw_obj& obj, uint64_t *psize, time_t *pmtime, uint64_t *epoch,
                           map<string, bufferlist> *attrs, bufferlist *first_chunk,
                           RGWObjVersionTracker *objv_tracker)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  time_t mtime = 0;

  ObjectReadOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }
  if (attrs) {
    op.getxattrs(&unfiltered_attrset, NULL);
  }
  if (psize || pmtime) {
    op.stat(&size, &mtime, NULL);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, NULL);
  }
  bufferlist outbl;
  r = ref.ioctx.operate(ref.oid, &op, &outbl);

  if (epoch) {
    *epoch = ref.ioctx.get_last_version();
  }

  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (attrs) {
    filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  return 0;
}

int RGWRados::get_bucket_stats(rgw_bucket& bucket, int shard_id, string *bucket_ver, string *master_ver,
    map<RGWObjCategory, RGWStorageStats>& stats, string *max_marker)
{
  map<string, rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  int r = cls_bucket_head(bucket, shard_id, headers, &bucket_instance_ids);
  if (r < 0)
    return r;

  assert(headers.size() == bucket_instance_ids.size());

  map<string, rgw_bucket_dir_header>::iterator iter = headers.begin();
  map<int, string>::iterator viter = bucket_instance_ids.begin();
  BucketIndexShardsManager ver_mgr;
  BucketIndexShardsManager master_ver_mgr;
  BucketIndexShardsManager marker_mgr;
  string shard_marker;
  char buf[64];
  for(; iter != headers.end(); ++iter, ++viter) {
    accumulate_raw_stats(iter->second, stats);
    snprintf(buf, sizeof(buf), "%lu", (unsigned long)iter->second.ver);
    ver_mgr.add(viter->first, string(buf));
    snprintf(buf, sizeof(buf), "%lu", (unsigned long)iter->second.master_ver);
    master_ver_mgr.add(viter->first, string(buf));
    if (shard_id >= 0) {
      *max_marker = iter->second.max_marker;
    } else {
      marker_mgr.add(viter->first, iter->second.max_marker);
    }
  }
  ver_mgr.to_string(bucket_ver);
  master_ver_mgr.to_string(master_ver);
  if (shard_id < 0) {
    marker_mgr.to_string(max_marker);
  }
  return 0;
}

class RGWGetBucketStatsContext : public RGWGetDirHeader_CB {
  RGWGetBucketStats_CB *cb;
  uint32_t pendings;
  map<RGWObjCategory, RGWStorageStats> stats;
  int ret_code;
  bool should_cb;
  Mutex lock;

public:
  RGWGetBucketStatsContext(RGWGetBucketStats_CB *_cb, uint32_t _pendings)
    : cb(_cb), pendings(_pendings), stats(), ret_code(0), should_cb(true),
    lock("RGWGetBucketStatsContext") {}

  void handle_response(int r, rgw_bucket_dir_header& header) {
    Mutex::Locker l(lock);
    if (should_cb) {
      if ( r >= 0) {
        accumulate_raw_stats(header, stats);
      } else {
        ret_code = r;
      }

      // Are we all done?
      if (--pendings == 0) {
        if (!ret_code) {
          cb->set_response(&stats);
        }
        cb->handle_response(ret_code);
        cb->put();
      }
    }
  }

  void unset_cb() {
    Mutex::Locker l(lock);
    should_cb = false;
  }
};

int RGWRados::get_bucket_stats_async(rgw_bucket& bucket, int shard_id, RGWGetBucketStats_CB *ctx)
{
  RGWBucketInfo binfo;
  RGWObjectCtx obj_ctx(this);

  int r = get_bucket_instance_info(obj_ctx, bucket, binfo, NULL, NULL);
  if (r < 0)
    return r;

  int num_aio = 0;
  RGWGetBucketStatsContext *get_ctx = new RGWGetBucketStatsContext(ctx, binfo.num_shards);
  assert(get_ctx);
  r = cls_bucket_head_async(bucket, shard_id, get_ctx, &num_aio);
  get_ctx->put();
  if (r < 0) {
    ctx->put();
    if (num_aio) {
      get_ctx->unset_cb();
    }
  }
  return r;
}

class RGWGetUserStatsContext : public RGWGetUserHeader_CB {
  RGWGetUserStats_CB *cb;

public:
  explicit RGWGetUserStatsContext(RGWGetUserStats_CB *_cb) : cb(_cb) {}
  void handle_response(int r, cls_user_header& header) {
    cls_user_stats& hs = header.stats;
    if (r >= 0) {
      RGWStorageStats stats;

      stats.num_kb = (hs.total_bytes + 1023) / 1024;
      stats.num_kb_rounded = (hs.total_bytes_rounded + 1023) / 1024;
      stats.num_objects = hs.total_entries;

      cb->set_response(stats);
    }

    cb->handle_response(r);

    cb->put();
  }
};

int RGWRados::get_user_stats(const rgw_user& user, RGWStorageStats& stats)
{
  string user_str = user.to_str();

  cls_user_header header;
  int r = cls_user_get_header(user_str, &header);
  if (r < 0)
    return r;

  cls_user_stats& hs = header.stats;

  stats.num_kb = (hs.total_bytes + 1023) / 1024;
  stats.num_kb_rounded = (hs.total_bytes_rounded + 1023) / 1024;
  stats.num_objects = hs.total_entries;

  return 0;
}

int RGWRados::get_user_stats_async(const rgw_user& user, RGWGetUserStats_CB *ctx)
{
  string user_str = user.to_str();

  RGWGetUserStatsContext *get_ctx = new RGWGetUserStatsContext(ctx);
  int r = cls_user_get_header_async(user_str, get_ctx);
  if (r < 0) {
    ctx->put();
    delete get_ctx;
    return r;
  }

  return 0;
}

void RGWRados::get_bucket_instance_entry(rgw_bucket& bucket, string& entry)
{
  if (bucket.tenant.empty()) {
    entry = bucket.name + ":" + bucket.bucket_id;
  } else {
    entry = bucket.tenant + ":" + bucket.name + ":" + bucket.bucket_id;
  }
}

void RGWRados::get_bucket_meta_oid(rgw_bucket& bucket, string& oid)
{
  string entry;
  get_bucket_instance_entry(bucket, entry);
  oid = RGW_BUCKET_INSTANCE_MD_PREFIX + entry;
}

void RGWRados::get_bucket_instance_obj(rgw_bucket& bucket, rgw_obj& obj)
{
  if (!bucket.oid.empty()) {
    obj.init(get_zone_params().domain_root, bucket.oid);
  } else {
    string oid;
    get_bucket_meta_oid(bucket, oid);
    obj.init(get_zone_params().domain_root, oid);
  }
}

int RGWRados::get_bucket_instance_info(RGWObjectCtx& obj_ctx, const string& meta_key, RGWBucketInfo& info,
                                       time_t *pmtime, map<string, bufferlist> *pattrs)
{
  int pos = meta_key.find(':');
  if (pos < 0) {
    return -EINVAL;
  }
  string oid = RGW_BUCKET_INSTANCE_MD_PREFIX + meta_key;

  return get_bucket_instance_from_oid(obj_ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_info(RGWObjectCtx& obj_ctx, rgw_bucket& bucket, RGWBucketInfo& info,
                                       time_t *pmtime, map<string, bufferlist> *pattrs)
{
  string oid;
  if (bucket.oid.empty()) {
    get_bucket_meta_oid(bucket, oid);
  } else {
    oid = bucket.oid;
  }

  return get_bucket_instance_from_oid(obj_ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_from_oid(RGWObjectCtx& obj_ctx, string& oid, RGWBucketInfo& info,
                                           time_t *pmtime, map<string, bufferlist> *pattrs,
                                           rgw_cache_entry_info *cache_info)
{
  ldout(cct, 20) << "reading from " << get_zone_params().domain_root << ":" << oid << dendl;

  bufferlist epbl;

  int ret = rgw_get_system_obj(this, obj_ctx, get_zone_params().domain_root, oid, epbl, &info.objv_tracker, pmtime, pattrs, cache_info);
  if (ret < 0) {
    return ret;
  }

  bufferlist::iterator iter = epbl.begin();
  try {
    ::decode(info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  info.bucket.oid = oid;
  return 0;
}

int RGWRados::get_bucket_entrypoint_info(RGWObjectCtx& obj_ctx,
                                         const string& tenant_name,
                                         const string& bucket_name,
                                         RGWBucketEntryPoint& entry_point,
                                         RGWObjVersionTracker *objv_tracker,
                                         time_t *pmtime,
                                         map<string, bufferlist> *pattrs,
                                         rgw_cache_entry_info *cache_info)
{
  bufferlist bl;
  string bucket_entry;

  rgw_make_bucket_entry_name(tenant_name, bucket_name, bucket_entry);
  int ret = rgw_get_system_obj(this, obj_ctx, get_zone_params().domain_root, bucket_entry, bl, objv_tracker, pmtime, pattrs, cache_info);
  if (ret < 0) {
    return ret;
  }

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(entry_point, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWRados::convert_old_bucket_info(RGWObjectCtx& obj_ctx,
                                      const string& tenant_name,
                                      const string& bucket_name)
{
  RGWBucketEntryPoint entry_point;
  time_t ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;

  ldout(cct, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket_name << dendl;

  int ret = get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, entry_point, &ot, &ep_mtime, &attrs);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: get_bucket_entrypont_info() returned " << ret << " bucket=" << bucket_name << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;
  info.bucket.oid = bucket_name;
  info.ep_objv = ot.read_version;

  ot.generate_new_write_ver(cct);

  ret = put_linked_bucket_info(info, false, ep_mtime, &ot.write_version, &attrs, true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
  }

  return 0;
}

int RGWRados::get_bucket_info(RGWObjectCtx& obj_ctx,
                              const string& tenant, const string& bucket_name, RGWBucketInfo& info,
                              time_t *pmtime, map<string, bufferlist> *pattrs)
{
  bucket_info_entry e;
  string bucket_entry;
  rgw_make_bucket_entry_name(tenant, bucket_name, bucket_entry);

  if (binfo_cache.find(bucket_entry, &e)) {
    info = e.info;
    if (pattrs)
      *pattrs = e.attrs;
    if (pmtime)
      *pmtime = e.mtime;
    return 0;
  }

  bufferlist bl;

  RGWBucketEntryPoint entry_point;
  time_t ep_mtime;
  RGWObjVersionTracker ot;
  rgw_cache_entry_info entry_cache_info;
  int ret = get_bucket_entrypoint_info(obj_ctx, tenant, bucket_name, entry_point, &ot, &ep_mtime, pattrs, &entry_cache_info);
  if (ret < 0) {
    /* only init these fields */
    info.bucket.tenant = tenant;
    info.bucket.name = bucket_name;
    return ret;
  }

  if (entry_point.has_bucket_info) {
    info = entry_point.old_bucket_info;
    info.bucket.oid = bucket_name;
    info.bucket.tenant = tenant;
    info.ep_objv = ot.read_version;
    ldout(cct, 20) << "rgw_get_bucket_info: old bucket info, bucket=" << info.bucket << " owner " << info.owner << dendl;
    return 0;
  }

  /* data is in the bucket instance object, we need to get attributes from there, clear everything
   * that we got
   */
  if (pattrs) {
    pattrs->clear();
  }

  ldout(cct, 20) << "rgw_get_bucket_info: bucket instance: " << entry_point.bucket << dendl;


  /* read bucket instance info */

  string oid;
  get_bucket_meta_oid(entry_point.bucket, oid);

  rgw_cache_entry_info cache_info;

  ret = get_bucket_instance_from_oid(obj_ctx, oid, e.info, &e.mtime, &e.attrs, &cache_info);
  e.info.ep_objv = ot.read_version;
  info = e.info;
  if (ret < 0) {
    info.bucket.tenant = tenant;
    info.bucket.name = bucket_name;
    // XXX and why return anything in case of an error anyway?
    return ret;
  }

  if (pmtime)
    *pmtime = e.mtime;
  if (pattrs)
    *pattrs = e.attrs;

  list<rgw_cache_entry_info *> cache_info_entries;
  cache_info_entries.push_back(&entry_cache_info);
  cache_info_entries.push_back(&cache_info);


  /* chain to both bucket entry point and bucket instance */
  if (!binfo_cache.put(this, bucket_entry, &e, cache_info_entries)) {
    ldout(cct, 20) << "couldn't put binfo cache entry, might have raced with data changes" << dendl;
  }

  return 0;
}

int RGWRados::put_bucket_entrypoint_info(const string& tenant_name, const string& bucket_name, RGWBucketEntryPoint& entry_point,
                                         bool exclusive, RGWObjVersionTracker& objv_tracker, time_t mtime,
                                         map<string, bufferlist> *pattrs)
{
  bufferlist epbl;
  ::encode(entry_point, epbl);
  string bucket_entry;
  rgw_make_bucket_entry_name(tenant_name, bucket_name, bucket_entry);
  return rgw_bucket_store_info(this, bucket_entry, epbl, exclusive, pattrs, &objv_tracker, mtime);
}

int RGWRados::put_bucket_instance_info(RGWBucketInfo& info, bool exclusive,
                              time_t mtime, map<string, bufferlist> *pattrs)
{
  info.has_instance_obj = true;
  bufferlist bl;

  ::encode(info, bl);

  string key;
  get_bucket_instance_entry(info.bucket, key); /* when we go through meta api, we don't use oid directly */
  return rgw_bucket_instance_store_info(this, key, bl, exclusive, pattrs, &info.objv_tracker, mtime);
}

int RGWRados::put_linked_bucket_info(RGWBucketInfo& info, bool exclusive, time_t mtime, obj_version *pep_objv,
                                     map<string, bufferlist> *pattrs, bool create_entry_point)
{
  bufferlist bl;

  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = put_bucket_instance_info(info, exclusive, mtime, pattrs);
  if (ret < 0) {
    return ret;
  }

  if (!create_head)
    return 0; /* done! */

  RGWBucketEntryPoint entry_point;
  entry_point.bucket = info.bucket;
  entry_point.owner = info.owner;
  entry_point.creation_time = info.creation_time;
  entry_point.linked = true;
  RGWObjVersionTracker ot;
  if (pep_objv && !pep_objv->tag.empty()) {
    ot.write_version = *pep_objv;
  } else {
    ot.generate_new_write_ver(cct);
    if (pep_objv) {
      *pep_objv = ot.write_version;
    }
  }
  ret = put_bucket_entrypoint_info(info.bucket.tenant, info.bucket.name, entry_point, exclusive, ot, mtime, NULL); 
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::omap_get_vals(rgw_obj& obj, bufferlist& header, const string& marker, uint64_t count, std::map<string, bufferlist>& m)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = ref.ioctx.omap_get_vals(ref.oid, marker, count, &m);
  if (r < 0)
    return r;

  return 0;
 
}

int RGWRados::omap_get_all(rgw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m)
{
  string start_after;

  return omap_get_vals(obj, header, start_after, (uint64_t)-1, m);
}

int RGWRados::omap_set(rgw_obj& obj, std::string& key, bufferlist& bl)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  ldout(cct, 15) << "omap_set bucket=" << bucket << " oid=" << ref.oid << " key=" << key << dendl;

  map<string, bufferlist> m;
  m[key] = bl;

  r = ref.ioctx.omap_set(ref.oid, m);

  return r;
}

int RGWRados::omap_set(rgw_obj& obj, std::map<std::string, bufferlist>& m)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = ref.ioctx.omap_set(ref.oid, m);

  return r;
}

int RGWRados::omap_del(rgw_obj& obj, const std::string& key)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  set<string> k;
  k.insert(key);

  r = ref.ioctx.omap_rm_keys(ref.oid, k);
  return r;
}

int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    rgw_bucket& bucket = ent.bucket;

    map<string, rgw_bucket_dir_header> headers;
    int r = cls_bucket_head(bucket, RGW_NO_SHARD, headers);
    if (r < 0)
      return r;

    map<string, rgw_bucket_dir_header>::iterator hiter = headers.begin();
    for (; hiter != headers.end(); ++hiter) {
      RGWObjCategory category = main_category;
      map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = (hiter->second.stats).find((uint8_t)category);
      if (iter != hiter->second.stats.end()) {
        struct rgw_bucket_category_stats& stats = iter->second;
        ent.count += stats.num_entries;
        ent.size += stats.total_size;
        ent.size_rounded += stats.total_size_rounded;
      }
    }
  }

  return m.size();
}

int RGWRados::append_async(rgw_obj& obj, size_t size, bufferlist& bl)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }
  librados::Rados *rad = get_rados_handle();
  librados::AioCompletion *completion = rad->aio_create_completion(NULL, NULL, NULL);

  r = ref.ioctx.aio_append(ref.oid, completion, bl, size);
  completion->release();
  return r;
}

int RGWRados::distribute(const string& key, bufferlist& bl)
{
  /*
   * we were called before watch was initialized. This can only happen if we're updating some system
   * config object (e.g., zone info) during init. Don't try to distribute the cache info for these
   * objects, they're currently only read on startup anyway.
   */
  if (!watch_initialized)
    return 0;

  string notify_oid;
  pick_control_oid(key, notify_oid);

  ldout(cct, 10) << "distributing notification oid=" << notify_oid << " bl.length()=" << bl.length() << dendl;
  int r = control_pool_ctx.notify2(notify_oid, bl, 0, NULL);
  return r;
}

int RGWRados::pool_iterate_begin(rgw_bucket& bucket, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  iter = io_ctx.nobjects_begin();

  return 0;
}

int RGWRados::pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<RGWObjEnt>& objs,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::NObjectIterator& iter = ctx.iter;

  if (iter == io_ctx.nobjects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.nobjects_end(); ++i, ++iter) {
    RGWObjEnt e;

    string oid = iter->get_oid();
    ldout(cct, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter->filter(oid, oid))
      continue;

    e.key.set(oid);
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.nobjects_end());

  return objs.size();
}
struct RGWAccessListFilterPrefix : public RGWAccessListFilter {
  string prefix;

  explicit RGWAccessListFilterPrefix(const string& _prefix) : prefix(_prefix) {}
  virtual bool filter(string& name, string& key) {
    return (prefix.compare(key.substr(0, prefix.size())) == 0);
  }
};

int RGWRados::list_raw_objects(rgw_bucket& pool, const string& prefix_filter,
			       int max, RGWListRawObjsCtx& ctx, list<string>& oids,
			       bool *is_truncated)
{
  RGWAccessListFilterPrefix filter(prefix_filter);

  if (!ctx.initialized) {
    int r = pool_iterate_begin(pool, ctx.iter_ctx);
    if (r < 0) {
      ldout(cct, 10) << "failed to list objects pool_iterate_begin() returned r=" << r << dendl;
      return r;
    }
    ctx.initialized = true;
  }

  vector<RGWObjEnt> objs;
  int r = pool_iterate(ctx.iter_ctx, max, objs, is_truncated, &filter);
  if (r < 0) {
    ldout(cct, 10) << "failed to list objects pool_iterate returned r=" << r << dendl;
    return r;
  }

  vector<RGWObjEnt>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    oids.push_back(iter->key.name);
  }

  return oids.size();
}

int RGWRados::list_bi_log_entries(rgw_bucket& bucket, int shard_id, string& marker, uint32_t max,
                                  std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  ldout(cct, 20) << __func__ << ": " << bucket << " marker " << marker << " shard_id=" << shard_id << " max " << max << dendl;
  result.clear();

  librados::IoCtx index_ctx;
  map<int, string> oids;
  map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  map<int, string> bucket_instance_ids;
  int r = open_bucket_index(bucket, index_ctx, oids, shard_id, &bucket_instance_ids);
  if (r < 0)
    return r;

  BucketIndexShardsManager marker_mgr;
  bool has_shards = (oids.size() > 1 || shard_id >= 0);
  // If there are multiple shards for the bucket index object, the marker
  // should have the pattern '{shard_id_1}#{shard_marker_1},{shard_id_2}#
  // {shard_marker_2}...', if there is no sharding, the bi_log_list should
  // only contain one record, and the key is the bucket instance id.
  r = marker_mgr.from_string(marker, shard_id);
  if (r < 0)
    return r;
 
  r = CLSRGWIssueBILogList(index_ctx, marker_mgr, max, oids, bi_log_lists, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  map<int, cls_rgw_bi_log_list_ret>::iterator miter = bi_log_lists.begin();
  for (; miter != bi_log_lists.end(); ++miter) {
    int shard_id = miter->first;
    vcurrents[shard_id] = miter->second.entries.begin();
    vends[shard_id] = miter->second.entries.end();
    if (truncated) {
      *truncated = (*truncated || miter->second.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator viter;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator eiter;
  while (total < max && has_more) {
    has_more = false;

    viter = vcurrents.begin();
    eiter = vends.begin();

    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());

      int shard_id = viter->first;
      list<rgw_bi_log_entry>::iterator& liter = viter->second;

      if (liter == eiter->second){
        continue;
      }
      rgw_bi_log_entry& entry = *(liter);
      if (has_shards) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", shard_id);
        string tmp_id;
        build_bucket_index_marker(buf, entry.id, &tmp_id);
        entry.id.swap(tmp_id);
      }
      marker_mgr.add(shard_id, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (viter = vcurrents.begin(), eiter = vends.begin(); viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());
      *truncated = (*truncated || (viter->second != eiter->second));
    }
  }

  // Refresh marker, if there are multiple shards, the output will look like
  // '{shard_oid_1}#{shard_marker_1},{shard_oid_2}#{shard_marker_2}...',
  // if there is no sharding, the simply marker (without oid) is returned
  if (has_shards) {
    marker_mgr.to_string(&marker);
  } else {
    if (!result.empty()) {
      marker = result.rbegin()->id;
    }
  }

  return 0;
}

int RGWRados::trim_bi_log_entries(rgw_bucket& bucket, int shard_id, string& start_marker, string& end_marker)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket, index_ctx, bucket_objs, shard_id);
  if (r < 0)
    return r;

  BucketIndexShardsManager start_marker_mgr;
  r = start_marker_mgr.from_string(start_marker, shard_id);
  if (r < 0)
    return r;
  BucketIndexShardsManager end_marker_mgr;
  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0)
    return r;

  return CLSRGWIssueBILogTrim(index_ctx, start_marker_mgr, end_marker_mgr, bucket_objs,
      cct->_conf->rgw_bucket_index_max_aio)();
}

int RGWRados::bi_get_instance(rgw_obj& obj, rgw_bucket_dir_entry *dirent)
{
  rgw_bucket bucket;
  rgw_rados_ref ref;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  rgw_cls_bi_entry bi_entry;
  r = bi_get(bucket, obj, InstanceIdx, &bi_entry);
  if (r < 0 && r != -ENOENT) {
    ldout(cct, 0) << "ERROR: bi_get() returned r=" << r << dendl;
  }
  if (r < 0) {
    return r;
  }
  bufferlist::iterator iter = bi_entry.data.begin();
  try {
    ::decode(*dirent, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode bi_entry()" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRados::bi_get(rgw_bucket& bucket, rgw_obj& obj, BIIndexType index_type, rgw_cls_bi_entry *entry)
{
  BucketShard bs(this);
  int ret = bs.init(bucket, obj);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  cls_rgw_obj_key key(obj.get_index_key_name(), obj.get_instance());
  
  ret = cls_rgw_bi_get(bs.index_ctx, bs.bucket_obj, index_type, key, entry);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_put(rgw_bucket& bucket, rgw_obj& obj, rgw_cls_bi_entry& entry)
{
  BucketShard bs(this);
  int ret = bs.init(bucket, obj);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  ret = cls_rgw_bi_put(bs.index_ctx, bs.bucket_obj, entry);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::bi_list(rgw_bucket& bucket, const string& obj_name, const string& marker, uint32_t max, list<rgw_cls_bi_entry> *entries, bool *is_truncated)
{
  rgw_obj obj(bucket, obj_name);
  BucketShard bs(this);
  int ret = bs.init(bucket, obj);
  if (ret < 0) {
    ldout(cct, 5) << "bs.init() returned ret=" << ret << dendl;
    return ret;
  }

  ret = cls_rgw_bi_list(bs.index_ctx, bs.bucket_obj, obj_name, marker, max, entries, is_truncated);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::gc_operate(string& oid, librados::ObjectWriteOperation *op)
{
  return gc_pool_ctx.operate(oid, op);
}

int RGWRados::gc_aio_operate(string& oid, librados::ObjectWriteOperation *op)
{
  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int r = gc_pool_ctx.aio_operate(oid, c, op);
  c->release();
  return r;
}

int RGWRados::gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl)
{
  return gc_pool_ctx.operate(oid, op, pbl);
}

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  return gc->list(index, marker, max, expired_only, result, truncated);
}

int RGWRados::process_gc()
{
  return gc->process();
}

int RGWRados::process_expire_objects()
{
  obj_expirer->inspect_all_shards(utime_t(), ceph_clock_now(cct));
  return 0;
}

int RGWRados::cls_rgw_init_index(librados::IoCtx& index_ctx, librados::ObjectWriteOperation& op, string& oid)
{
  bufferlist in;
  cls_rgw_bucket_init(op);
  int r = index_ctx.operate(oid, &op);
  return r;
}

int RGWRados::cls_obj_prepare_op(BucketShard& bs, RGWModifyOp op, string& tag,
                                 rgw_obj& obj, uint16_t bilog_flags)
{
  ObjectWriteOperation o;
  cls_rgw_obj_key key(obj.get_index_key_name(), obj.get_instance());
  cls_rgw_bucket_prepare_op(o, op, tag, key, obj.get_loc(), get_zone().log_data, bilog_flags);
  int r = bs.index_ctx.operate(bs.bucket_obj, &o);
  return r;
}

int RGWRados::cls_obj_complete_op(BucketShard& bs, RGWModifyOp op, string& tag,
                                  int64_t pool, uint64_t epoch,
                                  RGWObjEnt& ent, RGWObjCategory category,
				  list<rgw_obj_key> *remove_objs, uint16_t bilog_flags)
{
  list<cls_rgw_obj_key> *pro = NULL;
  list<cls_rgw_obj_key> ro;

  if (remove_objs) {
    for (list<rgw_obj_key>::iterator iter = remove_objs->begin(); iter != remove_objs->end(); ++iter) {
      cls_rgw_obj_key k;
      iter->transform(&k);
      ro.push_back(k);
    }
    pro = &ro;
  }

  ObjectWriteOperation o;
  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta.size = ent.size;
  dir_meta.accounted_size = ent.size;
  dir_meta.mtime = utime_t(ent.mtime, 0);
  dir_meta.etag = ent.etag;
  dir_meta.owner = ent.owner.to_str();
  dir_meta.owner_display_name = ent.owner_display_name;
  dir_meta.content_type = ent.content_type;
  dir_meta.category = category;

  rgw_bucket_entry_ver ver;
  ver.pool = pool;
  ver.epoch = epoch;
  cls_rgw_obj_key key(ent.key.name, ent.key.instance);
  cls_rgw_bucket_complete_op(o, op, tag, ver, key, dir_meta, pro,
                             get_zone().log_data, bilog_flags);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int ret = bs.index_ctx.aio_operate(bs.bucket_obj, c, &o);
  c->release();
  return ret;
}

int RGWRados::cls_obj_complete_add(BucketShard& bs, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   RGWObjEnt& ent, RGWObjCategory category,
                                   list<rgw_obj_key> *remove_objs, uint16_t bilog_flags)
{
  return cls_obj_complete_op(bs, CLS_RGW_OP_ADD, tag, pool, epoch, ent, category, remove_objs, bilog_flags);
}

int RGWRados::cls_obj_complete_del(BucketShard& bs, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   rgw_obj& obj,
                                   list<rgw_obj_key> *remove_objs,
                                   uint16_t bilog_flags)
{
  RGWObjEnt ent;
  obj.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, CLS_RGW_OP_DEL, tag, pool, epoch, ent, RGW_OBJ_CATEGORY_NONE, remove_objs, bilog_flags);
}

int RGWRados::cls_obj_complete_cancel(BucketShard& bs, string& tag, rgw_obj& obj, uint16_t bilog_flags)
{
  RGWObjEnt ent;
  obj.get_index_key(&ent.key);
  return cls_obj_complete_op(bs, CLS_RGW_OP_CANCEL, tag, -1 /* pool id */, 0, ent, RGW_OBJ_CATEGORY_NONE, NULL, bilog_flags);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(rgw_bucket& bucket, uint64_t timeout)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket, index_ctx, bucket_objs);
  if (r < 0)
    return r;

  return CLSRGWIssueSetTagTimeout(index_ctx, bucket_objs, cct->_conf->rgw_bucket_index_max_aio, timeout)();
}

int RGWRados::cls_bucket_list(rgw_bucket& bucket, int shard_id, rgw_obj_key& start, const string& prefix,
		              uint32_t num_entries, bool list_versions, map<string, RGWObjEnt>& m,
			      bool *is_truncated, rgw_obj_key *last_entry,
			      bool (*force_check_filter)(const string&  name))
{
  ldout(cct, 10) << "cls_bucket_list " << bucket << " start " << start.name << "[" << start.instance << "] num_entries " << num_entries << dendl;

  librados::IoCtx index_ctx;
  // key   - oid (for different shards if there is any)
  // value - list result for the corresponding oid (shard), it is filled by the AIO callback
  map<int, string> oids;
  map<int, struct rgw_cls_list_ret> list_results;
  int r = open_bucket_index(bucket, index_ctx, oids, shard_id);
  if (r < 0)
    return r;

  cls_rgw_obj_key start_key(start.name, start.instance);
  r = CLSRGWIssueBucketList(index_ctx, start_key, prefix, num_entries, list_versions,
                            oids, list_results, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  // Create a list of iterators that are used to iterate each shard
  vector<map<string, struct rgw_bucket_dir_entry>::iterator> vcurrents(list_results.size());
  vector<map<string, struct rgw_bucket_dir_entry>::iterator> vends(list_results.size());
  vector<string> vnames(list_results.size());
  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  *is_truncated = false;
  for (; iter != list_results.end(); ++iter) {
    vcurrents.push_back(iter->second.dir.m.begin());
    vends.push_back(iter->second.dir.m.end());
    vnames.push_back(oids[iter->first]);
    *is_truncated = (*is_truncated || iter->second.is_truncated);
  }

  // Create a map to track the next candidate entry from each shard, if the entry
  // from a specified shard is selected/erased, the next entry from that shard will
  // be inserted for next round selection
  map<string, size_t> candidates;
  for (size_t i = 0; i < vcurrents.size(); ++i) {
    if (vcurrents[i] != vends[i]) {
      candidates[vcurrents[i]->first] = i;
    }
  }

  map<string, bufferlist> updates;
  uint32_t count = 0;
  while (count < num_entries && !candidates.empty()) {
    r = 0;
    // Select the next one
    int pos = candidates.begin()->second;
    const string& name = vcurrents[pos]->first;
    struct rgw_bucket_dir_entry& dirent = vcurrents[pos]->second;

    // fill it in with initial values; we may correct later
    RGWObjEnt e;
    e.key.set(dirent.key.name, dirent.key.instance);
    e.size = dirent.meta.size;
    e.mtime = dirent.meta.mtime;
    e.etag = dirent.meta.etag;
    e.owner = dirent.meta.owner;
    e.owner_display_name = dirent.meta.owner_display_name;
    e.content_type = dirent.meta.content_type;
    e.tag = dirent.tag;
    e.flags = dirent.flags;
    e.versioned_epoch = dirent.versioned_epoch;

    bool force_check = force_check_filter && force_check_filter(dirent.key.name);
    if ((!dirent.exists && !dirent.is_delete_marker()) || !dirent.pending_map.empty() || force_check) {
      /* there are uncommitted ops. We need to check the current state,
       * and if the tags are old we need to do cleanup as well. */
      librados::IoCtx sub_ctx;
      sub_ctx.dup(index_ctx);
      r = check_disk_state(sub_ctx, bucket, dirent, e, updates[vnames[pos]]);
      if (r < 0 && r != -ENOENT) {
          return r;
      }
    }
    if (r >= 0) {
      m[name] = e;
      ldout(cct, 10) << "RGWRados::cls_bucket_list: got " << e.key.name << "[" << e.key.instance << "]" << dendl;
      ++count;
    }

    // Refresh the candidates map
    candidates.erase(candidates.begin());
    ++vcurrents[pos];
    if (vcurrents[pos] != vends[pos]) {
      candidates[vcurrents[pos]->first] = pos;
    }
  }

  // Suggest updates if there is any
  map<string, bufferlist>::iterator miter = updates.begin();
  for (; miter != updates.end(); ++miter) {
    if (miter->second.length()) {
      ObjectWriteOperation o;
      cls_rgw_suggest_changes(o, miter->second);
      // we don't care if we lose suggested updates, send them off blindly
      AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
      index_ctx.aio_operate(miter->first, c, &o);
        c->release();
    }
  }

  // Check if all the returned entries are consumed or not
  for (size_t i = 0; i < vcurrents.size(); ++i) {
    if (vcurrents[i] != vends[i])
      *is_truncated = true;
  }
  if (!m.empty())
    *last_entry = m.rbegin()->first;

  return 0;
}

int RGWRados::cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = get_zone_params().usage_log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(usage_log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(usage_log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rad->ioctx_create(usage_log_pool, io_ctx);
  }
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_rgw_usage_log_add(op, info);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::cls_obj_usage_log_read(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                                     string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated)
{
  librados::IoCtx io_ctx;

  *is_truncated = false;

  const char *usage_log_pool = get_zone_params().usage_log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  r = cls_rgw_usage_log_read(io_ctx, oid, user, start_epoch, end_epoch,
			     max_entries, read_iter, usage, is_truncated);

  return r;
}

int RGWRados::cls_obj_usage_log_trim(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = get_zone_params().usage_log_pool.name.c_str();
  librados::Rados *rad = get_rados_handle();
  int r = rad->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_rgw_usage_log_trim(op, user, start_epoch, end_epoch);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::remove_objs_from_index(rgw_bucket& bucket, list<rgw_obj_key>& oid_list)
{
  librados::IoCtx index_ctx;
  string dir_oid;

  int r = open_bucket_index(bucket, index_ctx, dir_oid);
  if (r < 0)
    return r;

  bufferlist updates;

  list<rgw_obj_key>::iterator iter;

  for (iter = oid_list.begin(); iter != oid_list.end(); ++iter) {
    rgw_obj_key& key = *iter;
    dout(2) << "RGWRados::remove_objs_from_index bucket=" << bucket << " obj=" << key.name << ":" << key.instance << dendl;
    rgw_bucket_dir_entry entry;
    entry.ver.epoch = (uint64_t)-1; // ULLONG_MAX, needed to that objclass doesn't skip out request
    key.transform(&entry.key);
    updates.append(CEPH_RGW_REMOVE);
    ::encode(entry, updates);
  }

  bufferlist out;

  r = index_ctx.exec(dir_oid, "rgw", "dir_suggest_changes", updates, out);

  return r;
}

int RGWRados::check_disk_state(librados::IoCtx io_ctx,
                               rgw_bucket& bucket,
                               rgw_bucket_dir_entry& list_state,
                               RGWObjEnt& object,
                               bufferlist& suggested_updates)
{
  rgw_obj obj;
  std::string oid, instance, loc, ns;
  rgw_obj_key key;
  key.set(list_state.key);
  oid = key.name;
  if (!rgw_obj::strip_namespace_from_object(oid, ns, instance)) {
    // well crap
    assert(0 == "got bad object name off disk");
  }
  obj.init(bucket, oid);
  obj.set_loc(list_state.locator);
  obj.set_ns(ns);
  obj.set_instance(key.instance);
  get_obj_bucket_and_oid_loc(obj, bucket, oid, loc);
  io_ctx.locator_set_key(loc);

  RGWObjState *astate = NULL;
  RGWObjectCtx rctx(this);
  int r = get_obj_state(&rctx, obj, &astate, NULL);
  if (r < 0)
    return r;

  list_state.pending_map.clear(); // we don't need this and it inflates size
  if (!astate->exists) {
      /* object doesn't exist right now -- hopefully because it's
       * marked as !exists and got deleted */
    if (list_state.exists) {
      /* FIXME: what should happen now? Work out if there are any
       * non-bad ways this could happen (there probably are, but annoying
       * to handle!) */
    }
    // encode a suggested removal of that key
    list_state.ver.epoch = io_ctx.get_last_version();
    list_state.ver.pool = io_ctx.get_id();
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE, list_state, suggested_updates);
    return -ENOENT;
  }

  string etag;
  string content_type;
  ACLOwner owner;

  object.size = astate->size;
  object.mtime = utime_t(astate->mtime, 0);

  map<string, bufferlist>::iterator iter = astate->attrset.find(RGW_ATTR_ETAG);
  if (iter != astate->attrset.end()) {
    etag = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_CONTENT_TYPE);
  if (iter != astate->attrset.end()) {
    content_type = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_ACL);
  if (iter != astate->attrset.end()) {
    r = decode_policy(iter->second, &owner);
    if (r < 0) {
      dout(0) << "WARNING: could not decode policy for object: " << obj << dendl;
    }
  }

  if (astate->has_manifest) {
    RGWObjManifest::obj_iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      rgw_obj loc = miter.get_location();

      if (loc.ns == RGW_OBJ_NS_MULTIPART) {
	dout(10) << "check_disk_state(): removing manifest part from index: " << loc << dendl;
	r = delete_obj_index(loc);
	if (r < 0) {
	  dout(0) << "WARNING: delete_obj_index() returned r=" << r << dendl;
	}
      }
    }
  }

  object.etag = etag;
  object.content_type = content_type;
  object.owner = owner.get_id();
  object.owner_display_name = owner.get_display_name();

  // encode suggested updates
  list_state.ver.pool = io_ctx.get_id();
  list_state.ver.epoch = astate->epoch;
  list_state.meta.size = object.size;
  list_state.meta.mtime.set_from_double(double(object.mtime));
  list_state.meta.category = main_category;
  list_state.meta.etag = etag;
  list_state.meta.content_type = content_type;
  if (astate->obj_tag.length() > 0)
    list_state.tag = astate->obj_tag.c_str();
  list_state.meta.owner = owner.get_id().to_str();
  list_state.meta.owner_display_name = owner.get_display_name();

  list_state.exists = true;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE, list_state, suggested_updates);
  return 0;
}

int RGWRados::cls_bucket_head(rgw_bucket& bucket, int shard_id, map<string, struct rgw_bucket_dir_header>& headers, map<int, string> *bucket_instance_ids)
{
  librados::IoCtx index_ctx;
  map<int, string> oids;
  map<int, struct rgw_cls_list_ret> list_results;
  int r = open_bucket_index(bucket, index_ctx, oids, list_results, shard_id, bucket_instance_ids);
  if (r < 0)
    return r;

  r = CLSRGWIssueGetDirHeader(index_ctx, oids, list_results, cct->_conf->rgw_bucket_index_max_aio)();
  if (r < 0)
    return r;

  map<int, struct rgw_cls_list_ret>::iterator iter = list_results.begin();
  for(; iter != list_results.end(); ++iter) {
    headers[oids[iter->first]] = iter->second.dir.header;
  }
  return 0;
}

int RGWRados::cls_bucket_head_async(rgw_bucket& bucket, int shard_id, RGWGetDirHeader_CB *ctx, int *num_aio)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;
  int r = open_bucket_index(bucket, index_ctx, bucket_objs, shard_id);
  if (r < 0)
    return r;

  map<int, string>::iterator iter = bucket_objs.begin();
  for (; iter != bucket_objs.end(); ++iter) {
    r = cls_rgw_get_dir_header_async(index_ctx, iter->second, static_cast<RGWGetDirHeader_CB*>(ctx->get()));
    if (r < 0) {
      ctx->put();
      break;
    } else {
      (*num_aio)++;
    }
  }
  return r;
}

int RGWRados::cls_user_get_header(const string& user_id, cls_user_header *header)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj obj(get_zone_params().user_uid_pool, buckets_obj_id);

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  int rc;
  ::cls_user_get_header(op, header, &rc);
  bufferlist ibl;
  r = ref.ioctx.operate(ref.oid, &op, &ibl);
  if (r < 0)
    return r;
  if (rc < 0)
    return rc;

  return 0;
}

int RGWRados::cls_user_get_header_async(const string& user_id, RGWGetUserHeader_CB *ctx)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj obj(get_zone_params().user_uid_pool, buckets_obj_id);

  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  r = ::cls_user_get_header_async(ref.ioctx, ref.oid, ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_sync_bucket_stats(rgw_obj& user_obj, rgw_bucket& bucket)
{
  map<string, struct rgw_bucket_dir_header> headers;
  int r = cls_bucket_head(bucket, RGW_NO_SHARD, headers);
  if (r < 0) {
    ldout(cct, 20) << "cls_bucket_header() returned " << r << dendl;
    return r;
  }

  cls_user_bucket_entry entry;

  bucket.convert(&entry.bucket);

  map<string, struct rgw_bucket_dir_header>::iterator hiter = headers.begin();
  for (; hiter != headers.end(); ++hiter) {
    map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = hiter->second.stats.begin();
    for (; iter != hiter->second.stats.end(); ++iter) {
      struct rgw_bucket_category_stats& header_stats = iter->second;
      entry.size += header_stats.total_size;
      entry.size_rounded += header_stats.total_size_rounded;
      entry.count += header_stats.num_entries;
    }
  }

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  r = cls_user_update_buckets(user_obj, entries, false);
  if (r < 0) {
    ldout(cct, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::update_user_bucket_stats(const string& user_id, rgw_bucket& bucket, RGWStorageStats& stats)
{
  cls_user_bucket_entry entry;

  entry.size = stats.num_kb * 1024;
  entry.size_rounded = stats.num_kb_rounded * 1024;
  entry.count += stats.num_objects;

  list<cls_user_bucket_entry> entries;
  entries.push_back(entry);

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj obj(get_zone_params().user_uid_pool, buckets_obj_id);

  int r = cls_user_update_buckets(obj, entries, false);
  if (r < 0) {
    ldout(cct, 20) << "cls_user_update_buckets() returned " << r << dendl;
    return r;
  }

  return 0;
}

int RGWRados::cls_user_list_buckets(rgw_obj& obj,
                                    const string& in_marker,
                                    const string& end_marker,
                                    const int max_entries,
                                    list<cls_user_bucket_entry>& entries,
                                    string * const out_marker,
                                    bool * const truncated)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  int rc;

  cls_user_bucket_list(op, in_marker, end_marker, max_entries, entries, out_marker, truncated, &rc);
  bufferlist ibl;
  r = ref.ioctx.operate(ref.oid, &op, &ibl);
  if (r < 0)
    return r;
  if (rc < 0)
    return rc;

  return 0;
}

int RGWRados::cls_user_update_buckets(rgw_obj& obj, list<cls_user_bucket_entry>& entries, bool add)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_user_set_buckets(op, entries, add);
  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::complete_sync_user_stats(const rgw_user& user_id)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_obj obj(get_zone_params().user_uid_pool, buckets_obj_id);
  return cls_user_complete_stats_sync(obj);
}

int RGWRados::cls_user_complete_stats_sync(rgw_obj& obj)
{
  rgw_rados_ref ref;
  rgw_bucket bucket;
  int r = get_obj_ref(obj, &ref, &bucket);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_complete_stats_sync(op);
  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_user_add_bucket(rgw_obj& obj, const cls_user_bucket_entry& entry)
{
  list<cls_user_bucket_entry> l;
  l.push_back(entry);

  return cls_user_update_buckets(obj, l, true);
}

int RGWRados::cls_user_remove_bucket(rgw_obj& obj, const cls_user_bucket& bucket)
{
  rgw_bucket b;
  rgw_rados_ref ref;
  int r = get_obj_ref(obj, &ref, &b);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_remove_bucket(op, bucket);
  r = ref.ioctx.operate(ref.oid, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size)
{
  return quota_handler->check_quota(bucket_owner, bucket, user_quota, bucket_quota, 1, obj_size);
}

void RGWRados::get_bucket_index_objects(const string& bucket_oid_base,
    uint32_t num_shards, map<int, string>& bucket_objects, int shard_id)
{
  if (!num_shards) {
    bucket_objects[0] = bucket_oid_base;
  } else {
    char buf[bucket_oid_base.size() + 32];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), i);
        bucket_objects[i] = buf;
      }
    } else {
      if ((uint32_t)shard_id > num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), shard_id);
      bucket_objects[shard_id] = buf;
    }
  }
}

void RGWRados::get_bucket_instance_ids(RGWBucketInfo& bucket_info, int shard_id, map<int, string> *result)
{
  rgw_bucket& bucket = bucket_info.bucket;
  string plain_id = bucket.name + ":" + bucket.bucket_id;
  if (!bucket_info.num_shards) {
    (*result)[0] = plain_id;
  } else {
    char buf[16];
    if (shard_id < 0) {
      for (uint32_t i = 0; i < bucket_info.num_shards; ++i) {
        snprintf(buf, sizeof(buf), ":%d", i);
        (*result)[i] = plain_id + buf;
      }
    } else {
      if ((uint32_t)shard_id > bucket_info.num_shards) {
        return;
      }
      snprintf(buf, sizeof(buf), ":%d", shard_id);
      (*result)[shard_id] = plain_id + buf;
    }
  }
}

int RGWRados::get_bucket_index_object(const string& bucket_oid_base, const string& obj_key,
    uint32_t num_shards, RGWBucketInfo::BIShardsHashType hash_type, string *bucket_obj, int *shard_id)
{
  int r = 0;
  switch (hash_type) {
    case RGWBucketInfo::MOD:
      if (!num_shards) {
        // By default with no sharding, we use the bucket oid as itself
        (*bucket_obj) = bucket_oid_base;
        if (shard_id) {
          *shard_id = -1;
        }
      } else {
        uint32_t sid = ceph_str_hash_linux(obj_key.c_str(), obj_key.size());
        uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
        sid = sid2 % MAX_BUCKET_INDEX_SHARDS_PRIME % num_shards;
        char buf[bucket_oid_base.size() + 32];
        snprintf(buf, sizeof(buf), "%s.%d", bucket_oid_base.c_str(), sid);
        (*bucket_obj) = buf;
        if (shard_id) {
          *shard_id = (int)sid;
        }
      }
      break;
    default:
      r = -ENOTSUP;
  }
  return r;
}

void RGWStateLog::oid_str(int shard, string& oid) {
  oid = RGW_STATELOG_OBJ_PREFIX + module_name + ".";
  char buf[16];
  snprintf(buf, sizeof(buf), "%d", shard);
  oid += buf;
}

int RGWStateLog::get_shard_num(const string& object) {
  uint32_t val = ceph_str_hash_linux(object.c_str(), object.length());
  return val % num_shards;
}

string RGWStateLog::get_oid(const string& object) {
  int shard = get_shard_num(object);
  string oid;
  oid_str(shard, oid);
  return oid;
}

int RGWStateLog::open_ioctx(librados::IoCtx& ioctx) {
  string pool_name;
  store->get_log_pool_name(pool_name);
  int r = store->get_rados_handle()->ioctx_create(pool_name.c_str(), ioctx);
  if (r < 0) {
    lderr(store->ctx()) << "ERROR: could not open rados pool" << dendl;
    return r;
  }
  return 0;
}

int RGWStateLog::store_entry(const string& client_id, const string& op_id, const string& object,
                  uint32_t state, bufferlist *bl, uint32_t *check_state)
{
  if (client_id.empty() ||
      op_id.empty() ||
      object.empty()) {
    ldout(store->ctx(), 0) << "client_id / op_id / object is empty" << dendl;
  }

  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx);
  if (r < 0)
    return r;

  string oid = get_oid(object);

  librados::ObjectWriteOperation op;
  if (check_state) {
    cls_statelog_check_state(op, client_id, op_id, object, *check_state);
  }
  utime_t ts = ceph_clock_now(store->ctx());
  bufferlist nobl;
  cls_statelog_add(op, client_id, op_id, object, ts, state, (bl ? *bl : nobl));
  r = ioctx.operate(oid, &op);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStateLog::remove_entry(const string& client_id, const string& op_id, const string& object)
{
  if (client_id.empty() ||
      op_id.empty() ||
      object.empty()) {
    ldout(store->ctx(), 0) << "client_id / op_id / object is empty" << dendl;
  }

  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx);
  if (r < 0)
    return r;

  string oid = get_oid(object);

  librados::ObjectWriteOperation op;
  cls_statelog_remove_by_object(op, object, op_id);
  r = ioctx.operate(oid, &op);
  if (r < 0) {
    return r;
  }

  return 0;
}

void RGWStateLog::init_list_entries(const string& client_id, const string& op_id, const string& object,
                                    void **handle)
{
  list_state *state = new list_state;
  state->client_id = client_id;
  state->op_id = op_id;
  state->object = object;
  if (object.empty()) {
    state->cur_shard = 0;
    state->max_shard = num_shards - 1;
  } else {
    state->cur_shard = state->max_shard = get_shard_num(object);
  }
  *handle = (void *)state;
}

int RGWStateLog::list_entries(void *handle, int max_entries,
                              list<cls_statelog_entry>& entries,
                              bool *done)
{
  list_state *state = static_cast<list_state *>(handle);

  librados::IoCtx ioctx;
  int r = open_ioctx(ioctx);
  if (r < 0)
    return r;

  entries.clear();

  for (; state->cur_shard <= state->max_shard && max_entries > 0; ++state->cur_shard) {
    string oid;
    oid_str(state->cur_shard, oid);

    librados::ObjectReadOperation op;
    list<cls_statelog_entry> ents;
    bool truncated;
    cls_statelog_list(op, state->client_id, state->op_id, state->object, state->marker,
                      max_entries, ents, &state->marker, &truncated);
    bufferlist ibl;
    r = ioctx.operate(oid, &op, &ibl);
    if (r == -ENOENT) {
      truncated = false;
      r = 0;
    }
    if (r < 0) {
      ldout(store->ctx(), 0) << "cls_statelog_list returned " << r << dendl;
      return r;
    }

    if (!truncated) {
      state->marker.clear();
    }

    max_entries -= ents.size();

    entries.splice(entries.end(), ents);

    if (truncated)
      break;
  }

  *done = (state->cur_shard > state->max_shard);

  return 0;
}

void RGWStateLog::finish_list_entries(void *handle)
{
  list_state *state = static_cast<list_state *>(handle);
  delete state;
}

void RGWStateLog::dump_entry(const cls_statelog_entry& entry, Formatter *f)
{
  f->open_object_section("statelog_entry");
  f->dump_string("client_id", entry.client_id);
  f->dump_string("op_id", entry.op_id);
  f->dump_string("object", entry.object);
  entry.timestamp.gmtime(f->dump_stream("timestamp"));
  if (!dump_entry_internal(entry, f)) {
    f->dump_int("state", entry.state);
  }
  f->close_section();
}

RGWOpState::RGWOpState(RGWRados *_store) : RGWStateLog(_store, _store->ctx()->_conf->rgw_num_zone_opstate_shards, string("obj_opstate"))
{
}

bool RGWOpState::dump_entry_internal(const cls_statelog_entry& entry, Formatter *f)
{
  string s;
  switch ((OpState)entry.state) {
    case OPSTATE_UNKNOWN:
      s = "unknown";
      break;
    case OPSTATE_IN_PROGRESS:
      s = "in-progress";
      break;
    case OPSTATE_COMPLETE:
      s = "complete";
      break;
    case OPSTATE_ERROR:
      s = "error";
      break;
    case OPSTATE_ABORT:
      s = "abort";
      break;
    case OPSTATE_CANCELLED:
      s = "cancelled";
      break;
    default:
      s = "invalid";
  }
  f->dump_string("state", s);
  return true;
}

int RGWOpState::state_from_str(const string& s, OpState *state)
{
  if (s == "unknown") {
    *state = OPSTATE_UNKNOWN;
  } else if (s == "in-progress") {
    *state = OPSTATE_IN_PROGRESS;
  } else if (s == "complete") {
    *state = OPSTATE_COMPLETE;
  } else if (s == "error") {
    *state = OPSTATE_ERROR;
  } else if (s == "abort") {
    *state = OPSTATE_ABORT;
  } else if (s == "cancelled") {
    *state = OPSTATE_CANCELLED;
  } else {
    return -EINVAL;
  }

  return 0;
}

int RGWOpState::set_state(const string& client_id, const string& op_id, const string& object, OpState state)
{
  uint32_t s = (uint32_t)state;
  return store_entry(client_id, op_id, object, s, NULL, NULL);
}

int RGWOpState::renew_state(const string& client_id, const string& op_id, const string& object, OpState state)
{
  uint32_t s = (uint32_t)state;
  return store_entry(client_id, op_id, object, s, NULL, &s);
}

RGWOpStateSingleOp::RGWOpStateSingleOp(RGWRados *store, const string& cid, const string& oid,
                                       const string& obj) : os(store), client_id(cid), op_id(oid), object(obj)
{
  cct = store->ctx();
  cur_state = RGWOpState::OPSTATE_UNKNOWN;
}

int RGWOpStateSingleOp::set_state(RGWOpState::OpState state) {
  last_update = ceph_clock_now(cct);
  cur_state = state;
  return os.set_state(client_id, op_id, object, state);
}

int RGWOpStateSingleOp::renew_state() {
  utime_t now = ceph_clock_now(cct);

  int rate_limit_sec = cct->_conf->rgw_opstate_ratelimit_sec;

  if (rate_limit_sec && now - last_update < rate_limit_sec) {
    return 0;
  }

  last_update = now;
  return os.renew_state(client_id, op_id, object, cur_state);
}


uint64_t RGWRados::instance_id()
{
  return get_rados_handle()->get_instance_id();
}

uint64_t RGWRados::next_bucket_id()
{
  Mutex::Locker l(bucket_id_lock);
  return ++max_bucket_id;
}

RGWRados *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool quota_threads, bool run_sync_thread)
{
  int use_cache = cct->_conf->rgw_cache_enabled;
  RGWRados *store = NULL;
  if (!use_cache) {
    store = new RGWRados;
  } else {
    store = new RGWCache<RGWRados>; 
  }

  if (store->initialize(cct, use_gc_thread, quota_threads, run_sync_thread) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

RGWRados *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *store = NULL;
  store = new RGWRados;

  store->set_context(cct);

  if (store->init_rados() < 0) {
    delete store;
    return NULL;
  }

  return store;
}

void RGWStoreManager::close_storage(RGWRados *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}

librados::Rados* RGWRados::get_rados_handle()
{
  if (num_rados_handles == 1) {
    return rados[0];
  } else {
    handle_lock.get_read();
    pthread_t id = pthread_self();
    std::map<pthread_t, int>:: iterator it = rados_map.find(id);

    if (it != rados_map.end()) {
      handle_lock.put_read();
      return rados[it->second];
    } else {
      handle_lock.put_read();
      handle_lock.get_write();
      uint32_t handle = next_rados_handle.read();
      if (handle == num_rados_handles) {
        next_rados_handle.set(0);
        handle = 0;
      }
      rados_map[id] = handle;
      next_rados_handle.inc();
      handle_lock.put_write();
      return rados[handle];
    }
  }
}

