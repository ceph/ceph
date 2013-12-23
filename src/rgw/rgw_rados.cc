#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "common/ceph_json.h"

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/Throttle.h"

#include "rgw_rados.h"
#include "rgw_cache.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h" /* for dumping s3policy in debug log */
#include "rgw_metadata.h"
#include "rgw_bucket.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "cls/log/cls_log_client.h"
#include "cls/statelog/cls_statelog_client.h"
#include "cls/lock/cls_lock_client.h"

#include "rgw_tools.h"

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

#define dout_subsys ceph_subsys_rgw

using namespace std;

static RGWCache<RGWRados> cached_rados_provider;
static RGWRados rados_provider;

static string notify_oid_prefix = "notify";
static string *notify_oids = NULL;
static string shadow_ns = "shadow";
static string dir_oid_prefix = ".dir.";
static string default_storage_pool = ".rgw.buckets";
static string avail_pools = ".pools.avail";

static string zone_info_oid_prefix = "zone_info.";
static string region_info_oid_prefix = "region_info.";

static string default_region_info_oid = "default.region";
static string region_map_oid = "region_map";
static string log_lock_name = "rgw_log_lock";

static RGWObjCategory main_category = RGW_OBJ_CATEGORY_MAIN;

#define RGW_USAGE_OBJ_PREFIX "usage."

#define RGW_DEFAULT_ZONE_ROOT_POOL ".rgw.root"
#define RGW_DEFAULT_REGION_ROOT_POOL ".rgw.root"

#define RGW_STATELOG_OBJ_PREFIX "statelog."


#define dout_subsys ceph_subsys_rgw

void RGWDefaultRegionInfo::dump(Formatter *f) const {
  encode_json("default_region", default_region, f);
}

void RGWDefaultRegionInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("default_region", default_region, obj);
}

int RGWRegion::get_pool_name(CephContext *cct, string *pool_name)
{
  *pool_name = cct->_conf->rgw_region_root_pool;
  if (pool_name->empty()) {
    *pool_name = RGW_DEFAULT_REGION_ROOT_POOL;
  } else if ((*pool_name)[0] != '.') {
    derr << "ERROR: region root pool name must start with a period" << dendl;
    return -EINVAL;
  }
  return 0;
}

int RGWRegion::read_default(RGWDefaultRegionInfo& default_info)
{
  string pool_name;

  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0) {
    return ret;
  }

  string oid = cct->_conf->rgw_default_region_info_oid;
  if (oid.empty()) {
    oid = default_region_info_oid;
  }

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;
  ret = rgw_get_system_obj(store, NULL, pool, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(default_info, iter);
  } catch (buffer::error& err) {
    derr << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  name = default_info.default_region;

  return 0;
}

int RGWRegion::set_as_default()
{
  string pool_name;
  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  string oid = cct->_conf->rgw_default_region_info_oid;
  if (oid.empty()) {
    oid = default_region_info_oid;
  }

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  RGWDefaultRegionInfo default_info;
  default_info.default_region = name;

  ::encode(default_info, bl);

  ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRegion::init(CephContext *_cct, RGWRados *_store, bool setup_region)
{
  cct = _cct;
  store = _store;

  if (!setup_region)
    return 0;

  string region_name = cct->_conf->rgw_region;

  if (region_name.empty()) {
    RGWDefaultRegionInfo default_info;
    int r = read_default(default_info);
    if (r == -ENOENT) {
      r = create_default();
      if (r == -EEXIST) { /* we may have raced with another region creation,
                             make sure we can read the region info and continue
                             as usual to make sure region creation is complete */
        ldout(cct, 0) << "create_default() returned -EEXIST, we raced with another region creation" << dendl;
        r = read_info(name);
      }
      if (r < 0)
        return r;
      r = set_as_default(); /* set this as default even if we weren't the creators */
      if (r < 0)
        return r;
      /*Re attempt to read region info from newly created default region */
      r = read_default(default_info);
      if (r < 0)
	return r;
    } else if (r < 0) {
      lderr(cct) << "failed reading default region info: " << cpp_strerror(-r) << dendl;
      return r;
    }
    region_name = default_info.default_region;
  }

  return read_info(region_name);
}

int RGWRegion::read_info(const string& region_name)
{
  string pool_name;
  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  name = region_name;

  string oid = region_info_oid_prefix + name;

  ret = rgw_get_system_obj(store, NULL, pool, oid, bl, NULL, NULL);
  if (ret < 0) {
    lderr(cct) << "failed reading region info from " << pool << ":" << oid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode region from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRegion::create_default()
{
  name = "default";
  string zone_name = "default";

  is_master = true;

  RGWRegionPlacementTarget placement_target;
  placement_target.name = "default-placement";
  placement_targets[placement_target.name] = placement_target;
  default_placement = "default-placement";

  RGWZone& default_zone = zones[zone_name];
  default_zone.name = zone_name;

  RGWZoneParams zone_params;
  zone_params.name = zone_name;
  zone_params.init_default(store);

  int r = zone_params.store_info(cct, store, *this);
  if (r < 0) {
    derr << "error storing zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = store_info(true);
  if (r < 0) {
    derr << "error storing region info: " << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

int RGWRegion::store_info(bool exclusive)
{
  string pool_name;
  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  rgw_bucket pool(pool_name.c_str());

  string oid = region_info_oid_prefix + name;

  bufferlist bl;
  ::encode(*this, bl);
  ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), exclusive, NULL, 0, NULL);

  return ret;
}

int RGWRegion::equals(const string& other_region)
{
  if (is_master && other_region.empty())
    return true;

  return (name == other_region);
}

void RGWZoneParams::init_default(RGWRados *store)
{
  domain_root = ".rgw";
  control_pool = ".rgw.control";
  gc_pool = ".rgw.gc";
  log_pool = ".log";
  intent_log_pool = ".intent-log";
  usage_log_pool = ".usage";
  user_keys_pool = ".users";
  user_email_pool = ".users.email";
  user_swift_pool = ".users.swift";
  user_uid_pool = ".users.uid";

  /* check for old pools config */
  rgw_obj obj(domain_root, avail_pools);
  int r =  store->obj_stat(NULL, obj, NULL, NULL, NULL, NULL, NULL, NULL);
  if (r < 0) {
    ldout(store->ctx(), 0) << "couldn't find old data placement pools config, setting up new ones for the zone" << dendl;
    /* a new system, let's set new placement info */
    RGWZonePlacementInfo default_placement;
    default_placement.index_pool = ".rgw.buckets.index";
    default_placement.data_pool = ".rgw.buckets";
    placement_pools["default-placement"] = default_placement;
  }
}

int RGWZoneParams::get_pool_name(CephContext *cct, string *pool_name)
{
  *pool_name = cct->_conf->rgw_zone_root_pool;
  if (pool_name->empty()) {
    *pool_name = RGW_DEFAULT_ZONE_ROOT_POOL;
  } else if ((*pool_name)[0] != '.') {
    derr << "ERROR: zone root pool name must start with a period" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWZoneParams::init_name(CephContext *cct, RGWRegion& region)
{
  name = cct->_conf->rgw_zone;

  if (name.empty()) {
    name = region.master_zone;

    if (name.empty()) {
      name = "default";
    }
  }
}

int RGWZoneParams::init(CephContext *cct, RGWRados *store, RGWRegion& region)
{
  init_name(cct, region);

  string pool_name;
  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  string oid = zone_info_oid_prefix + name;
  ret = rgw_get_system_obj(store, NULL, pool, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode zone info from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  is_master = (name == region.master_zone) || (region.master_zone.empty() && name == "default");

  ldout(cct, 2) << "zone " << name << " is " << (is_master ? "" : "NOT ") << "master" << dendl;

  return 0;
}

int RGWZoneParams::store_info(CephContext *cct, RGWRados *store, RGWRegion& region)
{
  init_name(cct, region);

  string pool_name;
  int ret = get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  rgw_bucket pool(pool_name.c_str());
  string oid = zone_info_oid_prefix + name;

  bufferlist bl;
  ::encode(*this, bl);
  ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);

  return ret;
}

void RGWRegionMap::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  ::encode(regions, bl);
  ::encode(master_region, bl);
  ::encode(bucket_quota, bl);
  ENCODE_FINISH(bl);
}

void RGWRegionMap::decode(bufferlist::iterator& bl) {
  DECODE_START(2, bl);
  ::decode(regions, bl);
  ::decode(master_region, bl);

  if (struct_v >= 2)
    ::decode(bucket_quota, bl);
  DECODE_FINISH(bl);

  regions_by_api.clear();
  for (map<string, RGWRegion>::iterator iter = regions.begin();
       iter != regions.end(); ++iter) {
    RGWRegion& region = iter->second;
    regions_by_api[region.api_name] = region;
    if (region.is_master) {
      master_region = region.name;
    }
  }
}

void RGWRegionMap::get_params(CephContext *cct, string& pool_name, string& oid)
{
  pool_name = cct->_conf->rgw_zone_root_pool;
  if (pool_name.empty()) {
    pool_name = RGW_DEFAULT_ZONE_ROOT_POOL;
  }
  oid = region_map_oid;
}

int RGWRegionMap::read(CephContext *cct, RGWRados *store)
{
  string pool_name, oid;

  get_params(cct, pool_name, oid);

  rgw_bucket pool(pool_name.c_str());

  bufferlist bl;
  int ret = rgw_get_system_obj(store, NULL, pool, oid, bl, NULL, NULL);
  if (ret < 0)
    return ret;


  Mutex::Locker l(lock);
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode region map info from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWRegionMap::store(CephContext *cct, RGWRados *store)
{
  string pool_name, oid;

  get_params(cct, pool_name, oid);

  rgw_bucket pool(pool_name.c_str());

  Mutex::Locker l(lock);

  bufferlist bl;
  ::encode(*this, bl);
  int ret = rgw_put_system_obj(store, pool, oid, bl.c_str(), bl.length(), false, NULL, 0, NULL);

  return ret;
}

int RGWRegionMap::update(RGWRegion& region)
{
  Mutex::Locker l(lock);

  if (region.is_master && !region.equals(master_region)) {
    derr << "cannot update region map, master_region conflict" << dendl;
    return -EINVAL;
  }
  map<string, RGWRegion>::iterator iter = regions.find(region.name);
  if (iter != regions.end()) {
    RGWRegion& old_region = iter->second;
    if (!old_region.api_name.empty()) {
      regions_by_api.erase(old_region.api_name);
    }
  }
  regions[region.name] = region;

  if (!region.api_name.empty()) {
    regions_by_api[region.api_name] = region;
  }

  if (region.is_master) {
    master_region = region.name;
  }
  return 0;
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

void RGWObjManifest::append(RGWObjManifest& m)
{
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;
}

void RGWObjVersionTracker::generate_new_write_ver(CephContext *cct)
{
  write_version.ver = 1;
#define TAG_LEN 24

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

int RGWPutObjProcessor::complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs)
{
  int r = do_complete(etag, mtime, set_mtime, attrs);
  if (r < 0)
    return r;

  is_complete = true;
  return 0;
}

RGWPutObjProcessor::~RGWPutObjProcessor()
{
  if (is_complete)
    return;

  list<rgw_obj>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    rgw_obj& obj = *iter;
    int r = store->delete_obj(obj_ctx, obj);
    if (r < 0 && r != -ENOENT) {
      ldout(store->ctx(), 0) << "WARNING: failed to remove obj (" << obj << "), leaked" << dendl;
    }
  }
}

int RGWPutObjProcessor_Plain::prepare(RGWRados *store, void *obj_ctx)
{
  RGWPutObjProcessor::prepare(store, obj_ctx);

  obj.init(bucket, obj_str);

  return 0;
};

int RGWPutObjProcessor_Plain::handle_data(bufferlist& bl, off_t _ofs, void **phandle)
{
  if (ofs != _ofs)
    return -EINVAL;

  data.append(bl);
  ofs += bl.length();

  return 0;
}

int RGWPutObjProcessor_Plain::do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs)
{
  RGWRados::PutObjMetaExtraParams params;
  params.set_mtime = set_mtime;
  params.mtime = mtime;
  params.data = &data;

  int r = store->put_obj_meta(obj_ctx, obj, data.length(), attrs,
                              RGW_OBJ_CATEGORY_MAIN, PUT_OBJ_CREATE,
                              params);
  return r;
}


int RGWPutObjProcessor_Aio::handle_obj_data(rgw_obj& obj, bufferlist& bl, off_t ofs, off_t abs_ofs, void **phandle)
{
  if ((uint64_t)abs_ofs + bl.length() > obj_len)
    obj_len = abs_ofs + bl.length();

  // For the first call pass -1 as the offset to
  // do a write_full.
  int r = store->aio_put_obj_data(NULL, obj,
                                     bl,
                                     ((ofs != 0) ? ofs : -1),
                                     false, phandle);

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

int RGWPutObjProcessor_Aio::throttle_data(void *handle)
{
  if (handle) {
    struct put_obj_aio_info info;
    info.handle = handle;
    pending.push_back(info);
  }
  size_t orig_size = pending.size();
  while (pending_has_completed()) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }

  /* resize window in case messages are draining too fast */
  if (orig_size - pending.size() >= max_chunks) {
    max_chunks++;
  }

  if (pending.size() > max_chunks) {
    int r = wait_pending_front();
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWPutObjProcessor_Atomic::write_data(bufferlist& bl, off_t ofs, void **phandle)
{
  if (ofs >= next_part_ofs)
    prepare_next_part(ofs);

  return RGWPutObjProcessor_Aio::handle_obj_data(cur_obj, bl, ofs - cur_part_ofs, ofs, phandle);
}

int RGWPutObjProcessor_Atomic::handle_data(bufferlist& bl, off_t ofs, void **phandle)
{
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

  pending_data_bl.claim_append(bl);
  if (pending_data_bl.length() < RGW_MAX_CHUNK_SIZE)
    return 0;

  pending_data_bl.splice(0, RGW_MAX_CHUNK_SIZE, &bl);

  if (!data_ofs && !immutable_head()) {
    first_chunk.claim(bl);
    obj_len = (uint64_t)first_chunk.length();
    prepare_next_part(first_chunk.length());
    data_ofs = obj_len;
    return 0;
  }
  off_t write_ofs = data_ofs;
  data_ofs = write_ofs + bl.length();
  return write_data(bl, write_ofs, phandle);
}

int RGWPutObjProcessor_Atomic::prepare(RGWRados *store, void *obj_ctx)
{
  RGWPutObjProcessor::prepare(store, obj_ctx);

  head_obj.init(bucket, obj_str);

  char buf[33];
  gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
  oid_prefix.append("_");
  oid_prefix.append(buf);
  oid_prefix.append("_");

  return 0;
}

void RGWPutObjProcessor_Atomic::prepare_next_part(off_t ofs) {
  int num_parts = manifest.objs.size();
  RGWObjManifestPart *part;

  /* first update manifest for written data */
  if (!num_parts) {
    part = &manifest.objs[cur_part_ofs];
    part->loc = head_obj;
  } else {
    part = &manifest.objs[cur_part_ofs];
    part->loc = cur_obj;
  }
  part->loc_ofs = 0;
  part->size = ofs - cur_part_ofs;

  if ((uint64_t)ofs > manifest.obj_size)
    manifest.obj_size = ofs;

  /* now update params for next part */

  cur_part_ofs = ofs;
  next_part_ofs = cur_part_ofs + part_size;
  char buf[16];

  cur_part_id++;
  snprintf(buf, sizeof(buf), "%d", cur_part_id);
  string cur_oid = oid_prefix;
  cur_oid.append(buf);
  cur_obj.init_ns(bucket, cur_oid, shadow_ns);

  add_obj(cur_obj);
};

void RGWPutObjProcessor_Atomic::complete_parts()
{
  if (obj_len > (uint64_t)cur_part_ofs)
    prepare_next_part(obj_len);
}

int RGWPutObjProcessor_Atomic::complete_writing_data()
{
  if (!data_ofs && !immutable_head()) {
    first_chunk.claim(pending_data_bl);
    obj_len = (uint64_t)first_chunk.length();
  }
  if (pending_data_bl.length()) {
    void *handle;
    int r = write_data(pending_data_bl, data_ofs, &handle);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: write_data() returned " << r << dendl;
      return r;
    }
    r = throttle_data(handle);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: throttle_data() returned " << r << dendl;
      return r;
    }
  }
  complete_parts();

  int r = drain_pending();
  if (r < 0)
    return r;

  return 0;
}

int RGWPutObjProcessor_Atomic::do_complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs) {
  int r = complete_writing_data();
  if (r < 0)
    return r;

  store->set_atomic(obj_ctx, head_obj);

  RGWRados::PutObjMetaExtraParams extra_params;

  extra_params.data = &first_chunk;
  extra_params.manifest = &manifest;
  extra_params.ptag = &unique_tag; /* use req_id as operation tag */
  extra_params.mtime = mtime;
  extra_params.set_mtime = set_mtime;

  r = store->put_obj_meta(obj_ctx, head_obj, obj_len, attrs,
                          RGW_OBJ_CATEGORY_MAIN, PUT_OBJ_CREATE,
                          extra_params);
  return r;
}

class RGWWatcher : public librados::WatchCtx {
  RGWRados *rados;
public:
  RGWWatcher(RGWRados *r) : rados(r) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    ldout(rados->ctx(), 10) << "RGWWatcher::notify() opcode=" << (int)opcode << " ver=" << ver << " bl.length()=" << bl.length() << dendl;
    rados->watch_cb(opcode, ver, bl);
  }
};

RGWObjState *RGWRadosCtx::get_state(rgw_obj& obj) {
  if (obj.object.size()) {
    return &objs_state[obj];
  } else {
    rgw_obj new_obj(store->zone.domain_root, obj.bucket.name);
    return &objs_state[new_obj];
  }
}

void RGWRadosCtx::set_atomic(rgw_obj& obj) {
  if (obj.object.size()) {
    objs_state[obj].is_atomic = true;
  } else {
    rgw_obj new_obj(store->zone.domain_root, obj.bucket.name);
    objs_state[new_obj].is_atomic = true;
  }
}

void RGWRadosCtx::set_prefetch_data(rgw_obj& obj) {
  if (obj.object.size()) {
    objs_state[obj].prefetch_data = true;
  } else {
    rgw_obj new_obj(store->zone.domain_root, obj.bucket.name);
    objs_state[new_obj].prefetch_data = true;
  }
}

void RGWRados::finalize()
{
  if (need_watch_notify()) {
    finalize_watch();
  }
  delete meta_mgr;
  delete data_log;
  if (use_gc_thread) {
    gc->stop_processor();
    delete gc;
    gc = NULL;
  }
  delete rest_master_conn;

  map<string, RGWRESTConn *>::iterator iter;
  for (iter = zone_conn_map.begin(); iter != zone_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }

  for (iter = region_conn_map.begin(); iter != region_conn_map.end(); ++iter) {
    RGWRESTConn *conn = iter->second;
    delete conn;
  }
  RGWQuotaHandler::free_handler(quota_handler);
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_rados()
{
  int ret;

  rados = new Rados();
  if (!rados)
    return -ENOMEM;

  ret = rados->init_with_context(cct);
  if (ret < 0)
   return ret;

  ret = rados->connect();
  if (ret < 0)
   return ret;

  meta_mgr = new RGWMetadataManager(cct, this);
  data_log = new RGWDataChangesLog(cct, this);

  return ret;
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::init_complete()
{
  int ret;

  ret = region.init(cct, this);
  if (ret < 0)
    return ret;

  ret = zone.init(cct, this, region);
  if (ret < 0)
    return ret;

  ret = region_map.read(cct, this);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldout(cct, 0) << "WARNING: cannot read region map" << dendl;
    }
    ret = region_map.update(region);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to update regionmap with local region info" << dendl;
      return -EIO;
    }
  } else {
    string master_region = region_map.master_region;
    if (master_region.empty()) {
      lderr(cct) << "ERROR: region map does not specify master region" << dendl;
      return -EINVAL;
    }
    map<string, RGWRegion>::iterator iter = region_map.regions.find(master_region);
    if (iter == region_map.regions.end()) {
      lderr(cct) << "ERROR: bad region map: inconsistent master region" << dendl;
      return -EINVAL;
    }
    RGWRegion& region = iter->second;
    rest_master_conn = new RGWRESTConn(cct, this, region.endpoints);

    for (iter = region_map.regions.begin(); iter != region_map.regions.end(); ++iter) {
      RGWRegion& region = iter->second;

      region_conn_map[region.name] = new RGWRESTConn(cct, this, region.endpoints);
    }
  }

  if (need_watch_notify()) {
    ret = init_watch();
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to initialize watch" << dendl;
      return ret;
    }
  }

  map<string, RGWZone>::iterator ziter;
  for (ziter = region.zones.begin(); ziter != region.zones.end(); ++ziter) {
    const string& name = ziter->first;
    RGWZone& z = ziter->second;
    if (name != zone.name) {
      ldout(cct, 20) << "generating connection object for zone " << name << dendl;
      zone_conn_map[name] = new RGWRESTConn(cct, this, z.endpoints);
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

  pools_initialized = true;

  gc = new RGWGC();
  gc->initialize(cct, this);

  if (use_gc_thread)
    gc->start_processor();

  quota_handler = RGWQuotaHandler::generate_handler(this);

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
    string& notify_oid = notify_oids[i];
    if (notify_oid.empty())
      continue;
    uint64_t watch_handle = watch_handles[i];
    control_pool_ctx.unwatch(notify_oid, watch_handle);

    RGWWatcher *watcher = watchers[i];
    delete watcher;
  }

  delete[] notify_oids;
  delete[] watch_handles;
  delete[] watchers;
}

int RGWRados::list_raw_prefixed_objs(string pool_name, const string& prefix, list<string>& result)
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
  string pool_name;
  int ret = RGWRegion::get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  return list_raw_prefixed_objs(pool_name, region_info_oid_prefix, regions);
}

int RGWRados::list_zones(list<string>& zones)
{
  string pool_name;
  int ret = RGWZoneParams::get_pool_name(cct, &pool_name);
  if (ret < 0)
    return ret;

  return list_raw_prefixed_objs(pool_name, zone_info_oid_prefix, zones);
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx()
{
  const string& pool = zone.domain_root.name;
  const char *pool_str = pool.c_str();
  int r = rados->ioctx_create(pool_str, root_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(pool_str);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(pool_str, root_pool_ctx);
  }

  return r;
}

int RGWRados::open_gc_pool_ctx()
{
  const char *gc_pool = zone.gc_pool.name.c_str();
  int r = rados->ioctx_create(gc_pool, gc_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(gc_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(gc_pool, gc_pool_ctx);
  }

  return r;
}

int RGWRados::init_watch()
{
  const char *control_pool = zone.control_pool.name.c_str();
  int r = rados->ioctx_create(control_pool, control_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(control_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(control_pool, control_pool_ctx);
    if (r < 0)
      return r;
  }

  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  notify_oids = new string[num_watchers];
  watchers = new RGWWatcher *[num_watchers];
  watch_handles = new uint64_t[num_watchers];

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

    RGWWatcher *watcher = new RGWWatcher(this);
    watchers[i] = watcher;

    r = control_pool_ctx.watch(notify_oid, 0, &watch_handles[i], watcher);
    if (r < 0)
      return r;
  }

  watch_initialized = true;

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

int RGWRados::open_bucket_pool_ctx(const string& bucket_name, const string& pool, librados::IoCtx&  io_ctx)
{
  int r = rados->ioctx_create(pool.c_str(), io_ctx);
  if (r != -ENOENT)
    return r;

  if (!pools_initialized)
    return r;

  r = rados->pool_create(pool.c_str());
  if (r < 0 && r != -EEXIST)
    return r;

  r = rados->ioctx_create(pool.c_str(), io_ctx);

  return r;
}

int RGWRados::open_bucket_data_ctx(rgw_bucket& bucket, librados::IoCtx& data_ctx)
{
  int r = open_bucket_pool_ctx(bucket.name, bucket.data_pool, data_ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::open_bucket_index_ctx(rgw_bucket& bucket, librados::IoCtx& index_ctx)
{
  int r = open_bucket_pool_ctx(bucket.name, bucket.index_pool, index_ctx);
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
  librados::ObjectIterator *state = new librados::ObjectIterator(root_pool_ctx.objects_begin());
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
  librados::ObjectIterator *state = (librados::ObjectIterator *)*handle;

  do {
    if (*state == root_pool_ctx.objects_end()) {
      delete state;
      return -ENOENT;
    }

    obj.name = (*state)->first;
    (*state)++;
  } while (obj.name[0] == '.'); /* skip all entries starting with '.' */

  return 0;
}


/**** logs ****/

struct log_list_state {
  string prefix;
  librados::IoCtx io_ctx;
  librados::ObjectIterator obit;
};

int RGWRados::log_list_init(const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->prefix = prefix;
  state->obit = state->io_ctx.objects_begin();
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_list_next(RGWAccessHandle handle, string *name)
{
  log_list_state *state = static_cast<log_list_state *>(handle);
  while (true) {
    if (state->obit == state->io_ctx.objects_end()) {
      delete state;
      return -ENOENT;
    }
    if (state->prefix.length() &&
	state->obit->first.find(state->prefix) != 0) {
      state->obit++;
      continue;
    }
    *name = state->obit->first;
    state->obit++;
    break;
  }
  return 0;
}

int RGWRados::log_remove(const string& name)
{
  librados::IoCtx io_ctx;
  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
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
  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, state->io_ctx);
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

int RGWRados::read_usage(string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                         bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  uint32_t num = max_entries;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, 0);

  if (usage_iter.index) {
    usage_log_hash(cct, user, hash, usage_iter.index);
  } else {
    hash = first_hash;
  }

  usage.clear();

  do {
    map<rgw_user_bucket, rgw_usage_log_entry> ret_usage;
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;

    int ret =  cls_obj_usage_log_read(hash, user, start_epoch, end_epoch, num,
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
      usage_log_hash(cct, user, hash, ++usage_iter.index);
    }
  } while (num && !*is_truncated && hash != first_hash);
  return 0;
}

int RGWRados::trim_usage(string& user, uint64_t start_epoch, uint64_t end_epoch)
{
  uint32_t index = 0;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, index);

  hash = first_hash;

  do {
    int ret =  cls_obj_usage_log_trim(hash, user, start_epoch, end_epoch);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

next:
    usage_log_hash(cct, user, hash, ++index);
  } while (hash != first_hash);

  return 0;
}

void RGWRados::shard_name(const string& prefix, unsigned max_shards, const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  char buf[16];
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

void RGWRados::time_log_prepare_entry(cls_log_entry& entry, const utime_t& ut, string& section, string& key, bufferlist& bl)
{
  cls_log_add_prepare_entry(entry, ut, section, key, bl);
}

int RGWRados::time_log_add(const string& oid, const utime_t& ut, const string& section, const string& key, bufferlist& bl)
{
  librados::IoCtx io_ctx;

  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rados->ioctx_create(log_pool, io_ctx);
  }
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_log_add(op, ut, section, key, bl);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::time_log_add(const string& oid, list<cls_log_entry>& entries)
{
  librados::IoCtx io_ctx;

  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rados->ioctx_create(log_pool, io_ctx);
  }
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_log_add(op, entries);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::time_log_list(const string& oid, utime_t& start_time, utime_t& end_time,
                            int max_entries, list<cls_log_entry>& entries,
			    const string& marker,
			    string *out_marker,
			    bool *truncated)
{
  librados::IoCtx io_ctx;

  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
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

  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
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

int RGWRados::time_log_trim(const string& oid, const utime_t& start_time, const utime_t& end_time,
			    const string& from_marker, const string& to_marker)
{
  librados::IoCtx io_ctx;

  const char *log_pool = zone.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;

  return cls_log_trim(io_ctx, oid, start_time, end_time, from_marker, to_marker);
}


int RGWRados::lock_exclusive(rgw_bucket& pool, const string& oid, utime_t& duration, 
                             string& zone_id, string& owner_id) {
  librados::IoCtx io_ctx;

  const char *pool_name = pool.name.c_str();
  
  int r = rados->ioctx_create(pool_name, io_ctx);
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

  int r = rados->ioctx_create(pool_name, io_ctx);
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
    ldout(cct, 15) << "Read AccessControlPolicy";
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
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed
 *     here.
 */
int RGWRados::list_objects(rgw_bucket& bucket, int max, string& prefix, string& delim,
			   string& marker, vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
			   bool get_content_type, string& ns, bool enforce_ns,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  int count = 0;
  bool truncated;

  if (bucket_is_system(bucket)) {
    return -EINVAL;
  }
  result.clear();

  rgw_obj marker_obj, prefix_obj;
  marker_obj.set_ns(ns);
  marker_obj.set_obj(marker);
  string cur_marker = marker_obj.object;

  prefix_obj.set_ns(ns);
  prefix_obj.set_obj(prefix);
  string cur_prefix = prefix_obj.object;

  do {
    std::map<string, RGWObjEnt> ent_map;
    int r = cls_bucket_list(bucket, cur_marker, cur_prefix, max - count, ent_map,
                            &truncated, &cur_marker);
    if (r < 0)
      return r;

    std::map<string, RGWObjEnt>::iterator eiter;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      string obj = eiter->first;
      string key = obj;

      bool check_ns = rgw_obj::translate_raw_obj_to_obj_in_ns(obj, ns);

      if (enforce_ns && !check_ns) {
        if (!ns.empty()) {
          /* we've iterated past the namespace we're searching -- done now */
          truncated = false;
          goto done;
        }

        /* we're not looking at the namespace this object is in, next! */
        continue;
      }

      if (filter && !filter->filter(obj, key))
        continue;

      if (prefix.size() &&  ((obj).compare(0, prefix.size(), prefix) != 0))
        continue;

      if (!delim.empty()) {
        int delim_pos = obj.find(delim, prefix.size());

        if (delim_pos >= 0) {
          common_prefixes[obj.substr(0, delim_pos + 1)] = true;
          continue;
        }
      }

      RGWObjEnt ent = eiter->second;
      ent.name = obj;
      ent.ns = ns;
      result.push_back(ent);
      count++;
    }
  } while (truncated && count < max);

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

  ret = rados->pool_create(pool.c_str(), 0);
  if (ret == -EEXIST)
    ret = 0;
  if (ret < 0)
    return ret;

  if (bucket.data_pool != pool) {
    ret = rados->pool_create(bucket.data_pool.c_str(), 0);
    if (ret == -EEXIST)
      ret = 0;
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWRados::init_bucket_index(rgw_bucket& bucket)
{
  librados::IoCtx index_ctx; // context for new bucket

  int r = open_bucket_index_ctx(bucket, index_ctx);
  if (r < 0)
    return r;

  string dir_oid =  dir_oid_prefix;
  dir_oid.append(bucket.marker);

  librados::ObjectWriteOperation op;
  op.create(true);
  r = cls_rgw_init_index(index_ctx, op, dir_oid);
  if (r < 0 && r != -EEXIST)
    return r;

  return 0;
}

/**
 * create a bucket with name bucket and the given list of attrs
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(RGWUserInfo& owner, rgw_bucket& bucket,
                            const string& region_name,
                            const string& placement_rule,
			    map<std::string, bufferlist>& attrs,
                            RGWBucketInfo& info,
                            obj_version *pobjv,
                            obj_version *pep_objv,
                            time_t creation_time,
                            rgw_bucket *pmaster_bucket,
			    bool exclusive)
{
#define MAX_CREATE_RETRIES 20 /* need to bound retries */
  string selected_placement_rule;
  for (int i = 0; i < MAX_CREATE_RETRIES; i++) {
    int ret = 0;
    ret = select_bucket_placement(owner, region_name, placement_rule, bucket.name, bucket, &selected_placement_rule);
    if (ret < 0)
      return ret;
    bufferlist bl;
    uint32_t nop = 0;
    ::encode(nop, bl);

    const string& pool = zone.domain_root.name;
    const char *pool_str = pool.c_str();
    librados::IoCtx id_io_ctx;
    int r = rados->ioctx_create(pool_str, id_io_ctx);
    if (r < 0)
      return r;

    if (!pmaster_bucket) {
      uint64_t iid = instance_id();
      uint64_t bid = next_bucket_id();
      char buf[32];
      snprintf(buf, sizeof(buf), "%s.%llu.%llu", zone.name.c_str(), (long long)iid, (long long)bid);
      bucket.marker = buf;
      bucket.bucket_id = bucket.marker;
    } else {
      bucket.marker = pmaster_bucket->marker;
      bucket.bucket_id = pmaster_bucket->bucket_id;
    }

    string dir_oid =  dir_oid_prefix;
    dir_oid.append(bucket.marker);

    r = init_bucket_index(bucket);
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
    info.region = region_name;
    info.placement_rule = selected_placement_rule;
    if (!creation_time)
      time(&info.creation_time);
    else
      info.creation_time = creation_time;
    ret = put_linked_bucket_info(info, exclusive, 0, pep_objv, &attrs, true);
    if (ret == -EEXIST) {
       /* we need to reread the info and return it, caller will have a use for it */
      r = get_bucket_info(NULL, bucket.name, info, NULL, NULL);
      if (r < 0) {
        if (r == -ENOENT) {
          continue;
        }
        ldout(cct, 0) << "get_bucket_info returned " << r << dendl;
        return r;
      }

      /* only remove it if it's a different bucket instance */
      if (info.bucket.bucket_id != bucket.bucket_id) {
        /* remove bucket meta instance */
        string entry;
        get_bucket_instance_entry(bucket, entry);
        r = rgw_bucket_instance_remove_entry(this, entry, &info.objv_tracker);
        if (r < 0)
          return r;

        /* remove bucket index */
        librados::IoCtx index_ctx; // context for new bucket
        int r = open_bucket_index_ctx(bucket, index_ctx);
        if (r < 0)
          return r;

        index_ctx.remove(dir_oid);
      }
      /* ret == -ENOENT here */
    }
    return ret;
  }

  /* this is highly unlikely */
  ldout(cct, 0) << "ERROR: could not create bucket, continuously raced with bucket creation and removal" << dendl;
  return -ENOENT;
}

int RGWRados::select_new_bucket_location(RGWUserInfo& user_info, const string& region_name, const string& request_rule,
                                         const string& bucket_name, rgw_bucket& bucket, string *pselected_rule)
{
  /* first check that rule exists within the specific region */
  map<string, RGWRegion>::iterator riter = region_map.regions.find(region_name);
  if (riter == region_map.regions.end()) {
    ldout(cct, 0) << "could not find region " << region_name << " in region map" << dendl;
    return -EINVAL;
  }
  /* now check that tag exists within region */
  RGWRegion& region = riter->second;

  /* find placement rule. Hierarchy: request rule > user default rule > region default rule */
  string rule = request_rule;
  if (rule.empty()) {
    rule = user_info.default_placement;
    if (rule.empty())
      rule = region.default_placement;
  }

  if (rule.empty()) {
    ldout(cct, 0) << "misconfiguration, should not have an empty placement rule name" << dendl;
    return -EIO;
  }

  if (!rule.empty()) {
    map<string, RGWRegionPlacementTarget>::iterator titer = region.placement_targets.find(rule);
    if (titer == region.placement_targets.end()) {
      ldout(cct, 0) << "could not find placement rule " << rule << " within region " << dendl;
      return -EINVAL;
    }

    /* now check tag for the rule, whether user is permitted to use rule */
    RGWRegionPlacementTarget& target_rule = titer->second;
    if (!target_rule.user_permitted(user_info.placement_tags)) {
      ldout(cct, 0) << "user not permitted to use placement rule" << dendl;
      return -EPERM;
    }
  }

  if (pselected_rule)
    *pselected_rule = rule;
  
  return set_bucket_location_by_rule(rule, bucket_name, bucket);
}

int RGWRados::set_bucket_location_by_rule(const string& location_rule, const std::string& bucket_name, rgw_bucket& bucket)
{
  bucket.name = bucket_name;

  if (location_rule.empty()) {
    /* we can only reach here if we're trying to set a bucket location from a bucket
     * created on a different zone, using a legacy / default pool configuration
     */
    return select_legacy_bucket_placement(bucket_name, bucket);
  }

  /*
   * make sure that zone has this rule configured. We're
   * checking it for the local zone, because that's where this bucket object is going to
   * reside.
   */
  map<string, RGWZonePlacementInfo>::iterator piter = zone.placement_pools.find(location_rule);
  if (piter == zone.placement_pools.end()) {
    /* couldn't find, means we cannot really place data for this bucket in this zone */
    if (region.equals(region_name)) {
      /* that's a configuration error, zone should have that rule, as we're within the requested
       * region */
      return -EINVAL;
    } else {
      /* oh, well, data is not going to be placed here, bucket object is just a placeholder */
      return 0;
    }
  }

  RGWZonePlacementInfo& placement_info = piter->second;

  bucket.data_pool = placement_info.data_pool;
  bucket.index_pool = placement_info.index_pool;

  return 0;

}

int RGWRados::select_bucket_placement(RGWUserInfo& user_info, const string& region_name, const string& placement_rule,
                                      const string& bucket_name, rgw_bucket& bucket, string *pselected_rule)
{
  if (!zone.placement_pools.empty()) {
    return select_new_bucket_location(user_info, region_name, placement_rule, bucket_name, bucket, pselected_rule);
  }

  if (pselected_rule)
    pselected_rule->clear();

  return select_legacy_bucket_placement(bucket_name, bucket);
}

int RGWRados::select_legacy_bucket_placement(const string& bucket_name, rgw_bucket& bucket)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string pool_name;
  bool write_map = false;

  rgw_obj obj(zone.domain_root, avail_pools);

  int ret = rgw_get_system_obj(this, NULL, zone.domain_root, avail_pools, map_bl, NULL, NULL);
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
    names.push_back(default_storage_pool);
    vector<int> retcodes;
    bufferlist bl;
    ret = create_pools(names, retcodes);
    if (ret < 0)
      return ret;
    ret = omap_set(obj, default_storage_pool, bl);
    if (ret < 0)
      return ret;
    m[default_storage_pool] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    ::encode(m, new_bl);
    ret = put_obj_data(NULL, obj, new_bl.c_str(), -1, new_bl.length(), false);
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
  bucket.name = bucket_name;

  return 0;

}

int RGWRados::update_placement_map()
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_obj obj(zone.domain_root, avail_pools);
  int ret = omap_get_all(obj, header, m);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  ::encode(m, new_bl);
  ret = put_obj_data(NULL, obj, new_bl.c_str(), -1, new_bl.length(), false);
  if (ret < 0) {
    ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWRados::add_bucket_placement(std::string& new_pool)
{
  int ret = rados->pool_lookup(new_pool.c_str());
  if (ret < 0) // DNE, or something
    return ret;

  rgw_obj obj(zone.domain_root, avail_pools);
  bufferlist empty_bl;
  ret = omap_set(obj, new_pool, empty_bl);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::remove_bucket_placement(std::string& old_pool)
{
  rgw_obj obj(zone.domain_root, avail_pools);
  int ret = omap_del(obj, old_pool);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::list_placement_set(set<string>& names)
{
  bufferlist header;
  map<string, bufferlist> m;

  rgw_obj obj(zone.domain_root, avail_pools);
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

  for (iter = names.begin(); iter != names.end(); ++iter) {
    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    string& name = *iter;
    int ret = rados->pool_create_async(name.c_str(), c);
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
int RGWRados::put_obj_meta_impl(void *ctx, rgw_obj& obj,  uint64_t size,
                  time_t *mtime, map<string, bufferlist>& attrs,
                  RGWObjCategory category, int flags,
                  map<string, bufferlist>* rmattrs,
                  const bufferlist *data,
                  RGWObjManifest *manifest,
		  const string *ptag,
                  list<string> *remove_objs,
                  bool modify_version,
                  RGWObjVersionTracker *objv_tracker,
                  time_t set_mtime)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;

  RGWObjState *state = NULL;

  if (flags & PUT_OBJ_EXCL) {
    if (!(flags & PUT_OBJ_CREATE))
	return -EINVAL;
    op.create(true); // exclusive create
  } else {
    bool reset_obj = (flags & PUT_OBJ_CREATE) != 0;
    r = prepare_atomic_for_write(rctx, obj, op, &state, reset_obj, ptag);
    if (r < 0)
      return r;
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

  if (data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    op.write_full(*data);
  }

  string etag;
  string content_type;
  bufferlist acl_bl;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  if (manifest) {
    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    ::encode(*manifest, bl);
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

  if (!op.size())
    return 0;

  string index_tag;
  uint64_t epoch;
  int64_t poolid;

  if (state) {
    index_tag = state->write_tag;
  }

  r = prepare_update_index(NULL, bucket, CLS_RGW_OP_ADD, obj, index_tag);
  if (r < 0)
    return r;

  r = io_ctx.operate(oid, &op);
  if (r < 0)
    goto done_cancel;

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  epoch = io_ctx.get_last_version();
  poolid = io_ctx.get_id();

  r = complete_atomic_overwrite(rctx, state, obj);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: complete_atomic_overwrite returned r=" << r << dendl;
  }

  r = complete_update_index(bucket, obj.object, index_tag, poolid, epoch, size,
                            ut, etag, content_type, &acl_bl, category, remove_objs);
  if (r < 0)
    goto done_cancel;

  if (mtime) {
    *mtime = set_mtime;
  }

  if (state) {
    /* update quota cache */
    quota_handler->update_stats(bucket, (state->exists ? 0 : 1), size, state->size);
  }

  return 0;

done_cancel:
  int ret = complete_update_index_cancel(bucket, obj.object, index_tag);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: complete_update_index_cancel() returned ret=" << ret << dendl;
  }
  /* we lost in a race, object was already overwritten, we
   * should treat it as a success
   */
  if (r == -ECANCELED)
    r = 0;
  return r;
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
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;

  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

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
  r = io_ctx.aio_operate(oid, c, &op);
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
    progress_cb(ofs, progress_data);

    void *handle;
    int ret = processor->handle_data(bl, ofs, &handle);
    if (ret < 0)
      return ret;

    if (opstate) {
      /* need to update opstate repository with new state. This is ratelimited, so we're not
       * really doing it every time
       */
      ret = opstate->renew_state();
      if (ret < 0) {
        /* could not renew state! might have been marked as cancelled */
        return ret;
      }
    }

    ret = processor->throttle_data(handle);
    if (ret < 0)
      return ret;

    return 0;
  }

  void set_extra_data_len(uint64_t len) {
    RGWGetDataCB::set_extra_data_len(len);
    processor->set_extra_data_len(len);
  }

  int complete(string& etag, time_t *mtime, time_t set_mtime, map<string, bufferlist>& attrs) {
    return processor->complete(etag, mtime, set_mtime, attrs);
  }
};

/*
 * prepare attrset, either replace it with new attrs, or keep it (other than acls).
 */
static void set_copy_attrs(map<string, bufferlist>& src_attrs, map<string, bufferlist>& attrs, bool replace_attrs, bool intra_region)
{
  if (replace_attrs) {
    if (!attrs[RGW_ATTR_ETAG].length())
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];

    src_attrs = attrs;
  } else {
    /* copying attrs from source, however acls should only be copied if it's intra-region operation */
    if (!intra_region)
      src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  }
}

class GetObjHandleDestructor {
  RGWRados *store;
  void **handle;

public:
    GetObjHandleDestructor(RGWRados *_store) : store(_store), handle(NULL) {}
    ~GetObjHandleDestructor() {
      if (handle) {
        store->finish_get_obj(handle);
      }
    }
    void set_handle(void **_h) {
      handle = _h;
    }
};

/**
 * Copy an object.
 * dest_obj: the object to copy into
 * src_obj: the object to copy from
 * attrs: if replace_attrs is set then these are placed on the new object
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(void *ctx,
               const string& user_id,
               const string& client_id,
               const string& op_id,
               req_info *info,
               const string& source_zone,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               RGWBucketInfo& dest_bucket_info,
               RGWBucketInfo& src_bucket_info,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               bool replace_attrs,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               string *ptag,
               struct rgw_err *err,
               void (*progress_cb)(off_t, void *),
               void *progress_data)
{
  int ret;
  uint64_t total_len, obj_size;
  time_t lastmod;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  bool remote_src;
  bool remote_dest;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  remote_dest = !region.equals(dest_bucket_info.region);
  remote_src = !region.equals(src_bucket_info.region);

  if (remote_src && remote_dest) {
    ldout(cct, 0) << "ERROR: can't copy object when both src and dest buckets are remote" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.object << " => " << dest_obj.bucket << ":" << dest_obj.object << dendl;

  void *handle = NULL;
  GetObjHandleDestructor handle_destructor(this);

  map<string, bufferlist> src_attrs;
  off_t ofs = 0;
  off_t end = -1;
  if (!remote_src && source_zone.empty()) {
    ret = prepare_get_obj(ctx, src_obj, &ofs, &end, &src_attrs,
                  mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &obj_size, NULL, &handle, err);
    if (ret < 0)
      return ret;

    handle_destructor.set_handle(&handle);
  } else {
    /* source is in a different region, copy it there */

    RGWRESTStreamReadRequest *in_stream_req;
    string tag;
    append_rand_alpha(cct, tag, tag, 32);

    RGWPutObjProcessor_Atomic processor(dest_obj.bucket, dest_obj.object,
                                        cct->_conf->rgw_obj_stripe_size, tag);
    ret = processor.prepare(this, ctx);
    if (ret < 0)
      return ret;

    RGWRESTConn *conn;
    if (source_zone.empty()) {
      if (dest_bucket_info.region.empty()) {
        /* source is in the master region */
        conn = rest_master_conn;
      } else {
        map<string, RGWRESTConn *>::iterator iter = region_conn_map.find(src_bucket_info.region);
        if (iter == region_conn_map.end()) {
          ldout(cct, 0) << "could not find region connection to region: " << source_zone << dendl;
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

    string obj_name = dest_obj.bucket.name + "/" + dest_obj.object;

    RGWOpStateSingleOp opstate(this, client_id, op_id, obj_name);

    int ret = opstate.set_state(RGWOpState::OPSTATE_IN_PROGRESS);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
      return ret;
    }
    RGWRadosPutObj cb(&processor, &opstate, progress_cb, progress_data);
    string etag;
    map<string, string> req_headers;
    time_t set_mtime;
   
    ret = conn->get_obj(user_id, info, src_obj, true, &cb, &in_stream_req);
    if (ret < 0)
      goto set_err_state;

    ret = conn->complete_request(in_stream_req, etag, &set_mtime, req_headers);
    if (ret < 0)
      goto set_err_state;

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
      }
    }

    set_copy_attrs(src_attrs, attrs, replace_attrs, !source_zone.empty());

    ret = cb.complete(etag, mtime, set_mtime, src_attrs);
    if (ret < 0)
      goto set_err_state;

    ret = opstate.set_state(RGWOpState::OPSTATE_COMPLETE);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate ret=" << ret << dendl;
    }

    return 0;
set_err_state:
    int r = opstate.set_state(RGWOpState::OPSTATE_ERROR);
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to set opstate r=" << ret << dendl;
    }
    return ret;
  }
  set_copy_attrs(src_attrs, attrs, replace_attrs, false);
  src_attrs.erase(RGW_ATTR_ID_TAG);

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  ret = get_obj_state(rctx, src_obj, &astate, NULL);
  if (ret < 0)
    return ret;

  vector<rgw_obj> ref_objs;

  bool copy_data = !astate->has_manifest;
  bool copy_first = false;
  if (astate->has_manifest) {
    if (astate->manifest.objs.size() < 2) {
      copy_data = true;
    } else {
      map<uint64_t, RGWObjManifestPart>::iterator iter = astate->manifest.objs.begin();
      RGWObjManifestPart part = iter->second;
      if (part.loc == src_obj) {
	if (part.size > RGW_MAX_CHUNK_SIZE)  // should never happen
	  copy_data = true;
	else
          copy_first = true;
      }
    }
  }


  if (remote_dest) {
    /* dest is in a different region, copy it there */

    string etag;

    RGWRESTStreamWriteRequest *out_stream_req;

    int ret = rest_master_conn->put_obj_init(user_id, dest_obj, astate->size, src_attrs, &out_stream_req);
    if (ret < 0)
      return ret;

    ret = get_obj_iterate(ctx, &handle, src_obj, 0, astate->size - 1, out_stream_req->get_out_cb());
    if (ret < 0)
      return ret;

    ret = rest_master_conn->complete_request(out_stream_req, etag, mtime);
    if (ret < 0)
      return ret;

    return 0;
  } else if (copy_data) { /* refcounting tail wouldn't work here, just copy the data */
    return copy_obj_data(ctx, &handle, end, dest_obj, src_obj, mtime, src_attrs, category, ptag, err);
  }

  map<uint64_t, RGWObjManifestPart>::iterator miter = astate->manifest.objs.begin();

  if (copy_first) // we need to copy first chunk, not increase refcount
    ++miter;

  RGWObjManifestPart *first_part = &miter->second;
  string oid, key;
  rgw_bucket bucket;
  get_obj_bucket_and_oid_key(first_part->loc, bucket, oid, key);
  librados::IoCtx io_ctx;
  PutObjMetaExtraParams ep;

  ret = open_bucket_data_ctx(bucket, io_ctx);
  if (ret < 0)
    return ret;

  bufferlist first_chunk;

  bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest; 
  ldout(cct, 0) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;


  string tag;

  if (ptag)
    tag = *ptag;

  if (tag.empty()) {
    append_rand_alpha(cct, tag, tag, 32);
  }

  if (!copy_itself) {
    for (; miter != astate->manifest.objs.end(); ++miter) {
      RGWObjManifestPart& part = miter->second;
      ObjectWriteOperation op;
      manifest.objs[miter->first] = part;
      cls_refcount_get(op, tag, true);

      get_obj_bucket_and_oid_key(part.loc, bucket, oid, key);
      io_ctx.locator_set_key(key);

      ret = io_ctx.operate(oid, &op);
      if (ret < 0)
        goto done_ret;

      ref_objs.push_back(part.loc);
    }
    manifest.obj_size = total_len;

    pmanifest = &manifest;
  } else {
    pmanifest = &astate->manifest;
    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = get_obj(ctx, NULL, &handle, src_obj, first_chunk, 0, RGW_MAX_CHUNK_SIZE);
    if (ret < 0)
      goto done_ret;

    first_part = &pmanifest->objs[0];
    first_part->loc = dest_obj;
    first_part->loc_ofs = 0;
    first_part->size = first_chunk.length();
  }

  ep.data = &first_chunk;
  ep.manifest = pmanifest;
  ep.ptag = &tag;

  ret = put_obj_meta(ctx, dest_obj, end + 1, src_attrs, category, PUT_OBJ_CREATE, ep);

  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL, NULL);

  return 0;

done_ret:
  if (!copy_itself) {
    vector<rgw_obj>::iterator riter;

    /* rollback reference */
    for (riter = ref_objs.begin(); riter != ref_objs.end(); ++riter) {
      ObjectWriteOperation op;
      cls_refcount_put(op, tag, true);

      get_obj_bucket_and_oid_key(*riter, bucket, oid, key);
      io_ctx.locator_set_key(key);

      int r = io_ctx.operate(oid, &op);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << *riter << dendl;
      }
    }
  }
  return ret;
}


int RGWRados::copy_obj_data(void *ctx,
	       void **handle, off_t end,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
	       time_t *mtime,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               string *ptag,
               struct rgw_err *err)
{
  bufferlist first_chunk;
  RGWObjManifest manifest;
  RGWObjManifestPart *first_part;
  map<string, bufferlist>::iterator iter;

  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  int ret, r;
  off_t ofs = 0;
  PutObjMetaExtraParams ep;

  do {
    bufferlist bl;
    ret = get_obj(ctx, NULL, handle, src_obj, bl, ofs, end);
    if (ret < 0)
      return ret;

    const char *data = bl.c_str();

    if (ofs < RGW_MAX_CHUNK_SIZE) {
      off_t len = min(RGW_MAX_CHUNK_SIZE - ofs, (off_t)ret);
      first_chunk.append(data, len);
      ofs += len;
      ret -= len;
      data += len;
    }

    // In the first call to put_obj_data, we pass ofs == -1 so that it will do
    // a write_full, wiping out whatever was in the object before this
    r = 0;
    if (ret > 0) {
      r = put_obj_data(ctx, shadow_obj, data, ((ofs == 0) ? -1 : ofs), ret, false);
    }
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  first_part = &manifest.objs[0];
  first_part->loc = dest_obj;
  first_part->loc_ofs = 0;
  first_part->size = first_chunk.length();

  if (ofs > RGW_MAX_CHUNK_SIZE) {
    RGWObjManifestPart& tail = manifest.objs[RGW_MAX_CHUNK_SIZE];
    tail.loc = shadow_obj;
    tail.loc_ofs = RGW_MAX_CHUNK_SIZE;
    tail.size = ofs - RGW_MAX_CHUNK_SIZE;
  }
  manifest.obj_size = ofs;

  ep.data = &first_chunk;
  ep.manifest = &manifest;
  ep.ptag = ptag;

  ret = put_obj_meta(ctx, dest_obj, end + 1, attrs, category, PUT_OBJ_CREATE, ep);
  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL, NULL);

  return ret;
done_err:
  delete_obj(ctx, shadow_obj);
  return r;
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
  string marker, prefix;
  bool is_truncated;

  do {
#define NUM_ENTRIES 1000
    r = cls_bucket_list(bucket, marker, prefix, NUM_ENTRIES, ent_map,
                        &is_truncated, &marker);
    if (r < 0)
      return r;

    string ns;
    std::map<string, RGWObjEnt>::iterator eiter;
    string obj;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      obj = eiter->first;

      if (rgw_obj::translate_raw_obj_to_obj_in_ns(obj, ns))
        return -ENOTEMPTY;
    }
  } while (is_truncated);

  r = rgw_bucket_delete_bucket_obj(this, bucket.name, objv_tracker);
  if (r < 0)
    return r;

  return 0;
}


int RGWRados::set_bucket_owner(rgw_bucket& bucket, ACLOwner& owner)
{
  RGWBucketInfo info;
  map<string, bufferlist> attrs;
  int r = get_bucket_info(NULL, bucket.name, info, NULL, &attrs);
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
    int r = get_bucket_info(NULL, bucket.name, info, NULL, &attrs);
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
  int ret = get_bucket_info(NULL, bucket.name, bucket_info, NULL);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::complete_atomic_overwrite(RGWRadosCtx *rctx, RGWObjState *state, rgw_obj& obj)
{
  if (!state || !state->has_manifest || state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  for (iter = state->manifest.objs.begin(); iter != state->manifest.objs.end(); ++iter) {
    rgw_obj& mobj = iter->second.loc;
    if (mobj == obj)
      continue;
    string oid, key;
    rgw_bucket bucket;
    get_obj_bucket_and_oid_key(mobj, bucket, oid, key);
    chain.push_obj(bucket.data_pool, oid, key);
  }

  string tag = state->obj_tag.c_str();
  int ret = gc->send_chain(chain, tag, false);  // do it async

  return ret;
}

int RGWRados::open_bucket_index(rgw_bucket& bucket, librados::IoCtx& index_ctx, string& bucket_oid)
{
  if (bucket_is_system(bucket))
    return -EINVAL;

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

static void translate_raw_stats(rgw_bucket_dir_header& header, map<RGWObjCategory, RGWBucketStats>& stats)
{
  map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.begin();
  for (; iter != header.stats.end(); ++iter) {
    RGWObjCategory category = (RGWObjCategory)iter->first;
    RGWBucketStats& s = stats[category];
    struct rgw_bucket_category_stats& header_stats = iter->second;
    s.category = (RGWObjCategory)iter->first;
    s.num_kb = ((header_stats.total_size + 1023) / 1024);
    s.num_kb_rounded = ((header_stats.total_size_rounded + 1023) / 1024);
    s.num_objects = header_stats.num_entries;
  }
}

int RGWRados::bucket_check_index(rgw_bucket& bucket,
				 map<RGWObjCategory, RGWBucketStats> *existing_stats,
				 map<RGWObjCategory, RGWBucketStats> *calculated_stats)
{
  librados::IoCtx index_ctx;
  string oid;

  int ret = open_bucket_index(bucket, index_ctx, oid);
  if (ret < 0)
    return ret;

  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  ret = cls_rgw_bucket_check_index_op(index_ctx, oid, &existing_header, &calculated_header);
  if (ret < 0)
    return ret;

  translate_raw_stats(existing_header, *existing_stats);
  translate_raw_stats(calculated_header, *calculated_stats);

  return 0;
}

int RGWRados::bucket_rebuild_index(rgw_bucket& bucket)
{
  librados::IoCtx index_ctx;
  string oid;

  int ret = open_bucket_index(bucket, index_ctx, oid);
  if (ret < 0)
    return ret;

  return cls_rgw_bucket_rebuild_index_op(index_ctx, oid);
}


int RGWRados::defer_gc(void *ctx, rgw_obj& obj)
{
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
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


/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_obj_impl(void *ctx, rgw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;

  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, obj, op, &state, false, NULL);
  if (r < 0)
    return r;

  bool ret_not_existed = (state && !state->exists);

  string tag;
  r = prepare_update_index(state, bucket, CLS_RGW_OP_DEL, obj, tag);
  if (r < 0)
    return r;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(&op);
  }

  cls_refcount_put(op, tag, true);
  r = io_ctx.operate(oid, &op);
  bool removed = (r >= 0);

  int64_t poolid = io_ctx.get_id();
  if (r >= 0 || r == -ENOENT) {
    uint64_t epoch = io_ctx.get_last_version();
    r = complete_update_index_del(bucket, obj.object, tag, poolid, epoch);
  } else {
    int ret = complete_update_index_cancel(bucket, obj.object, tag);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_update_index_cancel returned ret=" << ret << dendl;
    }
  }
  if (removed) {
    int ret = complete_atomic_overwrite(rctx, state, obj);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_atomic_removal returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  }

  atomic_write_finish(state, r);

  if (r < 0)
    return r;

  if (ret_not_existed)
    return -ENOENT;

  if (state) {
    /* update quota cache */
    quota_handler->update_stats(bucket, -1, 0, state->size);
  }

  return 0;
}

int RGWRados::delete_obj(void *ctx, rgw_obj& obj, RGWObjVersionTracker *objv_tracker)
{
  int r;

  r = delete_obj_impl(ctx, obj, objv_tracker);
  if (r == -ECANCELED)
    r = 0;

  return r;
}

int RGWRados::delete_obj_index(rgw_obj& obj)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  string tag;
  int r = complete_update_index_del(bucket, obj.object, tag, -1 /* pool */, 0);

  return r;
}

static void generate_fake_tag(CephContext *cct, map<string, bufferlist>& attrset, RGWObjManifest& manifest, bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  map<uint64_t, RGWObjManifestPart>::iterator mi = manifest.objs.begin();
  if (mi != manifest.objs.end()) {
    if (manifest.objs.size() > 1) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi->second.loc.object;
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

int RGWRados::get_obj_state(RGWRadosCtx *rctx, rgw_obj& obj, RGWObjState **state, RGWObjVersionTracker *objv_tracker)
{
  RGWObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs)
    return 0;

  int r = obj_stat(rctx, obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), objv_tracker);
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
      s->size = s->manifest.obj_size;
    } catch (buffer::error& err) {
      ldout(cct, 20) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldout(cct, 10) << "manifest: total_size = " << s->manifest.obj_size << dendl;
    map<uint64_t, RGWObjManifestPart>::iterator mi;
    for (mi = s->manifest.objs.begin(); mi != s->manifest.objs.end(); ++mi) {
      ldout(cct, 10) << "manifest: ofs=" << mi->first << " loc=" << mi->second.loc << dendl;
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
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;
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
int RGWRados::get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  rgw_bucket actual_bucket = bucket;
  string actual_obj = oid;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);

  if (actual_obj.size() == 0) {
    actual_obj = bucket.name;
    actual_bucket = zone.domain_root;
  }

  int r = open_bucket_data_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  if (rctx) {
    RGWObjState *state;
    r = get_obj_state(rctx, obj, &state, NULL);
    if (r < 0)
      return r;
    if (!state->exists)
      return -ENOENT;
    if (state->get_attr(name, dest))
      return 0;
    return -ENODATA;
  }

  ObjectReadOperation op;

  int rval;
  op.getxattr(name, &dest, &rval);
  
  r = io_ctx.operate(actual_obj, &op, NULL);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::append_atomic_test(RGWRadosCtx *rctx, rgw_obj& obj,
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

int RGWRados::prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& obj,
                            ObjectWriteOperation& op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  int r = get_obj_state(rctx, obj, pstate, NULL);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  bool need_guard = (state->has_manifest || (state->obj_tag.length() != 0)) && (!state->fake_tag);

  if (!state->is_atomic) {
    ldout(cct, 20) << "prepare_atomic_for_write_impl: state is not atomic. state=" << (void *)state << dendl;

    if (reset_obj) {
      op.create(false);
      op.remove(); // we're not dropping reference here, actually removing object
    }

    return 0;
  }

  if (need_guard) {
    /* first verify that the object wasn't replaced under */
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
    // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
  }

  if (reset_obj) {
    op.create(false);
    op.remove();
  }

  if (ptag) {
    state->write_tag = *ptag;
  } else {
    append_rand_alpha(cct, state->write_tag, state->write_tag, 32);
  }
  bufferlist bl;
  bl.append(state->write_tag.c_str(), state->write_tag.size() + 1);

  ldout(cct, 10) << "setting object write_tag=" << state->write_tag << dendl;

  op.setxattr(RGW_ATTR_ID_TAG, bl);

  return 0;
}

int RGWRados::prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& obj,
                            ObjectWriteOperation& op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  if (!rctx) {
    *pstate = NULL;
    return 0;
  }

  int r;
  r = prepare_atomic_for_write_impl(rctx, obj, op, pstate, reset_obj, ptag);

  return r;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl, RGWObjVersionTracker *objv_tracker)
{
  map<string, bufferlist> attrs;
  attrs[name] = bl;
  return set_attrs(ctx, obj, attrs, NULL, objv_tracker);
}

int RGWRados::set_attrs(void *ctx, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs,
                        RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  string actual_obj = oid;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  rgw_bucket actual_bucket = bucket;

  if (actual_obj.size() == 0) {
    actual_obj = bucket.name;
    actual_bucket = zone.domain_root;
  }

  int r = open_bucket_data_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, obj, op, &state);
  if (r < 0)
    return r;

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

  r = io_ctx.operate(actual_obj, &op);
  if (r < 0)
    return r;

  if (state) {
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
int RGWRados::prepare_get_obj(void *ctx, rgw_obj& obj,
            off_t *pofs, off_t *pend,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *total_size,
            uint64_t *obj_size,
            RGWObjVersionTracker *objv_tracker,
            void **handle,
            struct rgw_err *err)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  int r = -EINVAL;
  bufferlist etag;
  time_t ctime;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  RGWObjState *astate = NULL;
  off_t ofs = 0;
  off_t end = -1;

  map<string, bufferlist>::iterator iter;

  *handle = NULL;

  GetObjState *state = new GetObjState;
  if (!state)
    return -ENOMEM;

  *handle = state;

  r = open_bucket_data_ctx(bucket, state->io_ctx);
  if (r < 0)
    goto done_err;

  state->io_ctx.locator_set_key(key);

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  r = get_obj_state(rctx, obj, &astate, objv_tracker);
  if (r < 0)
    goto done_err;

  if (!astate->exists) {
    r = -ENOENT;
    goto done_err;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
    if (r < 0)
      goto done_err;
  }

  /* Convert all times go GMT to make them compatible */
  if (mod_ptr || unmod_ptr) {
    ctime = astate->mtime;

    if (mod_ptr) {
      ldout(cct, 10) << "If-Modified-Since: " << *mod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime < *mod_ptr) {
        r = -ERR_NOT_MODIFIED;
        goto done_err;
      }
    }

    if (unmod_ptr) {
      ldout(cct, 10) << "If-UnModified-Since: " << *unmod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime > *unmod_ptr) {
        r = -ERR_PRECONDITION_FAILED;
        goto done_err;
      }
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(rctx, obj, RGW_ATTR_ETAG, etag);
    if (r < 0)
      goto done_err;

    if (if_match) {
      string if_match_str = rgw_string_unquote(if_match);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(etag.c_str()) != 0) {
        r = -ERR_PRECONDITION_FAILED;
        goto done_err;
      }
    }

    if (if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(if_nomatch);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(etag.c_str()) == 0) {
        r = -ERR_NOT_MODIFIED;
        goto done_err;
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
      r = -ERANGE;
      goto done_err;
    }
    if (end >= (off_t)astate->size) {
      end = astate->size - 1;
    }
  }

  if (pofs)
    *pofs = ofs;
  if (pend)
    *pend = end;
  if (total_size)
    *total_size = (ofs <= end ? end + 1 - ofs : 0);
  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  delete new_ctx;

  return 0;

done_err:
  delete new_ctx;
  finish_get_obj(handle);
  return r;
}

int RGWRados::prepare_update_index(RGWObjState *state, rgw_bucket& bucket,
                                   RGWModifyOp op, rgw_obj& obj, string& tag)
{
  if (bucket_is_system(bucket))
    return 0;

  int ret = data_log->add_entry(obj.bucket);
  if (ret < 0) {
    lderr(cct) << "ERROR: failed writing data log" << dendl;
    return ret;
  }

  if (state && state->obj_tag.length()) {
    int len = state->obj_tag.length();
    char buf[len + 1];
    memcpy(buf, state->obj_tag.c_str(), len);
    buf[len] = '\0';
    tag = buf;
  } else {
    if (tag.empty()) {
      append_rand_alpha(cct, tag, tag, 32);
    }
  }
  ret = cls_obj_prepare_op(bucket, op, tag,
                               obj.object, obj.key);

  return ret;
}

int RGWRados::complete_update_index(rgw_bucket& bucket, string& oid, string& tag, int64_t poolid, uint64_t epoch, uint64_t size,
                                    utime_t& ut, string& etag, string& content_type, bufferlist *acl_bl, RGWObjCategory category,
                                    list<string> *remove_objs)
{
  if (bucket_is_system(bucket))
    return 0;

  RGWObjEnt ent;
  ent.name = oid;
  ent.size = size;
  ent.mtime = ut;
  ent.etag = etag;
  ACLOwner owner;
  if (acl_bl && acl_bl->length()) {
    int ret = decode_policy(*acl_bl, &owner);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.owner = owner.get_id();
  ent.owner_display_name = owner.get_display_name();
  ent.content_type = content_type;

  int ret = cls_obj_complete_add(bucket, tag, poolid, epoch, ent, category, remove_objs);

  return ret;
}


int RGWRados::clone_objs_impl(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        RGWObjCategory category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  rgw_bucket bucket;
  std::string dst_oid, dst_key;
  get_obj_bucket_and_oid_key(dst_obj, bucket, dst_oid, dst_key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  uint64_t size = 0;
  string etag;
  string content_type;
  bufferlist acl_bl;
  bool update_index = (category == RGW_OBJ_CATEGORY_MAIN ||
                       category == RGW_OBJ_CATEGORY_MULTIMETA);

  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  io_ctx.locator_set_key(dst_key);
  ObjectWriteOperation op;
  if (truncate_dest) {
    op.remove();
    op.set_op_flags(OP_FAILOK); // don't fail if object didn't exist
  }

  op.create(exclusive);


  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;
    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = bl.c_str();
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = bl.c_str();
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }
  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, dst_obj, op, &state, true, NULL);
  if (r < 0)
    return r;

  vector<RGWCloneRangeInfo>::iterator range_iter;
  for (range_iter = ranges.begin(); range_iter != ranges.end(); ++range_iter) {
    RGWCloneRangeInfo range = *range_iter;
    vector<RGWCloneRangeInfo>::iterator next_iter = range_iter;

    // merge ranges
    while (++next_iter !=  ranges.end()) {
      RGWCloneRangeInfo& next = *next_iter;
      if (range.src_ofs + (int64_t)range.len != next.src_ofs ||
          range.dst_ofs + (int64_t)range.len != next.dst_ofs)
        break;
      range_iter = next_iter;
      range.len += next.len;
    }
    if (range.len) {
      ldout(cct, 20) << "calling op.clone_range(dst_ofs=" << range.dst_ofs << ", src.object=" <<  range.src.object << " range.src_ofs=" << range.src_ofs << " range.len=" << range.len << dendl;
      if (xattr_cond) {
        string src_cmp_obj, src_cmp_key;
        get_obj_bucket_and_oid_key(range.src, bucket, src_cmp_obj, src_cmp_key);
        op.src_cmpxattr(src_cmp_obj, xattr_cond->first.c_str(),
                        LIBRADOS_CMPXATTR_OP_EQ, xattr_cond->second);
      }
      string src_oid, src_key;
      get_obj_bucket_and_oid_key(range.src, bucket, src_oid, src_key);
      if (range.dst_ofs + range.len > size)
        size = range.dst_ofs + range.len;
      op.clone_range(range.dst_ofs, src_oid, range.src_ofs, range.len);
    }
  }
  time_t mt;
  utime_t ut;
  if (pmtime) {
    op.mtime(pmtime);
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(cct);
    mt = ut.sec();
    op.mtime(&mt);
  }

  string tag;
  uint64_t epoch = 0;
  int64_t poolid = io_ctx.get_id();
  int ret;

  if (update_index) {
    ret = prepare_update_index(state, bucket, CLS_RGW_OP_ADD, dst_obj, tag);
    if (ret < 0)
      goto done;
  }

  ret = io_ctx.operate(dst_oid, &op);

  epoch = io_ctx.get_last_version();

done:
  atomic_write_finish(state, ret);

  if (update_index) {
    if (ret >= 0) {
      ret = complete_update_index(bucket, dst_obj.object, tag, poolid, epoch, size,
                                  ut, etag, content_type, &acl_bl, category, NULL);
    } else {
      int r = complete_update_index_cancel(bucket, dst_obj.object, tag);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: comlete_update_index_cancel() returned r=" << r << dendl;
      }
    }
  }

  return ret;
}

int RGWRados::clone_objs(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        RGWObjCategory category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  int r;

  r = clone_objs_impl(ctx, dst_obj, ranges, attrs, category, pmtime, truncate_dest, exclusive, xattr_cond);
  if (r == -ECANCELED)
    r = 0;

  return r;
}


int RGWRados::get_obj(void *ctx, RGWObjVersionTracker *objv_tracker, void **handle, rgw_obj& obj,
                      bufferlist& bl, off_t ofs, off_t end)
{
  rgw_bucket bucket;
  std::string oid, key;
  rgw_obj read_obj = obj;
  uint64_t read_ofs = ofs;
  uint64_t len, read_len;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  bool reading_from_head = true;
  ObjectReadOperation op;

  GetObjState *state = *(GetObjState **)handle;
  RGWObjState *astate = NULL;

  bool merge_bl = false;
  bufferlist *pbl = &bl;
  bufferlist read_bl;

  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  int r = get_obj_state(rctx, obj, &astate, NULL);
  if (r < 0)
    goto done_ret;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest && !astate->manifest.objs.empty()) {
    /* now get the relevant object part */
    map<uint64_t, RGWObjManifestPart>::iterator iter = astate->manifest.objs.upper_bound(ofs);
    /* we're now pointing at the next part (unless the first part starts at a higher ofs),
       so retract to previous part */
    if (iter != astate->manifest.objs.begin()) {
      --iter;
    }

    RGWObjManifestPart& part = iter->second;
    uint64_t part_ofs = iter->first;
    read_obj = part.loc;
    len = min(len, part.size - (ofs - part_ofs));
    read_ofs = part.loc_ofs + (ofs - part_ofs);
    reading_from_head = (read_obj == obj);

    if (!reading_from_head) {
      get_obj_bucket_and_oid_key(read_obj, bucket, oid, key);
    }
  }

  if (len > RGW_MAX_CHUNK_SIZE)
    len = RGW_MAX_CHUNK_SIZE;


  state->io_ctx.locator_set_key(key);

  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the atomic test */
    r = append_atomic_test(rctx, read_obj, op, &astate);
    if (r < 0)
      goto done_ret;

    if (astate) {
      if (!ofs && astate->data.length() >= len) {
        bl = astate->data;
        goto done;
      }

      if (ofs < astate->data.length()) {
        unsigned copy_len = min((uint64_t)astate->data.length() - ofs, len);
        astate->data.copy(ofs, copy_len, bl);
        read_len -= copy_len;
        read_ofs += copy_len;
        if (!read_len)
	  goto done;

        merge_bl = true;
        pbl = &read_bl;
      }
    }
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }


  ldout(cct, 20) << "rados->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
  op.read(read_ofs, read_len, pbl, NULL);

  r = state->io_ctx.operate(oid, &op, NULL);
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (merge_bl)
    bl.append(read_bl);

done:
  if (bl.length() > 0) {
    r = bl.length();
  }
  if (r < 0 || !len || ((off_t)(ofs + len - 1) == end)) {
    finish_get_obj(handle);
  }

done_ret:
  delete new_ctx;

  return r;
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
  void *ctx;
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

  get_obj_data(CephContext *_cct)
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

  if (d->is_cancelled())
    goto done;

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

int RGWRados::get_obj_iterate_cb(void *ctx, RGWObjState *astate,
		         rgw_obj& obj,
			 off_t obj_ofs,
                         off_t read_ofs, off_t len,
                         bool is_head_obj, void *arg)
{
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
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

  r = flush_read_list(d);
  if (r < 0)
    return r;

  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

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

  return 0;

done_err:
  ldout(cct, 20) << "cancelling io r=" << r << " obj_ofs=" << obj_ofs << dendl;
  d->set_cancelled(r);
  d->cancel_io(obj_ofs);

  return r;
}

int RGWRados::get_obj_iterate(void *ctx, void **handle, rgw_obj& obj,
                              off_t ofs, off_t end,
			      RGWGetDataCB *cb)
{
  struct get_obj_data *data = new get_obj_data(cct);
  bool done = false;

  GetObjState *state = *(GetObjState **)handle;

  data->rados = this;
  data->ctx = ctx;
  data->io_ctx.dup(state->io_ctx);
  data->client_cb = cb;

  int r = iterate_obj(ctx, obj, ofs, end, cct->_conf->rgw_get_obj_max_req_size, _get_obj_iterate_cb, (void *)data);
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
    r = flush_read_list(data);
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

void RGWRados::finish_get_obj(void **handle)
{
  if (*handle) {
    GetObjState *state = *(GetObjState **)handle;
    delete state;
    *handle = NULL;
  }
}

int RGWRados::iterate_obj(void *ctx, rgw_obj& obj,
                          off_t ofs, off_t end,
			  uint64_t max_chunk_size,
			  int (*iterate_obj_cb)(rgw_obj&, off_t, off_t, off_t, bool, RGWObjState *, void *),
	                  void *arg)
{
  rgw_bucket bucket;
  rgw_obj read_obj = obj;
  uint64_t read_ofs = ofs;
  uint64_t len;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWRadosCtx *new_ctx = NULL;
  bool reading_from_head = true;
  RGWObjState *astate = NULL;

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  int r = get_obj_state(rctx, obj, &astate, NULL);
  if (r < 0)
    goto done_err;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest) {
    /* now get the relevant object part */
    map<uint64_t, RGWObjManifestPart>::iterator iter = astate->manifest.objs.upper_bound(ofs);
    /* we're now pointing at the next part (unless the first part starts at a higher ofs),
       so retract to previous part */
    if (iter != astate->manifest.objs.begin()) {
      --iter;
    }

    for (; iter != astate->manifest.objs.end() && ofs <= end; ++iter) {
      RGWObjManifestPart& part = iter->second;
      off_t part_ofs = iter->first;
      off_t next_part_ofs = part_ofs + part.size;

      while (ofs < next_part_ofs && ofs <= end) {
        read_obj = part.loc;
        uint64_t read_len = min(len, part.size - (ofs - part_ofs));
        read_ofs = part.loc_ofs + (ofs - part_ofs);

        if (read_len > max_chunk_size) {
          read_len = max_chunk_size;
        }

        reading_from_head = (read_obj == obj);
        r = iterate_obj_cb(read_obj, ofs, read_ofs, read_len, reading_from_head, astate, arg);
	if (r < 0)
	  goto done_err;

	len -= read_len;
        ofs += read_len;
      }
    }
  } else {
    while (ofs <= end) {
      uint64_t read_len = min(len, max_chunk_size);

      r = iterate_obj_cb(obj, ofs, ofs, read_len, reading_from_head, astate, arg);
      if (r < 0)
	goto done_err;

      len -= read_len;
      ofs += read_len;
    }
  }

  return 0;

done_err:
  delete new_ctx;
  return r;
}

/* a simple object read */
int RGWRados::read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = static_cast<RGWRadosCtx *>(ctx);
  RGWObjState *astate = NULL;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectReadOperation op;

  r = append_atomic_test(rctx, obj, op, &astate);
  if (r < 0)
    return r;

  op.read(ofs, size, &bl, NULL);

  return io_ctx.operate(oid, &op, NULL);
}

int RGWRados::obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime, uint64_t *epoch, map<string, bufferlist> *attrs, bufferlist *first_chunk,
                       RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  time_t mtime = 0;

  ObjectReadOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }
  op.getxattrs(&unfiltered_attrset, NULL);
  op.stat(&size, &mtime, NULL);
  if (first_chunk) {
    op.read(0, RGW_MAX_CHUNK_SIZE, first_chunk, NULL);
  }
  bufferlist outbl;
  r = io_ctx.operate(oid, &op, &outbl);

  if (epoch)
    *epoch = io_ctx.get_last_version();

  if (r < 0)
    return r;

  map<string, bufferlist> attrset;
  map<string, bufferlist>::iterator iter;
  string check_prefix = RGW_ATTR_PREFIX;
  for (iter = unfiltered_attrset.lower_bound(check_prefix);
       iter != unfiltered_attrset.end(); ++iter) {
    if (!str_startswith(iter->first, check_prefix))
      break;
    attrset[iter->first] = iter->second;
  }

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (attrs)
    *attrs = attrset;

  return 0;
}

int RGWRados::get_bucket_stats(rgw_bucket& bucket, uint64_t *bucket_ver, uint64_t *master_ver, map<RGWObjCategory, RGWBucketStats>& stats,
                               string *max_marker)
{
  rgw_bucket_dir_header header;
  int r = cls_bucket_head(bucket, header);
  if (r < 0)
    return r;

  stats.clear();

  translate_raw_stats(header, stats);

  *bucket_ver = header.ver;
  *master_ver = header.master_ver;

  if (max_marker)
    *max_marker = header.max_marker;

  return 0;
}

class RGWGetBucketStatsContext : public RGWGetDirHeader_CB {
  RGWGetBucketStats_CB *cb;

public:
  RGWGetBucketStatsContext(RGWGetBucketStats_CB *_cb) : cb(_cb) {}
  void handle_response(int r, rgw_bucket_dir_header& header) {
    map<RGWObjCategory, RGWBucketStats> stats;

    if (r >= 0) {
      translate_raw_stats(header, stats);
      cb->set_response(header.ver, header.master_ver, &stats, header.max_marker);
    }

    cb->handle_response(r);

    cb->put();
  }
};

int RGWRados::get_bucket_stats_async(rgw_bucket& bucket, RGWGetBucketStats_CB *ctx)
{
  RGWGetBucketStatsContext *get_ctx = new RGWGetBucketStatsContext(ctx);
  int r = cls_bucket_head_async(bucket, get_ctx);
  if (r < 0) {
    ctx->put();
    delete get_ctx;
    return r;
  }

  return 0;
}

void RGWRados::get_bucket_instance_entry(rgw_bucket& bucket, string& entry)
{
  entry = bucket.name + ":" + bucket.bucket_id;
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
    obj.init(zone.domain_root, bucket.oid);
  } else {
    string oid;
    get_bucket_meta_oid(bucket, oid);
    obj.init(zone.domain_root, oid);
  }
}

int RGWRados::get_bucket_instance_info(void *ctx, const string& meta_key, RGWBucketInfo& info,
                                       time_t *pmtime, map<string, bufferlist> *pattrs)
{
  int pos = meta_key.find(':');
  if (pos < 0) {
    return -EINVAL;
  }
  string oid = RGW_BUCKET_INSTANCE_MD_PREFIX + meta_key;

  return get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_info(void *ctx, rgw_bucket& bucket, RGWBucketInfo& info,
                                       time_t *pmtime, map<string, bufferlist> *pattrs)
{
  string oid;
  if (!bucket.oid.empty()) {
    get_bucket_meta_oid(bucket, oid);
  } else {
    oid = bucket.oid;
  }

  return get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
}

int RGWRados::get_bucket_instance_from_oid(void *ctx, string& oid, RGWBucketInfo& info,
                                           time_t *pmtime, map<string, bufferlist> *pattrs)
{
  ldout(cct, 20) << "reading from " << zone.domain_root << ":" << oid << dendl;

  bufferlist epbl;

  int ret = rgw_get_system_obj(this, ctx, zone.domain_root, oid, epbl, &info.objv_tracker, pmtime, pattrs);
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

int RGWRados::get_bucket_entrypoint_info(void *ctx, const string& bucket_name,
                                         RGWBucketEntryPoint& entry_point,
                                         RGWObjVersionTracker *objv_tracker,
                                         time_t *pmtime,
                                         map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  int ret = rgw_get_system_obj(this, ctx, zone.domain_root, bucket_name, bl, objv_tracker, pmtime, pattrs);
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

int RGWRados::get_bucket_info(void *ctx, string& bucket_name, RGWBucketInfo& info,
                              time_t *pmtime, map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  RGWBucketEntryPoint entry_point;
  time_t ep_mtime;
  RGWObjVersionTracker ot;
  int ret = get_bucket_entrypoint_info(ctx, bucket_name, entry_point, &ot, &ep_mtime, pattrs);
  if (ret < 0) {
    info.bucket.name = bucket_name; /* only init this field */
    return ret;
  }

  if (entry_point.has_bucket_info) {
    info = entry_point.old_bucket_info;
    info.bucket.oid = bucket_name;
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

  if (pattrs)
    pattrs->clear();

  /* read bucket instance info */

  string oid;
  get_bucket_meta_oid(entry_point.bucket, oid);

  ret = get_bucket_instance_from_oid(ctx, oid, info, pmtime, pattrs);
  info.ep_objv = ot.read_version;
  if (ret < 0) {
    info.bucket.name = bucket_name;
    return ret;
  }
  return 0;
}

int RGWRados::put_bucket_entrypoint_info(const string& bucket_name, RGWBucketEntryPoint& entry_point,
                                         bool exclusive, RGWObjVersionTracker& objv_tracker, time_t mtime,
                                         map<string, bufferlist> *pattrs)
{
  bufferlist epbl;
  ::encode(entry_point, epbl);
  return rgw_bucket_store_info(this, bucket_name, epbl, exclusive, pattrs, &objv_tracker, mtime);
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
  ret = put_bucket_entrypoint_info(info.bucket.name, entry_point, exclusive, ot, mtime, NULL); 
  if (ret < 0)
    return ret;

  return 0;
}

int RGWRados::omap_get_vals(rgw_obj& obj, bufferlist& header, const string& marker, uint64_t count, std::map<string, bufferlist>& m)
{
  bufferlist bl;
  librados::IoCtx io_ctx;
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  r = io_ctx.omap_get_vals(oid, marker, count, &m);
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
  rgw_bucket bucket;
  std::string oid, okey;
  get_obj_bucket_and_oid_key(obj, bucket, oid, okey);

  ldout(cct, 15) << "omap_set bucket=" << bucket << " oid=" << oid << " key=" << key << dendl;

  librados::IoCtx io_ctx;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(okey);

  map<string, bufferlist> m;
  m[key] = bl;

  r = io_ctx.omap_set(oid, m);

  return r;
}

int RGWRados::omap_set(rgw_obj& obj, std::map<std::string, bufferlist>& m)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  librados::IoCtx io_ctx;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  r = io_ctx.omap_set(oid, m);

  return r;
}

int RGWRados::omap_del(rgw_obj& obj, const std::string& key)
{
  rgw_bucket bucket;
  std::string oid, okey;
  get_obj_bucket_and_oid_key(obj, bucket, oid, okey);

  librados::IoCtx io_ctx;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(okey);

  set<string> k;
  k.insert(key);

  r = io_ctx.omap_rm_keys(oid, k);
  return r;
}

int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    rgw_bucket& bucket = ent.bucket;

    rgw_bucket_dir_header header;
    int r = cls_bucket_head(bucket, header);
    if (r < 0)
      return r;

    ent.count = 0;
    ent.size = 0;

    RGWObjCategory category = main_category;
    map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.find((uint8_t)category);
    if (iter != header.stats.end()) {
      struct rgw_bucket_category_stats& stats = iter->second;
      ent.count = stats.num_entries;
      ent.size = stats.total_size;
      ent.size_rounded = stats.total_size_rounded;
    }
  }

  return m.size();
}

int RGWRados::append_async(rgw_obj& obj, size_t size, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);

  io_ctx.locator_set_key(key);

  r = io_ctx.aio_append(oid, completion, bl, size);
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
  int r = control_pool_ctx.notify(notify_oid, 0, bl);
  return r;
}

int RGWRados::pool_iterate_begin(rgw_bucket& bucket, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::ObjectIterator& iter = ctx.iter;

  int r = open_bucket_data_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  iter = io_ctx.objects_begin();

  return 0;
}

int RGWRados::pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<RGWObjEnt>& objs,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::ObjectIterator& iter = ctx.iter;

  if (iter == io_ctx.objects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.objects_end(); ++i, ++iter) {
    RGWObjEnt e;

    string oid = iter->first;
    ldout(cct, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter->filter(oid, oid))
      continue;

    e.name = oid;
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.objects_end());

  return objs.size();
}
struct RGWAccessListFilterPrefix : public RGWAccessListFilter {
  string prefix;

  RGWAccessListFilterPrefix(const string& _prefix) : prefix(_prefix) {}
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
      lderr(cct) << "failed to list objects pool_iterate_begin() returned r=" << r << dendl;
      return r;
    }
    ctx.initialized = true;
  }

  vector<RGWObjEnt> objs;
  int r = pool_iterate(ctx.iter_ctx, max, objs, is_truncated, &filter);
  if (r < 0) {
    lderr(cct) << "failed to list objects pool_iterate returned r=" << r << dendl;
    return r;
  }

  vector<RGWObjEnt>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    oids.push_back(iter->name);
  }

  return oids.size();
}

int RGWRados::list_bi_log_entries(rgw_bucket& bucket, string& marker, uint32_t max,
                                  std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  result.clear();

  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  std::list<rgw_bi_log_entry> entries;
  int ret = cls_rgw_bi_log_list(index_ctx, oid, marker, max - result.size(), entries, truncated);
  if (ret < 0)
    return ret;

  std::list<rgw_bi_log_entry>::iterator iter;
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    result.push_back(*iter);
  }

  return 0;
}

int RGWRados::trim_bi_log_entries(rgw_bucket& bucket, string& start_marker, string& end_marker)
{
  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  int ret = cls_rgw_bi_log_trim(index_ctx, oid, start_marker, end_marker);
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

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  return gc->list(index, marker, max, result, truncated);
}

int RGWRados::process_gc()
{
  return gc->process();
}

int RGWRados::cls_rgw_init_index(librados::IoCtx& index_ctx, librados::ObjectWriteOperation& op, string& oid)
{
  bufferlist in;
  cls_rgw_bucket_init(op);
  int r = index_ctx.operate(oid, &op);
  return r;
}

int RGWRados::cls_obj_prepare_op(rgw_bucket& bucket, RGWModifyOp op, string& tag,
                                 string& name, string& locator)
{
  librados::IoCtx index_ctx;
  string oid;

  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  cls_rgw_bucket_prepare_op(o, op, tag, name, locator, zone_public_config.log_data);
  r = index_ctx.operate(oid, &o);
  return r;
}

int RGWRados::cls_obj_complete_op(rgw_bucket& bucket, RGWModifyOp op, string& tag,
                                  int64_t pool, uint64_t epoch,
                                  RGWObjEnt& ent, RGWObjCategory category,
				  list<string> *remove_objs)
{
  librados::IoCtx index_ctx;
  string oid;

  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta.size = ent.size;
  dir_meta.mtime = utime_t(ent.mtime, 0);
  dir_meta.etag = ent.etag;
  dir_meta.owner = ent.owner;
  dir_meta.owner_display_name = ent.owner_display_name;
  dir_meta.content_type = ent.content_type;
  dir_meta.category = category;

  rgw_bucket_entry_ver ver;
  ver.pool = pool;
  ver.epoch = epoch;
  cls_rgw_bucket_complete_op(o, op, tag, ver, ent.name, dir_meta, remove_objs, zone_public_config.log_data);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  r = index_ctx.aio_operate(oid, c, &o);
  c->release();
  return r;
}

int RGWRados::cls_obj_complete_add(rgw_bucket& bucket, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   RGWObjEnt& ent, RGWObjCategory category,
                                   list<string> *remove_objs)
{
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag, pool, epoch, ent, category, remove_objs);
}

int RGWRados::cls_obj_complete_del(rgw_bucket& bucket, string& tag,
                                   int64_t pool, uint64_t epoch,
                                   string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_DEL, tag, pool, epoch, ent, RGW_OBJ_CATEGORY_NONE, NULL);
}

int RGWRados::cls_obj_complete_cancel(rgw_bucket& bucket, string& tag, string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag, -1 /* pool id */, 0, ent, RGW_OBJ_CATEGORY_NONE, NULL);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(rgw_bucket& bucket, uint64_t timeout)
{
  librados::IoCtx index_ctx;
  string oid;

  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  cls_rgw_bucket_set_tag_timeout(o, timeout);

  r = index_ctx.operate(oid, &o);

  return r;
}

int RGWRados::cls_bucket_list(rgw_bucket& bucket, string start, string prefix,
		              uint32_t num, map<string, RGWObjEnt>& m,
			      bool *is_truncated, string *last_entry,
			      bool (*force_check_filter)(const string&  name))
{
  ldout(cct, 10) << "cls_bucket_list " << bucket << " start " << start << " num " << num << dendl;

  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  struct rgw_bucket_dir dir;
  r = cls_rgw_list_op(index_ctx, oid, start, prefix, num, &dir, is_truncated);
  if (r < 0)
    return r;

  map<string, struct rgw_bucket_dir_entry>::iterator miter;
  bufferlist updates;
  for (miter = dir.m.begin(); miter != dir.m.end(); ++miter) {
    RGWObjEnt e;
    rgw_bucket_dir_entry& dirent = miter->second;

    // fill it in with initial values; we may correct later
    e.name = dirent.name;
    e.size = dirent.meta.size;
    e.mtime = dirent.meta.mtime;
    e.etag = dirent.meta.etag;
    e.owner = dirent.meta.owner;
    e.owner_display_name = dirent.meta.owner_display_name;
    e.content_type = dirent.meta.content_type;
    e.tag = dirent.tag;

    /* oh, that shouldn't happen! */
    if (e.name.empty()) {
      ldout(cct, 0) << "WARNING: got empty dirent name, skipping" << dendl;
      continue;
    }

    bool force_check = force_check_filter && force_check_filter(dirent.name);

    if (!dirent.exists || !dirent.pending_map.empty() || force_check) {
      /* there are uncommitted ops. We need to check the current state,
       * and if the tags are old we need to do cleanup as well. */
      librados::IoCtx sub_ctx;
      sub_ctx.dup(index_ctx);
      r = check_disk_state(sub_ctx, bucket, dirent, e, updates);
      if (r < 0) {
        if (r == -ENOENT)
          continue;
        else
          return r;
      }
    }
    m[e.name] = e;
    ldout(cct, 10) << "RGWRados::cls_bucket_list: got " << e.name << dendl;
  }

  if (dir.m.size()) {
    *last_entry = dir.m.rbegin()->first;
  }

  if (updates.length()) {
    ObjectWriteOperation o;
    cls_rgw_suggest_changes(o, updates);
    // we don't care if we lose suggested updates, send them off blindly
    AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
    r = index_ctx.aio_operate(oid, c, &o);
    c->release();
  }
  return m.size();
}

int RGWRados::cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = zone.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(usage_log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rados->ioctx_create(usage_log_pool, io_ctx);
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

  const char *usage_log_pool = zone.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  r = cls_rgw_usage_log_read(io_ctx, oid, user, start_epoch, end_epoch,
			     max_entries, read_iter, usage, is_truncated);

  return r;
}

int RGWRados::cls_obj_usage_log_trim(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = zone.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_rgw_usage_log_trim(op, user, start_epoch, end_epoch);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::remove_objs_from_index(rgw_bucket& bucket, list<string>& oid_list)
{
  librados::IoCtx index_ctx;
  string dir_oid;

  int r = open_bucket_index(bucket, index_ctx, dir_oid);
  if (r < 0)
    return r;

  bufferlist updates;

  list<string>::iterator iter;

  for (iter = oid_list.begin(); iter != oid_list.end(); ++iter) {
    string& oid = *iter;
    dout(2) << "RGWRados::remove_objs_from_index bucket=" << bucket << " oid=" << oid << dendl;
    rgw_bucket_dir_entry entry;
    entry.ver.epoch = (uint64_t)-1; // ULLONG_MAX, needed to that objclass doesn't skip out request
    entry.name = oid;
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
  std::string oid, key, ns;
  oid = list_state.name;
  if (!rgw_obj::strip_namespace_from_object(oid, ns)) {
    // well crap
    assert(0 == "got bad object name off disk");
  }
  obj.init(bucket, oid, list_state.locator, ns);
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  io_ctx.locator_set_key(key);

  RGWObjState *astate = NULL;
  RGWRadosCtx rctx(this);
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
    map<uint64_t, RGWObjManifestPart>::iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.objs.begin(); miter != manifest.objs.end(); ++miter) {
      RGWObjManifestPart& part = miter->second;

      rgw_obj& loc = part.loc;

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
  list_state.meta.owner = owner.get_id();
  list_state.meta.owner_display_name = owner.get_display_name();

  list_state.exists = true;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE, list_state, suggested_updates);
  return 0;
}

int RGWRados::cls_bucket_head(rgw_bucket& bucket, struct rgw_bucket_dir_header& header)
{
  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  r = cls_rgw_get_dir_header(index_ctx, oid, &header);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::cls_bucket_head_async(rgw_bucket& bucket, RGWGetDirHeader_CB *ctx)
{
  librados::IoCtx index_ctx;
  string oid;
  int r = open_bucket_index(bucket, index_ctx, oid);
  if (r < 0)
    return r;

  r = cls_rgw_get_dir_header_async(index_ctx, oid, ctx);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::check_quota(rgw_bucket& bucket, RGWQuotaInfo& quota_info, uint64_t obj_size)
{
  return quota_handler->check_quota(bucket, quota_info, 1, obj_size);
}

class IntentLogNameFilter : public RGWAccessListFilter
{
  string prefix;
  bool filter_exact_date;
public:
  IntentLogNameFilter(const char *date, struct tm *tm) : prefix(date) {
    filter_exact_date = !(tm->tm_hour || tm->tm_min || tm->tm_sec); /* if time was specified and is not 00:00:00
                                                                       we should look at objects from that date */
  }
  bool filter(string& name, string& key) {
    if (filter_exact_date)
      return name.compare(prefix) < 0;
    else
      return name.compare(0, prefix.size(), prefix) <= 0;
  }
};

enum IntentFlags { // bitmask
  I_DEL_OBJ = 1,
  I_DEL_DIR = 2,
};


int RGWRados::remove_temp_objects(string date, string time)
{
  struct tm tm;
  
  string format = "%Y-%m-%d";
  string datetime = date;
  if (datetime.size() != 10) {
    cerr << "bad date format" << std::endl;
    return -EINVAL;
  }

  if (!time.empty()) {
    if (time.size() != 5 && time.size() != 8) {
      cerr << "bad time format" << std::endl;
      return -EINVAL;
    }
    format.append(" %H:%M:%S");
    datetime.append(time.c_str());
  }
  memset(&tm, 0, sizeof(tm));
  const char *s = strptime(datetime.c_str(), format.c_str(), &tm);
  if (s && *s) {
    cerr << "failed to parse date/time" << std::endl;
    return -EINVAL;
  }
  time_t epoch = mktime(&tm);

  vector<RGWObjEnt> objs;
  
  int max = 1000;
  bool is_truncated;
  IntentLogNameFilter filter(date.c_str(), &tm);
  RGWPoolIterCtx iter_ctx;
  int r = pool_iterate_begin(zone.intent_log_pool, iter_ctx);
  if (r < 0) {
    cerr << "failed to list objects" << std::endl;
    return r;
  }
  do {
    objs.clear();
    r = pool_iterate(iter_ctx, max, objs, &is_truncated, &filter);
    if (r == -ENOENT)
      break;
    if (r < 0) {
      cerr << "failed to list objects" << std::endl;
    }
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      process_intent_log(zone.intent_log_pool, (*iter).name, epoch, I_DEL_OBJ | I_DEL_DIR, true);
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::process_intent_log(rgw_bucket& bucket, string& oid,
				 time_t epoch, int flags, bool purge)
{
  cout << "processing intent log " << oid << std::endl;
  rgw_obj obj(bucket, oid);

  unsigned chunk = 1024 * 1024;
  off_t pos = 0;
  bool eof = false;
  bool complete = true;
  int ret = 0;
  int r;

  bufferlist bl;
  bufferlist::iterator iter;
  off_t off;

  while (!eof || !iter.end()) {
    off = iter.get_off();
    if (!eof && (bl.length() - off) < chunk / 2) {
      bufferlist more;
      r = read(NULL, obj, pos, chunk, more);
      if (r < 0) {
        cerr << "error while reading from " <<  bucket << ":" << oid
	   << " " << cpp_strerror(-r) << std::endl;
        return -r;
      }
      eof = (more.length() < (off_t)chunk);
      pos += more.length();
      bufferlist old;
      old.substr_of(bl, off, bl.length() - off);
      bl.clear();
      bl.claim(old);
      bl.claim_append(more);
      iter = bl.begin();
    }
    
    struct rgw_intent_log_entry entry;
    try {
      ::decode(entry, iter);
    } catch (buffer::error& err) {
      cerr << "failed to decode intent log entry in " << bucket << ":" << oid << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      ret = -EIO;
      complete = false;
      break;
    }
    if (entry.op_time.sec() > epoch) {
      cerr << "skipping entry for obj=" << obj << " entry.op_time=" << entry.op_time.sec() << " requested epoch=" << epoch << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      complete = false;
      break;
    }
    switch (entry.intent) {
    case DEL_OBJ:
      if (!(flags & I_DEL_OBJ)) {
        complete = false;
        break;
      }
      r = delete_obj(NULL, entry.obj);
      if (r < 0 && r != -ENOENT) {
        cerr << "failed to remove obj: " << entry.obj << std::endl;
        complete = false;
      }
      break;
    case DEL_DIR:
      if (!(flags & I_DEL_DIR)) {
        complete = false;
        break;
      } else {
        librados::IoCtx index_ctx;
        string oid;
        int r = open_bucket_index(entry.obj.bucket, index_ctx, oid);
        if (r < 0)
          return r;
        ObjectWriteOperation op;
        op.remove();
        oid.append(entry.obj.bucket.marker);
        librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);
        r = index_ctx.aio_operate(oid, completion, &op);
        completion->release();
        if (r < 0 && r != -ENOENT) {
          cerr << "failed to remove bucket: " << entry.obj.bucket << std::endl;
          complete = false;
        }
      }
      break;
    default:
      complete = false;
    }
  }

  if (complete) {
    rgw_obj obj(bucket, oid);
    cout << "completed intent log: " << obj << (purge ? ", purging it" : "") << std::endl;
    if (purge) {
      r = delete_obj(NULL, obj);
      if (r < 0)
        cerr << "failed to remove obj: " << obj << std::endl;
    }
  }

  return ret;
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
  int r = store->rados->ioctx_create(pool_name.c_str(), ioctx);
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
  return rados->get_instance_id();
}

uint64_t RGWRados::next_bucket_id()
{
  Mutex::Locker l(bucket_id_lock);
  return ++max_bucket_id;
}

RGWRados *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread)
{
  int use_cache = cct->_conf->rgw_cache_enabled;
  RGWRados *store = NULL;
  if (!use_cache) {
    store = new RGWRados;
  } else {
    store = new RGWCache<RGWRados>; 
  }

  if (store->initialize(cct, use_gc_thread) < 0) {
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

