// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * SAL implementation for the CORTX Motr backend
 *
 * Copyright (C) 2021 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

extern "C" {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wextern-c-compat"
#include "motr/config.h"
#include "lib/types.h"
#include "lib/trace.h"   // m0_trace_set_mmapped_buffer
#include "motr/layout.h" // M0_OBJ_LAYOUT_ID
#include "helpers/helpers.h" // m0_ufid_next
#pragma clang diagnostic pop
}

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_compression.h"
#include "rgw_sal.h"
#include "rgw_sal_motr.h"
#include "rgw_bucket.h"

#define dout_subsys ceph_subsys_rgw

using std::string;
using std::map;
using std::vector;
using std::set;
using std::list;

static string mp_ns = RGW_OBJ_NS_MULTIPART;
static struct m0_ufid_generator ufid_gr;

namespace rgw::sal {

using ::ceph::encode;
using ::ceph::decode;

static std::string motr_global_indices[] = {
  RGW_MOTR_USERS_IDX_NAME,
  RGW_MOTR_BUCKET_INST_IDX_NAME,
  RGW_MOTR_BUCKET_HD_IDX_NAME,
  RGW_IAM_MOTR_ACCESS_KEY
};

void MotrMetaCache::invalid(const DoutPrefixProvider *dpp,
                           const string& name)
{
  cache.invalidate_remove(dpp, name);
}

int MotrMetaCache::put(const DoutPrefixProvider *dpp,
                       const string& name,
                       const bufferlist& data)
{
  ldpp_dout(dpp, 0) << "Put into cache: name = " << name << dendl;

  ObjectCacheInfo info;
  info.status = 0;
  info.data = data;
  info.flags = CACHE_FLAG_DATA;
  info.meta.mtime = ceph::real_clock::now();
  info.meta.size = data.length();
  cache.put(dpp, name, info, NULL);

  // Inform other rgw instances. Do nothing if it gets some error?
  int rc = distribute_cache(dpp, name, info, UPDATE_OBJ);
  if (rc < 0)
      ldpp_dout(dpp, 0) << "ERROR: failed to distribute cache for " << name << dendl;

  return 0;
}

int MotrMetaCache::get(const DoutPrefixProvider *dpp,
                       const string& name,
                       bufferlist& data)
{
  ObjectCacheInfo info;
  uint32_t flags = CACHE_FLAG_DATA;
  int rc = cache.get(dpp, name, info, flags, NULL);
  if (rc == 0) {
    if (info.status < 0)
      return info.status;

    bufferlist& bl = info.data;
    bufferlist::iterator it = bl.begin();
    data.clear();

    it.copy_all(data);
    ldpp_dout(dpp, 0) << "Cache hit: name = " << name << dendl;
    return 0;
  }
  ldpp_dout(dpp, 0) << "Cache miss: name = " << name << ", rc = "<< rc << dendl;
  if(rc == -ENODATA)
    return -ENOENT;

  return rc;
}

int MotrMetaCache::remove(const DoutPrefixProvider *dpp,
                          const string& name)

{
  cache.invalidate_remove(dpp, name);

  ObjectCacheInfo info;
  int rc = distribute_cache(dpp, name, info, INVALIDATE_OBJ);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " <<__func__<< "(): failed to distribute cache: rc =" << rc << dendl;
  }

  ldpp_dout(dpp, 0) << "Remove from cache: name = " << name << dendl;
  return 0;
}

int MotrMetaCache::distribute_cache(const DoutPrefixProvider *dpp,
                                    const string& normal_name,
                                    ObjectCacheInfo& obj_info, int op)
{
  return 0;
}

int MotrMetaCache::watch_cb(const DoutPrefixProvider *dpp,
                            uint64_t notify_id,
                            uint64_t cookie,
                            uint64_t notifier_id,
                            bufferlist& bl)
{
  return 0;
}

void MotrMetaCache::set_enabled(bool status)
{
  cache.set_enabled(status);
}

// TODO: properly handle the number of key/value pairs to get in
// one query. Now the POC simply tries to retrieve all `max` number of pairs
// with starting key `marker`.
int MotrUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
    const string& end_marker, uint64_t max, bool need_stats,
    BucketList &buckets, optional_yield y)
{
  int rc;
  vector<string> keys(max);
  vector<bufferlist> vals(max);
  bool is_truncated = false;

  ldpp_dout(dpp, 20) <<__func__<< ": list_user_buckets: marker=" << marker
                    << " end_marker=" << end_marker
                    << " max=" << max << dendl;

  // Retrieve all `max` number of pairs.
  buckets.clear();
  string user_info_iname = "motr.rgw.user.info." + info.user_id.id;
  keys[0] = marker;
  rc = store->next_query_by_name(user_info_iname, keys, vals);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  // Process the returned pairs to add into BucketList.
  uint64_t bcount = 0;
  for (const auto& bl: vals) {
    if (bl.length() == 0)
      break;

    RGWBucketEnt ent;
    auto iter = bl.cbegin();
    ent.decode(iter);

    std::time_t ctime = ceph::real_clock::to_time_t(ent.creation_time);
    ldpp_dout(dpp, 20) << "got creation time: << " << std::put_time(std::localtime(&ctime), "%F %T") << dendl;

    if (!end_marker.empty() &&
         end_marker.compare(ent.bucket.marker) <= 0)
      break;

    buckets.add(std::make_unique<MotrBucket>(this->store, ent, this));
    bcount++;
  }
  if (bcount == max)
    is_truncated = true;
  buckets.set_truncated(is_truncated);

  return 0;
}

int MotrUser::create_bucket(const DoutPrefixProvider* dpp,
                            const rgw_bucket& b,
                            const std::string& zonegroup_id,
                            rgw_placement_rule& placement_rule,
                            std::string& swift_ver_location,
                            const RGWQuotaInfo* pquota_info,
                            const RGWAccessControlPolicy& policy,
                            Attrs& attrs,
                            RGWBucketInfo& info,
                            obj_version& ep_objv,
                            bool exclusive,
                            bool obj_lock_enabled,
                            bool* existed,
                            req_info& req_info,
                            std::unique_ptr<Bucket>* bucket_out,
                            optional_yield y)
{
  int ret;
  std::unique_ptr<Bucket> bucket;

  // Look up the bucket. Create it if it doesn't exist.
  ret = this->store->get_bucket(dpp, this, b, &bucket, y);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  if (ret != -ENOENT) {
    *existed = true;
    if (swift_ver_location.empty()) {
      swift_ver_location = bucket->get_info().swift_ver_location;
    }
    placement_rule.inherit_from(bucket->get_info().placement_rule);

    // TODO: ACL policy
    // // don't allow changes to the acl policy
    //RGWAccessControlPolicy old_policy(ctx());
    //int rc = rgw_op_get_bucket_policy_from_attr(
    //           dpp, this, u, bucket->get_attrs(), &old_policy, y);
    //if (rc >= 0 && old_policy != policy) {
    //    bucket_out->swap(bucket);
    //    return -EEXIST;
    //}
  } else {
    placement_rule.name = "default";
    placement_rule.storage_class = "STANDARD";
    bucket = std::make_unique<MotrBucket>(store, b, this);
    bucket->set_attrs(attrs);

    *existed = false;
  }

  // TODO: how to handle zone and multi-site.

  if (!*existed) {
    info.placement_rule = placement_rule;
    info.bucket = b;
    info.owner = this->get_info().user_id;
    info.zonegroup = zonegroup_id;
    if (obj_lock_enabled)
      info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
    bucket->set_version(ep_objv);
    bucket->get_info() = info;

    // Create a new bucket: (1) Add a key/value pair in the
    // bucket instance index. (2) Create a new bucket index.
    MotrBucket* mbucket = static_cast<MotrBucket*>(bucket.get());
    ret = mbucket->put_info(dpp, y, ceph::real_time())? :
          mbucket->create_bucket_index() ? :
          mbucket->create_multipart_indices();
    if (ret < 0)
      ldpp_dout(dpp, 0) << "ERROR: failed to create bucket indices! " << ret << dendl;

     // Insert the bucket entry into the user info index.
     ret = mbucket->link_user(dpp, this, y);
     if (ret < 0)
       ldpp_dout(dpp, 0) << "ERROR: failed to add bucket entry! " << ret << dendl;
  } else {
    bucket->set_version(ep_objv);
    bucket->get_info() = info;
  }

  bucket_out->swap(bucket);

  return ret;
}

int MotrUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int MotrUser::read_stats(const DoutPrefixProvider *dpp,
    optional_yield y, RGWStorageStats* stats,
    ceph::real_time *last_stats_sync,
    ceph::real_time *last_stats_update)
{
  return 0;
}

/* stats - Not for first pass */
int MotrUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB *cb)
{
  return 0;
}

int MotrUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int MotrUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
    bool *is_truncated, RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int MotrUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return 0;
}

static int load_user_from_idx(const DoutPrefixProvider *dpp,
                              MotrStore *store,
                              RGWUserInfo& info, map<string, bufferlist> *attrs,
                              RGWObjVersionTracker *objv_tracker)
{
  struct MotrUserInfo muinfo;
  bufferlist bl;
  ldpp_dout(dpp, 20) << "info.user_id.id = "  << info.user_id.id << dendl;
  if (store->get_user_cache()->get(dpp, info.user_id.id, bl)) {
    // Cache misses
    int rc = store->do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                                      M0_IC_GET, info.user_id.id, bl);
    ldpp_dout(dpp, 20) << "do_idx_op_by_name() = "  << rc << dendl;
    if (rc < 0)
        return rc;

    // Put into cache.
    store->get_user_cache()->put(dpp, info.user_id.id, bl);
  }

  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  muinfo.decode(iter);
  info = muinfo.info;
  if (attrs)
    *attrs = muinfo.attrs;
  if (objv_tracker)
    objv_tracker->read_version = muinfo.user_version;

  return 0;
}

int MotrUser::load_user(const DoutPrefixProvider *dpp,
                        optional_yield y)
{
  ldpp_dout(dpp, 20) << "load user: user id =   " << info.user_id.to_str() << dendl;
  return load_user_from_idx(dpp, store, info, &attrs, &objv_tracker);
}

int MotrUser::create_user_info_idx()
{
  string user_info_iname = "motr.rgw.user.info." + info.user_id.id;
  return store->create_motr_idx_by_name(user_info_iname);
}

int MotrUser::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  for (auto& it : new_attrs)
    attrs[it.first] = it.second;

  return store_user(dpp, y, false);
}

int MotrUser::store_user(const DoutPrefixProvider* dpp,
                         optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  bufferlist bl;
  struct MotrUserInfo muinfo;
  RGWUserInfo orig_info;
  RGWObjVersionTracker objv_tr = {};
  obj_version& obj_ver = objv_tr.read_version;

  ldpp_dout(dpp, 20) << "Store_user(): User = " << info.user_id.id << dendl;
  std::string access_key;
  std::string secret_key;
  if (!info.access_keys.empty()) {
    std::map<std::string, RGWAccessKey>::const_iterator iter = info.access_keys.begin();
    const RGWAccessKey& k = iter->second;
    access_key = k.id;
    secret_key = k.key;
    MotrAccessKey MGWUserKeys(access_key, secret_key, info.user_id.id);
    store->store_access_key(dpp, y, MGWUserKeys);
  }

  orig_info.user_id.id = info.user_id.id;
  // XXX: we open and close motr idx 2 times in this method:
  // 1) on load_user_from_idx() here and 2) on do_idx_op_by_name(PUT) below.
  // Maybe this can be optimised later somewhow.
  int rc = load_user_from_idx(dpp, store, orig_info, nullptr, &objv_tr);
  ldpp_dout(dpp, 10) << "Get user: rc = " << rc << dendl;

  // Check if the user already exists
  if (rc == 0 && obj_ver.ver > 0) {
    if (old_info)
      *old_info = orig_info;

    if (obj_ver.ver != objv_tracker.read_version.ver) {
      rc = -ECANCELED;
      ldpp_dout(dpp, 0) << "ERROR: User Read version mismatch" << dendl;
      goto out;
    }

    if (exclusive)
      return rc;

    obj_ver.ver++;
  } else {
    obj_ver.ver = 1;
    obj_ver.tag = "UserTAG";
  }

  // Insert the user to user info index.
      
  muinfo.info = info;
  muinfo.attrs = attrs;
  muinfo.user_version = obj_ver;
  muinfo.encode(bl);
  rc = store->do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                                M0_IC_PUT, info.user_id.id, bl);
  ldpp_dout(dpp, 10) << "Store user to motr index: rc = " << rc << dendl;
  if (rc == 0) {
    objv_tracker.read_version = obj_ver;
    objv_tracker.write_version = obj_ver;
  }

  // Create user info index to store all buckets that are belong
  // to this bucket.
  rc = create_user_info_idx();
  if (rc < 0 && rc != -EEXIST) {
    ldpp_dout(dpp, 0) << "Failed to create user info index: rc = " << rc << dendl;
    goto out;
  }

  // Put the user info into cache.
  store->get_user_cache()->put(dpp, info.user_id.id, bl);

out:
  return rc;
}

int MotrUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int MotrBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y)
{
  int ret;

  ret = load_bucket(dpp, y);
  // TODO: implement bucket removal

  return ret;
}

int MotrBucket::remove_bucket_bypass_gc(int concurrent_max, bool
        keep_index_consistent,
        optional_yield y, const
        DoutPrefixProvider *dpp) {
  return 0;
}

int MotrBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
{
  bufferlist bl;
  struct MotrBucketInfo mbinfo;

  ldpp_dout(dpp, 20) << "put_info(): bucket_id=" << info.bucket.bucket_id << dendl;
  mbinfo.info = info;
  mbinfo.bucket_attrs = attrs;
  mbinfo.mtime = _mtime;
  mbinfo.bucket_version = bucket_version;
  mbinfo.encode(bl);

  // Insert bucket instance using bucket's marker (string).
  int rc = store->do_idx_op_by_name(RGW_MOTR_BUCKET_INST_IDX_NAME,
                                  M0_IC_PUT, info.bucket.name, bl, !exclusive);
  if (rc == 0)
    store->get_bucket_inst_cache()->put(dpp, info.bucket.name, bl);

  return rc;
}

int MotrBucket::load_bucket(const DoutPrefixProvider *dpp, optional_yield y, bool get_stats)
{
  // Get bucket instance using bucket's name (string). or bucket id?
  bufferlist bl;
  if (store->get_bucket_inst_cache()->get(dpp, info.bucket.name, bl)) {
    // Cache misses.
    ldpp_dout(dpp, 20) << "load_bucket(): name=" << info.bucket.name << dendl;
    int rc = store->do_idx_op_by_name(RGW_MOTR_BUCKET_INST_IDX_NAME,
                                      M0_IC_GET, info.bucket.name, bl);
    ldpp_dout(dpp, 20) << "load_bucket(): rc=" << rc << dendl;
    if (rc < 0)
      return rc;
    store->get_bucket_inst_cache()->put(dpp, info.bucket.name, bl);
  }

  struct MotrBucketInfo mbinfo;
  bufferlist& blr = bl;
  auto iter =blr.cbegin();
  mbinfo.decode(iter); //Decode into MotrBucketInfo.

  info = mbinfo.info;
  ldpp_dout(dpp, 20) << "load_bucket(): bucket_id=" << info.bucket.bucket_id << dendl;
  rgw_placement_rule placement_rule;
  placement_rule.name = "default";
  placement_rule.storage_class = "STANDARD";
  info.placement_rule = placement_rule;

  attrs = mbinfo.bucket_attrs;
  mtime = mbinfo.mtime;
  bucket_version = mbinfo.bucket_version;

  return 0;
}

int MotrBucket::link_user(const DoutPrefixProvider* dpp, User* new_user, optional_yield y)
{
  bufferlist bl;
  RGWBucketEnt new_bucket;
  ceph::real_time creation_time = get_creation_time();

  // RGWBucketEnt or cls_user_bucket_entry is the structure that is stored.
  new_bucket.bucket = info.bucket;
  new_bucket.size = 0;
  if (real_clock::is_zero(creation_time))
    creation_time = ceph::real_clock::now();
  new_bucket.creation_time = creation_time;
  new_bucket.encode(bl);
  std::time_t ctime = ceph::real_clock::to_time_t(new_bucket.creation_time);
  ldpp_dout(dpp, 20) << "got creation time: << " << std::put_time(std::localtime(&ctime), "%F %T") << dendl;

  // Insert the user into the user info index.
  string user_info_idx_name = "motr.rgw.user.info." + new_user->get_info().user_id.id;
  return store->do_idx_op_by_name(user_info_idx_name,
                                  M0_IC_PUT, info.bucket.name, bl);

}

int MotrBucket::unlink_user(const DoutPrefixProvider* dpp, User* new_user, optional_yield y)
{
  // Remove the user into the user info index.
  bufferlist bl;
  string user_info_idx_name = "motr.rgw.user.info." + new_user->get_info().user_id.id;
  return store->do_idx_op_by_name(user_info_idx_name,
                                  M0_IC_DEL, info.bucket.name, bl);
}

/* stats - Not for first pass */
int MotrBucket::read_stats(const DoutPrefixProvider *dpp, int shard_id,
    std::string *bucket_ver, std::string *master_ver,
    std::map<RGWObjCategory, RGWStorageStats>& stats,
    std::string *max_marker, bool *syncstopped)
{
  return 0;
}

int MotrBucket::create_bucket_index()
{
  string bucket_index_iname = "motr.rgw.bucket.index." + info.bucket.name;
  return store->create_motr_idx_by_name(bucket_index_iname);
}

int MotrBucket::create_multipart_indices()
{
  int rc;

  // Bucket multipart index stores in-progress multipart uploads.
  // Key is the object name + upload_id, value is a rgw_bucket_dir_entry.
  // An entry is inserted when a multipart upload is initialised (
  // MotrMultipartUpload::init()) and will be removed when the upload
  // is completed (MotrMultipartUpload::complete()).
  // MotrBucket::list_multiparts() will scan this index to return all
  // in-progress multipart uploads in the bucket.
  string bucket_multipart_iname = "motr.rgw.bucket." + info.bucket.name + ".multiparts";
  rc = store->create_motr_idx_by_name(bucket_multipart_iname);
  if (rc < 0) {
    ldout(store->cctx, 0) << "Failed to create bucket multipart index  " << bucket_multipart_iname << dendl;
    return rc;
  }

  return 0;
}


int MotrBucket::read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB *ctx)
{
  return 0;
}

int MotrBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int MotrBucket::update_container_stats(const DoutPrefixProvider *dpp)
{
  return 0;
}

int MotrBucket::check_bucket_shards(const DoutPrefixProvider *dpp)
{
  return 0;
}

int MotrBucket::chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker)
{
  // TODO: update bucket with new owner

  /* XXX: Update policies of all the bucket->objects with new user */
  return 0;
}

/* Make sure to call load_bucket() if you need it first */
bool MotrBucket::is_owner(User* user)
{
  return (info.owner.compare(user->get_id()) == 0);
}

int MotrBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
{
  /* XXX: Check if bucket contains any objects */
  return 0;
}

int MotrBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
    optional_yield y, bool check_size_only)
{
  /* Not Handled in the first pass as stats are also needed */
  return 0;
}

int MotrBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& new_attrs, optional_yield y)
{
  for (auto& it : new_attrs)
    attrs[it.first] = it.second;

  return put_info(dpp, y, ceph::real_time());
}

int MotrBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime)
{
  return 0;
}

/* XXX: usage and stats not supported in the first pass */
int MotrBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated,
    RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int MotrBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return 0;
}

int MotrBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
   * Delete all the object in the list from the object table of this
   * bucket
   */
  return 0;
}

int MotrBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  /* XXX: stats not supported yet */
  return 0;
}

int MotrBucket::rebuild_index(const DoutPrefixProvider *dpp)
{
  /* there is no index table in dbstore. Not applicable */
  return 0;
}

int MotrBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
{
  /* XXX: CHECK: set tag timeout for all the bucket objects? */
  return 0;
}

int MotrBucket::purge_instance(const DoutPrefixProvider *dpp)
{
  /* XXX: CHECK: for dbstore only single instance supported.
   * Remove all the objects for that instance? Anything extra needed?
   */
  return 0;
}

int MotrBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
{
  int ret = 0;
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  Attrs attrs = get_attrs();
  attrs[RGW_ATTR_ACL] = aclbl;

  // TODO: update bucket entry with the new attrs

  return ret;
}

std::unique_ptr<Object> MotrBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<MotrObject>(this->store, k, this);
}

int MotrBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
{
  int rc;
  vector<string> keys(max);
  vector<bufferlist> vals(max);

  ldpp_dout(dpp, 20) << "bucket=" << info.bucket.name
                    << " prefix=" << params.prefix
                    << " marker=" << params.marker
                    << " max=" << max << dendl;

  // Retrieve all `max` number of pairs.
  string bucket_index_iname = "motr.rgw.bucket.index." + info.bucket.name;
  keys[0] = params.marker.empty() ? params.prefix :
                                    params.marker.to_str();
  rc = store->next_query_by_name(bucket_index_iname, keys, vals, params.prefix,
                                                                 params.delim);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  // Process the returned pairs to add into ListResults.
  int i = 0;
  for (; i < rc; ++i) {
    if (vals[i].length() == 0) {
      results.common_prefixes[keys[i]] = true;
    } else {
      rgw_bucket_dir_entry ent;
      auto iter = vals[i].cbegin();
      ent.decode(iter);
      if (params.list_versions || ent.is_visible())
        results.objs.emplace_back(std::move(ent));
    }
  }

  if (i == max) {
    results.is_truncated = true;
    results.next_marker = keys[max - 1] + " ";
  } else {
    results.is_truncated = false;
  }

  return 0;
}

int MotrBucket::list_multiparts(const DoutPrefixProvider *dpp,
      const string& prefix,
      string& marker,
      const string& delim,
      const int& max_uploads,
      vector<std::unique_ptr<MultipartUpload>>& uploads,
      map<string, bool> *common_prefixes,
      bool *is_truncated)
{
  int rc;
  vector<string> key_vec(max_uploads);
  vector<bufferlist> val_vec(max_uploads);

  string bucket_multipart_iname =
      "motr.rgw.bucket." + this->get_name() + ".multiparts";
  key_vec[0].clear();
  key_vec[0].assign(marker.begin(), marker.end());
  rc = store->next_query_by_name(bucket_multipart_iname, key_vec, val_vec);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  // Process the returned pairs to add into ListResults.
  // The POC can only support listing all objects or selecting
  // with prefix.
  int ocount = 0;
  rgw_obj_key last_obj_key;
  *is_truncated = false;
  for (const auto& bl: val_vec) {
    if (bl.length() == 0)
      break;

    rgw_bucket_dir_entry ent;
    auto iter = bl.cbegin();
    ent.decode(iter);

    if (prefix.size() &&
        (0 != ent.key.name.compare(0, prefix.size(), prefix))) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ <<
        ": skippping \"" << ent.key <<
        "\" because doesn't match prefix" << dendl;
      continue;
    }

    rgw_obj_key key(ent.key);
    uploads.push_back(this->get_multipart_upload(key.name));
    last_obj_key = key;
    ocount++;
    if (ocount == max_uploads) {
      *is_truncated = true;
      break;
    }
  }
  marker = last_obj_key.name;

  // What is common prefix? We don't handle it for now.

  return 0;

}

int MotrBucket::abort_multiparts(const DoutPrefixProvider *dpp, CephContext *cct)
{
  return 0;
}

void MotrStore::finalize(void)
{
  // close connection with motr
  m0_client_fini(this->instance, true);
}

const RGWZoneGroup& MotrZone::get_zonegroup()
{
  return *zonegroup;
}

int MotrZone::get_zonegroup(const std::string& id, RGWZoneGroup& zg)
{
  /* XXX: for now only one zonegroup supported */
  zg = *zonegroup;
  return 0;
}

const RGWZoneParams& MotrZone::get_params()
{
  return *zone_params;
}

const rgw_zone_id& MotrZone::get_id()
{
  return cur_zone_id;
}

const RGWRealm& MotrZone::get_realm()
{
  return *realm;
}

const std::string& MotrZone::get_name() const
{
  return zone_params->get_name();
}

bool MotrZone::is_writeable()
{
  return true;
}

bool MotrZone::get_redirect_endpoint(std::string* endpoint)
{
  return false;
}

bool MotrZone::has_zonegroup_api(const std::string& api) const
{
  return false;
}

const std::string& MotrZone::get_current_period_id()
{
  return current_period->get_id();
}

std::unique_ptr<LuaScriptManager> MotrStore::get_lua_script_manager()
{
  return std::make_unique<MotrLuaScriptManager>(this);
}

int MotrObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **_state, optional_yield y, bool follow_olh)
{
  if (state == nullptr)
    state = new RGWObjState();
  *_state = state;

  // Get object's metadata (those stored in rgw_bucket_dir_entry).
  bufferlist bl;
  if (this->store->get_obj_meta_cache()->get(dpp, this->get_key().to_str(), bl)) {
    // Cache misses.
    string bucket_index_iname = "motr.rgw.bucket.index." + this->get_bucket()->get_name();
    int rc = this->store->do_idx_op_by_name(bucket_index_iname,
                                  M0_IC_GET, this->get_key().to_str(), bl);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "Failed to get object's entry from bucket index. " << dendl;
      return rc;
    }

    // Put into cache.
    this->store->get_obj_meta_cache()->put(dpp, this->get_key().to_str(), bl);
  }

  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  ent.decode(iter);

  // Set object's type.
  this->category = ent.meta.category;

  // Set object state.
  state->obj = get_obj();
  state->exists = true;
  state->size = ent.meta.size;
  state->accounted_size = ent.meta.size;
  state->mtime = ent.meta.mtime;

  state->has_attrs = true;
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) <<__func__<< ": object's etag:  " << ent.meta.etag << dendl;
  etag_bl.append(etag);
  state->attrset[RGW_ATTR_ETAG] = etag_bl;

  return 0;
}

MotrObject::~MotrObject() {
  delete state;
  this->close_mobj();
}

//  int MotrObject::read_attrs(const DoutPrefixProvider* dpp, Motr::Object::Read &read_op, optional_yield y, rgw_obj* target_obj)
//  {
//    read_op.params.attrs = &attrs;
//    read_op.params.target_obj = target_obj;
//    read_op.params.obj_size = &obj_size;
//    read_op.params.lastmod = &mtime;
//
//    return read_op.prepare(dpp);
//  }

int MotrObject::set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj)
{
  // TODO: implement
  ldpp_dout(dpp, 20) <<__func__<< ": MotrObject::set_obj_attrs()" << dendl;
  return 0;
}

int MotrObject::get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  if (this->category == RGWObjCategory::MultiMeta)
    return 0;

  string bname, key;
  if (target_obj) {
    bname = target_obj->bucket.name;
    key   = target_obj->key.to_str();
  } else {
    bname = this->get_bucket()->get_name();
    key   = this->get_key().to_str();
  }
  ldpp_dout(dpp, 20) << "MotrObject::get_obj_attrs(): "
                    << bname << "/" << key << dendl;

  // Get object's metadata (those stored in rgw_bucket_dir_entry).
  bufferlist bl;
  if (this->store->get_obj_meta_cache()->get(dpp, key, bl)) {
    // Cache misses.
    string bucket_index_iname = "motr.rgw.bucket.index." + bname;
    int rc = this->store->do_idx_op_by_name(bucket_index_iname, M0_IC_GET, key, bl);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "Failed to get object's entry from bucket index. " << dendl;
      return rc;
    }

    // Put into cache.
    this->store->get_obj_meta_cache()->put(dpp, key, bl);
  }

  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  ent.decode(iter);
  decode(attrs, iter);

  return 0;
}

int MotrObject::modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw_obj target = get_obj();
  int r = get_obj_attrs(rctx, y, dpp, &target);
  if (r < 0) {
    return r;
  }
  set_atomic(rctx);
  attrs[attr_name] = attr_val;
  return set_obj_attrs(dpp, rctx, &attrs, nullptr, y, &target);
}

int MotrObject::delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y)
{
  rgw_obj target = get_obj();
  Attrs rmattr;
  bufferlist bl;

  set_atomic(rctx);
  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, rctx, nullptr, &rmattr, y, &target);
}

/* RGWObjectCtx will be moved out of sal */
/* XXX: Placeholder. Should not be needed later after Dan's patch */
void MotrObject::set_atomic(RGWObjectCtx* rctx) const
{
  return;
}

/* RGWObjectCtx will be moved out of sal */
/* XXX: Placeholder. Should not be needed later after Dan's patch */
void MotrObject::set_prefetch_data(RGWObjectCtx* rctx)
{
  return;
}

/* RGWObjectCtx will be moved out of sal */
/* XXX: Placeholder. Should not be needed later after Dan's patch */
void MotrObject::set_compressed(RGWObjectCtx* rctx)
{
  return;
}

bool MotrObject::is_expired() {
  return false;
}

// Taken from rgw_rados.cc
void MotrObject::gen_rand_obj_instance_name()
{
  enum {OBJ_INSTANCE_LEN = 32};
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(store->ctx(), buf, OBJ_INSTANCE_LEN);
  key.set_instance(buf);
}

int MotrObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
    std::map<std::string, bufferlist> *m,
    bool* pmore, optional_yield y)
{
  return 0;
}

int MotrObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
    optional_yield y)
{
  return 0;
}

int MotrObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
    const std::set<std::string>& keys,
    Attrs* vals)
{
  return 0;
}

int MotrObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
    bool must_exist, optional_yield y)
{
  return 0;
}

MPSerializer* MotrObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return new MPMotrSerializer(dpp, store, this, lock_name);
}

int MotrObject::transition(RGWObjectCtx& rctx,
    Bucket* bucket,
    const rgw_placement_rule& placement_rule,
    const real_time& mtime,
    uint64_t olh_epoch,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  return 0;
}

bool MotrObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  /* XXX: support single default zone and zonegroup for now */
  return true;
}

int MotrObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx)
{
  return 0;
}

std::unique_ptr<Object::ReadOp> MotrObject::get_read_op(RGWObjectCtx* ctx)
{
  return std::make_unique<MotrObject::MotrReadOp>(this, ctx);
}

MotrObject::MotrReadOp::MotrReadOp(MotrObject *_source, RGWObjectCtx *_rctx) :
  source(_source),
  rctx(_rctx)
{ }

int MotrObject::MotrReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int rc;
  ldpp_dout(dpp, 20) <<__func__<< ": bucket=" << source->get_bucket()->get_name() << dendl;

  rgw_bucket_dir_entry ent;
  rc = source->get_bucket_dir_ent(dpp, ent);
  if (rc < 0)
    return rc;

  // Set source object's attrs. The attrs is key/value map and is used
  // in send_response_data() to set attributes, including etag.
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) <<__func__<< ": object's etag: " << ent.meta.etag << dendl;
  etag_bl.append(etag.c_str(), etag.size());
  source->get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));

  source->set_key(ent.key);
  source->set_obj_size(ent.meta.size);
  source->category = ent.meta.category;

  // Open the object here.
  if (source->category == RGWObjCategory::MultiMeta) {
    ldpp_dout(dpp, 20) <<__func__<< ": open obj parts..." << dendl;
    rc = source->get_part_objs(dpp, this->part_objs)? :
         source->open_part_objs(dpp, this->part_objs);
    return rc;
  } else {
    ldpp_dout(dpp, 20) <<__func__<< ": open object..." << dendl;
    return source->open_mobj(dpp);
  }
}

int MotrObject::MotrReadOp::read(int64_t off, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
{
  ldpp_dout(dpp, 20) << "MotrReadOp::read(): sync read." << dendl;
  return 0;
}

// RGWGetObj::execute() calls ReadOp::iterate() to read object from 'off' to 'end'.
// The returned data is processed in 'cb' which is a chain of post-processing
// filters such as decompression, de-encryption and sending back data to client
// (RGWGetObj_CB::handle_dta which in turn calls RGWGetObj::get_data_cb() to
// send data back.).
//
// POC implements a simple sync version of iterate() function in which it reads
// a block of data each time and call 'cb' for post-processing.
int MotrObject::MotrReadOp::iterate(const DoutPrefixProvider* dpp, int64_t off, int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int rc;

  if (source->category == RGWObjCategory::MultiMeta)
    rc = source->read_multipart_obj(dpp, off, end, cb, part_objs);
  else
    rc = source->read_mobj(dpp, off, end, cb);

  return rc;
}

int MotrObject::MotrReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  //return 0;
  return -ENODATA;
}

std::unique_ptr<Object::DeleteOp> MotrObject::get_delete_op(RGWObjectCtx* ctx)
{
  return std::make_unique<MotrObject::MotrDeleteOp>(this, ctx);
}

MotrObject::MotrDeleteOp::MotrDeleteOp(MotrObject *_source, RGWObjectCtx *_rctx) :
  source(_source),
  rctx(_rctx)
{ }

// Implementation of DELETE OBJ also requires MotrObject::get_obj_state()
// to retrieve and set object's state from object's metadata.
//
// TODO:
// 1. The POC only remove the object's entry from bucket index and delete
// corresponding Motr objects. It doesn't handle the DeleteOp::params.
// Delete::delete_obj() in rgw_rados.cc shows how rados backend process the
// params.
// 2. Delete an object when its versioning is turned on.
int MotrObject::MotrDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
{
  ldpp_dout(dpp, 20) << "delete " << source->get_key().to_str() << " from " << source->get_bucket()->get_name() << dendl;

  // Delete from the cache first.
  source->store->get_obj_meta_cache()->remove(dpp, source->get_key().to_str());

  // Delete the object's entry from the bucket index.
  bufferlist bl;
  string bucket_index_iname = "motr.rgw.bucket.index." + source->get_bucket()->get_name();
  int rc = source->store->do_idx_op_by_name(bucket_index_iname,
                                            M0_IC_DEL, source->get_key().to_str(), bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "Failed to del object's entry from bucket index. " << dendl;
    return rc;
  }

  // Remove the motr objects.
  if (source->category == RGWObjCategory::MultiMeta)
    rc = source->delete_part_objs(dpp);
  else
    rc = source->delete_mobj(dpp);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "Failed to delete the object from Motr. " << dendl;
    return rc;
  }

  //result.delete_marker = parent_op.result.delete_marker;
  //result.version_id = parent_op.result.version_id;
  return 0;
}

int MotrObject::delete_object(const DoutPrefixProvider* dpp, RGWObjectCtx* obj_ctx, optional_yield y, bool prevent_versioning)
{
  MotrObject::MotrDeleteOp del_op(this, obj_ctx);
  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = bucket->get_info().versioning_status();

  return del_op.delete_obj(dpp, y);
}

int MotrObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
    Completions* aio, bool keep_index_consistent,
    optional_yield y)
{
  /* XXX: Make it async */
  return 0;
}

int MotrObject::copy_object(RGWObjectCtx& obj_ctx,
    User* user,
    req_info* info,
    const rgw_zone_id& source_zone,
    rgw::sal::Object* dest_object,
    rgw::sal::Bucket* dest_bucket,
    rgw::sal::Bucket* src_bucket,
    const rgw_placement_rule& dest_placement,
    ceph::real_time* src_mtime,
    ceph::real_time* mtime,
    const ceph::real_time* mod_ptr,
    const ceph::real_time* unmod_ptr,
    bool high_precision_time,
    const char* if_match,
    const char* if_nomatch,
    AttrsMod attrs_mod,
    bool copy_if_newer,
    Attrs& attrs,
    RGWObjCategory category,
    uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at,
    std::string* version_id,
    std::string* tag,
    std::string* etag,
    void (*progress_cb)(off_t, void *),
    void* progress_data,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
      return 0;
}

int MotrObject::swift_versioning_restore(RGWObjectCtx* obj_ctx,
    bool& restored,
    const DoutPrefixProvider* dpp)
{
  return 0;
}

int MotrObject::swift_versioning_copy(RGWObjectCtx* obj_ctx,
    const DoutPrefixProvider* dpp,
    optional_yield y)
{
  return 0;
}

MotrAtomicWriter::MotrAtomicWriter(const DoutPrefixProvider *dpp,
          optional_yield y,
          std::unique_ptr<rgw::sal::Object> _head_obj,
          MotrStore* _store,
          const rgw_user& _owner, RGWObjectCtx& obj_ctx,
          const rgw_placement_rule *_ptail_placement_rule,
          uint64_t _olh_epoch,
          const std::string& _unique_tag) :
        Writer(dpp, y),
        store(_store),
              owner(_owner),
              ptail_placement_rule(_ptail_placement_rule),
              olh_epoch(_olh_epoch),
              unique_tag(_unique_tag),
              obj(_store, _head_obj->get_key(), _head_obj->get_bucket()) {}

static const unsigned MAX_BUFVEC_NR = 256;

int MotrAtomicWriter::prepare(optional_yield y)
{
  total_data_size = 0;

  if (obj.is_opened())
    return 0;

  int rc = m0_bufvec_empty_alloc(&buf, MAX_BUFVEC_NR) ?:
           m0_bufvec_alloc(&attr, MAX_BUFVEC_NR, 1) ?:
           m0_indexvec_alloc(&ext, MAX_BUFVEC_NR);
  if (rc != 0)
    this->cleanup();

  return rc;
}

int MotrObject::create_mobj(const DoutPrefixProvider *dpp, uint64_t sz)
{
  if (mobj != nullptr) {
    ldpp_dout(dpp, 0) <<__func__<< "ERROR: object is already opened" << dendl;
    return -EINVAL;
  }

  int rc = m0_ufid_next(&ufid_gr, 1, &meta.oid);
  if (rc != 0) {
    ldpp_dout(dpp, 0) <<__func__<< "ERROR: m0_ufid_next() failed: " << rc << dendl;
    return rc;
  }

  char fid_str[M0_FID_STR_LEN];
  snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&meta.oid));
  ldpp_dout(dpp, 20) <<__func__<< ": sz=" << sz << " oid=" << fid_str << dendl;

  int64_t lid = m0_layout_find_by_objsz(store->instance, nullptr, sz);
  M0_ASSERT(lid > 0);

  M0_ASSERT(mobj == nullptr);
  mobj = new m0_obj();
  m0_obj_init(mobj, &store->container.co_realm, &meta.oid, lid);

  struct m0_op *op = nullptr;
  mobj->ob_entity.en_flags |= M0_ENF_META;
  rc = m0_entity_create(nullptr, &mobj->ob_entity, &op);
  if (rc != 0) {
    this->close_mobj();
    ldpp_dout(dpp, 0) << "ERROR: m0_entity_create() failed: " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 20) <<__func__<< ": call m0_op_launch()..." << dendl;
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    this->close_mobj();
    ldpp_dout(dpp, 0) << "ERROR: failed to create motr object: " << rc << dendl;
    return rc;
  }

  meta.layout_id = mobj->ob_attr.oa_layout_id;
  meta.pver      = mobj->ob_attr.oa_pver;
  ldpp_dout(dpp, 20) <<__func__<< ": lid=0x" << std::hex << meta.layout_id
                     << std::dec << " rc=" << rc << dendl;

  // TODO: add key:user+bucket+key+obj.meta.oid value:timestamp to
  // gc.queue.index. See more at github.com/Seagate/cortx-rgw/issues/7.

  return rc;
}

int MotrObject::open_mobj(const DoutPrefixProvider *dpp)
{
  char fid_str[M0_FID_STR_LEN];
  snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&meta.oid));
  ldpp_dout(dpp, 20) <<__func__<< ": oid=" << fid_str << dendl;

  int rc;
  if (meta.layout_id == 0) {
    rgw_bucket_dir_entry ent;
    rc = this->get_bucket_dir_ent(dpp, ent);
    if (rc < 0)
      return rc;
  }

  if (meta.layout_id == 0)
    return -ENOENT;

  M0_ASSERT(mobj == nullptr);
  mobj = new m0_obj();
  memset(mobj, 0, sizeof *mobj);
  m0_obj_init(mobj, &store->container.co_realm, &meta.oid, store->conf.mc_layout_id);

  struct m0_op *op = nullptr;
  mobj->ob_attr.oa_layout_id = meta.layout_id;
  mobj->ob_attr.oa_pver      = meta.pver;
  mobj->ob_entity.en_flags  |= M0_ENF_META;
  rc = m0_entity_open(&mobj->ob_entity, &op);
  if (rc != 0) {
    ldpp_dout(dpp, 0) << "ERROR: m0_entity_open() failed: rc=" << rc << dendl;
    this->close_mobj();
    return rc;
  }
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc < 0) {
    ldpp_dout(dpp, 10) << "ERROR: failed to open motr object: rc=" << rc << dendl;
    this->close_mobj();
    return rc;
  }

  ldpp_dout(dpp, 20) <<__func__<< ": rc=" << rc << dendl;

  return 0;
}

int MotrObject::delete_mobj(const DoutPrefixProvider *dpp)
{
  int rc;

  // Open the object.
  if (mobj == nullptr) {
    rc = this->open_mobj(dpp);
    if (rc < 0)
      return rc;
  }

  // Create an DELETE op and execute it (sync version).
  struct m0_op *op = nullptr;
  mobj->ob_entity.en_flags |= M0_ENF_META;
  rc = m0_entity_delete(&mobj->ob_entity, &op);
  if (rc != 0) {
    ldpp_dout(dpp, 0) << "ERROR: m0_entity_delete() failed: " << rc << dendl;
    return rc;
  }
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open motr object: " << rc << dendl;
    return rc;
  }

  this->close_mobj();

  return 0;
}

void MotrObject::close_mobj()
{
  if (mobj == nullptr)
    return;
  m0_obj_fini(mobj);
  delete mobj; mobj = nullptr;
}

int MotrObject::write_mobj(const DoutPrefixProvider *dpp, bufferlist&& data, uint64_t offset)
{
  int rc;
  unsigned bs, left;
  struct m0_op *op;
  char *start, *p;
  struct m0_bufvec buf;
  struct m0_bufvec attr;
  struct m0_indexvec ext;

  left = data.length();
  if (left == 0)
    return 0;

  rc = m0_bufvec_empty_alloc(&buf, 1) ?:
       m0_bufvec_alloc(&attr, 1, 1) ?:
       m0_indexvec_alloc(&ext, 1);
  if (rc != 0)
    goto out;

  bs = this->get_optimal_bs(left);
  ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs << dendl;

  start = data.c_str();

  for (p = start; left > 0; left -= bs, p += bs, offset += bs) {
    if (left < bs)
      bs = this->get_optimal_bs(left);
    if (left < bs) {
      data.append_zero(bs - left);
      left = bs;
      p = data.c_str();
    }
    buf.ov_buf[0] = p;
    buf.ov_vec.v_count[0] = bs;
    ext.iv_index[0] = offset;
    ext.iv_vec.v_count[0] = bs;
    attr.ov_vec.v_count[0] = 0;

    op = nullptr;
    rc = m0_obj_op(this->mobj, M0_OC_WRITE, &ext, &buf, &attr, 0, 0, &op);
    if (rc != 0)
      goto out;
    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0)
      goto out;
  }

out:
  m0_indexvec_free(&ext);
  m0_bufvec_free(&attr);
  m0_bufvec_free2(&buf);
  return rc;
}

int MotrObject::read_mobj(const DoutPrefixProvider* dpp, int64_t off, int64_t end, RGWGetDataCB* cb)
{
  int rc;
  unsigned bs, actual;
  struct m0_op *op;
  struct m0_bufvec buf;
  struct m0_bufvec attr;
  struct m0_indexvec ext;

  // make end pointer exclusive:
  // it's easier to work with it this way
  end++;
  ldpp_dout(dpp, 20) << "MotrObject::read_mobj(): off=" << off <<
                       " end=" << end << dendl;
  // As `off` may not be parity group size aligned, even using optimal
  // buffer block size, simply reading data from offset `off` could come
  // across parity group boundary. And Motr only allows page-size aligned
  // offset.
  //
  // The optimal size of each IO should also take into account the data
  // transfer size to s3 client. For example, 16MB may be nice to read
  // data from motr, but it could be too big for network transfer.
  //
  // TODO: We leave proper handling of offset in the future.
  bs = this->get_optimal_bs(end - off);
  ldpp_dout(dpp, 20) << "MotrObject::read_mobj(): bs=" << bs << dendl;

  rc = m0_bufvec_empty_alloc(&buf, 1) ? :
       m0_bufvec_alloc(&attr, 1, 1) ? :
       m0_indexvec_alloc(&ext, 1);
  if (rc < 0)
    goto out;

  actual = bs;
  for (; off < end; off += actual) {
    if (end - off < bs)
        actual = end - off;
    ldpp_dout(dpp, 20) << "MotrObject::read_mobj(): off=" << off <<
                                            " actual=" << actual << dendl;
    bufferlist bl;
    buf.ov_buf[0] = bl.append_hole(bs).c_str();
    buf.ov_vec.v_count[0] = bs;
    ext.iv_index[0] = off;
    ext.iv_vec.v_count[0] = bs;
    attr.ov_vec.v_count[0] = 0;

    // Read from Motr.
    op = nullptr;
    rc = m0_obj_op(this->mobj, M0_OC_READ, &ext, &buf, &attr, 0, 0, &op);
    ldpp_dout(dpp, 20) << "MotrObject::read_mobj(): init read op rc=" << rc << dendl;
    if (rc != 0)
      goto out;
    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0)
      goto out;

    // Call `cb` to process returned data.
    ldpp_dout(dpp, 20) << "MotrObject::read_mobj(): call cb to process data" << dendl;
    cb->handle_data(bl, off, actual);
  }

out:
  m0_indexvec_free(&ext);
  m0_bufvec_free(&attr);
  m0_bufvec_free2(&buf);
  this->close_mobj();

  return rc;
}

int MotrObject::get_bucket_dir_ent(const DoutPrefixProvider *dpp, rgw_bucket_dir_entry& ent)
{
  int rc = 0;
  string bucket_index_iname = "motr.rgw.bucket.index." + this->get_bucket()->get_name();
  int max = 1000;
  vector<string> keys(max);
  vector<bufferlist> vals(max);
  bufferlist bl;
  bufferlist::const_iterator iter;

  if (this->get_bucket()->get_info().versioning_status() == BUCKET_VERSIONED ||
      this->get_bucket()->get_info().versioning_status() == BUCKET_SUSPENDED) {

    rgw_bucket_dir_entry ent_to_check;

    if (this->store->get_obj_meta_cache()->get(dpp, this->get_name(), bl) == 0) {
      iter = bl.cbegin();
      ent_to_check.decode(iter);
      if (ent_to_check.is_current()) {
        ent = ent_to_check;
        rc = 0;
        goto out;
      }
    }

    ldpp_dout(dpp, 20) <<__func__<< ": versioned bucket!" << dendl;
    keys[0] = this->get_name();
    rc = store->next_query_by_name(bucket_index_iname, keys, vals);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
      return rc;
    }

    rc = -ENOENT;
    for (const auto& bl: vals) {
      if (bl.length() == 0)
        break;

      iter = bl.cbegin();
      ent_to_check.decode(iter);
      if (ent_to_check.is_current()) {
        ldpp_dout(dpp, 20) <<__func__<< ": found current version!" << dendl;
        ent = ent_to_check;
        rc = 0;

        this->store->get_obj_meta_cache()->put(dpp, this->get_name(), bl);

        break;
      }
    }
  } else {
    if (this->store->get_obj_meta_cache()->get(dpp, this->get_key().to_str(), bl)) {
      ldpp_dout(dpp, 20) <<__func__<< ": non-versioned bucket!" << dendl;
      rc = this->store->do_idx_op_by_name(bucket_index_iname,
                                          M0_IC_GET, this->get_key().to_str(), bl);
      if (rc < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to get object's entry from bucket index: rc="
                          << rc << dendl;
        return rc;
      }
      this->store->get_obj_meta_cache()->put(dpp, this->get_key().to_str(), bl);
    }

    bufferlist& blr = bl;
    iter = blr.cbegin();
    ent.decode(iter);
  }

out:
  if (rc == 0) {
    sal::Attrs dummy;
    decode(dummy, iter);
    meta.decode(iter);
    ldpp_dout(dpp, 20) <<__func__<< ": lid=0x" << std::hex << meta.layout_id << dendl;
  } else
    ldpp_dout(dpp, 0) <<__func__<< ": rc=" << rc << dendl;

  return rc;
}

int MotrObject::update_version_entries(const DoutPrefixProvider *dpp)
{
  int rc;
  int max = 10;
  vector<string> keys(max);
  vector<bufferlist> vals(max);

  string bucket_index_iname = "motr.rgw.bucket.index." + this->get_bucket()->get_name();
  keys[0] = this->get_name();
  rc = store->next_query_by_name(bucket_index_iname, keys, vals);
  ldpp_dout(dpp, 20) << "get all versions, name = " << this->get_name() << "rc = " << rc << dendl;
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  // no entries returned.
  if (rc == 0)
    return 0;

  for (const auto& bl: vals) {
    if (bl.length() == 0)
      break;

    rgw_bucket_dir_entry ent;
    auto iter = bl.cbegin();
    ent.decode(iter);

    if (0 != ent.key.name.compare(0, this->get_name().size(), this->get_name()))
      continue;

    if (!ent.is_current())
      continue;

    // Remove from the cache.
    store->get_obj_meta_cache()->remove(dpp, this->get_name());

    rgw::sal::Attrs attrs;
    decode(attrs, iter);
    MotrObject::Meta meta;
    meta.decode(iter);

    ent.flags = rgw_bucket_dir_entry::FLAG_VER;
    string key;
    if (ent.key.instance.empty())
      key = ent.key.name;
    else {
      char buf[ent.key.name.size() + ent.key.instance.size() + 16];
      snprintf(buf, sizeof(buf), "%s[%s]", ent.key.name.c_str(), ent.key.instance.c_str());
      key = buf;
    }
    ldpp_dout(dpp, 20) << "update one version, key = " << key << dendl;
    bufferlist ent_bl;
    ent.encode(ent_bl);
    encode(attrs, ent_bl);
    meta.encode(ent_bl);

    rc = store->do_idx_op_by_name(bucket_index_iname,
                                  M0_IC_PUT, key, ent_bl);
    if (rc < 0)
      break;
  }
  return rc;
}

// Scan object_nnn_part_index to get all parts then open their motr objects.
// TODO: all parts are opened in the POC. But for a large object, for example
// a 5GB object will have about 300 parts (for default 15MB part). A better
// way of managing opened object may be needed.
int MotrObject::get_part_objs(const DoutPrefixProvider* dpp,
                              std::map<int, std::unique_ptr<MotrObject>>& part_objs)
{
  int rc;
  int max_parts = 1000;
  int marker = 0;
  uint64_t off = 0;
  bool truncated = false;
  std::unique_ptr<rgw::sal::MultipartUpload> upload;

  upload = this->get_bucket()->get_multipart_upload(this->get_name(), string());

  do {
    rc = upload->list_parts(dpp, store->ctx(), max_parts, marker, &marker, &truncated);
    if (rc == -ENOENT) {
      rc = -ERR_NO_SUCH_UPLOAD;
    }
    if (rc < 0)
      return rc;

    std::map<uint32_t, std::unique_ptr<MultipartPart>>& parts = upload->get_parts();
    for (auto part_iter = parts.begin(); part_iter != parts.end(); ++part_iter) {

      MultipartPart *mpart = part_iter->second.get();
      MotrMultipartPart *mmpart = static_cast<MotrMultipartPart *>(mpart);
      uint32_t part_num = mmpart->get_num();
      uint64_t part_size = mmpart->get_size();

      string part_obj_name = this->get_bucket()->get_name() + "." +
 	                     this->get_key().to_str() +
	                     ".part." + std::to_string(part_num);
      std::unique_ptr<rgw::sal::Object> obj;
      obj = this->bucket->get_object(rgw_obj_key(part_obj_name));
      std::unique_ptr<rgw::sal::MotrObject> mobj(static_cast<rgw::sal::MotrObject *>(obj.release()));

      ldpp_dout(dpp, 20) << "get_part_objs: off = " << off << ", size = " << part_size << dendl;
      mobj->part_off = off;
      mobj->part_size = part_size;
      mobj->part_num = part_num;
      mobj->meta = mmpart->meta;

      part_objs.emplace(part_num, std::move(mobj));

      off += part_size;
    }
  } while (truncated);

  return 0;
}

int MotrObject::open_part_objs(const DoutPrefixProvider* dpp,
                               std::map<int, std::unique_ptr<MotrObject>>& part_objs)
{
  //for (auto& iter: part_objs) {
  for (auto iter = part_objs.begin(); iter != part_objs.end(); ++iter) {
    MotrObject* obj = static_cast<MotrObject *>(iter->second.get());
    ldpp_dout(dpp, 20) << "open_part_objs: name = " << obj->get_name() << dendl;
    int rc = obj->open_mobj(dpp);
    if (rc < 0)
      return rc;
  }

  return 0;
}

int MotrObject::delete_part_objs(const DoutPrefixProvider* dpp)
{
  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  upload = this->get_bucket()->get_multipart_upload(this->get_name(), string());
  std::unique_ptr<rgw::sal::MotrMultipartUpload> mupload(static_cast<rgw::sal::MotrMultipartUpload *>(upload.release()));
  return mupload->delete_parts(dpp);
}

int MotrObject::read_multipart_obj(const DoutPrefixProvider* dpp,
                                   int64_t off, int64_t end, RGWGetDataCB* cb,
				   std::map<int, std::unique_ptr<MotrObject>>& part_objs)
{
  int64_t cursor = off;

  ldpp_dout(dpp, 20) << "read_multipart_obj: off=" << off << " end=" << end << dendl;

  // Find the parts which are in the (off, end) range and
  // read data from it. Note: `end` argument is inclusive.
  for (auto iter = part_objs.begin(); iter != part_objs.end(); ++iter) {
    MotrObject* obj = static_cast<MotrObject *>(iter->second.get());
    int64_t part_off = obj->part_off;
    int64_t part_size = obj->part_size;
    int64_t part_end = obj->part_off + obj->part_size - 1;
    ldpp_dout(dpp, 20) << "read_multipart_obj: part_off=" << part_off
                                          << " part_end=" << part_end << dendl;
    if (part_end < off)
      continue;

    int64_t local_off = cursor - obj->part_off;
    int64_t local_end = part_end < end? part_size - 1 : end - part_off;
    ldpp_dout(dpp, 20) << "real_multipart_obj: name=" << obj->get_name()
                                          << " local_off=" << local_off
                                          << " local_end=" << local_end << dendl;
    int rc = obj->read_mobj(dpp, local_off, local_end, cb);
    if (rc < 0)
        return rc;

    cursor = part_end + 1;
    if (cursor > end)
      break;
  }

  return 0;
}

static unsigned roundup(unsigned x, unsigned by)
{
  return ((x - 1) / by + 1) * by;
}

unsigned MotrObject::get_optimal_bs(unsigned len)
{
  struct m0_pool_version *pver;

  pver = m0_pool_version_find(&store->instance->m0c_pools_common,
                              &mobj->ob_attr.oa_pver);
  M0_ASSERT(pver != nullptr);
  struct m0_pdclust_attr *pa = &pver->pv_attr;
  uint64_t lid = M0_OBJ_LAYOUT_ID(meta.layout_id);
  unsigned unit_sz = m0_obj_layout_id_to_unit_size(lid);
  unsigned grp_sz  = unit_sz * pa->pa_N;

  // bs should be max 4-times pool-width deep counting by 1MB units, or
  // 8-times deep counting by 512K units, 16-times deep by 256K units,
  // and so on. Several units to one target will be aggregated to make
  // fewer network RPCs, disk i/o operations and BE transactions.
  // For unit sizes of 32K or less, the depth is 128, which
  // makes it 32K * 128 == 4MB - the maximum amount per target when
  // the performance is still good on LNet (which has max 1MB frames).
  // TODO: it may be different on libfabric, should be re-measured.
  unsigned depth = 128 / ((unit_sz + 0x7fff) / 0x8000);
  if (depth == 0)
    depth = 1;
  // P * N / (N + K + S) - number of data units to span the pool-width
  unsigned max_bs = depth * unit_sz * pa->pa_P * pa->pa_N /
                                     (pa->pa_N + pa->pa_K + pa->pa_S);
  max_bs = roundup(max_bs, grp_sz); // multiple of group size
  if (len >= max_bs)
    return max_bs;
  else if (len <= grp_sz)
    return grp_sz;
  else
    return roundup(len, grp_sz);
}

void MotrAtomicWriter::cleanup()
{
  m0_indexvec_free(&ext);
  m0_bufvec_free(&attr);
  m0_bufvec_free2(&buf);
  acc_data.clear();
  obj.close_mobj();
}

unsigned MotrAtomicWriter::populate_bvec(unsigned len, bufferlist::iterator &bi)
{
  unsigned i, l, done = 0;
  const char *data;

  for (i = 0; i < MAX_BUFVEC_NR && len > 0; ++i) {
    l = bi.get_ptr_and_advance(len, &data);
    buf.ov_buf[i] = (char*)data;
    buf.ov_vec.v_count[i] = l;
    ext.iv_index[i] = acc_off;
    ext.iv_vec.v_count[i] = l;
    attr.ov_vec.v_count[i] = 0;
    acc_off += l;
    len -= l;
    done += l;
  }
  buf.ov_vec.v_nr = i;
  ext.iv_vec.v_nr = i;

  return done;
}

int MotrAtomicWriter::write()
{
  int rc;
  unsigned bs, left;
  struct m0_op *op;
  bufferlist::iterator bi;

  left = acc_data.length();

  if (!obj.is_opened()) {
    rc = obj.create_mobj(dpp, left);
    if (rc == -EEXIST)
      rc = obj.open_mobj(dpp);
    if (rc != 0) {
      char fid_str[M0_FID_STR_LEN];
      snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&obj.meta.oid));
      ldpp_dout(dpp, 0) << "ERROR: failed to create/open motr object "
                        << fid_str << " (" << obj.get_bucket()->get_name()
                        << "/" << obj.get_key().to_str() << "): rc=" << rc
                        << dendl;
      goto err;
    }
  }

  total_data_size += left;

  bs = obj.get_optimal_bs(left);
  ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs << dendl;

  bi = acc_data.begin();
  while (left > 0) {
    if (left < bs)
      bs = obj.get_optimal_bs(left);
    if (left < bs) {
      acc_data.append_zero(bs - left);
      auto off = bi.get_off();
      bufferlist tmp;
      acc_data.splice(off, bs, &tmp);
      acc_data.clear();
      acc_data.append(tmp.c_str(), bs); // make it a single buf
      bi = acc_data.begin();
      left = bs;
    }

    left -= this->populate_bvec(bs, bi);

    op = nullptr;
    rc = m0_obj_op(obj.mobj, M0_OC_WRITE, &ext, &buf, &attr, 0, 0, &op);
    if (rc != 0)
      goto err;
    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0)
      goto err;
  }
  acc_data.clear();

  return 0;

err:
  this->cleanup();
  return rc;
}

static const unsigned MAX_ACC_SIZE = 32 * 1024 * 1024;

// Accumulate enough data first to make a reasonable decision about the
// optimal unit size for a new object, or bs for existing object (32M seems
// enough for 4M units in 8+2 parity groups, a common config on wide pools),
// and then launch the write operations.
int MotrAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  if (data.length() == 0) { // last call, flush data
    int rc = 0;
    if (acc_data.length() != 0)
      rc = this->write();
    this->cleanup();
    return rc;
  }

  if (acc_data.length() == 0)
    acc_off = offset;

  acc_data.append(std::move(data));
  if (acc_data.length() < MAX_ACC_SIZE)
    return 0;

  return this->write();
}

int MotrAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  int rc = 0;

  if (acc_data.length() != 0) { // check again, just in case
    rc = this->write();
    this->cleanup();
    if (rc != 0)
      return rc;
  }

  bufferlist bl;
  rgw_bucket_dir_entry ent;

  // Set rgw_bucet_dir_entry. Some of the member of this structure may not
  // apply to motr. For example the storage_class.
  //
  // Checkout AtomicObjectProcessor::complete() in rgw_putobj_processor.cc
  // and RGWRados::Object::Write::write_meta() in rgw_rados.cc for what and
  // how to set the dir entry. Only set the basic ones for POC, no ACLs and
  // other attrs.
  obj.get_key().get_index_key(&ent.key);
  ent.meta.size = total_data_size;
  ent.meta.accounted_size = total_data_size;
  ent.meta.mtime = real_clock::is_zero(set_mtime)? ceph::real_clock::now() : set_mtime;
  ent.meta.etag = etag;
  ent.meta.owner = owner.to_str();
  ent.meta.owner_display_name = obj.get_bucket()->get_owner()->get_display_name();
  bool is_versioned = obj.get_key().have_instance();
  if (is_versioned)
    ent.flags = rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ldpp_dout(dpp, 20) <<__func__<< ": key=" << obj.get_key().to_str()
                    << " etag: " << etag << " user_data=" << user_data << dendl;
  if (user_data)
    ent.meta.user_data = *user_data;
  ent.encode(bl);

  RGWBucketInfo &info = obj.get_bucket()->get_info();
  if (info.obj_lock_enabled() && info.obj_lock.has_rule()) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date = info.obj_lock.get_lock_until_date(ent.meta.mtime);
      string mode = info.obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist retention_bl;
      obj_retention.encode(retention_bl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = retention_bl;
    }
  }
  encode(attrs, bl);
  obj.meta.encode(bl);
  ldpp_dout(dpp, 20) <<__func__<< ": lid=0x" << std::hex << obj.meta.layout_id
                                                           << dendl;
  if (is_versioned) {
    // get the list of all versioned objects with the same key and
    // unset their FLAG_CURRENT later, if do_idx_op_by_name() is successful.
    // Note: without distributed lock on the index - it is possible that 2
    // CURRENT entries would appear in the bucket. For example, consider the
    // following scenario when two clients are trying to add the new object
    // version concurrently:
    //   client 1: reads all the CURRENT entries
    //   client 2: updates the index and sets the new CURRENT
    //   client 1: updates the index and sets the new CURRENT
    // At the step (1) client 1 would not see the new current record from step (2),
    // so it won't update it. As a result, two CURRENT version entries will appear
    // in the bucket.
    // TODO: update the current version (unset the flag) and insert the new current
    // version can be launched in one motr op. This requires change at do_idx_op()
    // and do_idx_op_by_name().
    rc = obj.update_version_entries(dpp);
    if (rc < 0)
      return rc;
  }
  // Insert an entry into bucket index.
  string bucket_index_iname = "motr.rgw.bucket.index." + obj.get_bucket()->get_name();
  rc = store->do_idx_op_by_name(bucket_index_iname,
                                M0_IC_PUT, obj.get_key().to_str(), bl);
  if (rc == 0)
    store->get_obj_meta_cache()->put(dpp, obj.get_key().to_str(), bl);

  return rc;
}

int MotrMultipartUpload::delete_parts(const DoutPrefixProvider *dpp)
{
  int rc;
  int max_parts = 1000;
  int marker = 0;
  bool truncated = false;

  // Scan all parts and delete the corresponding motr objects.
  do {
    rc = this->list_parts(dpp, store->ctx(), max_parts, marker, &marker, &truncated);
    if (rc == -ENOENT) {
      rc = -ERR_NO_SUCH_UPLOAD;
    }
    if (rc < 0)
      return rc;

    std::map<uint32_t, std::unique_ptr<MultipartPart>>& parts = this->get_parts();
    for (auto part_iter = parts.begin(); part_iter != parts.end(); ++part_iter) {

      MultipartPart *mpart = part_iter->second.get();
      MotrMultipartPart *mmpart = static_cast<MotrMultipartPart *>(mpart);
      uint32_t part_num = mmpart->get_num();

      // Delete the part object. Note that the part object is  not
      // inserted into bucket index, only the corresponding motr object
      // needs to be delete. That is why we don't call
      // MotrObject::delete_object().
      string part_obj_name = bucket->get_name() + "." +
 	                     mp_obj.get_key() +
	                     ".part." + std::to_string(part_num);
      std::unique_ptr<rgw::sal::Object> obj;
      obj = this->bucket->get_object(rgw_obj_key(part_obj_name));
      std::unique_ptr<rgw::sal::MotrObject> mobj(static_cast<rgw::sal::MotrObject *>(obj.release()));
      rc = mobj->delete_mobj(dpp);
      if (rc < 0) {
        ldpp_dout(dpp, 0) << "Failed to delete the object from Motr. " << dendl;
        return rc;
      }
    }
  } while (truncated);

  // Delete object part index.
  std::string oid = mp_obj.get_key();
  string obj_part_iname = "motr.rgw.object." + bucket->get_name() + "." + oid + ".parts";
  return store->delete_motr_idx_by_name(obj_part_iname);
}

int MotrMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct,
                                RGWObjectCtx *obj_ctx)
{
  int rc;

  // Scan all parts and delete the corresponding motr objects.
  rc = this->delete_parts(dpp);
  if (rc < 0)
    return rc;

  // Remove the upload from bucket multipart index.
  bufferlist bl;
  std::unique_ptr<rgw::sal::Object> meta_obj;
  meta_obj = get_meta_obj();
  string bucket_multipart_iname =
      "motr.rgw.bucket." + meta_obj->get_bucket()->get_name() + ".multiparts";
  return store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_DEL, meta_obj->get_key().to_str(), bl);
  return 0;
}

std::unique_ptr<rgw::sal::Object> MotrMultipartUpload::get_meta_obj()
{
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(get_meta(), string(), mp_ns));
  std::unique_ptr<rgw::sal::MotrObject> mobj(static_cast<rgw::sal::MotrObject *>(obj.release()));
  mobj->set_category(RGWObjCategory::MultiMeta);
  return mobj;
}

struct motr_multipart_upload_info
{
  rgw_placement_rule dest_placement;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(dest_placement, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(dest_placement, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(motr_multipart_upload_info)

int MotrMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
                              RGWObjectCtx* obj_ctx, ACLOwner& _owner,
			      rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs)
{
  int rc;
  std::string oid = mp_obj.get_key();

  owner = _owner;

  do {
    char buf[33];
    string tmp_obj_name;
    gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
    std::string upload_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
    upload_id.append(buf);

    mp_obj.init(oid, upload_id);
    tmp_obj_name = mp_obj.get_meta();

    std::unique_ptr<rgw::sal::Object> obj;
    obj = bucket->get_object(rgw_obj_key(tmp_obj_name, string(), mp_ns));
    // the meta object will be indexed with 0 size, we c
    obj->set_in_extra_data(true);
    obj->set_hash_source(oid);

    motr_multipart_upload_info upload_info;
    upload_info.dest_placement = dest_placement;
    bufferlist mpbl;
    encode(upload_info, mpbl);

    // Create an initial entry in the bucket. The entry will be
    // updated when multipart upload is completed, for example,
    // size, etag etc.
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    obj->get_key().get_index_key(&ent.key);
    ent.meta.owner = owner.get_id().to_str();
    ent.meta.category = RGWObjCategory::MultiMeta;
    ent.meta.mtime = ceph::real_clock::now();
    ent.meta.user_data.assign(mpbl.c_str(), mpbl.c_str() + mpbl.length());
    ent.encode(bl);

    // Insert an entry into bucket multipart index so it is not shown
    // when listing a bucket.
    string bucket_multipart_iname =
      "motr.rgw.bucket." + obj->get_bucket()->get_name() + ".multiparts";
    rc = store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_PUT, obj->get_key().to_str(), bl);

  } while (rc == -EEXIST);

  if (rc < 0)
    return rc;

  // Create object part index.
  // TODO: add bucket as part of the name.
  string obj_part_iname = "motr.rgw.object." + bucket->get_name() + "." + oid + ".parts";
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::init(): object part index=" << obj_part_iname << dendl;
  rc = store->create_motr_idx_by_name(obj_part_iname);
  if (rc == -EEXIST)
    rc = 0;
  if (rc < 0)
    // TODO: clean the bucket index entry
    ldpp_dout(dpp, 0) << "Failed to create object multipart index  " << obj_part_iname << dendl;

  return rc;
}

int MotrMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				     int num_parts, int marker,
				     int *next_marker, bool *truncated,
				     bool assume_unsorted)
{
  int rc;
  vector<string> key_vec(num_parts);
  vector<bufferlist> val_vec(num_parts);

  std::string oid = mp_obj.get_key();
  string obj_part_iname = "motr.rgw.object." + bucket->get_name() + "." + oid + ".parts";
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::list_parts(): object part index = " << obj_part_iname << dendl;
  key_vec[0].clear();
  key_vec[0] = "part.";
  char buf[32];
  snprintf(buf, sizeof(buf), "%08d", marker + 1);
  key_vec[0].append(buf);
  rc = store->next_query_by_name(obj_part_iname, key_vec, val_vec);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  int last_num = 0;
  int part_cnt = 0;
  uint32_t expected_next = marker + 1;
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::list_parts(): marker = " << marker << dendl;
  for (const auto& bl: val_vec) {
    if (bl.length() == 0)
      break;

    ldpp_dout(dpp, 20) << "MotrMultipartUpload::list_parts(): get part_info "  << dendl;
    RGWUploadPartInfo info;
    auto iter = bl.cbegin();
    info.decode(iter);
    rgw::sal::Attrs attrs_dummy;
    decode(attrs_dummy, iter);
    MotrObject::Meta meta;
    meta.decode(iter);

    ldpp_dout(dpp, 20) << "MotrMultipartUpload::list_parts(): part_num=" << info.num
                                             << " part_size=" << info.size << dendl;
    if (info.num != expected_next)
      return -EINVAL;

    if ((int)info.num > marker) {
      last_num = info.num;
      parts.emplace(info.num, std::make_unique<MotrMultipartPart>(info, meta));
    }

    part_cnt++;
    expected_next++;
  }

  // Does it have more parts?
  if (truncated)
    *truncated = part_cnt < num_parts? false : true;
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::list_parts(): truncated=" << *truncated << dendl;

  if (next_marker)
    *next_marker = last_num;

  return 0;
}

// Heavily copy from rgw_sal_rados.cc
int MotrMultipartUpload::complete(const DoutPrefixProvider *dpp,
				   optional_yield y, CephContext* cct,
				   map<int, string>& part_etags,
				   list<rgw_obj_index_key>& remove_objs,
				   uint64_t& accounted_size, bool& compressed,
				   RGWCompressionInfo& cs_info, off_t& off,
				   std::string& tag, ACLOwner& owner,
				   uint64_t olh_epoch,
				   rgw::sal::Object* target_obj,
				   RGWObjectCtx* obj_ctx)
{
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int rc;

  ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): enter" << dendl;
  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs attrs = target_obj->get_attrs();

  do {
    ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): list_parts()" << dendl;
    rc = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
    if (rc == -ENOENT) {
      rc = -ERR_NO_SUCH_UPLOAD;
    }
    if (rc < 0)
      return rc;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
      rc = -ERR_INVALID_PART;
      return rc;
    }
    ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): parts.size()=" << parts.size() << dendl;

    for (auto obj_iter = parts.begin();
         etags_iter != part_etags.end() && obj_iter != parts.end();
	 ++etags_iter, ++obj_iter, ++handled_parts) {
      MultipartPart *mpart = obj_iter->second.get();
      MotrMultipartPart *mmpart = static_cast<MotrMultipartPart *>(mpart);
      RGWUploadPartInfo *part = &mmpart->info;

      uint64_t part_size = part->accounted_size;
      ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): part_size=" << part_size << dendl;
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        rc = -ERR_TOO_SMALL;
        return rc;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
			 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      }
      string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->etag) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      }

      hex_to_buf(part->etag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));
      ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): calc etag " << dendl;

      string oid = mp_obj.get_part(part->num);
      rgw_obj src_obj;
      src_obj.init_ns(bucket->get_key(), oid, mp_ns);

#if 0 // does Motr backend need it?
      /* update manifest for part */
      if (part->manifest.empty()) {
        ldpp_dout(dpp, 0) << "ERROR: empty manifest for object part: obj="
			 << src_obj << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      } else {
        manifest.append(dpp, part->manifest, store->get_zone());
      }
      ldpp_dout(dpp, 0) << "MotrMultipartUpload::complete(): manifest " << dendl;
#endif

      bool part_compressed = (part->cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != part->cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << part->cs_info.compression_type << ")" << dendl;
          rc = -ERR_INVALID_PART;
          return rc;
      }

      ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): part compression" << dendl;
      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : part->cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = part->cs_info.compression_type;
        cs_info.orig_size += part->cs_info.orig_size;
        compressed = true;
      }

      // We may not need to do the following as remove_objs are those
      // don't show when listing a bucket. As we store in-progress uploaded
      // object's metadata in a separate index, they are not shown when
      // listing a bucket.
      rgw_obj_index_key remove_key;
      src_obj.key.get_index_key(&remove_key);
      remove_objs.push_back(remove_key);

      off += part_size;
      accounted_size += part->accounted_size;
      ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): off=" << off << ", accounted_size = " << accounted_size << dendl;
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	   sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)part_etags.size());
  etag = final_etag_str;
  ldpp_dout(dpp, 20) << "calculated etag: " << etag << dendl;
  etag_bl.append(etag);
  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  // Read the object's the multipart_upload_info.
  // TODO: all those index name and key  constructions should be implemented as
  // member functions.
  bufferlist bl;
  std::unique_ptr<rgw::sal::Object> meta_obj;
  meta_obj = get_meta_obj();
  string bucket_multipart_iname =
      "motr.rgw.bucket." + meta_obj->get_bucket()->get_name() + ".multiparts";
  rc = this->store->do_idx_op_by_name(bucket_multipart_iname,
                                      M0_IC_GET, meta_obj->get_key().to_str(), bl);
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): read entry from bucket multipart index rc=" << rc << dendl;
  if (rc < 0)
    return rc;
  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto ent_iter = blr.cbegin();
  ent.decode(ent_iter);

  // Update the dir entry and insert it to the bucket index so
  // the object will be seen when listing the bucket.
  bufferlist update_bl;
  target_obj->get_key().get_index_key(&ent.key);  // Change to offical name :)
  ent.meta.size = off;
  ent.meta.accounted_size = accounted_size;
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): obj size=" << ent.meta.size
                           << " obj accounted size=" << ent.meta.accounted_size << dendl;
  ent.meta.mtime = ceph::real_clock::now();
  ent.meta.etag = etag;
  ent.encode(update_bl);
  encode(attrs, update_bl);
  MotrObject::Meta meta_dummy;
  meta_dummy.encode(update_bl);

  string bucket_index_iname = "motr.rgw.bucket.index." + meta_obj->get_bucket()->get_name();
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): target_obj name=" << target_obj->get_name()
                                  << " target_obj oid=" << target_obj->get_oid() << dendl;
  rc = store->do_idx_op_by_name(bucket_index_iname, M0_IC_PUT,
                                target_obj->get_name(), update_bl);
  if (rc < 0)
    return rc;

  // Put into metadata cache.
  store->get_obj_meta_cache()->put(dpp, target_obj->get_name(), update_bl);

  // Now we can remove it from bucket multipart index.
  ldpp_dout(dpp, 20) << "MotrMultipartUpload::complete(): remove from bucket multipartindex " << dendl;
  return store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_DEL, meta_obj->get_key().to_str(), bl);
}

int MotrMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y, RGWObjectCtx* obj_ctx, rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
{
  if (!rule && !attrs) {
    return 0;
  }

  if (rule) {
    if (!placement.empty()) {
      *rule = &placement;
      if (!attrs) {
        /* Don't need attrs, done */
        return 0;
      }
    } else {
      *rule = nullptr;
    }
  }

  std::unique_ptr<rgw::sal::Object> meta_obj;
  meta_obj = get_meta_obj();
  meta_obj->set_in_extra_data(true);

  // Read the object's the multipart_upload_info.
  bufferlist bl;
  string bucket_multipart_iname =
      "motr.rgw.bucket." + meta_obj->get_bucket()->get_name() + ".multiparts";
  int rc = this->store->do_idx_op_by_name(bucket_multipart_iname,
                                          M0_IC_GET, meta_obj->get_key().to_str(), bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "Failed to get object's entry from bucket index. " << dendl;
    return rc;
  }

  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto ent_iter = blr.cbegin();
  ent.decode(ent_iter);

  if (attrs) {
    bufferlist etag_bl;
    string& etag = ent.meta.etag;
    ldpp_dout(dpp, 20) << "object's etag:  " << ent.meta.etag << dendl;
    etag_bl.append(etag.c_str(), etag.size());
    attrs->emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));
    if (!rule || *rule != nullptr) {
      /* placement was cached; don't actually read */
      return 0;
    }
  }

  /* Decode multipart_upload_info */
  motr_multipart_upload_info upload_info;
  bufferlist mpbl;
  mpbl.append(ent.meta.user_data.c_str(), ent.meta.user_data.size());
  auto mpbl_iter = mpbl.cbegin();
  upload_info.decode(mpbl_iter);
  placement = upload_info.dest_placement;
  *rule = &placement;

  return 0;
}

std::unique_ptr<Writer> MotrMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  return std::make_unique<MotrMultipartWriter>(dpp, y, this,
				 std::move(_head_obj), store, owner,
				 obj_ctx, ptail_placement_rule, part_num, part_num_str);
}

int MotrMultipartWriter::prepare(optional_yield y)
{
  string part_obj_name = head_obj->get_bucket()->get_name() + "." +
	                 head_obj->get_key().to_str() +
	                 ".part." + std::to_string(part_num);
  ldpp_dout(dpp, 20) << "bucket=" << head_obj->get_bucket()->get_name() << "part_obj_name=" << part_obj_name << dendl;
  part_obj = std::make_unique<MotrObject>(this->store, rgw_obj_key(part_obj_name), head_obj->get_bucket());
  if (part_obj == nullptr)
    return -ENOMEM;

  // s3 client may retry uploading part, so the part may have already
  // been created.
  int rc = part_obj->create_mobj(dpp, store->cctx->_conf->rgw_max_chunk_size);
  if (rc == -EEXIST) {
    rc = part_obj->open_mobj(dpp);
    if (rc < 0)
      return rc;
  }
  return rc;
}

int MotrMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  int rc = part_obj->write_mobj(dpp, std::move(data), offset);
  if (rc == 0) {
    actual_part_size += data.length();
    ldpp_dout(dpp, 20) << " write_mobj(): actual_part_size=" << actual_part_size << dendl;
  }
  return rc;
}

int MotrMultipartWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  // Should the dir entry(object metadata) be updated? For example
  // mtime.

  ldpp_dout(dpp, 20) << "MotrMultipartWriter::complete(): enter" << dendl;
  // Add an entry into object_nnn_part_index.
  bufferlist bl;
  RGWUploadPartInfo info;
  info.num = part_num;
  info.etag = etag;
  info.size = actual_part_size;
  info.accounted_size = accounted_size;
  info.modified = real_clock::now();

  bool compressed;
  int rc = rgw_compression_info_from_attrset(attrs, compressed, info.cs_info);
  ldpp_dout(dpp, 20) << "MotrMultipartWriter::complete(): compression rc=" << rc << dendl;
  if (rc < 0) {
    ldpp_dout(dpp, 1) << "cannot get compression info" << dendl;
    return rc;
  }
  encode(info, bl);
  encode(attrs, bl);
  part_obj->meta.encode(bl);

  string p = "part.";
  char buf[32];
  snprintf(buf, sizeof(buf), "%08d", (int)part_num);
  p.append(buf);
  string obj_part_iname = "motr.rgw.object." + head_obj->get_bucket()->get_name() + "." +
	                  head_obj->get_key().to_str() + ".parts";
  ldpp_dout(dpp, 20) << "MotrMultipartWriter::complete(): object part index = " << obj_part_iname << dendl;
  rc = store->do_idx_op_by_name(obj_part_iname, M0_IC_PUT, p, bl);
  if (rc < 0) {
    return rc == -ENOENT ? -ERR_NO_SUCH_UPLOAD : rc;
  }

  return 0;
}

std::unique_ptr<RGWRole> MotrStore::get_role(std::string name,
    std::string tenant,
    std::string path,
    std::string trust_policy,
    std::string max_session_duration_str,
    std::multimap<std::string,std::string> tags)
{
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> MotrStore::get_role(std::string id)
{
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

int MotrStore::get_roles(const DoutPrefixProvider *dpp,
    optional_yield y,
    const std::string& path_prefix,
    const std::string& tenant,
    vector<std::unique_ptr<RGWRole>>& roles)
{
  return 0;
}

std::unique_ptr<RGWOIDCProvider> MotrStore::get_oidc_provider()
{
  RGWOIDCProvider* p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int MotrStore::get_oidc_providers(const DoutPrefixProvider *dpp,
    const std::string& tenant,
    vector<std::unique_ptr<RGWOIDCProvider>>& providers)
{
  return 0;
}

std::unique_ptr<MultipartUpload> MotrBucket::get_multipart_upload(const std::string& oid,
                                std::optional<std::string> upload_id,
                                ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<MotrMultipartUpload>(store, this, oid, upload_id, owner, mtime);
}

std::unique_ptr<Writer> MotrStore::get_append_writer(const DoutPrefixProvider *dpp,
        optional_yield y,
        std::unique_ptr<rgw::sal::Object> _head_obj,
        const rgw_user& owner, RGWObjectCtx& obj_ctx,
        const rgw_placement_rule *ptail_placement_rule,
        const std::string& unique_tag,
        uint64_t position,
        uint64_t *cur_accounted_size) {
  return nullptr;
}

std::unique_ptr<Writer> MotrStore::get_atomic_writer(const DoutPrefixProvider *dpp,
        optional_yield y,
        std::unique_ptr<rgw::sal::Object> _head_obj,
        const rgw_user& owner, RGWObjectCtx& obj_ctx,
        const rgw_placement_rule *ptail_placement_rule,
        uint64_t olh_epoch,
        const std::string& unique_tag) {
  return std::make_unique<MotrAtomicWriter>(dpp, y,
                  std::move(_head_obj), this, owner, obj_ctx,
                  ptail_placement_rule, olh_epoch, unique_tag);
}

std::unique_ptr<User> MotrStore::get_user(const rgw_user &u)
{
  ldout(cctx, 20) << "bucket's user:  " << u.to_str() << dendl;
  return std::make_unique<MotrUser>(this, u);
}

int MotrStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string &key, optional_yield y, std::unique_ptr<User> *user)
{
  int rc;
  User *u;
  bufferlist bl;
  RGWUserInfo uinfo;
  MotrAccessKey access_key;

  rc = do_idx_op_by_name(RGW_IAM_MOTR_ACCESS_KEY,
                           M0_IC_GET, key, bl);
  if (rc < 0){
    ldout(cctx, 0) << "Access key not found: rc = " << rc << dendl;
    return rc;
  }

  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  access_key.decode(iter);

  uinfo.user_id.id = access_key.user_id;
  ldout(cctx, 0) << "Loading user: " << uinfo.user_id.id << dendl;
  rc = load_user_from_idx(dpp, this, uinfo, nullptr, nullptr);
  if (rc < 0){
    ldout(cctx, 0) << "Failed to load user: rc = " << rc << dendl;
    return rc;
  }
  u = new MotrUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  user->reset(u);
  return 0;
}

int MotrStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  return 0;
}

int MotrStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  /* Swift keys and subusers are not supported for now */
  return 0;
}

int MotrStore::store_access_key(const DoutPrefixProvider *dpp, optional_yield y, MotrAccessKey access_key)
{
  int rc;
  bufferlist bl;
  access_key.encode(bl);
  rc = do_idx_op_by_name(RGW_IAM_MOTR_ACCESS_KEY,
                                M0_IC_PUT, access_key.id, bl);
  if (rc < 0){
    ldout(cctx, 0) << "Failed to store key: rc = " << rc << dendl;
    return rc;
  }
  return rc;
}

std::unique_ptr<Object> MotrStore::get_object(const rgw_obj_key& k)
{
  return std::make_unique<MotrObject>(this, k);
}


int MotrStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  int ret;
  Bucket* bp;

  bp = new MotrBucket(this, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret < 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int MotrStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  Bucket* bp;

  bp = new MotrBucket(this, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int MotrStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

bool MotrStore::is_meta_master()
{
  return true;
}

int MotrStore::forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version *objv,
    bufferlist& in_data,
    JSONParser *jp, req_info& info,
    optional_yield y)
{
  return 0;
}

std::string MotrStore::zone_unique_id(uint64_t unique_num)
{
  return "";
}

std::string MotrStore::zone_unique_trans_id(const uint64_t unique_num)
{
  return "";
}

int MotrStore::cluster_stat(RGWClusterStat& stats)
{
  return 0;
}

std::unique_ptr<Lifecycle> MotrStore::get_lifecycle(void)
{
  return 0;
}

std::unique_ptr<Completions> MotrStore::get_completions(void)
{
  return 0;
}

std::unique_ptr<Notification> MotrStore::get_notification(Object* obj, Object* src_obj, struct req_state* s,
    rgw::notify::EventType event_type, const string* object_name)
{
  return std::make_unique<MotrNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification>  MotrStore::get_notification(const DoutPrefixProvider* dpp, Object* obj,
        Object* src_obj, RGWObjectCtx* rctx, rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket,
        std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y)
{
  return std::make_unique<MotrNotification>(obj, src_obj, event_type);
}

int MotrStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
  return 0;
}

int MotrStore::log_op(const DoutPrefixProvider *dpp, string& oid, bufferlist& bl)
{
  return 0;
}

int MotrStore::register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
    const map<string, string>& meta)
{
  return 0;
}

void MotrStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                              RGWRateLimitInfo& user_ratelimit,
                              RGWRateLimitInfo& anon_ratelimit)
{
  return;
}

void MotrStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
{
  // XXX: Not handled for the first pass
  return;
}

int MotrStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled)
{
  return 0;
}

int MotrStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
    std::optional<rgw_zone_id> zone,
    std::optional<rgw_bucket> bucket,
    RGWBucketSyncPolicyHandlerRef *phandler,
    optional_yield y)
{
  return 0;
}

RGWDataSyncStatusManager* MotrStore::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  return 0;
}

int MotrStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated,
    RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int MotrStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return 0;
}

int MotrStore::get_config_key_val(string name, bufferlist *bl)
{
  return 0;
}

int MotrStore::meta_list_keys_init(const DoutPrefixProvider *dpp, const string& section, const string& marker, void** phandle)
{
  return 0;
}

int MotrStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<string>& keys, bool* truncated)
{
  return 0;
}

void MotrStore::meta_list_keys_complete(void* handle)
{
  return;
}

std::string MotrStore::meta_get_marker(void* handle)
{
  return "";
}

int MotrStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
{
  return 0;
}

int MotrStore::open_idx(struct m0_uint128 *id, bool create, struct m0_idx *idx)
{
  m0_idx_init(idx, &container.co_realm, id);

  if (!create)
    return 0; // nothing to do more

  // create index or make sure it's created
  struct m0_op *op = nullptr;
  int rc = m0_entity_create(nullptr, &idx->in_entity, &op);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: m0_entity_create() failed: " << rc << dendl;
    goto out;
  }

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0 && rc != -EEXIST)
    ldout(cctx, 0) << "ERROR: index create failed: " << rc << dendl;
out:
  return rc;
}

static void set_m0bufvec(struct m0_bufvec *bv, vector<uint8_t>& vec)
{
  *bv->ov_buf = reinterpret_cast<char*>(vec.data());
  *bv->ov_vec.v_count = vec.size();
}

// idx must be opened with open_idx() beforehand
int MotrStore::do_idx_op(struct m0_idx *idx, enum m0_idx_opcode opcode,
                         vector<uint8_t>& key, vector<uint8_t>& val, bool update)
{
  int rc, rc_i;
  struct m0_bufvec k, v, *vp = &v;
  uint32_t flags = 0;
  struct m0_op *op = nullptr;

  if (m0_bufvec_empty_alloc(&k, 1) != 0) {
    ldout(cctx, 0) << "ERROR: failed to allocate key bufvec" << dendl;
    return -ENOMEM;
  }

  if (opcode == M0_IC_PUT || opcode == M0_IC_GET) {
    rc = -ENOMEM;
    if (m0_bufvec_empty_alloc(&v, 1) != 0) {
      ldout(cctx, 0) << "ERROR: failed to allocate value bufvec" << dendl;
      goto out;
    }
  }

  set_m0bufvec(&k, key);
  if (opcode == M0_IC_PUT)
    set_m0bufvec(&v, val);

  if (opcode == M0_IC_DEL)
    vp = nullptr;

  if (opcode == M0_IC_PUT && update)
    flags |= M0_OIF_OVERWRITE;

  rc = m0_idx_op(idx, opcode, &k, vp, &rc_i, flags, &op);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: failed to init index op: " << rc << dendl;
    goto out;
  }

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: op failed: " << rc << dendl;
    goto out;
  }

  if (rc_i != 0) {
    ldout(cctx, 0) << "ERROR: idx op failed: " << rc_i << dendl;
    rc = rc_i;
    goto out;
  }

  if (opcode == M0_IC_GET) {
    val.resize(*v.ov_vec.v_count);
    memcpy(reinterpret_cast<char*>(val.data()), *v.ov_buf, *v.ov_vec.v_count);
  }

out:
  m0_bufvec_free2(&k);
  if (opcode == M0_IC_GET)
    m0_bufvec_free(&v); // cleanup buffer after GET
  else if (opcode == M0_IC_PUT)
    m0_bufvec_free2(&v);

  return rc;
}

// Retrieve a range of key/value pairs starting from keys[0].
int MotrStore::do_idx_next_op(struct m0_idx *idx,
                              vector<vector<uint8_t>>& keys,
                              vector<vector<uint8_t>>& vals)
{
  int rc;
  uint32_t i = 0;
  int nr_kvp = vals.size();
  int *rcs = new int[nr_kvp];
  struct m0_bufvec k, v;
  struct m0_op *op = nullptr;

  rc = m0_bufvec_empty_alloc(&k, nr_kvp)?:
       m0_bufvec_empty_alloc(&v, nr_kvp);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: failed to allocate kv bufvecs" << dendl;
    return rc;
  }

  set_m0bufvec(&k, keys[0]);

  rc = m0_idx_op(idx, M0_IC_NEXT, &k, &v, rcs, 0, &op);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: failed to init index op: " << rc << dendl;
    goto out;
  }

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: op failed: " << rc << dendl;
    goto out;
  }

  for (i = 0; i < v.ov_vec.v_nr; ++i) {
    if (rcs[i] < 0)
      break;

    vector<uint8_t>& key = keys[i];
    vector<uint8_t>& val = vals[i];
    key.resize(k.ov_vec.v_count[i]);
    val.resize(v.ov_vec.v_count[i]);
    memcpy(reinterpret_cast<char*>(key.data()), k.ov_buf[i], k.ov_vec.v_count[i]);
    memcpy(reinterpret_cast<char*>(val.data()), v.ov_buf[i], v.ov_vec.v_count[i]);
  }

out:
  k.ov_vec.v_nr = i;
  v.ov_vec.v_nr = i;
  m0_bufvec_free(&k);
  m0_bufvec_free(&v); // cleanup buffer after GET

  delete []rcs;
  return rc ?: i;
}

// Retrieve a number of key/value pairs under the prefix starting
// from the marker at key_out[0].
int MotrStore::next_query_by_name(string idx_name,
                                  vector<string>& key_out,
                                  vector<bufferlist>& val_out,
                                  string prefix, string delim)
{
  unsigned nr_kvp = std::min(val_out.size(), 100UL);
  struct m0_idx idx = {};
  vector<vector<uint8_t>> keys(nr_kvp);
  vector<vector<uint8_t>> vals(nr_kvp);
  struct m0_uint128 idx_id;
  int i = 0, j, k = 0;

  index_name_to_motr_fid(idx_name, &idx_id);
  int rc = open_motr_idx(&idx_id, &idx);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: next_query_by_name(): failed to open index: rc="
                   << rc << dendl;
    goto out;
  }

  // Only the first element for keys needs to be set for NEXT query.
  // The keys will be set will the returned keys from motr index.
  ldout(cctx, 20) <<__func__<< ": next_query_by_name(): index=" << idx_name
                  << " prefix=" << prefix << " delim=" << delim << dendl;
  keys[0].assign(key_out[0].begin(), key_out[0].end());
  for (i = 0; i < (int)val_out.size(); i += k, k = 0) {
    rc = do_idx_next_op(&idx, keys, vals);
    ldout(cctx, 20) << "do_idx_next_op() = " << rc << dendl;
    if (rc < 0) {
      ldout(cctx, 0) << "ERROR: NEXT query failed. " << rc << dendl;
      goto out;
    }

    string dir;
    for (j = 0, k = 0; j < rc; ++j) {
      string key(keys[j].begin(), keys[j].end());
      size_t pos = std::string::npos;
      if (!delim.empty())
        pos = key.find(delim, prefix.length());
      if (pos != std::string::npos) { // DIR entry
        dir.assign(key, 0, pos + 1);
        if (dir.compare(0, prefix.length(), prefix) != 0)
          goto out;
        if (i + k == 0 || dir != key_out[i + k - 1]) // a new one
          key_out[i + k++] = dir;
        continue;
      }
      dir = "";
      if (key.compare(0, prefix.length(), prefix) != 0)
        goto out;
      key_out[i + k] = key;
      bufferlist& vbl = val_out[i + k];
      vbl.append(reinterpret_cast<char*>(vals[j].data()), vals[j].size());
      ++k;
    }

    if (rc < (int)nr_kvp) // there are no more keys to fetch
      break;

    string next_key;
    if (dir != "")
      next_key = dir + "\xff"; // skip all dir content in 1 step
    else
      next_key = key_out[i + k - 1] + " ";
    ldout(cctx, 0) << "do_idx_next_op(): next_key=" << next_key << dendl;
    keys[0].assign(next_key.begin(), next_key.end());
  }

out:
  m0_idx_fini(&idx);
  return rc < 0 ? rc : i + k;
}

int MotrStore::delete_motr_idx_by_name(string iname)
{
  struct m0_idx idx;
  struct m0_uint128 idx_id;
  struct m0_op *op = nullptr;

  index_name_to_motr_fid(iname, &idx_id);
  m0_idx_init(&idx, &container.co_realm, &idx_id);
  m0_entity_open(&idx.in_entity, &op);
  int rc = m0_entity_delete(&idx.in_entity, &op);
  if (rc < 0)
    goto out;

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc == -ENOENT) // race deletion??
    rc = 0;
  else if (rc < 0)
    ldout(cctx, 0) << "ERROR: index create failed: " << rc << dendl;

out:
  m0_idx_fini(&idx);
  return rc;
}

int MotrStore::open_motr_idx(struct m0_uint128 *id, struct m0_idx *idx)
{
  m0_idx_init(idx, &container.co_realm, id);
  return 0;
}

// The following marcos are from dix/fid_convert.h which are not exposed.
enum {
      M0_DIX_FID_DEVICE_ID_OFFSET   = 32,
      M0_DIX_FID_DIX_CONTAINER_MASK = (1ULL << M0_DIX_FID_DEVICE_ID_OFFSET)
                                      - 1,
};

// md5 is used here, a more robust way to convert index name to fid is
// needed to avoid collision.
void MotrStore::index_name_to_motr_fid(string iname, struct m0_uint128 *id)
{
  unsigned char md5[16];  // 128/8 = 16
  MD5 hash;

  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update((const unsigned char *)iname.c_str(), iname.length());
  hash.Final(md5);
  memcpy(&id->u_hi, md5, 8);
  memcpy(&id->u_lo, md5 + 8, 8);
  ldout(cctx, 20) << "id = 0x" << std::hex << id->u_hi << ":0x" << std::hex << id->u_lo  << dendl;

  struct m0_fid *fid = (struct m0_fid*)id;
  m0_fid_tset(fid, m0_dix_fid_type.ft_id,
              fid->f_container & M0_DIX_FID_DIX_CONTAINER_MASK, fid->f_key);
  ldout(cctx, 20) << "converted id = 0x" << std::hex << id->u_hi << ":0x" << std::hex << id->u_lo  << dendl;
}

int MotrStore::do_idx_op_by_name(string idx_name, enum m0_idx_opcode opcode,
                                 string key_str, bufferlist &bl, bool update)
{
  struct m0_idx idx;
  vector<uint8_t> key(key_str.begin(), key_str.end());
  vector<uint8_t> val;
  struct m0_uint128 idx_id;
  index_name_to_motr_fid(idx_name, &idx_id);
  int rc = open_motr_idx(&idx_id, &idx);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: failed to open index: " << rc << dendl;
    goto out;
  }
  if (opcode == M0_IC_PUT)
    val.assign(bl.c_str(), bl.c_str() + bl.length());

  ldout(cctx, 20) <<__func__<< ": do_idx_op_by_name(): op="
                 << (opcode == M0_IC_PUT ? "PUT" : "GET")
                 << " idx=" << idx_name << " key=" << key_str << dendl;
  rc = do_idx_op(&idx, opcode, key, val, update);
  if (rc == 0 && opcode == M0_IC_GET)
    // Append the returned value (blob) to the bufferlist.
    bl.append(reinterpret_cast<char*>(val.data()), val.size());

out:
  m0_idx_fini(&idx);
  return rc;
}

int MotrStore::create_motr_idx_by_name(string iname)
{
  struct m0_idx idx = {};
  struct m0_uint128 id;

  index_name_to_motr_fid(iname, &id);
  m0_idx_init(&idx, &container.co_realm, &id);

  // create index or make sure it's created
  struct m0_op *op = nullptr;
  int rc = m0_entity_create(nullptr, &idx.in_entity, &op);
  if (rc != 0) {
    ldout(cctx, 0) << "ERROR: m0_entity_create() failed: " << rc << dendl;
    goto out;
  }

  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0 && rc != -EEXIST)
    ldout(cctx, 0) << "ERROR: index create failed: " << rc << dendl;
out:
  m0_idx_fini(&idx);
  return rc;
}

// If a global index is checked (if it has been create) every time
// before they're queried (put/get), which takes 2 Motr operations to
// complete the query. As the global indices' name and FID are known
// already when MotrStore is created, we move the check and creation
// in newMotrStore().
// Similar method is used for per bucket/user index. For example,
// bucket instance index is created when creating the bucket.
int MotrStore::check_n_create_global_indices()
{
  int rc = 0;

  for (const auto& iname : motr_global_indices) {
    rc = create_motr_idx_by_name(iname);
    if (rc < 0 && rc != -EEXIST)
      break;
    rc = 0;
  }

  return rc;
}

std::string MotrStore::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
{
  char id[M0_FID_STR_LEN];
  struct m0_confc *confc = m0_reqh2confc(&instance->m0c_reqh);

  m0_fid_print(id, ARRAY_SIZE(id), &confc->cc_root->co_id);
  return std::string(id);
}

int MotrStore::init_metadata_cache(const DoutPrefixProvider *dpp,
                                   CephContext *cct)
{
  this->obj_meta_cache = new MotrMetaCache(dpp, cct);
  this->get_obj_meta_cache()->set_enabled(true);

  this->user_cache = new MotrMetaCache(dpp, cct);
  this->get_user_cache()->set_enabled(true);

  this->bucket_inst_cache = new MotrMetaCache(dpp, cct);
  this->get_bucket_inst_cache()->set_enabled(true);

  return 0;
}

} // namespace rgw::sal

extern "C" {

void *newMotrStore(CephContext *cct)
{
  int rc = -1;
  rgw::sal::MotrStore *store = new rgw::sal::MotrStore(cct);

  if (store) {
    store->conf.mc_is_oostore     = true;
    // XXX: these params should be taken from config settings and
    // cct somehow?
    store->instance = nullptr;
    const auto& proc_ep  = g_conf().get_val<std::string>("motr_my_endpoint");
    const auto& ha_ep    = g_conf().get_val<std::string>("motr_ha_endpoint");
    const auto& proc_fid = g_conf().get_val<std::string>("motr_my_fid");
    const auto& profile  = g_conf().get_val<std::string>("motr_profile_fid");
    const auto& admin_proc_ep  = g_conf().get_val<std::string>("admin_motr_endpoint");
    const auto& admin_proc_fid = g_conf().get_val<std::string>("admin_motr_fid");
    const int init_flags = cct->get_init_flags();
    ldout(cct, 0) << "INFO: motr my endpoint: " << proc_ep << dendl;
    ldout(cct, 0) << "INFO: motr ha endpoint: " << ha_ep << dendl;
    ldout(cct, 0) << "INFO: motr my fid:      " << proc_fid << dendl;
    ldout(cct, 0) << "INFO: motr profile fid: " << profile << dendl;
    store->conf.mc_local_addr  = proc_ep.c_str();
    store->conf.mc_process_fid = proc_fid.c_str();

    ldout(cct, 0) << "INFO: init flags:        " << init_flags << dendl;
    ldout(cct, 0) << "INFO: admin motr endpoint:  " << admin_proc_ep << dendl;
    ldout(cct, 0) << "INFO: admin motr  fid:    " << admin_proc_fid << dendl;

    // HACK this is so that radosge-admin uses a different client
    if (init_flags == 0) {
      store->conf.mc_process_fid = admin_proc_fid.c_str();
      store->conf.mc_local_addr  = admin_proc_ep.c_str();
    } else {
      store->conf.mc_process_fid = proc_fid.c_str();
      store->conf.mc_local_addr  = proc_ep.c_str();
    }
    store->conf.mc_ha_addr     = ha_ep.c_str();
    store->conf.mc_profile     = profile.c_str();

    ldout(cct, 50) << "INFO: motr profile fid:  " << store->conf.mc_profile << dendl;
    ldout(cct, 50) << "INFO: ha addr:  " << store->conf.mc_ha_addr << dendl;
    ldout(cct, 50) << "INFO: process fid:  " << store->conf.mc_process_fid << dendl;
    ldout(cct, 50) << "INFO: motr endpoint:  " << store->conf.mc_local_addr << dendl;

    store->conf.mc_tm_recv_queue_min_len =     64;
    store->conf.mc_max_rpc_msg_size      = 524288;
    store->conf.mc_idx_service_id  = M0_IDX_DIX;
    store->dix_conf.kc_create_meta = false;
    store->conf.mc_idx_service_conf = &store->dix_conf;

    if (!g_conf().get_val<bool>("motr_tracing_enabled")) {
      m0_trace_level_allow(M0_WARN); // allow errors and warnings in syslog anyway
      m0_trace_set_mmapped_buffer(false);
    }

    store->instance = nullptr;
    rc = m0_client_init(&store->instance, &store->conf, true);
    if (rc != 0) {
      ldout(cct, 0) << "ERROR: m0_client_init() failed: " << rc << dendl;
      goto out;
    }

    m0_container_init(&store->container, nullptr, &M0_UBER_REALM, store->instance);
    rc = store->container.co_realm.re_entity.en_sm.sm_rc;
    if (rc != 0) {
      ldout(cct, 0) << "ERROR: m0_container_init() failed: " << rc << dendl;
      goto out;
    }

    rc = m0_ufid_init(store->instance, &ufid_gr);
    if (rc != 0) {
      ldout(cct, 0) << "ERROR: m0_ufid_init() failed: " << rc << dendl;
      goto out;
    }

    // Create global indices if not yet.
    rc = store->check_n_create_global_indices();
    if (rc != 0) {
      ldout(cct, 0) << "ERROR: check_n_create_global_indices() failed: " << rc << dendl;
      goto out;
    }

  }

out:
  if (rc != 0) {
    delete store;
    return nullptr;
  }
  return store;
}

}
