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
#include <chrono>

extern "C" {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wextern-c-compat"
#include "motr/config.h"
#include "lib/types.h"
#include "lib/trace.h"   // m0_trace_set_mmapped_buffer
#include "motr/layout.h" // M0_OBJ_LAYOUT_ID
#include "helpers/helpers.h" // m0_ufid_next
#include "lib/thread.h"	     // m0_thread_adopt
#pragma clang diagnostic pop
}

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_compression.h"
#include "rgw_sal.h"
#include "rgw_sal_motr.h"
#include "rgw_bucket.h"
#include "rgw_quota.h"
#include "motr/addb/rgw_addb.h"

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

class MotrADDBLogger {
private:
  uint64_t req_id;
  bool is_m0_thread = false;
  struct m0_thread thread;
  static struct m0* m0_instance;
public:
  MotrADDBLogger() {
    struct m0_thread_tls *tls = m0_thread_tls();

    req_id = (uint64_t)-1;
    memset(&thread, 0, sizeof(struct m0_thread));

    // m0_thread_tls() always return non-NULL pointer to
    // actual thread tls. Motr and non-Motr threads can be
    // distinguished by checking of addb2_mach. Motr thread
    // has addb2_mach assigned, while non-Motr haven't.
    if (tls->tls_addb2_mach == NULL)
    {
      M0_ASSERT(m0_instance != nullptr);
      m0_thread_adopt(&thread, m0_instance);
    } else {
      is_m0_thread = true;
    }
  }

  ~MotrADDBLogger() {
    if (!is_m0_thread) {
      m0_addb2_force_all();
      m0_thread_arch_shun();
    }
  }

  void set_id(uint64_t id) {
    req_id = id;
  }

  void set_id(RGWObjectCtx* rctx) {
    struct req_state* s = static_cast<req_state*>(rctx->get_private());
    req_id = s->id;
  }

  uint64_t get_id() {
    return req_id;
  }

  static void set_m0_instance(struct m0* instance) {
    m0_instance = instance;
  }
};

struct m0* MotrADDBLogger::m0_instance = nullptr;

static thread_local MotrADDBLogger addb_logger;

static std::string motr_global_indices[] = {
  RGW_MOTR_USERS_IDX_NAME,
  RGW_MOTR_BUCKET_INST_IDX_NAME,
  RGW_MOTR_BUCKET_HD_IDX_NAME,
  RGW_IAM_MOTR_ACCESS_KEY,
  RGW_IAM_MOTR_EMAIL_KEY
};

// version-id(31 byte = base62 timstamp(8-byte) + UUID(23 byte)
#define TS_LEN 8
#define UUID_LEN 23

// Use NULL_REF macro for handling null object reference entry
#define NULL_REF "^null"

static uint64_t roundup(uint64_t x, uint64_t by)
{
  if (x == 0)
    return 0;
  return ((x - 1) / by + 1) * by;
}

static uint64_t rounddown(uint64_t x, uint64_t by)
{
  return x / by * by;
}

std::string base62_encode(uint64_t value, size_t pad)
{
  // Integer to Base62 encoding table. Characters are sorted in
  // lexicographical order, which makes the encoded result
  // also sortable in the same way as the integer source.
  constexpr std::array<char, 62> base62_chars{
      // 0-9
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      // A-Z
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
      'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
      // a-z
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

  std::string ret;
  ret.reserve(TS_LEN);
  if (value == 0) {
    ret = base62_chars[0];
  }

  while (value > 0) {
    ret += base62_chars[value % base62_chars.size()];
    value /= base62_chars.size();
  }
  reverse(ret.begin(), ret.end());
  if (ret.size() < pad) ret.insert(0, pad - ret.size(), base62_chars[0]);

  return ret;
}

inline std::string get_bucket_name(const std::string& tenant,  const std::string& bucket)
{
  if (tenant != "")
    return tenant + "$" + bucket;
  else
    return bucket;
}

int static update_bucket_stats(const DoutPrefixProvider *dpp, MotrStore *store,
                               std::string owner, std::string bucket_name,
                               uint64_t size, uint64_t actual_size,
                               uint64_t num_objects = 1, bool add_stats = true) {
  uint64_t multiplier = add_stats ? 1 : -1;
  bufferlist bl;
  std::string user_stats_iname = "motr.rgw.user.stats." + owner;
  rgw_bucket_dir_header bkt_header;
  int rc = store->do_idx_op_by_name(user_stats_iname,
                            M0_IC_GET, bucket_name, bl);
  if (rc != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed to get the bucket header."
      << " bucket = " << bucket_name << ", ret = " << rc << dendl;
    return rc;
  }

  bufferlist::const_iterator bitr = bl.begin();
  bkt_header.decode(bitr);
  rgw_bucket_category_stats& bkt_stat = bkt_header.stats[RGWObjCategory::Main];
  bkt_stat.num_entries += multiplier * num_objects;
  bkt_stat.total_size += multiplier * size;
  bkt_stat.actual_size += multiplier * actual_size;

  bl.clear();
  bkt_header.encode(bl);
  rc = store->do_idx_op_by_name(user_stats_iname, M0_IC_PUT, bucket_name, bl);
  return rc;
}

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
      ldpp_dout(dpp, 0) << __func__ <<": ERROR: failed to distribute cache for " << name << dendl;

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
    ldpp_dout(dpp, 0) << "ERROR: " <<__func__<< ": failed to distribute cache: rc = " << rc << dendl;
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

  ldpp_dout(dpp, 20) << __func__ << ": list_user_buckets: marker=" << marker
                    << " end_marker=" << end_marker
                    << " max=" << max << dendl;

  // Retrieve all `max` number of pairs.
  buckets.clear();
  string user_info_iname = "motr.rgw.user.info." + info.user_id.to_str();
  keys[0] = marker;
  rc = store->next_query_by_name(user_info_iname, keys, vals);
  if (rc < 0) {
    ldpp_dout(dpp, 0) <<  __func__ << ": ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  } else if (rc == 0) {
    ldpp_dout(dpp, 0) <<  __func__ << ": No buckets to list. " << rc << dendl;
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
    ldpp_dout(dpp, 20) << __func__ << "got creation time: << " << std::put_time(std::localtime(&ctime), "%F %T") << dendl;

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
    // if (swift_ver_location.empty()) {
    //   swift_ver_location = bucket->get_info().swift_ver_location;
    // }
    // placement_rule.inherit_from(bucket->get_info().placement_rule);

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

  if (!*existed){
    // TODO: how to handle zone and multi-site.
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
    // "put_info" accepts boolean value mentioning whether to create new or update existing. 
    // "yield" is not a boolean flag hence explicitly passing true to create a new record.
    ret = mbucket->put_info(dpp, true, ceph::real_time())? :
          mbucket->create_bucket_index() ? :
          mbucket->create_multipart_indices();
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to create bucket indices! " << ret << dendl;
      return ret;
    }

    // Insert the bucket entry into the user info index.
    ret = mbucket->link_user(dpp, this, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to add bucket entry! " << ret << dendl;
      return ret;
    }

    // Add bucket entry in user stats index table.
    std::string user_stats_iname = "motr.rgw.user.stats." + info.owner.to_str();
    bufferlist blst;
    rgw_bucket_dir_header bkt_header;
    bkt_header.encode(blst);
    std::string bkt_name = get_bucket_name(b.tenant, b.name);
    ret = store->do_idx_op_by_name(user_stats_iname,
                              M0_IC_PUT, bkt_name, blst);

    if (ret != 0) {
      ldpp_dout(dpp, 20) << __func__ << ": Failed to add the stats entry "
        << "for the bucket = " << bkt_name << ", ret = " << ret << dendl;
      return ret;
    }
    
    ldpp_dout(dpp, 20) << __func__ << ": Added an empty stats entry for "
        << "the bucket = " << bkt_name << ", ret = " << ret << dendl; 
  } else {
    return -EEXIST;
    // bucket->set_version(ep_objv);
    // bucket->get_info() = info;
  }

  bucket_out->swap(bucket);

  return ret;
}

int MotrUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int rc = 0;
  if (not attrs.empty())
    return rc;
  
  struct MotrUserInfo muinfo;
  bufferlist bl;
  if (store->get_user_cache()->get(dpp, info.user_id.to_str(), bl)) {
    // Cache miss
    rc = store->do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                                      M0_IC_GET, info.user_id.to_str(), bl);
    ldpp_dout(dpp, 20) << __func__ << ": do_idx_op_by_name, rc = "  << rc << dendl;
    if (rc < 0)
        return rc;
    // Put into cache.
    store->get_user_cache()->put(dpp, info.user_id.to_str(), bl);
  }
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  muinfo.decode(iter);
  attrs = muinfo.attrs;
  ldpp_dout(dpp, 20) << __func__ << ": user attributes fetched successfully." << dendl;

  return rc;
}

int MotrUser::read_stats(const DoutPrefixProvider *dpp,
    optional_yield y, RGWStorageStats* stats,
    ceph::real_time *last_stats_sync,
    ceph::real_time *last_stats_update)
{
  int rc, num_of_entries, max_entries = 100; // to fetch in chunks of 100
  vector<string> keys(max_entries);
  vector<bufferlist> vals(max_entries);
  std::string user_stats_iname = "motr.rgw.user.stats." + info.user_id.to_str();
  rgw_bucket_dir_header bkt_header;

  do {
    rc = store->next_query_by_name(user_stats_iname, keys, vals);
    if (rc < 0) {
      ldpp_dout(dpp, 20) << __func__ << ": failed to get the user stats info for user  = "
                        << info.user_id.to_str() << dendl;
      return rc;
    } else if (rc == 0) {
      ldpp_dout(dpp, 20) << __func__ << ": No bucket to fetch the stats." << dendl;
      return rc;
    }
    num_of_entries = rc;

    for (int i = 0 ; i < num_of_entries; i++) {
      bufferlist::const_iterator bitr = vals[i].begin();
      bkt_header.decode(bitr);

      for (const auto& pair : bkt_header.stats) {
        const rgw_bucket_category_stats& header_stats = pair.second;
        stats->num_objects += header_stats.num_entries;
        stats->size += header_stats.total_size;
        stats->size_rounded += rgw_rounded_kb(header_stats.actual_size) * 1024;
      }
    }
    keys[0] = keys[num_of_entries-1]; // keys[0] will be used as a marker in next loop.
  } while(num_of_entries == max_entries);

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
  return -ENOENT;
}

int MotrUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return 0;
}

int MotrUser::load_user_from_idx(const DoutPrefixProvider *dpp,
                              MotrStore *store,
                              RGWUserInfo& info, map<string, bufferlist> *attrs,
                              RGWObjVersionTracker *objv_tr)
{
  struct MotrUserInfo muinfo;
  bufferlist bl;
  ldpp_dout(dpp, 20) << __func__ << ": info.user_id.id = "  << info.user_id.id << dendl;
  if (store->get_user_cache()->get(dpp, info.user_id.to_str(), bl)) {
    // Cache misses
    int rc = store->do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                                      M0_IC_GET, info.user_id.to_str(), bl);
    ldpp_dout(dpp, 20) << __func__ << ": do_idx_op_by_name(), rc = "  << rc << dendl;
    if (rc < 0)
        return rc;

    // Put into cache.
    store->get_user_cache()->put(dpp, info.user_id.to_str(), bl);
  }

  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  muinfo.decode(iter);
  info = muinfo.info;
  if (attrs)
    *attrs = muinfo.attrs;
  if (objv_tr)
  {
    objv_tr->read_version = muinfo.user_version;
    objv_tracker.read_version = objv_tr->read_version;
  }

  if (!info.access_keys.empty()) {
    for(auto key : info.access_keys) {
      access_key_tracker.insert(key.first);
    }
  }

  return 0;
}

int MotrUser::load_user(const DoutPrefixProvider *dpp,
                        optional_yield y)
{
  ldpp_dout(dpp, 20) << __func__ << ": user id =   " << info.user_id.to_str() << dendl;
  return load_user_from_idx(dpp, store, info, &attrs, &objv_tracker);
}

int MotrUser::create_user_info_idx()
{
  string user_info_iname = "motr.rgw.user.info." + info.user_id.to_str();
  return store->create_motr_idx_by_name(user_info_iname);
}

int inline MotrUser::create_user_stats_idx()
{
  string user_stats_iname = "motr.rgw.user.stats." + info.user_id.to_str();
  return store->create_motr_idx_by_name(user_stats_iname);
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

  ldpp_dout(dpp, 20) << __func__ << ": User = " << info.user_id.id << dendl;
  orig_info.user_id = info.user_id;
  // XXX: we open and close motr idx 2 times in this method:
  // 1) on load_user_from_idx() here and 2) on do_idx_op_by_name(PUT) below.
  // Maybe this can be optimised later somewhow.
  int rc = load_user_from_idx(dpp, store, orig_info, nullptr, &objv_tr);
  ldpp_dout(dpp, 10) << __func__ << ": load_user_from_idx, rc = " << rc << dendl;

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
                                M0_IC_PUT, info.user_id.to_str(), bl);
  ldpp_dout(dpp, 10) << __func__ << ": store user to motr index: rc = " << rc << dendl;
  if (rc == 0) {
    objv_tracker.read_version = obj_ver;
    objv_tracker.write_version = obj_ver;
  }
  
  // Store access key in access key index
  if (!info.access_keys.empty()) {
    std::string access_key;
    std::string secret_key;
    std::map<std::string, RGWAccessKey>::const_iterator iter = info.access_keys.begin();
    const RGWAccessKey& k = iter->second;
    access_key = k.id;
    secret_key = k.key;
    MotrAccessKey MGWUserKeys(access_key, secret_key, info.user_id.to_str());
    store->store_access_key(dpp, y, MGWUserKeys);
    access_key_tracker.insert(access_key);
  }

  // Check if any key need to be deleted
  if (access_key_tracker.size() != info.access_keys.size()) {
    std::string key_for_deletion;
    for (auto key : access_key_tracker) {
      if (!info.get_key(key)) {
        key_for_deletion = key;
        ldpp_dout(dpp, 0) << __func__ << ": deleting access key: " << key_for_deletion << dendl;
        store->delete_access_key(dpp, y, key_for_deletion);
        if (rc < 0) {
          ldpp_dout(dpp, 0) << __func__ << ": unable to delete access key" << rc << dendl;
        }
      }
    }
    if(rc >= 0){
      access_key_tracker.erase(key_for_deletion);
    }
  }

  if (!info.user_email.empty()) {
     MotrEmailInfo MGWEmailInfo(info.user_id.to_str(), info.user_email);
     store->store_email_info(dpp, y, MGWEmailInfo);
  }

  // Create user info index to store all buckets that are belong
  // to this bucket.
  rc = create_user_info_idx();
  if (rc < 0 && rc != -EEXIST) {
    ldpp_dout(dpp, 0) << __func__ << ": failed to create user info index: rc = " << rc << dendl;
    goto out;
  }

  // Create user stats index to store stats for
  // all the buckets belonging to a user.
  rc = create_user_stats_idx();
  if (rc < 0 && rc != -EEXIST) {
    ldpp_dout(dpp, 0) << __func__
      << "Failed to create user stats index: rc = " << rc << dendl;
    goto out;
  }

  // Put the user info into cache.
  rc = store->get_user_cache()->put(dpp, info.user_id.to_str(), bl);

out:
  return rc;
}

int MotrUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  // Remove user info from cache
  // Delete access keys for user
  // Delete user info
  // Delete user from user index
  // Delete email for user - TODO
  bufferlist bl;
  int rc;
  // Remove the user info from cache.
  store->get_user_cache()->remove(dpp, info.user_id.to_str());

  // Delete all access key of user
  if (!info.access_keys.empty()) {
    for(auto acc_key = info.access_keys.begin(); acc_key != info.access_keys.end(); acc_key++) {
      auto access_key = acc_key->first;
      rc = store->delete_access_key(dpp, y, access_key);
      // TODO 
      // Check error code for access_key does not exist
      // Continue to next step only if delete failed because key doesn't exists
      if (rc < 0){
        ldpp_dout(dpp, 0) << __func__ << ": unable to delete access key" << rc << dendl;
      }
    }
  }

  //Delete email id 
  if (!info.user_email.empty()) {
    rc = store->do_idx_op_by_name(RGW_IAM_MOTR_EMAIL_KEY,
		             M0_IC_DEL, info.user_email, bl);
    if (rc < 0 && rc != -ENOENT) {
       ldpp_dout(dpp, 0) << __func__ << ": unable to delete email id " << rc << dendl;
    }
  }
  
  // Delete user info index
  string user_info_iname = "motr.rgw.user.info." + info.user_id.to_str();
  store->delete_motr_idx_by_name(user_info_iname);
  ldpp_dout(dpp, 10) << __func__ << ": deleted user info index - " << user_info_iname << dendl;

  // Delete user stats index
  string user_stats_iname = "motr.rgw.user.stats." + info.user_id.to_str();
  store->delete_motr_idx_by_name(user_stats_iname);
  ldpp_dout(dpp, 10) << "Deleted user stats index - " << user_stats_iname << dendl;

  // Delete user from user index
  rc = store->do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                           M0_IC_DEL, info.user_id.to_str(), bl);
  if (rc < 0){
    ldpp_dout(dpp, 0) << __func__ << ": unable to delete user from user index " << rc << dendl;
    return rc;
  }

  // TODO 
  // Delete email for user
  // rc = store->do_idx_op_by_name(RGW_IAM_MOTR_EMAIL_KEY,
  //                          M0_IC_DEL, info.user_email, bl);
  // if (rc < 0){
  //   ldpp_dout(dpp, 0) << "Unable to delete email for user" << rc << dendl;
  //   return rc;
  // }
  return 0;
}


int MotrBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y)
{
  int ret;
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);
  ldpp_dout(dpp, 20) << __func__ << ": entry =" << tenant_bkt_name << dendl;

  // Refresh info
  ret = load_bucket(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) <<  __func__ << ": ERROR: load_bucket failed rc = " << ret << dendl;
    return ret;
  }

  ListParams params;
  params.list_versions = true;
  params.allow_unordered = true;

  ListResults results;

  // 1. Check if Bucket has objects.
  // If bucket contains objects and delete_children is true, delete all objects.
  // Else throw error that bucket is not empty.
  do {
    results.objs.clear();

    // Check if bucket has objects.
    ret = list(dpp, params, 1000, results, y);
    if (ret < 0) {
      return ret;
    }

    // If result contains entries, bucket is not empty.
    if (!results.objs.empty() && !delete_children) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: could not remove non-empty bucket " << info.bucket.name << dendl;
      return -ENOTEMPTY;
    }

    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      /* xxx dang */
      ret = rgw_remove_object(dpp, store, this, key);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 0) << __func__ << ": ERROR: rgw_remove_object failed rc = " << ret << dendl;
	      return ret;
      }
    }
  } while(results.is_truncated);

  // 2. Abort Mp uploads on the bucket.
  ret = abort_multiparts(dpp, store->ctx());
  if (ret < 0) {
    return ret;
  }

  // 3. Remove mp index??
  string bucket_multipart_iname = "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  ret = store->delete_motr_idx_by_name(bucket_multipart_iname);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to remove multipart index rc = " << ret << dendl;
    return ret;
  }

  // 4. Delete the bucket stats.
  bufferlist blst;
  std::string user_stats_iname = "motr.rgw.user.stats." + info.owner.to_str();

  ret = store->do_idx_op_by_name(user_stats_iname,
                            M0_IC_DEL, tenant_bkt_name, blst);

  if (ret != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed to delete the stats entry "
                      << "for the bucket = " << tenant_bkt_name
                      << ", ret = " << ret << dendl;
  }
  else
    ldpp_dout(dpp, 20) << __func__ << ": Deleted the stats successfully for the "
                      << " bucket = " << tenant_bkt_name << dendl;

  // 5. Remove the bucket from user info index. (unlink user)
  ret = this->unlink_user(dpp, info.owner, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: unlink_user failed rc = " << ret << dendl;
    return ret;
  }

  // 6. Remove bucket index.
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  ret = store->delete_motr_idx_by_name(bucket_index_iname);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: unlink_user failed rc = " << ret << dendl;
    return ret;
  }

  // 7. Remove bucket instance info.
  bufferlist bl;
  ret = store->get_bucket_inst_cache()->remove(dpp, tenant_bkt_name);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to remove bucket instance from cache rc = "
      << ret << dendl;
    return ret;
  }

  ret = store->do_idx_op_by_name(RGW_MOTR_BUCKET_INST_IDX_NAME,
                                  M0_IC_DEL, tenant_bkt_name, bl);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to remove bucket instance rc = "
      << ret << dendl;
    return ret;
  }

  // TODO :
  // 8. Remove Notifications
  // if bucket has notification definitions associated with it
  // they should be removed (note that any pending notifications on the bucket are still going to be sent)

  // 9. Forward request to master.
  if (forward_to_master) {
    bufferlist in_data;
    ret = store->forward_request_to_master(dpp, owner, &bucket_version, in_data, nullptr, *req_info, y);
    if (ret < 0) {
      if (ret == -ENOENT) {
        /* adjust error, we want to return with NoSuchBucket and not
        * NoSuchKey */
        ret = -ERR_NO_SUCH_BUCKET;
      }
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: forward to master failed. ret=" << ret << dendl;
      return ret;
    }
  }

  ldpp_dout(dpp, 20) << __func__ << ": exit=" << tenant_bkt_name << dendl;

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
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);

  ldpp_dout(dpp, 20) << __func__ << ": bucket_id=" << info.bucket.bucket_id << dendl;
  mbinfo.info = info;
  mbinfo.bucket_attrs = attrs;
  mbinfo.mtime = _mtime;
  mbinfo.bucket_version = bucket_version;
  mbinfo.encode(bl);

  // Insert bucket instance using bucket's marker (string).
  int rc = store->do_idx_op_by_name(RGW_MOTR_BUCKET_INST_IDX_NAME,
                                  M0_IC_PUT, tenant_bkt_name, bl, !exclusive);
  if (rc == 0)
    store->get_bucket_inst_cache()->put(dpp, tenant_bkt_name, bl);

  return rc;
}

int MotrBucket::load_bucket(const DoutPrefixProvider *dpp, optional_yield y, bool get_stats)
{
  // Get bucket instance using bucket's name (string). or bucket id?
  bufferlist bl;
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);
  if (store->get_bucket_inst_cache()->get(dpp, tenant_bkt_name, bl)) {
    // Cache misses.
    ldpp_dout(dpp, 20) << __func__ << ": name =" << tenant_bkt_name << dendl;
    int rc = store->do_idx_op_by_name(RGW_MOTR_BUCKET_INST_IDX_NAME,
                                      M0_IC_GET, tenant_bkt_name, bl);
    ldpp_dout(dpp, 20) << __func__ << ": do_idx_op_by_name, rc = " << rc << dendl;
    if (rc < 0)
      return rc;
    store->get_bucket_inst_cache()->put(dpp, tenant_bkt_name, bl);
  }

  struct MotrBucketInfo mbinfo;
  bufferlist& blr = bl;
  auto iter =blr.cbegin();
  mbinfo.decode(iter); //Decode into MotrBucketInfo.

  info = mbinfo.info;
  ldpp_dout(dpp, 20) << __func__ << ": bucket_id=" << info.bucket.bucket_id << dendl;
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
  ldpp_dout(dpp, 20) << __func__ << ": got creation time: "
                     << std::put_time(std::localtime(&ctime), "%F %T") << dendl;
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);

  // Insert the user into the user info index.
  string user_info_idx_name = "motr.rgw.user.info." + new_user->get_info().user_id.to_str();
  return store->do_idx_op_by_name(user_info_idx_name,
                                  M0_IC_PUT, tenant_bkt_name, bl);

}

int MotrBucket::unlink_user(const DoutPrefixProvider* dpp, const rgw_user &bucket_owner, optional_yield y)
{
  // Remove the user into the user info index.
  bufferlist bl;
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);
  string user_info_idx_name = "motr.rgw.user.info." + bucket_owner.to_str();
  return store->do_idx_op_by_name(user_info_idx_name,
                                  M0_IC_DEL, tenant_bkt_name, bl);
}

/* stats - Not for first pass */
int MotrBucket::read_stats(const DoutPrefixProvider *dpp, int shard_id,
    std::string *bucket_ver, std::string *master_ver,
    std::map<RGWObjCategory, RGWStorageStats>& stats,
    std::string *max_marker, bool *syncstopped)
{
  std::string user_stats_iname = "motr.rgw.user.stats." + info.owner.to_str();
  bufferlist bl;
  int rc = this->store->do_idx_op_by_name(user_stats_iname,
                                  M0_IC_GET, info.bucket.get_key(), bl);
  if (rc < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": failed to get the bucket stats for bucket = "
                       << info.bucket.get_key() << dendl;
    return rc;
  }

  rgw_bucket_dir_header bkt_header;
  ceph::buffer::list::const_iterator bitr = bl.begin();
  bkt_header.decode(bitr);
  for(const auto& [category, bkt_stat]: bkt_header.stats) {
    RGWStorageStats& s = stats[category];
    s.num_objects = bkt_stat.num_entries;
    s.size = bkt_stat.total_size;
    s.size_rounded = bkt_stat.actual_size;
  }
  return 0;
}

int MotrBucket::create_bucket_index()
{
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  return store->create_motr_idx_by_name(bucket_index_iname);
}

int MotrBucket::create_multipart_indices()
{
  int rc;
  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);

  // Bucket multipart index stores in-progress multipart uploads.
  // Key is the object name + upload_id, value is a rgw_bucket_dir_entry.
  // An entry is inserted when a multipart upload is initialised (
  // MotrMultipartUpload::init()) and will be removed when the upload
  // is completed (MotrMultipartUpload::complete()).
  // MotrBucket::list_multiparts() will scan this index to return all
  // in-progress multipart uploads in the bucket.
  string bucket_multipart_iname = "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  rc = store->create_motr_idx_by_name(bucket_multipart_iname);
  if (rc < 0) {
    ldout(store->cctx, 0) << __func__ << ": failed to create bucket multipart index "
                          << bucket_multipart_iname << dendl;
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

int MotrBucket::check_quota(const DoutPrefixProvider *dpp,
    RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota,
    uint64_t obj_size, optional_yield y, bool check_size_only) {
  RGWQuotaHandler* quota_handler = \
    RGWQuotaHandler::generate_handler(dpp, store, false);

  ldpp_dout(dpp, 20) << __func__ << ": called. check_size_only = "
     << check_size_only << ", obj_size = " << obj_size << dendl;

  int rc = quota_handler->check_quota(dpp, info.owner, info.bucket,
                                      user_quota, bucket_quota,
                                      check_size_only ? 0 : 1, obj_size, y);
  return rc;
}

int MotrBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& new_attrs, optional_yield y)
{
  // Assign updated bucket attributes map to attrs map variable
  attrs = new_attrs;
  // "put_info" second bool argument is meant to update existing metadata,
  // which is not needed here. So explicitly passing false.
  return put_info(dpp, false, ceph::real_time());
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
  return -ENOENT;
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
  if (max == 0)  // Return an emtpy response.
    return 0;

  string tenant_bkt_name = get_bucket_name(info.bucket.tenant, info.bucket.name);

  ldpp_dout(dpp, 20) << __func__ << ": bucket=" << tenant_bkt_name
                    << " prefix=" << params.prefix
                    << " marker=" << params.marker
                    << " max=" << max << dendl;
  int batch_size = 100;
  vector<string> keys(batch_size);
  vector<bufferlist> vals(batch_size);

  // Retrieve all `max` number of pairs.
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;

  // Modify the marker based on its type
  keys[0] = params.prefix;
  if (!params.marker.empty()) {
    keys[0] = params.marker.to_str();
    // Get the position of delimiter string
    int delim_pos = keys[0].find(params.delim, params.prefix.length());
    // If delimiter is present at the very end, append "\xff" to skip all
    // the dir entries, else append " " to skip the maker key.
    if (delim_pos == (int)(keys[0].length() - params.delim.length()))
      keys[0].append("\xff");
    else
      keys[0].append(" ");
  }
  
  results.is_truncated = false;
  int keycount=0;
  std::string next_key;
  while (keycount <= max) {
    if(!next_key.empty())
      keys[0] = next_key;
    rc = store->next_query_by_name(bucket_index_iname, keys, vals, params.prefix,
                                   params.delim);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: next_query_by_name failed, rc = " << rc << dendl;
      return rc;
    }
    ldpp_dout(dpp, 20) << __func__ << ": items: " << rc << dendl;
    // Process the returned pairs to add into ListResults.
     for (int i = 0; i < rc; ++i) {
      ldpp_dout(dpp, 70) << __func__ << ": key["<<i<<"] :"<<keys[i]<<dendl;
      if(i == 0 && !next_key.empty()) {
        ldpp_dout(dpp, 70) << __func__ << ": skipping previous next_key: " << next_key << dendl;
        continue;
      }
      if (vals[i].length() == 0) {
        results.common_prefixes[keys[i]] = true;
      } else {
        rgw_bucket_dir_entry ent;
        auto iter = vals[i].cbegin();
        ent.decode(iter);
        std::string null_ref_key = ent.key.name + NULL_REF;
        if(keys[i] == null_ref_key){
          ldpp_dout(dpp, 70) << __func__ << ": skipping key "<<keys[i]<<dendl;
            continue;
        }
        if ( params.list_versions || ent.is_visible()) {
          if (keycount < max) {
            results.objs.emplace_back(std::move(ent));
            ldpp_dout(dpp, 70) << __func__ << ": adding key "<<keys[i]<<" to result"<<dendl;
          }
          if (keycount == max) {
            // One extra key is successfully fetched.
            results.next_marker = keys[i-1];
            results.is_truncated = true;
            ldpp_dout(dpp, 20) << __func__ << ": adding key "<<keys[i-1]<<" to next_marker"<<dendl;
            break;
          }
          keycount++;
        }
      }
    }
    if(rc == 0 || rc < batch_size || results.is_truncated) {
      break;
    }
    next_key = keys[rc-1]; // next marker key
    keys.clear();
    vals.clear();
    keys.resize(batch_size);
    vals.resize(batch_size);
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
  int rc = 0;
  if( max_uploads <= 0 )
	return rc;
  int upl = max_uploads;
  if( marker != "" )
	upl++;
  vector<string> key_vec(upl);
  vector<bufferlist> val_vec(upl);
  string tenant_bkt_name = get_bucket_name(this->get_tenant(), this->get_name());

  string bucket_multipart_iname =
      "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  key_vec[0].clear();
  key_vec[0].assign(marker.begin(), marker.end());
  rc = store->next_query_by_name(bucket_multipart_iname, key_vec, val_vec, prefix, delim);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: next_query_by_name failed, rc = " << rc << dendl;
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
      continue;

    if((marker != "") && (ocount == 0))
    {
        ocount++;
        continue;
    }
    rgw_bucket_dir_entry ent;
    auto iter = bl.cbegin();
    ent.decode(iter);

    rgw_obj_key key(ent.key);
    if (prefix.size() &&
        (0 != key.name.compare(0, prefix.size(), prefix))) {
      ldpp_dout(dpp, 20) << __func__ <<
        ": skippping \"" << ent.key <<
        "\" because doesn't match prefix" << dendl;
      continue;
    }

    uploads.push_back(this->get_multipart_upload(key.name));
    last_obj_key = key;
    ocount++;
    if (ocount == upl) {
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

uint64_t MotrStore::get_new_req_id()
{
  uint64_t req_id = ceph::util::generate_random_number<uint64_t>();

  addb_logger.set_id(req_id);
  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_GET_NEW_REQ_ID, RGW_ADDB_PHASE_START);
  
  return req_id;
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
  return (zonegroup->api_name == api);
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
  rgw_bucket_dir_entry ent;
  int rc = this->get_bucket_dir_ent(dpp, ent);
  if (rc < 0)
    return rc;

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

int MotrObject::fetch_obj_entry_and_key(const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& ent, std::string& bname, std::string& key, rgw_obj* target_obj)
{
  int rc = this->get_bucket_dir_ent(dpp, ent);
  if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": ERROR: failed to get object entry. rc=" << rc << dendl;
      return rc;
    }

  read_bucket_info(dpp, bname, key, target_obj);

  // getting key for version cases
  if(ent.key.instance == "null")
  {
    rc = this->fetch_null_obj_reference(dpp, key);
    if (rc < 0)
      return rc;
    ldpp_dout(dpp, 20) <<__func__<< ": fetching null version key instance " << key << dendl;
  } else {
    key =  ent.key.name + "[" + ent.key.instance + "]";
     ldpp_dout(dpp, 20) <<__func__<< ": fetching null version key instance for latest object " << key << dendl;
  }

   return 0;
}

int MotrObject::set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj)
{
  // TODO : Set tags for multipart objects
  if (this->category == RGWObjCategory::MultiMeta)
    return 0;

  rgw_bucket_dir_entry ent;
  string bname, key;
  int rc;

  rc = fetch_obj_entry_and_key(dpp, ent, bname, key, target_obj);
  if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to get key or object's entry from bucket index. rc= " << rc << dendl;
      return rc;
    }
  // set attributes present in setattrs
  if (setattrs != nullptr){
    for (auto& it : *setattrs){
      attrs[it.first]=it.second;
      ldpp_dout(dpp, 0) <<__func__<< " adding "<< it.first << " to attribute list." << dendl;
    }
  }

  // delete attributes present in delattrs
  if (delattrs != nullptr){
    for (auto& it: *delattrs){
      auto del_it = attrs.find(it.first);
        if (del_it != attrs.end()) {
            ldpp_dout(dpp, 0) <<__func__<< " removing "<< it.first << " from attribute list." << dendl;
          attrs.erase(del_it);
        }
    }
  }
  bufferlist update_bl;
  string bucket_index_iname = "motr.rgw.bucket.index." + bname;

  ent.meta.mtime = ceph::real_clock::now();
  ent.encode(update_bl);
  encode(attrs, update_bl);
  meta.encode(update_bl);

  rc = this->store->do_idx_op_by_name(bucket_index_iname, M0_IC_PUT, key, update_bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": Failed to put object's entry to bucket index. rc=" << rc << dendl;
    return rc;
  }
  // Put into cache.
  this->store->get_obj_meta_cache()->put(dpp, key, update_bl);

  return 0;
}

int MotrObject::get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  req_state *s = (req_state *) rctx->get_private();
  string req_method = s->info.method;
  /* TODO: Temp fix: Enabled Multipart-GET Obj. and disabled other multipart request methods */
  if (this->category == RGWObjCategory::MultiMeta && (req_method == "POST" || req_method == "PUT"))
   return 0;

  int rc;
  rgw_bucket_dir_entry ent;
  string bname, key;
  rc = fetch_obj_entry_and_key(dpp, ent, bname, key, target_obj);
  if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to get key or object's entry from bucket index. rc= " << rc << dendl;
      return rc;
    }

  return 0;
}

void MotrObject::read_bucket_info(const DoutPrefixProvider* dpp, std::string& bname, std::string& key, rgw_obj* target_obj)
{
  if (target_obj) {
    bname = get_bucket_name(target_obj->bucket.tenant, target_obj->bucket.name);
    key   = target_obj->key.to_str();
  } else {
    bname = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
    key   = this->get_key().to_str();
  }
  ldpp_dout(dpp, 20) << __func__ << ": for bucket " << bname << "/" << key << dendl;
}

int MotrObject::modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw_obj target = get_obj();
  sal::Attrs set_attrs;

  set_atomic(rctx);
  set_attrs[attr_name] = attr_val;
  return set_obj_attrs(dpp, rctx, &set_attrs, nullptr, y, &target);
}

int MotrObject::delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y)
{
  rgw_obj target = get_obj();
  Attrs rm_attr;
  bufferlist bl;

  set_atomic(rctx);
  rm_attr[attr_name] = bl;
  return set_obj_attrs(dpp, rctx, nullptr, &rm_attr, y, &target);
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
  // Creating version-id based on timestamp value
  // to list/store object versions in lexicographically sorted order.
  char buf[UUID_LEN + 1];
  std::string version_id;
  // As the version ID timestamp is encoded in Base62, the maximum value
  // for 8-characters is 62^8 - 1. This is the maximum time interval in ms.
  constexpr uint64_t max_ts_count = 218340105584895;
  using UnsignedMillis = std::chrono::duration<uint64_t, std::milli>;
  const auto ms_since_epoch = std::chrono::time_point_cast<UnsignedMillis>(
                              std::chrono::system_clock::now()).time_since_epoch().count();
  uint64_t cur_time = max_ts_count - ms_since_epoch;
  auto version_ts = base62_encode(cur_time, TS_LEN);
  gen_rand_alphanumeric_no_underscore(store->ctx(), buf, UUID_LEN+1);
  version_id = version_ts + buf;
  key.set_instance(version_id);
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
{ 
  struct req_state* s = static_cast<req_state*>(_rctx->get_private());
  ADDB(RGW_ADDB_REQUEST_OPCODE_ID, addb_logger.get_id(), s->op_type);
}

int MotrObject::MotrReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  int rc;
  ldpp_dout(dpp, 20) <<__func__<< ": bucket=" << source->get_bucket()->get_name() << dendl;

  rgw_bucket_dir_entry ent;
  rc = source->get_bucket_dir_ent(dpp, ent);
  if(ent.is_delete_marker())
  {
    rc = -ENOENT;
    return rc;
  }
  if (rc < 0)
    return rc;

  // In case of un-versioned/suspended case,
  // GET and HEAD object output should ignore versiodId field.
  if (!source->have_instance()) {
    if (ent.key.instance == "null")
      ent.key.instance.clear();
  }

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
  *params.lastmod = ent.meta.mtime;

  if (params.mod_ptr || params.unmod_ptr) {
    // Convert all times go GMT to make them compatible
    obj_time_weight src_weight;
    src_weight.init(*params.lastmod, params.mod_zone_id, params.mod_pg_ver);
    src_weight.high_precision = params.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = params.high_precision_time;

    // Check if-modified-since condition
    if (params.mod_ptr && !params.if_nomatch) {
      dest_weight.init(*params.mod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << __func__ << ": If-Modified-Since: " << dest_weight << " & "
                         << "Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    // Check if-unmodified-since condition
    if (params.unmod_ptr && !params.if_match) {
      dest_weight.init(*params.unmod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << __func__ << ": If-UnModified-Since: " << dest_weight << " & "
                         << "Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  // Check if-match condition
  if (params.if_match) {
    string if_match_str = rgw_string_unquote(params.if_match);
    ldpp_dout(dpp, 10) << __func__ << ": ETag: " << etag << " & "
                       << "If-Match: " << if_match_str << dendl;     
    if (if_match_str.compare(etag) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  // Check if-none-match condition
  if (params.if_nomatch) {
    string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
    ldpp_dout(dpp, 10) << __func__ << ": ETag: " << etag << " & "
                       << "If-NoMatch: " << if_nomatch_str << dendl;
    if (if_nomatch_str.compare(etag) == 0) {
      return -ERR_NOT_MODIFIED;
    }
  }
  return 0;
}

int MotrObject::MotrReadOp::read(int64_t off, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
{
  ldpp_dout(dpp, 20) <<  __func__ << ": sync read." << dendl;
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

  addb_logger.set_id(rctx);

  if (source->category == RGWObjCategory::MultiMeta) {
    ldpp_dout(dpp, 20) <<__func__<< ": open obj parts..." << dendl;
    rc = source->get_part_objs(dpp, this->part_objs)? :
         source->open_part_objs(dpp, this->part_objs);
    if (rc < 0) {
      ldpp_dout(dpp, 10) << __func__ << ": ERROR: failed to open motr object: rc = " << rc << dendl;
      return rc;
    }
    rc = source->read_multipart_obj(dpp, off, end, cb, part_objs);
  }
  else {
    ldpp_dout(dpp, 20) <<__func__<< ": open object..." << dendl;
    rc = source->open_mobj(dpp);
    if (rc < 0) {
      ldpp_dout(dpp, 10) << __func__ << ": ERROR: failed to open motr object: rc = " << rc << dendl;
      return rc;
    }
    rc = source->read_mobj(dpp, off, end, cb);
  }
  return rc;
}

int MotrObject::MotrReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  if (source == nullptr)
    return -ENODATA;
  rgw::sal::Attrs &attrs = source->get_attrs();
  auto iter = attrs.find(name);
  if (iter != attrs.end()) {
    dest = iter->second;
    return 0;
  }
  return -ENODATA;
}

std::unique_ptr<Object::DeleteOp> MotrObject::get_delete_op(RGWObjectCtx* ctx)
{
  return std::make_unique<MotrObject::MotrDeleteOp>(this, ctx);
}

MotrObject::MotrDeleteOp::MotrDeleteOp(MotrObject *_source, RGWObjectCtx *_rctx) :
  source(_source),
  rctx(_rctx)
{ 
  addb_logger.set_id(rctx);
}

// Implementation of DELETE OBJ also requires MotrObject::get_obj_state()
// to retrieve and set object's state from object's metadata.
int MotrObject::MotrDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
{
  int rc;
  bufferlist bl;
  string tenant_bkt_name = get_bucket_name(source->get_bucket()->get_tenant(), source->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  std::string delete_key;
  std::string null_ref_key = source->get_name() + NULL_REF;
  bool del_null_ref_key = false;
  rgw_bucket_dir_entry ent;
  RGWBucketInfo &info = source->get_bucket()->get_info();
  rc = source->get_bucket_dir_ent(dpp, ent);
  if (rc < 0) {
    ldpp_dout(dpp, 0)<<__func__<< ": Failed to get object's entry from bucket index. rc="<< rc << dendl;
    return rc;
  }
  if (source->have_instance()) {
    rgw_obj_key& key = source->get_key();
    delete_key = key.to_str();
    if (key.have_null_instance()) {
      // Read null reference key to get null version obj key.
      rc = source->fetch_null_obj_reference(dpp, delete_key);
      if (rc < 0)
        return rc;
    }
  } else {
    // // TODO: Avoid this lookup, Find a another way to fetch null version key.
    if (ent.key.instance == "null") {
      rc = source->fetch_null_obj_reference(dpp, delete_key);
      if (rc < 0)
        return rc;
      ldpp_dout(dpp, 0)<<__func__<< ": instance is null -  delete_key "<< delete_key << dendl;
    }
    else
      delete_key = source->get_name() + "[" + source->get_instance() + "]";
  }

  //TODO: When integrating with background GC for object deletion,
  // we should consider adding object entry to GC before deleting the metadata.
  // Delete from the cache first.
  source->store->get_obj_meta_cache()->remove(dpp, delete_key);

  // for delete-object without version-id parameter case,
  // if delete-marker is latest entry in bucket index, 
  // then return 0 without deleting any object.
  if (!source->have_instance() && ent.is_delete_marker()) {
    ldpp_dout(dpp, 0)<<__func__<< ": delete-marker is already present as latest entry in bucket index." << dendl;
    rc = 0;
    return 0;
  }

  // Remove the motr object.
  // versioning enabled and suspended case.
  if (info.versioned()) {
    if (source->have_instance()) {
      // delete object permanently.
      result.version_id = ent.key.instance;
      if (ent.is_delete_marker())
        result.delete_marker = true;
      ldpp_dout(dpp, 20) <<  __func__ << "delete " << delete_key << " from "
                            << tenant_bkt_name << dendl;

      rc = source->remove_mobj_and_index_entry(
          dpp, ent, delete_key, bucket_index_iname, tenant_bkt_name);
      if (rc < 0) {
        ldpp_dout(dpp, 0) << __func__ << ":Failed to delete the object from Motr."
	                          <<" key = " << delete_key << dendl;
        return rc;
      }
      // if deleted object version is the latest version,
      // then update is-latest flag to true for previous version.
      if (ent.is_current()) {
        ldpp_dout(dpp, 20)<<__func__<< "Updating previous version entries " << dendl;
        bool set_is_latest=true;
        rc = source->update_version_entries(dpp, set_is_latest);
        if (rc < 0)
          return rc;
      }
      // delete null reference key
      if (ent.key.instance == "null")
        del_null_ref_key = true;
    } else {
      // generate version-id for delete marker.
      result.delete_marker = true;
      source->gen_rand_obj_instance_name();
      std::string del_marker_ver_id = source->get_instance();

      result.version_id = del_marker_ver_id;
      if (!info.versioning_enabled()) {
        // for suspended bucket delete-marker's version-id = "null"
        result.version_id = "null";
        // if latest version is null version, then delete the null version-object and
        // add reference of delete-marker in null reference key.
        if (ent.key.instance == "null") {
          source->set_instance(ent.key.instance);
          rc = source->remove_mobj_and_index_entry(
            dpp, ent, delete_key, bucket_index_iname, tenant_bkt_name);
          if (rc < 0) {
            ldpp_dout(dpp, 0) << "Failed to delete the object from Motr. key- "<< delete_key << dendl;
            return rc;
          }
        }
        source->set_instance(del_marker_ver_id);
        // update delete-marker reference in null reference key.
        rc = source->update_null_reference(dpp, ent);
        if (rc<0) {
          ldpp_dout(dpp, 0) << "Failed to update null reference key bucket." << dendl;
          return rc;
        }
      }
      // update is-latest=false for current version entry.
      ldpp_dout(dpp, 20) << __func__ << " Updating previous version entries " << dendl;
      rc = source->update_version_entries(dpp);
      if (rc < 0)
        return rc;
      // creating a delete marker
      bufferlist del_mark_bl;
      rgw_bucket_dir_entry ent_del_marker;
      ent_del_marker.key.name = source->get_name();
      ent_del_marker.key.instance = result.version_id;
      ent_del_marker.meta.owner = params.obj_owner.get_id().to_str();
      ent_del_marker.meta.owner_display_name = params.obj_owner.get_display_name();
      ent_del_marker.flags = rgw_bucket_dir_entry::FLAG_DELETE_MARKER | rgw_bucket_dir_entry::FLAG_CURRENT;
      if (real_clock::is_zero(params.mtime))
        ent_del_marker.meta.mtime = real_clock::now();
      else
        ent_del_marker.meta.mtime = params.mtime;

      rgw::sal::Attrs attrs;
      ent_del_marker.encode(del_mark_bl);
      encode(attrs, del_mark_bl);
      ent_del_marker.meta.encode(del_mark_bl);

      // key for delete marker - obj1[delete-markers's ver-id].
      std::string delete_marker_key = source->get_key().to_str();
      ldpp_dout(dpp, 20)<<__func__<< "Add delete marker in bucket index, key=  " <<  delete_marker_key << dendl;
      rc = source->store->do_idx_op_by_name(bucket_index_iname,
                                            M0_IC_PUT, delete_marker_key, del_mark_bl);
      if(rc < 0) {
        ldpp_dout(dpp, 0) << "Failed to add delete marker in bucket." << dendl;
        return rc;
      }
      // Update in the cache.
      source->store->get_obj_meta_cache()->put(dpp, delete_marker_key, del_mark_bl);
    }
  } else {
    // Unversioned flow
    // handling empty size object case
    ldpp_dout(dpp, 20) << "delete " << delete_key << " from " << tenant_bkt_name << dendl;
    rc = source->remove_mobj_and_index_entry(
        dpp, ent, delete_key, bucket_index_iname, tenant_bkt_name);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "Failed to delete the object from Motr. key- "<< delete_key << dendl;
      return rc;
    }
    // delete null reference key.
    del_null_ref_key = true;
  }

  if (del_null_ref_key) {
    bl.clear();
    ldpp_dout(dpp, 0) <<__func__<< ": Deleting null reference key" << dendl;
    rc = source->store->do_idx_op_by_name(bucket_index_iname,
              M0_IC_DEL, null_ref_key, bl);
    if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< " ERROR: Unable to delete null reference key" << dendl;
      return rc;
    }
  }
  return 0;
}

int MotrObject::remove_mobj_and_index_entry(
    const DoutPrefixProvider* dpp, rgw_bucket_dir_entry& ent,
    std::string delete_key, std::string bucket_index_iname,
    std::string bucket_name) {
  int rc;
  bufferlist bl;
  uint64_t size_rounded = 0;

  // handling empty size object case
  if (ent.meta.size != 0) {
    if (ent.meta.category == RGWObjCategory::MultiMeta) {
      this->set_category(RGWObjCategory::MultiMeta);
      rc = this->delete_part_objs(dpp, &size_rounded);
    } else {
      // Handling Simple Object Deletion
      // Open the object if not already open.
      // No need to close mobj as delete_mobj will open it again
      if (mobj == nullptr) {
        rc = this->open_mobj(dpp);
        if (rc < 0) {
          ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(),
               RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_ERROR);
          return rc;
        }
      }
      uint64_t lid = M0_OBJ_LAYOUT_ID(mobj->ob_attr.oa_layout_id);
      uint64_t unit_sz = m0_obj_layout_id_to_unit_size(lid);
      size_rounded = roundup(ent.meta.size, unit_sz);

      rc = this->delete_mobj(dpp);
    }
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "Failed to delete the object " << delete_key  <<" from Motr. " << dendl;
      return rc;
    }
  }
  rc = this->store->do_idx_op_by_name(bucket_index_iname,
                                      M0_IC_DEL, delete_key, bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << "Failed to delete object's entry " << delete_key 
                                      << " from bucket index. " << dendl;
    return rc;
  }

  // Subtract object size & count from the bucket stats.
  if (ent.is_delete_marker())
    return rc;
  rc = update_bucket_stats(dpp, this->store, ent.meta.owner, bucket_name,
                           ent.meta.size, size_rounded, 1, false);
  if (rc != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed stats substraction for the "
      << "bucket/obj = " << bucket_name << "/" << delete_key
      << ", rc = " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 70) << __func__ << ": Stats subtracted successfully for the "
      << "bucket/obj = " << bucket_name << "/" << delete_key
      << ", rc = " << rc << dendl;

  return rc;
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

int MotrCopyObj_CB::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  int rc = 0;
  ldpp_dout(m_dpp, 20) << "Offset = " << bl_ofs << " Length = "
                       << " Write Offset = " << write_offset << bl_len << dendl;


  //offset is zero and bufferlength is equal to bl_len
  if (!bl_ofs && bl_len == bl.length()) {
    bufferptr bptr(bl.c_str(), bl_len);
    bufferlist blist;
    blist.push_back(bptr);
    rc = m_dst_writer->process(std::move(blist), write_offset);
    if(rc < 0){
      ldpp_dout(m_dpp, 20) << "ERROR: writer process bl_ofs=0 && " <<
                          "bl_len = " << bl.length() << " Write Offset = " <<
                          write_offset << "failed rc = " << rc << dendl;
    }
    write_offset += bl_len;
    return rc;
  }
  
  bufferptr bp(bl.c_str() + bl_ofs, bl_len);
  bufferlist new_bl;
  new_bl.push_back(bp);

  rc = m_dst_writer->process(std::move(new_bl), write_offset);
  if(rc < 0){
    ldpp_dout(m_dpp, 20) << "ERROR: writer process failed rc = " << rc
                         << " Write Offset = " << write_offset << dendl;
    return rc;
  }
  write_offset += bl_len;

  ldpp_dout(m_dpp, 20) << "MotrCopyObj_CB handle_data called rc = " << rc << dendl;
  return rc;
}


int MotrObject::copy_object_same_zone(RGWObjectCtx& obj_ctx,
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
  int rc = 0;
  std::string ver_id;
  std::string req_id;

  ldpp_dout(dpp, 20) << "Src Object Name : " << this->get_key().get_oid() << dendl;
  ldpp_dout(dpp, 20) << "Dest Object Name : " << dest_object->get_key().get_oid() << dendl;

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = this->get_read_op(&obj_ctx);

  // prepare read op
  read_op->params.lastmod = src_mtime;
  read_op->params.if_match = if_match;
  read_op->params.if_nomatch = if_nomatch;
  read_op->params.mod_ptr = mod_ptr;
  read_op->params.unmod_ptr = unmod_ptr;

  rc = read_op->prepare(y, dpp);
  if(rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: read op prepare failed rc = " << rc << dendl;
    return rc;
  }

  if(version_id){
    ver_id = *version_id;
  }
  if(tag){
    req_id = *tag;
  }

  // prepare write op
  std::shared_ptr<rgw::sal::Writer> dst_writer = store->get_atomic_writer(dpp, y,
        dest_object->clone(),
        user->get_id(), obj_ctx,
        &dest_placement, olh_epoch, req_id);

  rc = dst_writer->prepare(y);
  if(rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: writer prepare failed rc = " << rc << dendl;
    return rc;
  }

  // Create filter object.
  MotrCopyObj_CB cb(dpp, dst_writer);
  MotrCopyObj_Filter* filter = &cb;

  // Get offsets.
  int64_t cur_ofs = 0, cur_end = obj_size;
  rc = this->range_to_ofs(obj_size, cur_ofs, cur_end);
  if (rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: read op range_to_ofs failed rc = " << rc << dendl;
    return rc;
  }

  // read::iterate -> handle_data() -> write::process
  rc = read_op->iterate(dpp, cur_ofs, cur_end, filter, y);
  if (rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: read op iterate failed rc = " << rc << dendl;
    return rc;
  }

  real_time time = ceph::real_clock::now();
  if(mtime){
    *mtime = time;
  }

  //fetch etag.
  bufferlist bl;
  rc = read_op->get_attr(dpp, RGW_ATTR_ETAG, bl, y);
  if (rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: read op iterate failed rc = " << rc << dendl;
    return rc;
  }
  string etag_str;
  etag_str.assign(bl.c_str(), bl.length());

  if(etag){
    *etag = etag_str;
  }

  real_time del_time;

  // write::complete - overwrite and md handling done here
  rc = dst_writer->complete(obj_size, etag_str,
                      mtime, time,
                      attrs,
                      del_time,
                      if_match,
                      if_nomatch,
                      nullptr,
                      nullptr,
                      nullptr,
                      y);
  if (rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: writer complete failed rc = " << rc << dendl;
    return rc;
  }

  return rc;
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
  int rc = 0;
  auto& src_zonegrp = src_bucket->get_info().zonegroup;
  auto& dest_zonegrp = dest_bucket->get_info().zonegroup;

  if(src_zonegrp.compare(dest_zonegrp) != 0){
    ldpp_dout(dpp, 0) << __func__ << "Unsupported Action Requested." << dendl;
    return -ERR_NOT_IMPLEMENTED;
  }

  ldpp_dout(dpp, 20) << __func__ << "Src and Dest Zonegroups are same."
                    << "src_zonegrp : " << src_zonegrp
                    << "dest_zonegrp : " << dest_zonegrp << dendl;

  //
  // Check if src object is encrypted.
  rgw::sal::Attrs &src_attrs = this->get_attrs();
  if (src_attrs.count(RGW_ATTR_CRYPT_MODE)) {
    // Current implementation does not follow S3 spec and even
    // may result in data corruption silently when copying
    // multipart objects acorss pools. So reject COPY operations
    //on encrypted objects before it is fully functional.
    ldpp_dout(dpp, 0) << "ERROR: copy op for encrypted object has not been implemented." << dendl;
    return -ERR_NOT_IMPLEMENTED;
  }

  rc = copy_object_same_zone(obj_ctx,
                            user,
                            info,
                            source_zone,
                            dest_object,
                            dest_bucket,
                            src_bucket,
                            dest_placement,
                            src_mtime,
                            mtime,
                            mod_ptr,
                            unmod_ptr,
                            high_precision_time,
                            if_match,
                            if_nomatch,
                            attrs_mod,
                            copy_if_newer,
                            attrs,
                            category,
                            olh_epoch,
                            delete_at,
                            version_id,
                            tag,
                            etag,
                            progress_cb,
                            progress_data,
                            dpp,
                            y);
  if (rc < 0){
    ldpp_dout(dpp, 20) << "ERROR: copy_object_same_zone failed rc = " << rc << dendl;
    return rc;
  }

  ldpp_dout(dpp, 20) << "Copy op completed rc = " << rc << dendl;
  return rc;
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
        obj(_store, _head_obj->get_key(), _head_obj->get_bucket()) {
  struct req_state* s = static_cast<req_state*>(obj_ctx.get_private());
  req_id = s->id;
  addb_logger.set_id(req_id);

  ADDB(RGW_ADDB_REQUEST_OPCODE_ID, addb_logger.get_id(), s->op_type);
}

static const unsigned MAX_BUFVEC_NR = 256;

int MotrAtomicWriter::prepare(optional_yield y)
{
  total_data_size = 0;

  addb_logger.set_id(req_id);

  if (obj.is_opened())
    return 0;

  rgw_bucket_dir_entry ent;

  int rc = m0_bufvec_empty_alloc(&buf, MAX_BUFVEC_NR) ?:
           m0_bufvec_alloc(&attr, MAX_BUFVEC_NR, 1) ?:
           m0_indexvec_alloc(&ext, MAX_BUFVEC_NR);
  if (rc != 0)
    this->cleanup();

  return rc;
}

int MotrObject::create_mobj(const DoutPrefixProvider *dpp, uint64_t sz)
{
  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_START);

  if (mobj != nullptr) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_ERROR);
    ldpp_dout(dpp, 0) <<__func__<< "ERROR: object is already opened" << dendl;
    
    return -EINVAL;
  }

  int rc = m0_ufid_next(&ufid_gr, 1, &meta.oid);
  if (rc != 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_ERROR);
    ldpp_dout(dpp, 0) <<__func__<< "ERROR: m0_ufid_next() failed: " << rc << dendl;

    return rc;
  }
  expected_obj_size = sz;

  ldpp_dout(dpp, 20) << __func__ << ": key=" << this->get_key().to_str() << ", meta:oid=[0x" << std::hex
                                 << meta.oid.u_hi << ":0x" << std::hex  << meta.oid.u_lo << "]" << dendl;

  int64_t lid = m0_layout_find_by_objsz(store->instance, nullptr, sz);
  if (lid <= 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": failed to get lid: " << lid << dendl;
    return lid == 0 ? -EAGAIN : (int)lid;
  }

  M0_ASSERT(mobj == nullptr);
  mobj = new m0_obj();
  m0_obj_init(mobj, &store->container.co_realm, &meta.oid, lid);

  struct m0_op *op = nullptr;
  mobj->ob_entity.en_flags |= M0_ENF_META;
  rc = m0_entity_create(nullptr, &mobj->ob_entity, &op);
  if (rc != 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_ERROR);
    this->close_mobj();
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: m0_entity_create() failed, rc = " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 20) <<__func__<< ": call m0_op_launch()..." << dendl;
  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), 
       m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_ERROR);
    this->close_mobj();
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to create motr object. rc = " << rc << dendl;
    return rc;
  }

  meta.layout_id = mobj->ob_attr.oa_layout_id;
  meta.pver      = mobj->ob_attr.oa_pver;
  ldpp_dout(dpp, 20) << __func__ << ": key=" << this->get_key() << ", meta:oid=[0x" << std::hex << meta.oid.u_hi
                                  << ":0x" << std::hex << meta.oid.u_lo << "], meta:pvid=[0x" << std::hex
                                  << meta.pver.f_container << ":0x" << std::hex << meta.pver.f_key
                                  << "], meta:layout_id=0x" << std::hex << meta.layout_id << dendl;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_CREATE_MOBJ, RGW_ADDB_PHASE_DONE);
  // TODO: add key:user+bucket+key+obj.meta.oid value:timestamp to
  // gc.queue.index. See more at github.com/Seagate/cortx-rgw/issues/7.

  return rc;
}

int MotrObject::open_mobj(const DoutPrefixProvider *dpp)
{
  ldpp_dout(dpp, 20) << __func__ << ": key=" << this->get_key().to_str() << ", meta:oid=[0x" << std::hex
                                 << meta.oid.u_hi  << ":0x" << std::hex  << meta.oid.u_lo << "]" << dendl;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_START);

  int rc;
  if (meta.layout_id == 0) {
    rgw_bucket_dir_entry ent;
    rc = this->get_bucket_dir_ent(dpp, ent);
    if (rc < 0) {
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_ERROR);
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: get_bucket_dir_ent failed: rc = " << rc << dendl;
      return rc;
    }
  }

  if (meta.layout_id == 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_DONE);
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: did not find motr obj details." << dendl;
    return -ENOENT;
  }

  M0_ASSERT(mobj == nullptr);
  mobj = new m0_obj();
  memset(mobj, 0, sizeof *mobj);
  m0_obj_init(mobj, &store->container.co_realm, &meta.oid, store->conf.mc_layout_id);

  struct m0_op *op = nullptr;
  mobj->ob_attr.oa_layout_id = meta.layout_id;
  mobj->ob_attr.oa_pver      = meta.pver;
  mobj->ob_entity.en_flags  |= M0_ENF_META;
  ldpp_dout(dpp, 20) << __func__ << ": key=" << this->get_key().to_str() << ", meta:oid=[0x" << std::hex << meta.oid.u_hi
                                 << ":0x" << meta.oid.u_lo << "], meta:pvid=[0x" << std::hex << meta.pver.f_container
                                 << ":0x" << meta.pver.f_key << "], meta:layout_id=0x" << std::hex << meta.layout_id << dendl;
  rc = m0_entity_open(&mobj->ob_entity, &op);
  if (rc != 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_ERROR);
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: m0_entity_open() failed: rc =" << rc << dendl;
    this->close_mobj();
    return rc;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc < 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_ERROR);
    ldpp_dout(dpp, 10) << __func__ << ": ERROR: failed to open motr object: rc =" << rc << dendl;
    this->close_mobj();
    return rc;
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_OPEN_MOBJ, RGW_ADDB_PHASE_DONE);
  ldpp_dout(dpp, 20) <<__func__<< ": exit. rc =" << rc << dendl;

  return 0;
}

int MotrObject::delete_mobj(const DoutPrefixProvider *dpp)
{
  int rc;
  char fid_str[M0_FID_STR_LEN];
  snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&meta.oid));

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_START);

  if (!meta.oid.u_hi || !meta.oid.u_lo) {
    ldpp_dout(dpp, 20) << __func__ << ": invalid motr object oid=" << fid_str << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_ERROR);
    return -EINVAL;
  }
  ldpp_dout(dpp, 20) << __func__ << ": deleting motr object oid=" << fid_str << dendl;

  // Open the object.
  if (mobj == nullptr) {
    rc = this->open_mobj(dpp);
    if (rc < 0) {
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_ERROR);
      return rc;
    }
  }

  // Create an DELETE op and execute it (sync version).
  struct m0_op *op = nullptr;
  mobj->ob_entity.en_flags |= M0_ENF_META;
  rc = m0_entity_delete(&mobj->ob_entity, &op);
  if (rc != 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: m0_entity_delete() failed. rc = " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_ERROR);
    return rc;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to open motr object. rc = " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_ERROR);
    return rc;
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DELETE_MOBJ, RGW_ADDB_PHASE_DONE);

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

int MotrObject::write_mobj(const DoutPrefixProvider *dpp, bufferlist&& in_buffer, uint64_t offset)
{
  int rc;
  uint32_t flags = M0_OOF_FULL;
  int64_t bs, left;
  struct m0_op *op;
  char *start, *p;
  struct m0_bufvec buf;
  struct m0_bufvec attr;
  struct m0_indexvec ext;

  bufferlist data = std::move(in_buffer);

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_START);

  left = data.length();
  if (left == 0) {
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_DONE);
    
    return 0;
  }

  processed_bytes += left;
  int64_t available_data = 0;
  if (io_ctxt.accumulated_buffer_list.size() > 0) {
    // We are in data accumulation mode
    available_data = io_ctxt.total_bufer_sz;
  }

  bs = this->get_optimal_bs(left);
  ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs << dendl;
  if ((left + available_data) < bs) {
    // Determine if there are any further chunks/bytes from socket to be processed
    int64_t remaining_bytes = expected_obj_size - processed_bytes;
    if (remaining_bytes > 0) {
      if (io_ctxt.accumulated_buffer_list.size() == 0) {
        // Save offset
        io_ctxt.start_offset = offset;
      }
      // Append current buffer to the list of accumulated buffers
      ldpp_dout(dpp, 20) <<__func__<< " More data (" <<  remaining_bytes << " bytes) in-flight. Accumulating buffer..." << dendl;
      io_ctxt.accumulated_buffer_list.push_back(std::move(data));
      io_ctxt.total_bufer_sz += left;
      return 0;
    } else {
      // This is last IO. Check if we have previously accumulated buffers.
      // If not, simply use in_buffer/data
      if (io_ctxt.accumulated_buffer_list.size() > 0) {
        // Append last buffer
        io_ctxt.accumulated_buffer_list.push_back(std::move(data));
        io_ctxt.total_bufer_sz += left;
      }
    }
  } else if ((left + available_data) == bs)  {
    // Ready to write data to Motr. Add it to accumulated buffer
    if (io_ctxt.accumulated_buffer_list.size() > 0) {
      io_ctxt.accumulated_buffer_list.push_back(std::move(data));
      io_ctxt.total_bufer_sz += left;
    } // else, simply use in_buffer
  }

  rc = m0_bufvec_empty_alloc(&buf, 1) ?:
       m0_bufvec_alloc(&attr, 1, 1) ?:
       m0_indexvec_alloc(&ext, 1);
  if (rc != 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": buffer allocation failed, rc =" << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs << dendl;
  if (io_ctxt.accumulated_buffer_list.size() > 0) {
    // We have IO buffers accumulated. Transform it into single buffer.
    data.clear();
    for(auto &buffer: io_ctxt.accumulated_buffer_list) {
      data.claim_append(std::move(buffer));
    }
    offset = io_ctxt.start_offset;
    left = data.length();
    bs = this->get_optimal_bs(left);
    ldpp_dout(dpp, 20) <<__func__<< ": accumulated left=" << left << " bs=" << bs << dendl;
    io_ctxt.accumulated_buffer_list.clear();
  } else {
    // No accumulated buffers.
  }

  start = data.c_str();
  for (p = start; left > 0; left -= bs, p += bs, offset += bs) {
    if (left < bs) {
      bs = this->get_optimal_bs(left, true);
      flags |= M0_OOF_LAST;
    }
    if (left < bs) {
      ldpp_dout(dpp, 20) <<__func__<< " left ="<< left << ",bs=" << bs << ", Padding [" << (bs - left) << "] bytes" << dendl;
      data.append_zero(bs - left);
      p = data.c_str();
    }
    buf.ov_buf[0] = p;
    buf.ov_vec.v_count[0] = bs;
    ext.iv_index[0] = offset;
    ext.iv_vec.v_count[0] = bs;
    attr.ov_vec.v_count[0] = 0;

    ldpp_dout(dpp, 20) <<__func__<< ": write buffer bytes=[" << bs << "], at offset=[" << offset << "]" << dendl;
    op = nullptr;
    rc = m0_obj_op(this->mobj, M0_OC_WRITE, &ext, &buf, &attr, 0, flags, &op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": write failed, m0_obj_op rc="<< rc << dendl;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_ERROR);
      goto out;
    }
    ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": write failed, m0_op_wait rc="<< rc << dendl;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_ERROR);
      goto out;
    }
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_WRITE_MOBJ, RGW_ADDB_PHASE_DONE);
out:
  m0_indexvec_free(&ext);
  m0_bufvec_free(&attr);
  m0_bufvec_free2(&buf);
  // Reset io_ctxt state
  io_ctxt.start_offset = 0;
  io_ctxt.total_bufer_sz = 0;
  return rc;
}

int MotrObject::read_mobj(const DoutPrefixProvider* dpp, int64_t start, int64_t end, RGWGetDataCB* cb)
{
  int rc;
  uint32_t flags = 0;
  unsigned bs, skip;
  int64_t left = end + 1, off;
  uint64_t req_id;
  struct m0_op *op;
  struct m0_bufvec buf;
  struct m0_bufvec attr;
  struct m0_indexvec ext;

  req_id = addb_logger.get_id();
  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_READ_MOBJ, RGW_ADDB_PHASE_START);

  ldpp_dout(dpp, 20) <<__func__<< ": start=" << start << " end=" << end << dendl;

  rc = m0_bufvec_empty_alloc(&buf, 1) ? :
       m0_bufvec_alloc(&attr, 1, 1) ? :
       m0_indexvec_alloc(&ext, 1);
  if (rc < 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": vecs alloc failed: rc="<< rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_READ_MOBJ, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  bs = this->get_optimal_bs(left);

  for (off = 0; left > 0; left -= bs, off += bs) {
    if (left < bs)
      bs = this->get_optimal_bs(left); // multiple of groups

    if (start >= off + bs)
      continue; // to the next block

    // At the last parity group we must read up to the last
    // object's unit and provide the M0_OOF_LAST flag, so
    // that in case of degraded read mode, libmotr could
    // know which units to use for the data recovery.
    if ((size_t)off + bs >= obj_size) {
      bs = roundup(obj_size - off, get_unit_sz());
      flags |= M0_OOF_LAST;
    } else if (left < bs) {
      // Somewhere in the middle of the object.
      bs = this->get_optimal_bs(left, true); // multiple of units
    }

    // Skip reading the units which are not requested.
    if (start > off) {
      skip = rounddown(start, get_unit_sz()) - off;
      off += skip;
      bs -= skip;
      left -= skip;
    }

    // Read from Motr.
    ldpp_dout(dpp, 20) <<__func__<< ": off=" << off << " bs=" << bs << dendl;
    bufferlist bl;
    buf.ov_buf[0] = bl.append_hole(bs).c_str();
    buf.ov_vec.v_count[0] = bs;
    ext.iv_index[0] = off;
    ext.iv_vec.v_count[0] = bs;
    attr.ov_vec.v_count[0] = 0;

    op = nullptr;
    rc = m0_obj_op(this->mobj, M0_OC_READ, &ext, &buf, &attr, 0, flags, &op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": motr op failed: rc=" << rc << dendl;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_READ_MOBJ, RGW_ADDB_PHASE_ERROR);
      goto out;
    }

    ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": m0_op_wait failed: rc=" << rc << dendl;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_READ_MOBJ, RGW_ADDB_PHASE_ERROR);
      goto out;
    }

    // Call `cb` to process returned data.
    skip = 0;
    if (start > off)
      skip = start - off;
    if(cb){
      ldpp_dout(dpp, 20) << __func__  << " call cb to process data" << dendl;
      cb->handle_data(bl, skip, (left < bs ? left : bs) - skip);
      if (rc != 0){
        ldpp_dout(dpp, 0) << __func__ << " handle_data failed rc =" << rc << dendl;
        goto out;
      }
    }

    addb_logger.set_id(req_id);
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_READ_MOBJ, RGW_ADDB_PHASE_DONE);
out:
  m0_indexvec_free(&ext);
  m0_bufvec_free(&attr);
  m0_bufvec_free2(&buf);
  this->close_mobj();

  return rc;
}

int MotrObject::fetch_null_obj_reference(const DoutPrefixProvider *dpp, std::string& prev_null_obj_key, bool raise_error)
{
  int rc = 0;
  // Read the null index entry
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  std::string null_ref_key = this->get_name() + NULL_REF;
  bufferlist bl;
  bufferlist::const_iterator iter;
  rgw_bucket_dir_entry ent_null_ref;

  // Check entry in the cache
  if (this->store->get_obj_meta_cache()->get(dpp, null_ref_key, bl)) {
    rc = this->store->do_idx_op_by_name(bucket_index_iname,
                              M0_IC_GET, null_ref_key, bl);
    ldpp_dout(dpp, 20) <<__func__<< ":  GET null index entry "<< null_ref_key <<", rc = "<< rc << dendl;
    // For the first put-object, null ref entry will not be present in the bucket and rc = -ENOENT.
    // Handle motr return code for above scenario.
    if (rc == -ENOENT && raise_error == false) {
      rc = 0;
      return rc;
    }
    if (rc < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: Failed to get key- "<< null_ref_key <<"from index rc=" << rc << dendl;
      return rc;
    }
    bufferlist& blr = bl;
    iter = blr.cbegin();
    ent_null_ref.decode(iter);
  }
  prev_null_obj_key = ent_null_ref.key.name + '[' + ent_null_ref.key.instance + ']';
  return rc;
}

int MotrObject::fetch_null_obj(const DoutPrefixProvider *dpp, bufferlist& bl, bool raise_error)
{
  int rc = 0;
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  std::string null_obj_key;

  // Get content of the key and replace it's instance with null
  // then add into cache
  rc = this->fetch_null_obj_reference(dpp, null_obj_key, raise_error);
  if (rc < 0)
     return rc;
  // 1st put, no null reference entry present in bucket.
  if (null_obj_key.empty()) {
    ldpp_dout(dpp, 20) << __func__ << ": Null reference index entry is not present in the bucket." << dendl;
    return rc;
  }
  if (this->store->get_obj_meta_cache()->get(dpp, null_obj_key, bl)) {
    rc = this->store->do_idx_op_by_name(bucket_index_iname,
            M0_IC_GET, null_obj_key, bl);

    if (rc < 0){
      ldpp_dout(dpp, 0) << __func__ << "ERROR: Key - " << null_obj_key << "does not exist. rc=" << rc << dendl;
      return rc;
    }
    // Put into the cache
    this->store->get_obj_meta_cache()->put(dpp, null_obj_key, bl);
  }
  return rc;
}

int MotrObject::get_bucket_dir_ent(const DoutPrefixProvider *dpp, rgw_bucket_dir_entry& ent)
{
  int rc = 0;
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  int max = 2;
  vector<string> keys(max);
  vector<bufferlist> vals(max);
  bufferlist bl;
  bufferlist::const_iterator iter;

  if(this->have_instance())
  {
    if(this->get_key().have_null_instance())
    {
      rc = this->fetch_null_obj(dpp, bl);
      // error log handled in above function.
      if (rc < 0)
        return rc;

      bufferlist& blr = bl;
      iter = blr.cbegin();
      ent.decode(iter);
      goto out;
    }

    // Check entry in the cache
    if (this->store->get_obj_meta_cache()->get(dpp, this->get_key().to_str(), bl) == 0) {
      iter = bl.cbegin();
      ent.decode(iter);
      rc = 0;
      goto out;
    }
    // Cache miss.
    rc = this->store->do_idx_op_by_name(bucket_index_iname,
                        M0_IC_GET, this->get_key().to_str(), bl);
    if(rc < 0) {
      ldpp_dout(dpp, 0) << __func__ << " ERROR: do_idx_op_by_name failed to get object's entry: rc="
                        << rc << dendl;
      return rc;
    }

    bufferlist& blr2 = bl;
    iter = blr2.cbegin();
    ent.decode(iter);

    // Put into the cache
    this->store->get_obj_meta_cache()->put(dpp, this->get_key().to_str(), bl);
    goto out;
  }
  else
  {
    // Cache miss.
    keys[0] = this->get_name() + "[";

    // Retrieve all 'max' number of pairs.
    rc = store->next_query_by_name(bucket_index_iname, keys, vals, this->get_name());
    if (rc < 0) {
      ldpp_dout(dpp, 0) << __func__ << "ERROR: NEXT query failed. " << rc << dendl;
      return rc;
      } 
    // No key found
    if(rc == 0){
      ldpp_dout(dpp, 0) << __func__ << "ERROR: Key does not exist. " << rc << dendl;
      return -ENOENT;
      }

    ldpp_dout(dpp, 20) <<__func__<< ": found current version!" << dendl;
    rc=0;
    iter = vals[0].cbegin();
    ent.decode(iter);
    this->store->get_obj_meta_cache()->put(dpp, this->get_key().to_str(), bl);
    goto out;
  }

out:
  if (rc == 0) {
    decode(attrs, iter);
    meta.decode(iter);
    ldpp_dout(dpp, 20) <<__func__<< ": lid=0x" << std::hex << meta.layout_id << dendl;

    char fid_str[M0_FID_STR_LEN];
    snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&meta.oid));
    ldpp_dout(dpp, 70) << __func__ << ": oid=" << fid_str << dendl;
  } else
    ldpp_dout(dpp, 0) <<__func__<< ": rc = " << rc << dendl;

  return rc;
}

int MotrObject::update_version_entries(const DoutPrefixProvider *dpp, bool set_is_latest)
{
  int max = 2;
  vector<string> keys(max);
  vector<bufferlist> vals(max);
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;

  keys[0] = this->get_name() + "[";
  int rc = store->next_query_by_name(bucket_index_iname, keys, vals);
  ldpp_dout(dpp, 20) << "get all versions, name = " << this->get_name() << "rc = " << rc << dendl;
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: NEXT query failed. rc = " << rc << dendl;
    return rc;
  }

  // no entries returned.
  if (rc == 0)
  {
    ldpp_dout(dpp, 0) <<__func__<<": No entries found, rc = " << rc << dendl;
    return 0;
  }
  rgw_bucket_dir_entry ent;
  int i = 0;
  for (; i < rc; ++i) {
    if (vals[i].length() == 0)
      break;
  
    auto iter = vals[i].cbegin();
    ent.decode(iter);

    std::string null_ref_key = ent.key.name + NULL_REF;
    if(keys[i] == null_ref_key)
      continue;

    if (0 != ent.key.name.compare(0, this->get_name().size(), this->get_name()))
      continue;

    // In case of (delete-object flow) we are setting set_is_latest=true,
    // and if it is true then update is-latest flag to true for previous version.
    // in case of (put-object flow) we are not passing set_is_latest parameter(default value is false),
    // and if it is false then update is-latest flag to false for previous version.
    if (!ent.is_current())
    {
      if(!set_is_latest)
        continue;
    }
    // Remove from the cache.
    store->get_obj_meta_cache()->remove(dpp, this->get_name());
    rgw::sal::Attrs attrs;
    decode(attrs, iter);
    MotrObject::Meta meta;
    meta.decode(iter);
    
    if(set_is_latest)
    {
      // delete-object flow
      // set is-latest=true for delete-marker/ normal object.
      if(ent.is_delete_marker())
        ent.flags = rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
      else
        ent.flags = rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
    }
    else{
      // put-object flow, set is-latest=false for delete-marker/ normal object.
      if(ent.is_delete_marker())
        ent.flags = rgw_bucket_dir_entry::FLAG_DELETE_MARKER | rgw_bucket_dir_entry::FLAG_VER;
      else
        ent.flags = rgw_bucket_dir_entry::FLAG_VER;
    }
    string key;
    if (ent.key.instance.empty())
      key = ent.key.name;
    else 
      key = keys[i];
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

      ldpp_dout(dpp, 20) << __func__ << ": off = " << off << ", size = " << part_size << dendl;
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
    ldpp_dout(dpp, 20) << __func__ << ": name = " << obj->get_name() << dendl;
    int rc = obj->open_mobj(dpp);
    if (rc < 0)
      return rc;
  }

  return 0;
}

int MotrObject::delete_part_objs(const DoutPrefixProvider* dpp,
                                 uint64_t* size_rounded) {
  string version_id = this->get_instance();
  std::unique_ptr<rgw::sal::MultipartUpload> upload;
  upload = this->get_bucket()->get_multipart_upload(this->get_name(), string());
  std::unique_ptr<rgw::sal::MotrMultipartUpload> mupload(static_cast<rgw::sal::MotrMultipartUpload *>(upload.release()));
  return mupload->delete_parts(dpp, version_id, size_rounded);
}

int MotrObject::read_multipart_obj(const DoutPrefixProvider* dpp,
                                   int64_t off, int64_t end, RGWGetDataCB* cb,
				   std::map<int, std::unique_ptr<MotrObject>>& part_objs)
{
  int64_t cursor = off;

  ldpp_dout(dpp, 20) << __func__ << ": off=" << off << " end=" << end << dendl;

  // Find the parts which are in the (off, end) range and
  // read data from it. Note: `end` argument is inclusive.
  for (auto iter = part_objs.begin(); iter != part_objs.end(); ++iter) {
    MotrObject* obj = static_cast<MotrObject *>(iter->second.get());
    int64_t part_off = obj->part_off;
    int64_t part_size = obj->part_size;
    int64_t part_end = obj->part_off + obj->part_size - 1;
    ldpp_dout(dpp, 20) << __func__ << ": part_off=" << part_off
                                          << " part_end=" << part_end << dendl;
    if (part_end < off)
      continue;

    int64_t local_off = cursor - obj->part_off;
    int64_t local_end = part_end < end? part_size - 1 : end - part_off;
    ldpp_dout(dpp, 20) << __func__ << ": name=" << obj->get_name()
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

unsigned MotrObject::get_unit_sz()
{
  uint64_t lid = M0_OBJ_LAYOUT_ID(meta.layout_id);
  return m0_obj_layout_id_to_unit_size(lid);
}

// The optimal bs will be rounded up to the unit size, if last is true,
// so use M0_OOF_LAST flag to avoid RMW for the last block.
// Otherwise, bs will be rounded up to the group size.
unsigned MotrObject::get_optimal_bs(unsigned len, bool last)
{
  struct m0_pool_version *pver;

  pver = m0_pool_version_find(&store->instance->m0c_pools_common,
                              &mobj->ob_attr.oa_pver);
  M0_ASSERT(pver != nullptr);
  struct m0_pdclust_attr *pa = &pver->pv_attr;
  unsigned unit_sz = get_unit_sz();
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
  else if (last)
    return roundup(len, unit_sz);
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

int MotrAtomicWriter::write(bool last)
{
  int rc;
  uint32_t flags = M0_OOF_FULL;
  int64_t bs, done, left;
  struct m0_op *op;
  bufferlist::iterator bi;

  left = acc_data.length();

  addb_logger.set_id(req_id);

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_WRITE, RGW_ADDB_PHASE_START);
  
  if (!obj.is_opened()) {
    rc = obj.create_mobj(dpp, left);
    if (rc == -EEXIST)
      rc = obj.open_mobj(dpp);
    if (rc != 0) {
      char fid_str[M0_FID_STR_LEN];
      snprintf(fid_str, ARRAY_SIZE(fid_str), U128X_F, U128_P(&obj.meta.oid));
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: failed to create/open motr object "
                        << fid_str << " (" << obj.get_bucket()->get_name()
                        << "/" << obj.get_key().to_str() << "): rc = " << rc
                        << dendl;
      goto err;
    }
  }

  bs = obj.get_optimal_bs(left, last);
  ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs
                               << " last=" << last << dendl;
  bi = acc_data.begin();
  while (left > 0) {
    if (left < bs) {
      if (!last)
        break; // accumulate more data
      bs = obj.get_optimal_bs(left, last);
    }
    if (left < bs) { // align data to unit-size
      ldpp_dout(dpp, 20) <<__func__<< " Padding [" << (bs - left) << "] bytes" << dendl;
      acc_data.append_zero(bs - left);
      auto off = bi.get_off();
      bufferlist tmp;
      acc_data.splice(off, bs, &tmp);
      acc_data.clear();
      acc_data.append(tmp.c_str(), bs); // make it a single buf
      bi = acc_data.begin();
      left = bs;
    }
    ldpp_dout(dpp, 20) <<__func__<< ": left=" << left << " bs=" << bs << dendl;
    done = this->populate_bvec(bs, bi);
    left -= done;

    if (last)
      flags |= M0_OOF_LAST;

    op = nullptr;
    rc = m0_obj_op(obj.mobj, M0_OC_WRITE, &ext, &buf, &attr, 0, flags, &op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": write failed, m0_obj_op rc="<< rc << dendl;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_WRITE, RGW_ADDB_PHASE_ERROR);
      goto err;
    }

    ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));

    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);
    if (rc != 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": write failed, m0_op_wait rc="<< rc << dendl;
      goto err;
    }

    total_data_size += done;
  }

  if (last) {
    acc_data.clear();
  } else if (bi.get_remaining() < acc_data.length()) {
    // Clear from the accumulator what has been written already.
    // XXX Optimise this, if possible, to avoid copying.
    ldpp_dout(dpp, 0) <<__func__<< ": cleanup "<< acc_data.length() -
                                                  bi.get_remaining()
                                << " bytes from the accumulator" << dendl;
    bufferlist tmp;
    bi.copy(bi.get_remaining(), tmp);
    acc_data.clear();
    acc_data.append(std::move(tmp));
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_WRITE, RGW_ADDB_PHASE_DONE);
  return 0;

err:
  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_WRITE, RGW_ADDB_PHASE_ERROR);
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
      rc = this->write(true);
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

int MotrObject::update_null_reference(const DoutPrefixProvider *dpp, rgw_bucket_dir_entry& ent)
{
  int rc = 0;
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());

  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  std::string null_ref_key = this->get_name() + NULL_REF;
  bufferlist::const_iterator iter;
  bufferlist bl_null_idx_val;
  rgw_bucket_dir_entry current_null_key_ref;
 
  current_null_key_ref.key.name = this->get_name();
  current_null_key_ref.key.instance = this->get_instance();
  current_null_key_ref.encode(bl_null_idx_val);
  // add new null entry to the motr
  // (key:{obj1^null}, value:{obj1[v123]}) (this is null object version)
  rc = this->store->do_idx_op_by_name(bucket_index_iname,
                            M0_IC_PUT, null_ref_key, bl_null_idx_val);  
  if(rc < 0)
  { 
    ldpp_dout(dpp, 0) <<__func__<< " ERROR: Unable to PUT null index key" << dendl;
    return rc;
  }
  ldpp_dout(dpp, 20) <<__func__<< ": Update null version index key, rc : " << rc << dendl; 
  store->get_obj_meta_cache()->put(dpp, null_ref_key, bl_null_idx_val);
  return rc;
}

int MotrObject::overwrite_null_obj(const DoutPrefixProvider *dpp)
{
  int rc;
  string tenant_bkt_name = get_bucket_name(this->get_bucket()->get_tenant(), this->get_bucket()->get_name());
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  std::string obj_type = "simple object";
  rgw_bucket_dir_entry old_ent;
  bufferlist old_check_bl;
  bool raise_error = false;
  rc = this->fetch_null_obj(dpp, old_check_bl, raise_error);
  if (rc < 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": Failed to fetch null object, rc : " << rc << dendl;
    return rc;
  }
  //TODO: Remove this call in optimization tkt CORTX-31977
  std::string null_obj_key;
  rc = this->fetch_null_obj_reference(dpp, null_obj_key, raise_error);
  if (rc < 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": Failed to fetch null reference key, rc : " << rc << dendl;
    return rc;
  }

  if (rc == 0 && old_check_bl.length() > 0) {
      auto ent_iter = old_check_bl.cbegin();
      old_ent.decode(ent_iter);
      rgw::sal::Attrs attrs;
      decode(attrs, ent_iter);
      this->meta.decode(ent_iter);
      this->set_instance(std::move(old_ent.key.instance));
      if (old_ent.meta.category == RGWObjCategory::MultiMeta)
          obj_type = "multipart object";
      ldpp_dout(dpp, 20) <<__func__<< ": Old " << obj_type << " exists" << dendl;
      rc = this->remove_mobj_and_index_entry(
          dpp, old_ent, null_obj_key, bucket_index_iname, tenant_bkt_name);
      if (rc == 0) {
          ldpp_dout(dpp, 20) <<__func__<< ": Old " << obj_type << " ["
            << this->get_name() <<  "] deleted succesfully" << dendl;
      } else {
          ldpp_dout(dpp, 0) << __func__<<": Failed to delete old " << obj_type << " ["
            << this->get_name() <<  "]. Error = " << rc << dendl;
          // TODO: This will be handled during GC
      }
    }
  return rc;
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

  addb_logger.set_id(req_id);

  if (acc_data.length() != 0) { // check again, just in case
    rc = this->write(true);
    this->cleanup();
    if (rc != 0)
      return rc;
  }

  bufferlist bl;
  rgw_bucket_dir_entry ent;

  // Set rgw_bucket_dir_entry. Some of the member of this structure may not
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
  uint64_t size_rounded = 0;
  // For 0kb Object layout_id will not be available.
  if(ent.meta.size != 0)
  {
    uint64_t lid = M0_OBJ_LAYOUT_ID(obj.meta.layout_id);
    uint64_t unit_sz = m0_obj_layout_id_to_unit_size(lid);
    size_rounded = roundup(ent.meta.size, unit_sz);
  }
  RGWBucketInfo &info = obj.get_bucket()->get_info();

  if (!obj.get_key().have_instance()) {
    // generate-version-id for null version.
    obj.gen_rand_obj_instance_name();
    ent.key.instance = "null";
   }

  // Set version and current flag in case of both versioning enabled and suspended case.
  if (info.versioned())
    ent.flags = rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;

  ldpp_dout(dpp, 20) << __func__ << ": key=" << obj.get_key().to_str() << ", meta:oid=[0x" << std::hex << obj.meta.oid.u_hi
                                 << ":0x" << obj.meta.oid.u_lo << "], meta:pvid=[0x" << std::hex << obj.meta.pver.f_container
                                 << ":0x" << obj.meta.pver.f_key << "], meta:layout_id=0x" << std::hex << obj.meta.layout_id
                                 << " etag=" << etag << " user_data=" << user_data << dendl;
  if (user_data)
    ent.meta.user_data = *user_data;

  ent.encode(bl);

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

  // Update existing object version entries in a bucket,
  // in case of both versioning enabled and suspended.
  if (info.versioned()) {
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

  string tenant_bkt_name = get_bucket_name(obj.get_bucket()->get_tenant(), obj.get_bucket()->get_name());
  // Insert an entry into bucket index.
  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;

  if (!info.versioning_enabled()) {
    std::unique_ptr<rgw::sal::Object> old_obj = obj.get_bucket()->get_object(rgw_obj_key(obj.get_name()));
    rgw::sal::MotrObject *old_mobj = static_cast<rgw::sal::MotrObject *>(old_obj.get());
    rc = old_mobj->overwrite_null_obj(dpp);
    if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to overwrite null object, rc : " << rc << dendl;
      return rc;
    }
    rc = obj.update_null_reference(dpp, ent);
    if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to update null reference, rc : " << rc << dendl;
      return rc;
    }
  }

  rc = store->do_idx_op_by_name(bucket_index_iname,
                               M0_IC_PUT, obj.get_key().to_str(), bl);
  if (rc != 0) {
    // TODO: handle this object leak via gc.  
    ldpp_dout(dpp, 0) << __func__ << ": index operation failed, rc = "<< rc << dendl;
    return rc;
  }
  store->get_obj_meta_cache()->put(dpp, obj.get_key().to_str(), bl);


  // Add object size and count in bucket stats entry.
  rc = update_bucket_stats(dpp, store, owner.to_str(), tenant_bkt_name,
                           total_data_size, size_rounded);
  if (rc != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed stats additon for the bucket/obj = "
      << tenant_bkt_name << "/" << obj.get_name() << ", rc = " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 70) << __func__ << ": Stats added successfully for the bucket/obj = "
    << tenant_bkt_name << "/" << obj.get_name() << ", rc = " << rc << dendl;

  // TODO: We need to handle the object leak caused by parallel object upload by
  // making use of background gc, which is currently not enabled for motr.
  return rc;
}

int MotrMultipartUpload::delete_parts(const DoutPrefixProvider *dpp, std::string version_id, uint64_t* size_rounded)
{
  int rc;
  int max_parts = 1000;
  int total_parts_fetched = 0;
  uint64_t total_size = 0, total_size_rounded = 0;
  int marker = 0;
  bool truncated = false;

  // Scan all parts and delete the corresponding motr objects.
  do {
    rc = this->list_parts(dpp, store->ctx(), max_parts, marker, &marker, &truncated);
    if (rc == -ENOENT) {
      truncated = false;
      rc = 0;
    }
    if (rc < 0)
      return rc;

    std::map<uint32_t, std::unique_ptr<MultipartPart>>& parts = this->get_parts();
    total_parts_fetched += parts.size();
    for (auto part_iter = parts.begin(); part_iter != parts.end(); ++part_iter) {

      MultipartPart *mpart = part_iter->second.get();
      MotrMultipartPart *mmpart = static_cast<MotrMultipartPart *>(mpart);
      uint32_t part_num = mmpart->get_num();
      total_size += mmpart->get_size();
      total_size_rounded += mmpart->get_size_rounded();

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
      mobj->meta = mmpart->meta;
      rc = mobj->delete_mobj(dpp);
      if (rc < 0) {
        ldpp_dout(dpp, 0) << __func__ << ": failed to delete object from Motr. rc = " << rc << dendl;
        return rc;
      }
    }
  } while (truncated);

  string tenant_bkt_name = get_bucket_name(bucket->get_tenant(), bucket->get_name());
  string upload_id = get_upload_id();
  string key_name;

  if (upload_id.length() == 0){
    std::unique_ptr<rgw::sal::Object> obj_ver = this->bucket->get_object(rgw_obj_key(this->get_key()));
    rgw::sal::MotrObject *mobj_ver = static_cast<rgw::sal::MotrObject *>(obj_ver.get());
    RGWBucketInfo &info = this->bucket->get_info();

    // if the bucket is unversioned and instance is empty
    // then fetch the null object reference to get instance.
    key_name = this->get_key() + "[" + version_id + "]";
    if(!info.versioned() || version_id == "null")
    {
      int ret_rc = mobj_ver->fetch_null_obj_reference(dpp, key_name);
      if(ret_rc < 0) {
        ldpp_dout(dpp, 0) << __func__ << " : failed to get null object reference, ret_rc : "<< ret_rc << dendl;
        return ret_rc;
      }
    }

    rc = store->get_upload_id(tenant_bkt_name, key_name, upload_id);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: get_upload_id failed. rc = " << rc << dendl;
      return rc;
    }
  }
  if (size_rounded != nullptr)
    *size_rounded = total_size_rounded;

  if (get_upload_id().length()) {
    // Subtract size & count of all the parts if multipart is not completed.
    rc = update_bucket_stats(dpp, store,
                             bucket->get_owner()->get_id().to_str(), tenant_bkt_name,
                             total_size, total_size_rounded, total_parts_fetched, false);
    if (rc != 0) {
      ldpp_dout(dpp, 20) << __func__ << ": Failed stats substraction for the "
        << "bucket/obj = " << tenant_bkt_name << "/" << mp_obj.get_key()
        << ", rc = " << rc << dendl;
      return rc;
    }
    ldpp_dout(dpp, 70) << __func__ << ": Stats subtracted successfully for the "
        << "bucket/obj = " << tenant_bkt_name << "/" << mp_obj.get_key()
        << ", rc = " << rc << dendl;
  }

  // Delete object part index.
  string obj_part_iname = "motr.rgw.object." + tenant_bkt_name + "." + mp_obj.get_key() + 
                          "." + upload_id + ".parts";
  return store->delete_motr_idx_by_name(obj_part_iname);
}

int MotrMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct,
                                RGWObjectCtx *obj_ctx)
{
  int rc;
  // Check if multipart upload exists
  bufferlist bl;
  std::unique_ptr<rgw::sal::Object> meta_obj;
  meta_obj = get_meta_obj();
  string tenant_bkt_name = get_bucket_name(meta_obj->get_bucket()->get_tenant(), meta_obj->get_bucket()->get_name());
  string bucket_multipart_iname =
      "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  rc = store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_GET, meta_obj->get_key().to_str(), bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": failed to get multipart upload. rc = " << rc << dendl;
    return rc == -ENOENT ? -ERR_NO_SUCH_UPLOAD : rc;
  }

  // Scan all parts and delete the corresponding motr objects.
  rc = this->delete_parts(dpp);
  if (rc < 0)
    return rc;

  bl.clear();
  // Remove the upload from bucket multipart index.
  rc = store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_DEL, meta_obj->get_key().to_str(), bl);
  if (rc != 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": WARNING:  index opration failed, M0_IC_DEL rc = "<< rc << dendl;
  }
  return rc;
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
  string upload_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(dest_placement, bl);
    encode(upload_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(dest_placement, bl);
    decode(upload_id, bl);
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
    upload_info.upload_id = upload_id;
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
    std::unique_ptr<RGWObjTags> obj_tags;
    req_state *s = (req_state *) obj_ctx->get_private();
    /* handle object tagging */
    // Verify tags exists and add to attrs
    if (s->info.env->exists("HTTP_X_AMZ_TAGGING")){
      auto tag_str = s->info.env->get("HTTP_X_AMZ_TAGGING");
      obj_tags = std::make_unique<RGWObjTags>();
      int ret = obj_tags->set_from_string(tag_str);
      if (ret < 0){
        ldpp_dout(dpp, 0) << "setting obj tags failed with rc=" << ret << dendl;
        if (ret == -ERR_INVALID_TAG){
          ret = -EINVAL; //s3 returns only -EINVAL for PUT requests
        }
        return ret;
      }
      bufferlist tags_bl;
      obj_tags->encode(tags_bl);
      attrs[RGW_ATTR_TAGS] = tags_bl;
    }
    encode(attrs, bl);
    // Insert an entry into bucket multipart index so it is not shown
    // when listing a bucket.
    string tenant_bkt_name = get_bucket_name(obj->get_bucket()->get_tenant(), obj->get_bucket()->get_name());
    string bucket_multipart_iname =
      "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
    rc = store->do_idx_op_by_name(bucket_multipart_iname,
                                  M0_IC_PUT, obj->get_key().to_str(), bl);

  } while (rc == -EEXIST);

  if (rc < 0) {
    ldpp_dout(dpp, 0) <<__func__<< ": index opration failed, M0_IC_PUT rc = "<< rc << dendl;
    return rc;
  }
  string tenant_bkt_name = get_bucket_name(bucket->get_tenant(), bucket->get_name());
  //This is multipart init, you will always have upload id here.
  string upload_id = get_upload_id();

  // Create object part index.
  string obj_part_iname = "motr.rgw.object." + tenant_bkt_name + "." + mp_obj.get_key() +
                          "." + upload_id + ".parts";
  ldpp_dout(dpp, 20) << __func__ << ": object part index=" << obj_part_iname << dendl;
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
  int rc = 0;
  if (num_parts <= 0 or marker < 0)
    return rc;

  vector<string> key_vec(num_parts);
  vector<bufferlist> val_vec(num_parts);

  string tenant_bkt_name = get_bucket_name(bucket->get_tenant(), bucket->get_name());
  string upload_id = get_upload_id();

  if(upload_id.length() == 0){
    std::unique_ptr<rgw::sal::Object> obj_ver = this->bucket->get_object(rgw_obj_key(this->get_key()));
    rgw::sal::MotrObject *mobj_ver = static_cast<rgw::sal::MotrObject *>(obj_ver.get());
    rgw_bucket_dir_entry ent;
    std::string key_name;

    // Get the object entry
    int ret_rc = mobj_ver->get_bucket_dir_ent(dpp, ent);
    if(ret_rc < 0)
      return ret_rc;

    if (!ent.is_delete_marker()) {
      // key_name = test_obj1[zWs1hl8neLT5wggBth5qjgOzUuXt20E]
      key_name = ent.key.name + "[" + ent.key.instance + "]";

      //fetch the version-id in case of null version-id
      if(ent.key.instance == "null") {
        ret_rc = mobj_ver->fetch_null_obj_reference(dpp, key_name);
        if(ret_rc < 0) {
          ldpp_dout(dpp, 0) << __func__ << " : failed to get null object reference, ret_rc : "<< ret_rc << dendl;
          return ret_rc;
        }
      }
      rc = store->get_upload_id(tenant_bkt_name, key_name, upload_id);
      if (rc < 0) {
        ldpp_dout(dpp, 0) << __func__ << ": ERROR: get_upload_id failed. rc = " << rc << dendl;
        return rc;
      }
    }
  }

  string obj_part_iname = "motr.rgw.object." + tenant_bkt_name + "." + mp_obj.get_key() + 
                          "." + upload_id + ".parts";
  ldpp_dout(dpp, 20) << __func__ << ": object part index = " << obj_part_iname << dendl;
  key_vec[0].clear();
  key_vec[0] = "part.";
  char buf[32];
  snprintf(buf, sizeof(buf), "%08d", marker + 1);
  key_vec[0].append(buf);
  rc = store->next_query_by_name(obj_part_iname, key_vec, val_vec);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": ERROR: NEXT query failed. rc = " << rc << dendl;
    return rc;
  }

  int last_num = 0;
  int part_cnt = 0;
  ldpp_dout(dpp, 20) << __func__ << ": marker = " << marker << dendl;
  parts.clear();

  for (const auto& bl: val_vec) {
    if (bl.length() == 0)
      break;

    RGWUploadPartInfo info;
    auto iter = bl.cbegin();
    info.decode(iter);
    rgw::sal::Attrs attrs_dummy;
    decode(attrs_dummy, iter);
    MotrObject::Meta meta;
    meta.decode(iter);

    ldpp_dout(dpp, 20) << __func__ << ": part_num=" << info.num
                                             << " part_size=" << info.size << dendl;
    ldpp_dout(dpp, 20) << __func__ << ": key=" << mp_obj.get_key() << ", meta:oid=[0x" << std::hex << meta.oid.u_hi
                                   << ":0x" << std::hex << meta.oid.u_lo << "], meta:pvid=[0x" << std::hex
                                   << meta.pver.f_container << ":0x" << std::hex << meta.pver.f_key
                                   << "], meta:layout_id=0x" << std::hex << meta.layout_id << dendl;

    if ((int)info.num > marker) {
      last_num = info.num;
      parts.emplace(info.num, std::make_unique<MotrMultipartPart>(info, meta));
    }

    part_cnt++;
  }

  // Does it have more parts?
  if (truncated)
    *truncated = part_cnt < num_parts? false : true;
  ldpp_dout(dpp, 20) << __func__ << ": truncated=" << *truncated << dendl;

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

  ldpp_dout(dpp, 20) << __func__ << ": enter" << dendl;
  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs &attrs = target_obj->get_attrs();

  do {
    ldpp_dout(dpp, 20) <<  __func__ << ": list_parts()" << dendl;
    rc = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
    if (rc == -ENOENT) {
      rc = -ERR_NO_SUCH_UPLOAD;
    }
    if (rc < 0)
      return rc;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << __func__ << ": ERROR: total parts mismatch: have: " << total_parts
                        << " expected: " << part_etags.size() << dendl;
      rc = -ERR_INVALID_PART;
      return rc;
    }
    ldpp_dout(dpp, 20) <<  __func__ << ": parts.size()=" << parts.size() << dendl;

    for (auto obj_iter = parts.begin();
        etags_iter != part_etags.end() && obj_iter != parts.end();
        ++etags_iter, ++obj_iter, ++handled_parts) {
      MultipartPart *mpart = obj_iter->second.get();
      MotrMultipartPart *mmpart = static_cast<MotrMultipartPart *>(mpart);
      RGWUploadPartInfo *part = &mmpart->info;

      uint64_t part_size = part->accounted_size;
      ldpp_dout(dpp, 20) << __func__ << ":  part_size=" << part_size << dendl;
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        rc = -ERR_TOO_SMALL;
        return rc;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << __func__ << ": ERROR: parts num mismatch: next requested: "
                          << etags_iter->first << " next uploaded: "
                          << obj_iter->first << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      }
      string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->etag) != 0) {
        ldpp_dout(dpp, 0) << __func__ << ": ERROR: etag mismatch: part: " << etags_iter->first
                          << " etag: " << etags_iter->second << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      }

      hex_to_buf(part->etag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char *)petag, sizeof(petag));
      ldpp_dout(dpp, 20) << __func__ << ": calc etag " << dendl;

      string oid = mp_obj.get_part(part->num);
      rgw_obj src_obj;
      src_obj.init_ns(bucket->get_key(), oid, mp_ns);

#if 0 // does Motr backend need it?
      /* update manifest for part */
      if (part->manifest.empty()) {
        ldpp_dout(dpp, 0) << __func__ << ": ERROR: empty manifest for object part: obj="
			 << src_obj << dendl;
        rc = -ERR_INVALID_PART;
        return rc;
      } else {
        manifest.append(dpp, part->manifest, store->get_zone());
      }
      ldpp_dout(dpp, 0) << __func__ << ": manifest " << dendl;
#endif

      bool part_compressed = (part->cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != part->cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << __func__ << ": ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << part->cs_info.compression_type << ")" << dendl;
          rc = -ERR_INVALID_PART;
          return rc;
      }

      ldpp_dout(dpp, 20) << __func__ << ": part compression" << dendl;
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
      ldpp_dout(dpp, 20) << __func__ << ": off=" << off << ", accounted_size = " << accounted_size << dendl;
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	   sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)part_etags.size());
  etag = final_etag_str;
  ldpp_dout(dpp, 20) << __func__ << ": calculated etag: " << etag << dendl;
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
  string tenant_bkt_name = get_bucket_name(meta_obj->get_bucket()->get_tenant(), meta_obj->get_bucket()->get_name());
  string bucket_multipart_iname =
      "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  rc = this->store->do_idx_op_by_name(bucket_multipart_iname,
                                      M0_IC_GET, meta_obj->get_key().to_str(), bl);
  ldpp_dout(dpp, 20) << __func__ << ": read entry from bucket multipart index rc = " << rc << dendl;
  if (rc < 0) {
    return rc == -ENOENT ? -ERR_NO_SUCH_UPLOAD : rc;
  }
  rgw::sal::Attrs temp_attrs;
  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto ent_iter = blr.cbegin();
  ent.decode(ent_iter);
  decode(temp_attrs, ent_iter);

  // Add tag to attrs[RGW_ATTR_TAGS] key only if temp_attrs has tagging info
  if (temp_attrs.find(RGW_ATTR_TAGS) != temp_attrs.end()) {
    attrs[RGW_ATTR_TAGS] = temp_attrs[RGW_ATTR_TAGS];
  }
  // Update the dir entry and insert it to the bucket index so
  // the object will be seen when listing the bucket.
  bufferlist update_bl, old_check_bl;
  target_obj->get_key().get_index_key(&ent.key);  // Change to offical name :)
  ent.meta.size = off;
  ent.meta.accounted_size = accounted_size;
  ldpp_dout(dpp, 20) << __func__ << ": obj size=" << ent.meta.size
                           << " obj accounted size=" << ent.meta.accounted_size << dendl;
  ent.meta.mtime = ceph::real_clock::now();
  ent.meta.etag = etag;

  RGWBucketInfo &info = target_obj->get_bucket()->get_info();

  if (!target_obj->get_key().have_instance()) {
    // generate-version-id for null version.
    target_obj->gen_rand_obj_instance_name();
    ent.key.instance = "null";
  }
  ent.encode(update_bl);
  encode(attrs, update_bl);
  MotrObject::Meta meta_dummy;
  meta_dummy.encode(update_bl);

  string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;
  ldpp_dout(dpp, 20) << __func__ << ": target_obj name=" << target_obj->get_name()
                                  << " target_obj oid=" << target_obj->get_oid() << dendl;

  std::unique_ptr<rgw::sal::Object> obj_ver = target_obj->get_bucket()->get_object(rgw_obj_key(target_obj->get_name()));
  rgw::sal::MotrObject *mobj_ver = static_cast<rgw::sal::MotrObject *>(obj_ver.get());
  
  // Check for bucket versioning
  // Update existing object version entries in a bucket,
  // in case of both versioning enabled and suspended.
  if(info.versioned())
  {
    string bucket_index_iname = "motr.rgw.bucket.index." + tenant_bkt_name;

    rc = mobj_ver->update_version_entries(dpp);
    ldpp_dout(dpp, 20) << __func__ << ": update_version_entries, rc = " << rc << dendl;
    if (rc < 0)
      return rc;
  }

  if (!info.versioning_enabled()) {
    int rc;
    rc = mobj_ver->overwrite_null_obj(dpp);
    if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to overwrite null object, rc : " << rc << dendl;
      return rc;
    }
    ent.key.instance = target_obj->get_instance();
    mobj_ver->set_instance(ent.key.instance);
    rc = mobj_ver->update_null_reference(dpp, ent);
    if (rc < 0) {
      ldpp_dout(dpp, 0) <<__func__<< ": Failed to update null reference, rc : " << rc << dendl;
      return rc;
    }
  }

  rc = store->do_idx_op_by_name(bucket_index_iname, M0_IC_PUT,
                                target_obj->get_key().to_str(), update_bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": index operation failed, M0_IC_PUT rc = " << rc << dendl;
    return rc;
  }
  
  // Increment size & count for new multipart obj in bucket stats entry.
  std::string bkt_owner = target_obj->get_bucket()->get_owner()->get_id().to_str();
  rc = update_bucket_stats(dpp, store, bkt_owner, tenant_bkt_name,
                           0, 0, total_parts - 1, false);
  if (rc != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed stats update for the "
      << "bucket/obj = " << tenant_bkt_name << "/" << target_obj->get_key().to_str()
      << ", rc = " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 70) << __func__ << ": Updated stats successfully for the "
      << "bucket/obj = " << tenant_bkt_name << "/" << target_obj->get_key().to_str()
      << ", rc = " << rc << dendl;

  // Put into metadata cache.
  store->get_obj_meta_cache()->put(dpp, target_obj->get_key().to_str(), update_bl);

  ldpp_dout(dpp, 20) << __func__ << ": remove from bucket multipart index " << dendl;
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
  string tenant_bkt_name = get_bucket_name(meta_obj->get_bucket()->get_tenant(), meta_obj->get_bucket()->get_name());
  string bucket_multipart_iname =
      "motr.rgw.bucket." + tenant_bkt_name + ".multiparts";
  int rc = this->store->do_idx_op_by_name(bucket_multipart_iname,
                                          M0_IC_GET, meta_obj->get_key().to_str(), bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": Failed to get multipart info. rc = " << rc << dendl;
    return rc == -ENOENT ? -ERR_NO_SUCH_UPLOAD : rc;
  }

  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto ent_iter = blr.cbegin();
  ent.decode(ent_iter);

  if (attrs) {
    bufferlist etag_bl;
    string& etag = ent.meta.etag;
    ldpp_dout(dpp, 20) << __func__ << ": object's etag:  " << ent.meta.etag << dendl;
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
  ldpp_dout(dpp, 20) << __func__ << ": bucket=" << head_obj->get_bucket()->get_name()
                     << "part_obj_name=" << part_obj_name << dendl;
  part_obj = std::make_unique<MotrObject>(this->store, rgw_obj_key(part_obj_name), head_obj->get_bucket());
  if (part_obj == nullptr)
    return -ENOMEM;

  // s3 client may retry uploading part, so the part may have already
  // been created.
  ldpp_dout(dpp, 20) << __func__ << ": creating object for size =" << expected_part_size << dendl;
  int rc = part_obj->create_mobj(dpp, expected_part_size);
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
    actual_part_size = part_obj->get_processed_bytes();
    ldpp_dout(dpp, 20) << __func__ << ": actual_part_size=" << actual_part_size << dendl;
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

  ldpp_dout(dpp, 20) << __func__ << ": enter" << dendl;
  // Add an entry into object_nnn_part_index.
  bufferlist bl;
  RGWUploadPartInfo info;
  info.num = part_num;
  info.etag = etag;
  info.size = actual_part_size;
  uint64_t size_rounded = 0;
  //For 0kb Object layout_id will not be available. 
  if(info.size != 0)
  {
    uint64_t lid = M0_OBJ_LAYOUT_ID(part_obj->meta.layout_id);
    uint64_t unit_sz = m0_obj_layout_id_to_unit_size(lid);
    size_rounded = roundup(info.size, unit_sz);
  }
  info.size_rounded = size_rounded;
  info.accounted_size = accounted_size;
  info.modified = real_clock::now();
  uint64_t old_part_size = 0, old_part_size_rounded = 0;
  bool old_part_exist = false;

  bool compressed;
  int rc = rgw_compression_info_from_attrset(attrs, compressed, info.cs_info);
  ldpp_dout(dpp, 20) << __func__ << ": compression rc = " << rc << dendl;
  if (rc < 0) {
    ldpp_dout(dpp, 1) << __func__ <<": cannot get compression info" << dendl;
    return rc;
  }
  encode(info, bl);
  encode(attrs, bl);
  part_obj->meta.encode(bl);

  string p = "part.";
  char buf[32];
  snprintf(buf, sizeof(buf), "%08d", (int)part_num);
  p.append(buf);
  string tenant_bkt_name = get_bucket_name(head_obj->get_bucket()->get_tenant(), head_obj->get_bucket()->get_name());
  //This is a MultipartComplete operation so this should always have valid upload id.
  string upload_id_str = upload_id;
  string obj_part_iname = "motr.rgw.object." + tenant_bkt_name + "." +
	                  head_obj->get_key().to_str() + "." + upload_id_str + ".parts";
  ldpp_dout(dpp, 20) << __func__ << ": object part index = " << obj_part_iname << dendl;

  // Before updating object part index with entry for new part, check if
  // old part exists. Perform M0_IC_GET operation on object part index.
  bufferlist old_part_check_bl;
  rc = store->do_idx_op_by_name(obj_part_iname, M0_IC_GET, p, old_part_check_bl);
  if (rc == 0 && old_part_check_bl.length() > 0) {
    // Old part exists. Try to delete it.
    RGWUploadPartInfo old_part_info;
    std::map<std::string, bufferlist> dummy_attr;
    string part_obj_name = head_obj->get_bucket()->get_name() + "." +
                          head_obj->get_key().to_str() +
                          ".part." + std::to_string(part_num);
    std::unique_ptr<MotrObject> old_part_obj =
        std::make_unique<MotrObject>(this->store, rgw_obj_key(part_obj_name),head_obj->get_bucket());
    if (old_part_obj == nullptr)
      return -ENOMEM;

    auto bl_iter = old_part_check_bl.cbegin();
    decode(old_part_info, bl_iter);
    decode(dummy_attr, bl_iter);
    old_part_obj->meta.decode(bl_iter);
    char oid_str[M0_FID_STR_LEN];
    snprintf(oid_str, ARRAY_SIZE(oid_str), U128X_F, U128_P(&old_part_obj->meta.oid));
    rgw::sal::MotrObject *old_mobj = static_cast<rgw::sal::MotrObject *>(old_part_obj.get());
    ldpp_dout(dpp, 20) << __func__ << ": Old part with oid [" << oid_str << "] exists" << dendl;
    old_part_size = old_part_info.accounted_size;
    old_part_size_rounded = old_part_info.size_rounded;
    old_part_exist = true;
    // Delete old object
    rc = old_mobj->delete_mobj(dpp);
    if (rc == 0) {
      ldpp_dout(dpp, 20) << __func__ << ": Old part [" << p <<  "] deleted succesfully" << dendl;
    } else {
      ldpp_dout(dpp, 0) << __func__ << ": Failed to delete old part [" << p <<  "]. Error = " << rc << dendl;
      return rc;
    }
  }

  rc = store->do_idx_op_by_name(obj_part_iname, M0_IC_PUT, p, bl);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": failed to add part obj in part index, rc = " << rc << dendl;
    return rc == -ENOENT ? -ERR_NO_SUCH_UPLOAD : rc;
  }

   rc = update_bucket_stats(dpp, store,
                           head_obj->get_bucket()->get_owner()->get_id().to_str(),
                           tenant_bkt_name,
                           actual_part_size - old_part_size,
                           size_rounded - old_part_size_rounded,
                           1 - old_part_exist);
  if (rc != 0) {
    ldpp_dout(dpp, 20) << __func__ << ": Failed stats update for the "
      << "obj/part = " << head_obj->get_key().to_str() << "/" << part_num
      << ", rc = " << rc << dendl;
    return rc;
  }
  ldpp_dout(dpp, 70) << __func__ << ": Updated stats successfully for the "
      << "obj/part = " << head_obj->get_key().to_str() << "/" << part_num
      << ", rc = " << rc << dendl;

  return 0;
}

int MotrStore::get_upload_id(string tenant_bkt_name, string key_name, string& upload_id){
  int rc = 0;
  bufferlist bl;

  string index_name = "motr.rgw.bucket.index." + tenant_bkt_name;

  rc = this->do_idx_op_by_name(index_name,
                              M0_IC_GET, key_name, bl);
  if (rc < 0) {
    //ldpp_dout(cctx, 0) << "ERROR: NEXT query failed. " << rc << dendl;
    return rc;
  }

  rgw_bucket_dir_entry ent;
  bufferlist& blr = bl;
  auto ent_iter = blr.cbegin();
  ent.decode(ent_iter);

  motr_multipart_upload_info upload_info;
  bufferlist mpbl;
  mpbl.append(ent.meta.user_data.c_str(), ent.meta.user_data.size());
  auto mpbl_iter = mpbl.cbegin();
  upload_info.decode(mpbl_iter);

  upload_id.clear();
  upload_id.append(upload_info.upload_id);

  return rc;
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
  ldout(cctx, 20) << __func__ << ": bucket's user:  " << u.to_str() << dendl;
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
    ldout(cctx, 0) << __func__ << ": access key not found: rc = " << rc << dendl;
    return rc;
  }

  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  access_key.decode(iter);

  uinfo.user_id.from_str(access_key.user_id);
  ldout(cctx, 0) << __func__ << ": loading user: " << uinfo.user_id.id << dendl;
  rc = MotrUser().load_user_from_idx(dpp, this, uinfo, nullptr, nullptr);
  if (rc < 0){
    ldout(cctx, 0) << __func__ << ": failed to load user: rc = " << rc << dendl;
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
  int rc;
  User *u;
  bufferlist bl;
  RGWUserInfo uinfo;
  MotrEmailInfo email_info; 
  rc = do_idx_op_by_name(RGW_IAM_MOTR_EMAIL_KEY,
                           M0_IC_GET, email, bl);
  if (rc < 0){
    ldout(cctx, 0) << __func__ << ": email Id not found: rc = " << rc << dendl;
    return rc;
  }
  auto iter = bl.cbegin();
  email_info.decode(iter);
  ldout(cctx, 0) << __func__ << ": loading user: " << email_info.user_id << dendl;
  uinfo.user_id.from_str(email_info.user_id);
  rc = MotrUser().load_user_from_idx(dpp, this, uinfo, nullptr, nullptr);
  if (rc < 0){
    ldout(cctx, 0) << __func__ << ": failed to load user: rc = " << rc << dendl;
    return rc;
  }
  u = new MotrUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  user->reset(u);  
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
    ldout(cctx, 0) << __func__ << ": failed to store key: rc = " << rc << dendl;
    return rc;
  }
  return rc;
}

int MotrStore::delete_access_key(const DoutPrefixProvider *dpp, optional_yield y, std::string access_key)
{
  int rc;
  bufferlist bl;
  rc = do_idx_op_by_name(RGW_IAM_MOTR_ACCESS_KEY,
                                M0_IC_DEL, access_key, bl);
  if (rc < 0){
    ldout(cctx, 0) << __func__ << ": failed to delete key: rc = " << rc << dendl;
  }
  return rc;
}

int MotrStore::store_email_info(const DoutPrefixProvider *dpp, optional_yield y, MotrEmailInfo& email_info )
{
  int rc;
  bufferlist bl;
  email_info.encode(bl);
  rc = do_idx_op_by_name(RGW_IAM_MOTR_EMAIL_KEY,
                                M0_IC_PUT, email_info.email_id, bl);
  if (rc < 0) {
    ldout(cctx, 0) << __func__ << ": failed to store the user by email as key: rc = " << rc << dendl;
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
  return -ENOENT;
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
int MotrStore::list_users(const DoutPrefixProvider* dpp, const std::string& metadata_key,
                        std::string& marker, int max_entries, void *&handle,
                        bool* truncated, std::list<std::string>& users)
{
  int rc;
  bufferlist bl;
  if (max_entries <= 0 or max_entries > 1000) {
    max_entries = 1000; 
  }
  vector<string> keys(max_entries + 1);
  vector<bufferlist> vals(max_entries + 1);
  
  if(!(marker.empty())){
    rc = do_idx_op_by_name(RGW_MOTR_USERS_IDX_NAME,
                                  M0_IC_GET, marker, bl);
    if (rc < 0) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid marker. " << rc << dendl;
      return rc;
    }
    else {
      keys[0] = marker;
    }
  }

  rc = next_query_by_name(RGW_MOTR_USERS_IDX_NAME, keys, vals);
  if (rc < 0) {
    ldpp_dout(dpp, 0) << __func__ <<": ERROR: NEXT query failed. rc = " << rc << dendl;
    return rc;
  }
  if (!(keys.back()).empty()) {
    *truncated = true;
    marker = keys.back(); 
  }
  for (int i = 0; i < int(keys.size()) - 1; i++) {
    if (keys[i].empty()) {
      break;
    }
    users.push_back(keys[i]);
  }
  return rc;
}

static void set_m0bufvec(struct m0_bufvec *bv, vector<uint8_t>& vec)
{
  *bv->ov_buf = reinterpret_cast<char*>(vec.data());
  *bv->ov_vec.v_count = vec.size();
}

// idx must be opened with open_motr_idx() beforehand
int MotrStore::do_idx_op(struct m0_idx *idx, enum m0_idx_opcode opcode,
                         vector<uint8_t>& key, vector<uint8_t>& val, bool update)
{
  int rc, rc_i;
  struct m0_bufvec k, v, *vp = &v;
  uint32_t flags = 0;
  struct m0_op *op = nullptr;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_START);
  rc = m0_bufvec_empty_alloc(&k, 1);
  if (rc != 0) {
    ldout(cctx, 0) << __func__ <<": ERROR: failed to allocate key bufvec. rc =" << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
	 RGW_ADDB_FUNC_DO_IDX_OP,
	 RGW_ADDB_PHASE_ERROR);
    return -ENOMEM;
  }

  if (opcode == M0_IC_PUT || opcode == M0_IC_GET) {
    rc = m0_bufvec_empty_alloc(&v, 1);
    if (rc != 0) {
      ldout(cctx, 0) << __func__ << ": ERROR: failed to allocate value bufvec, rc = " << rc << dendl;
      rc = -ENOMEM;
      ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
           RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_ERROR);
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
    ldout(cctx, 0) << __func__ << ": ERROR: failed to init index op: " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: op failed: " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  if (rc_i != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: idx op failed: " << rc_i << dendl;
    rc = rc_i;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  if (opcode == M0_IC_GET) {
    val.resize(*v.ov_vec.v_count);
    memcpy(reinterpret_cast<char*>(val.data()), *v.ov_buf, *v.ov_vec.v_count);
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DO_IDX_OP, RGW_ADDB_PHASE_DONE);
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

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DO_IDX_NEXT_OP, RGW_ADDB_PHASE_START);

  rc = m0_bufvec_empty_alloc(&k, nr_kvp)?:
       m0_bufvec_empty_alloc(&v, nr_kvp);
  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: failed to allocate kv bufvecs" << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_NEXT_OP, RGW_ADDB_PHASE_ERROR);
    return rc;
  }

  set_m0bufvec(&k, keys[0]);

  rc = m0_idx_op(idx, M0_IC_NEXT, &k, &v, rcs, 0, &op);
  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: failed to init index op: " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_NEXT_OP, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: op failed: " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DO_IDX_NEXT_OP, RGW_ADDB_PHASE_ERROR);
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

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DO_IDX_NEXT_OP, RGW_ADDB_PHASE_DONE);
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
    ldout(cctx, 0) << __func__ << ": ERROR: failed to open index: rc = "
                   << rc << dendl;
    goto out;
  }

  // Only the first element for keys needs to be set for NEXT query.
  // The keys will be set will the returned keys from motr index.
  ldout(cctx, 20) <<__func__<< ": index=" << idx_name << " keys[0]=" << key_out[0]
                  << " prefix=" << prefix << " delim=" << delim  << dendl;
  keys[0].assign(key_out[0].begin(), key_out[0].end());
  for (i = 0; i < (int)val_out.size(); i += k, k = 0) {
    rc = do_idx_next_op(&idx, keys, vals);
    ldout(cctx, 20) << __func__ << ": do_idx_next_op() = " << rc << dendl;
    if (rc < 0) {
      ldout(cctx, 0) << __func__ << ": ERROR: NEXT query failed. " << rc << dendl;
      goto out;
    } else if (rc == 0) {
      ldout(cctx, 20) << __func__ << ": No more entries in the table." << dendl;
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
    ldout(cctx, 0) << __func__ << ": do_idx_next_op(): next_key=" << next_key << dendl;
    keys[0].assign(next_key.begin(), next_key.end());

    int keys_left = val_out.size() - (i + k);  // i + k gives next index.
    // Resizing keys & vals vector when `keys_left < batch size`.
    if (keys_left < (int)nr_kvp) {
      keys.resize(keys_left);
      vals.resize(keys_left);
    }
  }

out:
  m0_idx_fini(&idx);
  return rc < 0 ? rc : i + k;
}

int MotrStore::delete_motr_idx_by_name(string iname)
{
  struct m0_idx idx = {};
  struct m0_uint128 idx_id;
  struct m0_op *op = nullptr;

  ldout(cctx, 20) << __func__ << ": iname=" << iname << dendl;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DELETE_IDX_BY_NAME, RGW_ADDB_PHASE_START);

  index_name_to_motr_fid(iname, &idx_id);
  m0_idx_init(&idx, &container.co_realm, &idx_id);
  m0_entity_open(&idx.in_entity, &op);
  int rc = m0_entity_delete(&idx.in_entity, &op);
  if (rc < 0) {
    ldout(cctx, 0) << __func__ <<": m0_entity_delete failed, rc = " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DELETE_IDX_BY_NAME, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);

  ldout(cctx, 70) << __func__ << ": waiting for op completion" << dendl;

  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc == -ENOENT) // race deletion??
    rc = 0;
  else if (rc < 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: index create failed. rc = " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_DELETE_IDX_BY_NAME, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ldout(cctx, 20) << __func__ << ": delete_motr_idx_by_name rc =" << rc << dendl;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_DELETE_IDX_BY_NAME, RGW_ADDB_PHASE_DONE);
out:
  ldout(cctx, 20) << "delete_motr_idx_by_name rc=" << rc << dendl;
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
  ldout(cctx, 20) << __func__ << ": id = 0x" << std::hex << id->u_hi << ":0x" << std::hex << id->u_lo  << dendl;

  struct m0_fid *fid = (struct m0_fid*)id;
  m0_fid_tset(fid, m0_dix_fid_type.ft_id,
              fid->f_container & M0_DIX_FID_DIX_CONTAINER_MASK, fid->f_key);
  ldout(cctx, 20) << __func__ << ": converted id = 0x" << std::hex << id->u_hi << ":0x" << std::hex << id->u_lo  << dendl;
}

int MotrStore::do_idx_op_by_name(string idx_name, enum m0_idx_opcode opcode,
                                 string key_str, bufferlist &bl, bool update)
{
  struct m0_idx idx = {};
  vector<uint8_t> key(key_str.begin(), key_str.end());
  vector<uint8_t> val;
  struct m0_uint128 idx_id;

  index_name_to_motr_fid(idx_name, &idx_id);
  int rc = open_motr_idx(&idx_id, &idx);
  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: failed to open index rc = " << rc << dendl;
    goto out;
  }

  if (opcode == M0_IC_PUT)
    val.assign(bl.c_str(), bl.c_str() + bl.length());

  ldout(cctx, 20) <<__func__<< ": op=" << (opcode == M0_IC_PUT ? "PUT" : "GET")
                 << " idx=" << idx_name << " key=" << key_str << dendl;
  rc = do_idx_op(&idx, opcode, key, val, update);
  if (rc == 0 && opcode == M0_IC_GET)
    // Append the returned value (blob) to the bufferlist.
    bl.append(reinterpret_cast<char*>(val.data()), val.size());
  if (rc < 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: index operation "<< opcode << " failed, rc = " << rc << dendl;
  }
out:
  m0_idx_fini(&idx);
  return rc;
}

int MotrStore::create_motr_idx_by_name(string iname)
{
  struct m0_idx idx = {};
  struct m0_uint128 id;

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_CREATE_IDX_BY_NAME, RGW_ADDB_PHASE_START);

  index_name_to_motr_fid(iname, &id);
  m0_idx_init(&idx, &container.co_realm, &id);

  // create index or make sure it's created
  struct m0_op *op = nullptr;
  int rc = m0_entity_create(nullptr, &idx.in_entity, &op);
  if (rc != 0) {
    ldout(cctx, 0) << __func__ << ": ERROR: m0_entity_create() failed, rc= " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_IDX_BY_NAME, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ADDB(RGW_ADDB_REQUEST_TO_MOTR_ID, addb_logger.get_id(), m0_sm_id_get(&op->op_sm));
  m0_op_launch(&op, 1);
  rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
       m0_rc(op);
  m0_op_fini(op);
  m0_op_free(op);

  if (rc != 0 && rc != -EEXIST) {
    ldout(cctx, 0) << __func__ << ": ERROR: index create failed, rc = " << rc << dendl;
    ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
         RGW_ADDB_FUNC_CREATE_IDX_BY_NAME, RGW_ADDB_PHASE_ERROR);
    goto out;
  }

  ADDB(RGW_ADDB_REQUEST_ID, addb_logger.get_id(), 
       RGW_ADDB_FUNC_CREATE_IDX_BY_NAME, RGW_ADDB_PHASE_DONE);
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
                                   CephContext *cct, bool use_cache)
{
  this->obj_meta_cache = new MotrMetaCache(dpp, cct);
  this->get_obj_meta_cache()->set_enabled(use_cache);

  this->user_cache = new MotrMetaCache(dpp, cct);
  this->get_user_cache()->set_enabled(use_cache);

  this->bucket_inst_cache = new MotrMetaCache(dpp, cct);
  this->get_bucket_inst_cache()->set_enabled(use_cache);

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
    const auto& admin_proc_ep  = g_conf().get_val<std::string>("motr_admin_endpoint");
    const auto& admin_proc_fid = g_conf().get_val<std::string>("motr_admin_fid");
    const bool addb_enabled = g_conf().get_val<bool>("motr_addb_enabled");
    const int init_flags = cct->get_init_flags();
    ldout(cct, 0) << "INFO: motr my endpoint: " << proc_ep << dendl;
    ldout(cct, 0) << "INFO: motr ha endpoint: " << ha_ep << dendl;
    ldout(cct, 0) << "INFO: motr my fid:      " << proc_fid << dendl;
    ldout(cct, 0) << "INFO: motr profile fid: " << profile << dendl;
    ldout(cct, 0) << "INFO: motr addb enabled: " << addb_enabled << dendl;
    store->conf.mc_local_addr  = proc_ep.c_str();
    store->conf.mc_process_fid = proc_fid.c_str();

    ldout(cct, 0) << "INFO: init flags:       " << init_flags << dendl;
    ldout(cct, 0) << "INFO: motr admin endpoint: " << admin_proc_ep << dendl;
    ldout(cct, 0) << "INFO: motr admin fid:   " << admin_proc_fid << dendl;

    // HACK this is so that radosge-admin uses a different client
    if (init_flags == 0) {
      store->conf.mc_process_fid = admin_proc_fid.c_str();
      store->conf.mc_local_addr  = admin_proc_ep.c_str();
    } else {
      store->conf.mc_process_fid = proc_fid.c_str();
      store->conf.mc_local_addr  = proc_ep.c_str();
    }
    store->conf.mc_ha_addr      = ha_ep.c_str();
    store->conf.mc_profile      = profile.c_str();
    store->conf.mc_is_addb_init = addb_enabled;

    ldout(cct, 50) << "INFO: motr profile fid:  " << store->conf.mc_profile << dendl;
    ldout(cct, 50) << "INFO: ha addr:  " << store->conf.mc_ha_addr << dendl;
    ldout(cct, 50) << "INFO: process fid:  " << store->conf.mc_process_fid << dendl;
    ldout(cct, 50) << "INFO: motr endpoint:  " << store->conf.mc_local_addr << dendl;
    ldout(cct, 50) << "INFO: motr addb enabled:  " << store->conf.mc_is_addb_init << dendl;

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
      ldout(cct, 0) << __func__ << ": ERROR: m0_client_init() failed: " << rc << dendl;
      goto out;
    }
    rgw::sal::MotrADDBLogger::set_m0_instance(store->instance->m0c_motr);

    m0_container_init(&store->container, nullptr, &M0_UBER_REALM, store->instance);
    rc = store->container.co_realm.re_entity.en_sm.sm_rc;
    if (rc != 0) {
      ldout(cct, 0) << __func__ << ": ERROR: m0_container_init() failed: " << rc << dendl;
      goto out;
    }

    rc = m0_ufid_init(store->instance, &ufid_gr);
    if (rc != 0) {
      ldout(cct, 0) << __func__ << ": ERROR: m0_ufid_init() failed: " << rc << dendl;
      goto out;
    }

    // Create global indices if not yet.
    rc = store->check_n_create_global_indices();
    if (rc != 0) {
      ldout(cct, 0) << __func__ << ": ERROR: check_n_create_global_indices() failed: " << rc << dendl;
      goto out;
    }

  }

out:
  if (rc != 0) {
    rgw::sal::MotrADDBLogger::set_m0_instance(nullptr);
    delete store;
    return nullptr;
  }
  return store;
}

}
