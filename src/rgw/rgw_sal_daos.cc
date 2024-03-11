// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * SAL implementation for the CORTX DAOS backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_daos.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <filesystem>
#include <system_error>

#include "common/Clock.h"
#include "common/errno.h"
#include "rgw_bucket.h"
#include "rgw_compression.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;

namespace fs = std::filesystem;

namespace rgw::sal {

using ::ceph::decode;
using ::ceph::encode;

int DaosUser::list_buckets(const DoutPrefixProvider* dpp, const string& marker,
                           const string& end_marker, uint64_t max,
                           bool need_stats, BucketList& buckets,
                           optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: list_user_buckets: marker=" << marker
                     << " end_marker=" << end_marker << " max=" << max << dendl;
  int ret = 0;
  bool is_truncated = false;
  buckets.clear();
  vector<struct ds3_bucket_info> bucket_infos(max);
  daos_size_t bcount = bucket_infos.size();
  vector<vector<uint8_t>> values(bcount, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (daos_size_t i = 0; i < bcount; i++) {
    bucket_infos[i].encoded = values[i].data();
    bucket_infos[i].encoded_length = values[i].size();
  }

  char daos_marker[DS3_MAX_BUCKET_NAME];
  std::strncpy(daos_marker, marker.c_str(), sizeof(daos_marker));
  ret = ds3_bucket_list(&bcount, bucket_infos.data(), daos_marker,
                        &is_truncated, store->ds3, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_list: bcount=" << bcount
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_list failed!" << ret << dendl;
    return ret;
  }

  bucket_infos.resize(bcount);
  values.resize(bcount);

  for (const auto& bi : bucket_infos) {
    DaosBucketInfo dbinfo;
    bufferlist bl;
    bl.append(reinterpret_cast<char*>(bi.encoded), bi.encoded_length);
    auto iter = bl.cbegin();
    dbinfo.decode(iter);
    buckets.add(std::make_unique<DaosBucket>(this->store, dbinfo.info, this));
  }

  buckets.set_truncated(is_truncated);
  return 0;
}

int DaosUser::create_bucket(
    const DoutPrefixProvider* dpp, const rgw_bucket& b,
    const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
    std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
    const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
    obj_version& ep_objv, bool exclusive, bool obj_lock_enabled, bool* existed,
    req_info& req_info, std::unique_ptr<Bucket>* bucket_out, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: create_bucket:" << b.name << dendl;
  int ret;
  std::unique_ptr<Bucket> bucket;

  // Look up the bucket. Create it if it doesn't exist.
  ret = this->store->get_bucket(dpp, this, b, &bucket, y);
  if (ret != 0 && ret != -ENOENT) {
    return ret;
  }

  if (ret != -ENOENT) {
    *existed = true;
    if (swift_ver_location.empty()) {
      swift_ver_location = bucket->get_info().swift_ver_location;
    }
    placement_rule.inherit_from(bucket->get_info().placement_rule);

    // TODO: ACL policy
    // // don't allow changes to the acl policy
    // RGWAccessControlPolicy old_policy(ctx());
    // int rc = rgw_op_get_bucket_policy_from_attr(
    //           dpp, this, u, bucket->get_attrs(), &old_policy, y);
    // if (rc >= 0 && old_policy != policy) {
    //    bucket_out->swap(bucket);
    //    return -EEXIST;
    //}
  } else {
    placement_rule.name = "default";
    placement_rule.storage_class = "STANDARD";
    bucket = std::make_unique<DaosBucket>(store, b, this);
    bucket->set_attrs(attrs);

    *existed = false;
  }

  // TODO: how to handle zone and multi-site.

  if (!*existed) {
    info.placement_rule = placement_rule;
    info.bucket = b;
    info.owner = this->get_info().user_id;
    info.zonegroup = zonegroup_id;
    info.creation_time = ceph::real_clock::now();
    if (obj_lock_enabled)
      info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
    bucket->set_version(ep_objv);
    bucket->get_info() = info;

    // Create a new bucket:
    DaosBucket* daos_bucket = static_cast<DaosBucket*>(bucket.get());
    bufferlist bl;
    std::unique_ptr<struct ds3_bucket_info> bucket_info =
        daos_bucket->get_encoded_info(bl, ceph::real_time());
    ret = ds3_bucket_create(bucket->get_name().c_str(), bucket_info.get(),
                            nullptr, store->ds3, nullptr);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_create failed! ret=" << ret
                        << dendl;
      return ret;
    }
  } else {
    bucket->set_version(ep_objv);
    bucket->get_info() = info;
  }

  bucket_out->swap(bucket);

  return ret;
}

int DaosUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::read_stats(const DoutPrefixProvider* dpp, optional_yield y,
                         RGWStorageStats* stats,
                         ceph::real_time* last_stats_sync,
                         ceph::real_time* last_stats_update) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* stats - Not for first pass */
int DaosUser::read_stats_async(const DoutPrefixProvider* dpp,
                               RGWGetUserStats_CB* cb) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::complete_flush_stats(const DoutPrefixProvider* dpp,
                                   optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch, uint32_t max_entries,
                         bool* is_truncated, RGWUsageIter& usage_iter,
                         map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::load_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();
  ldpp_dout(dpp, 20) << "DEBUG: load_user, name=" << name << dendl;

  DaosUserInfo duinfo;
  int ret = read_user(dpp, name, &duinfo);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: load_user failed, name=" << name << dendl;
    return ret;
  }

  info = duinfo.info;
  attrs = duinfo.attrs;
  objv_tracker.read_version = duinfo.user_version;
  return 0;
}

int DaosUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                    Attrs& new_attrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: merge_and_store_attrs, new_attrs=" << new_attrs
                     << dendl;
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;
  }
  return store_user(dpp, y, false);
}

int DaosUser::store_user(const DoutPrefixProvider* dpp, optional_yield y,
                         bool exclusive, RGWUserInfo* old_info) {
  const string name = info.user_id.to_str();
  ldpp_dout(dpp, 10) << "DEBUG: Store_user(): User name=" << name << dendl;

  // Read user
  int ret = 0;
  struct DaosUserInfo duinfo;
  ret = read_user(dpp, name, &duinfo);
  obj_version obj_ver = duinfo.user_version;
  std::unique_ptr<struct ds3_user_info> old_user_info;
  std::vector<const char*> old_access_ids;

  // Check if the user already exists
  if (ret == 0 && obj_ver.ver) {
    // already exists.

    if (old_info) {
      *old_info = duinfo.info;
    }

    if (objv_tracker.read_version.ver != obj_ver.ver) {
      // Object version mismatch.. return ECANCELED
      ret = -ECANCELED;
      ldpp_dout(dpp, 0) << "User Read version mismatch read_version="
                        << objv_tracker.read_version.ver
                        << " obj_ver=" << obj_ver.ver << dendl;
      return ret;
    }

    if (exclusive) {
      // return
      return ret;
    }
    obj_ver.ver++;

    for (auto const& [id, key] : duinfo.info.access_keys) {
      old_access_ids.push_back(id.c_str());
    }
    old_user_info.reset(
        new ds3_user_info{.name = duinfo.info.user_id.to_str().c_str(),
                          .email = duinfo.info.user_email.c_str(),
                          .access_ids = old_access_ids.data(),
                          .access_ids_nr = old_access_ids.size()});
  } else {
    obj_ver.ver = 1;
    obj_ver.tag = "UserTAG";
  }

  bufferlist bl;
  std::unique_ptr<struct ds3_user_info> user_info =
      get_encoded_info(bl, obj_ver);

  ret = ds3_user_set(name.c_str(), user_info.get(), old_user_info.get(),
                     store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_set failed, name=" << name
                      << " ret=" << ret << dendl;
  }

  return ret;
}

int DaosUser::read_user(const DoutPrefixProvider* dpp, std::string name,
                        DaosUserInfo* duinfo) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get(name.c_str(), &user_info, store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get failed, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo->decode(iter);
  return ret;
}

std::unique_ptr<struct ds3_user_info> DaosUser::get_encoded_info(
    bufferlist& bl, obj_version& obj_ver) {
  // Encode user data
  struct DaosUserInfo duinfo;
  duinfo.info = info;
  duinfo.attrs = attrs;
  duinfo.user_version = obj_ver;
  duinfo.encode(bl);

  // Initialize ds3_user_info
  access_ids.clear();
  for (auto const& [id, key] : info.access_keys) {
    access_ids.push_back(id.c_str());
  }
  return std::unique_ptr<struct ds3_user_info>(
      new ds3_user_info{.name = info.user_id.to_str().c_str(),
                        .email = info.user_email.c_str(),
                        .access_ids = access_ids.data(),
                        .access_ids_nr = access_ids.size(),
                        .encoded = bl.c_str(),
                        .encoded_length = bl.length()});
}

int DaosUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();

  // TODO: the expectation is that the object version needs to be passed in as a
  // method arg see int DB::remove_user(const DoutPrefixProvider *dpp,
  // RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv)
  obj_version obj_ver;
  bufferlist bl;
  std::unique_ptr<struct ds3_user_info> user_info =
      get_encoded_info(bl, obj_ver);

  // Remove user
  int ret = ds3_user_remove(name.c_str(), user_info.get(), store->ds3, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_set failed, name=" << name
                      << " ret=" << ret << dendl;
  }
  return ret;
}

DaosBucket::~DaosBucket() { close(nullptr); }

int DaosBucket::open(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: open, name=" << info.bucket.name.c_str()
                     << dendl;
  // Idempotent
  if (is_open()) {
    return 0;
  }

  int ret = ds3_bucket_open(get_name().c_str(), &ds3b, store->ds3, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_open, name=" << get_name()
                     << ", ret=" << ret << dendl;

  return ret;
}

int DaosBucket::close(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: close" << dendl;
  // Idempotent
  if (!is_open()) {
    return 0;
  }

  int ret = ds3_bucket_close(ds3b, nullptr);
  ds3b = nullptr;
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_close ret=" << ret << dendl;

  return ret;
}

std::unique_ptr<struct ds3_bucket_info> DaosBucket::get_encoded_info(
    bufferlist& bl, ceph::real_time _mtime) {
  DaosBucketInfo dbinfo;
  dbinfo.info = info;
  dbinfo.bucket_attrs = attrs;
  dbinfo.mtime = _mtime;
  dbinfo.bucket_version = bucket_version;
  dbinfo.encode(bl);

  auto bucket_info = std::make_unique<struct ds3_bucket_info>();
  bucket_info->encoded = bl.c_str();
  bucket_info->encoded_length = bl.length();
  std::strncpy(bucket_info->name, get_name().c_str(), sizeof(bucket_info->name));
  return bucket_info;
}

int DaosBucket::remove_bucket(const DoutPrefixProvider* dpp,
                              bool delete_children, bool forward_to_master,
                              req_info* req_info, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: remove_bucket, delete_children="
                    
                     << delete_children
                    
                     << " forward_to_master=" << forward_to_master << dendl;

  return ds3_bucket_destroy(get_name().c_str(), delete_children, store->ds3,
                            nullptr);
}

int DaosBucket::remove_bucket_bypass_gc(int concurrent_max,
                                        bool keep_index_consistent,
                                        optional_yield y,
                                        const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: remove_bucket_bypass_gc, concurrent_max="
                    
                     << concurrent_max
                    
                     << " keep_index_consistent=" << keep_index_consistent
                    
                     << dendl;
  return ds3_bucket_destroy(get_name().c_str(), true, store->ds3, nullptr);
}

int DaosBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive,
                         ceph::real_time _mtime) {
  ldpp_dout(dpp, 20) << "DEBUG: put_info(): bucket name=" << get_name()
                     << dendl;

  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  bufferlist bl;
  std::unique_ptr<struct ds3_bucket_info> bucket_info =
      get_encoded_info(bl, ceph::real_time());

  ret = ds3_bucket_set_info(bucket_info.get(), ds3b, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_set_info failed: " << ret << dendl;
  }
  return ret;
}

int DaosBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                            bool get_stats) {
  ldpp_dout(dpp, 20) << "DEBUG: load_bucket(): bucket name=" << get_name()
                     << dendl;
  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  bufferlist bl;
  DaosBucketInfo dbinfo;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_bucket_info bucket_info = {.encoded = bl.append_hole(size).c_str(),
                                        .encoded_length = size};

  ret = ds3_bucket_get_info(&bucket_info, ds3b, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_get_info failed: " << ret << dendl;
    return ret;
  }

  auto iter = bl.cbegin();
  dbinfo.decode(iter);
  info = dbinfo.info;
  rgw_placement_rule placement_rule;
  placement_rule.name = "default";
  placement_rule.storage_class = "STANDARD";
  info.placement_rule = placement_rule;

  attrs = dbinfo.bucket_attrs;
  mtime = dbinfo.mtime;
  bucket_version = dbinfo.bucket_version;
  return ret;
}

/* stats - Not for first pass */
int DaosBucket::read_stats(const DoutPrefixProvider* dpp,
                           const bucket_index_layout_generation& idx_layout,
                           int shard_id, std::string* bucket_ver,
                           std::string* master_ver,
                           std::map<RGWObjCategory, RGWStorageStats>& stats,
                           std::string* max_marker, bool* syncstopped) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::read_stats_async(
    const DoutPrefixProvider* dpp,
    const bucket_index_layout_generation& idx_layout, int shard_id,
    RGWGetBucketStats_CB* ctx) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::sync_user_stats(const DoutPrefixProvider* dpp,
                                optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::update_container_stats(const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_bucket_shards(const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::chown(const DoutPrefixProvider* dpp, User& new_user,
                      optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* Make sure to call load_bucket() if you need it first */
bool DaosBucket::is_owner(User* user) {
  return (info.owner.compare(user->get_id()) == 0);
}

int DaosBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y) {
  /* XXX: Check if bucket contains any objects */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_quota(const DoutPrefixProvider* dpp, RGWQuota& quota,
                            uint64_t obj_size, optional_yield y,
                            bool check_size_only) {
  /* Not Handled in the first pass as stats are also needed */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                      Attrs& new_attrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: merge_and_store_attrs, new_attrs=" << new_attrs
                     << dendl;
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;
  }

  return put_info(dpp, y, ceph::real_time());
}

int DaosBucket::try_refresh_info(const DoutPrefixProvider* dpp,
                                 ceph::real_time* pmtime) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* XXX: usage and stats not supported in the first pass */
int DaosBucket::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch, uint32_t max_entries,
                           bool* is_truncated, RGWUsageIter& usage_iter,
                           map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::remove_objs_from_index(
    const DoutPrefixProvider* dpp,
    std::list<rgw_obj_index_key>& objs_to_unlink) {
  /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
   * Delete all the object in the list from the object table of this
   * bucket
   */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_index(
    const DoutPrefixProvider* dpp,
    std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
    std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) {
  /* XXX: stats not supported yet */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::rebuild_index(const DoutPrefixProvider* dpp) {
  /* there is no index table in DAOS. Not applicable */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::set_tag_timeout(const DoutPrefixProvider* dpp,
                                uint64_t timeout) {
  /* XXX: CHECK: set tag timeout for all the bucket objects? */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::purge_instance(const DoutPrefixProvider* dpp) {
  /* XXX: CHECK: for DAOS only single instance supported.
   * Remove all the objects for that instance? Anything extra needed?
   */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::set_acl(const DoutPrefixProvider* dpp,
                        RGWAccessControlPolicy& acl, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: set_acl" << dendl;
  int ret = 0;
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  Attrs attrs = get_attrs();
  attrs[RGW_ATTR_ACL] = aclbl;

  return ret;
}

std::unique_ptr<Object> DaosBucket::get_object(const rgw_obj_key& k) {
  return std::make_unique<DaosObject>(this->store, k, this);
}

bool compare_rgw_bucket_dir_entry(rgw_bucket_dir_entry& entry1,
                                  rgw_bucket_dir_entry& entry2) {
  return (entry1.key < entry2.key);
}

bool compare_multipart_upload(std::unique_ptr<MultipartUpload>& upload1,
                              std::unique_ptr<MultipartUpload>& upload2) {
  return (upload1->get_key() < upload2->get_key());
}

int DaosBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
                     ListResults& results, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: list bucket=" << get_name() << " max=" << max
                     << " params=" << params << dendl;
  // End
  if (max == 0) {
    return 0;
  }

  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  // Init needed structures
  vector<struct ds3_object_info> object_infos(max);
  uint32_t nobj = object_infos.size();
  vector<vector<uint8_t>> values(nobj, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < nobj; i++) {
    object_infos[i].encoded = values[i].data();
    object_infos[i].encoded_length = values[i].size();
  }

  vector<struct ds3_common_prefix_info> common_prefixes(max);
  uint32_t ncp = common_prefixes.size();

  char daos_marker[DS3_MAX_KEY_BUFF];
  std::strncpy(daos_marker, params.marker.get_oid().c_str(), sizeof(daos_marker));

  ret = ds3_bucket_list_obj(&nobj, object_infos.data(), &ncp,
                            common_prefixes.data(), params.prefix.c_str(),
                            params.delim.c_str(), daos_marker,
                            params.list_versions, &results.is_truncated, ds3b);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_list_obj failed, name="
                      << get_name() << ", ret=" << ret << dendl;
    return ret;
  }

  object_infos.resize(nobj);
  values.resize(nobj);
  common_prefixes.resize(ncp);

  // Fill common prefixes
  for (auto const& cp : common_prefixes) {
    results.common_prefixes[cp.prefix] = true;
  }

  // Decode objs
  for (auto const& obj : object_infos) {
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    bl.append(reinterpret_cast<char*>(obj.encoded), obj.encoded_length);
    auto iter = bl.cbegin();
    ent.decode(iter);
    if (params.list_versions || ent.is_visible()) {
      results.objs.emplace_back(std::move(ent));
    }
  }

  if (!params.allow_unordered) {
    std::sort(results.objs.begin(), results.objs.end(),
              compare_rgw_bucket_dir_entry);
  }

  return ret;
}

int DaosBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const string& prefix, string& marker,
    const string& delim, const int& max_uploads,
    vector<std::unique_ptr<MultipartUpload>>& uploads,
    map<string, bool>* common_prefixes, bool* is_truncated) {
  ldpp_dout(dpp, 20) << "DEBUG: list_multiparts" << dendl;
  // End of uploading
  if (max_uploads == 0) {
    *is_truncated = false;
    return 0;
  }

  // Init needed structures
  vector<struct ds3_multipart_upload_info> multipart_upload_infos(max_uploads);
  uint32_t nmp = multipart_upload_infos.size();
  vector<vector<uint8_t>> values(nmp, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < nmp; i++) {
    multipart_upload_infos[i].encoded = values[i].data();
    multipart_upload_infos[i].encoded_length = values[i].size();
  }

  vector<struct ds3_common_prefix_info> cps(max_uploads);
  uint32_t ncp = cps.size();

  char daos_marker[DS3_MAX_KEY_BUFF];
  std::strncpy(daos_marker, marker.c_str(), sizeof(daos_marker));

  int ret = ds3_bucket_list_multipart(
      get_name().c_str(), &nmp, multipart_upload_infos.data(), &ncp, cps.data(),
      prefix.c_str(), delim.c_str(), daos_marker, is_truncated, store->ds3);

  multipart_upload_infos.resize(nmp);
  values.resize(nmp);
  cps.resize(ncp);

  // Fill common prefixes
  for (auto const& cp : cps) {
    (*common_prefixes)[cp.prefix] = true;
  }

  for (auto const& mp : multipart_upload_infos) {
    // Decode the xattr
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    bl.append(reinterpret_cast<char*>(mp.encoded), mp.encoded_length);
    auto iter = bl.cbegin();
    ent.decode(iter);
    string name = ent.key.name;

    ACLOwner owner(rgw_user(ent.meta.owner));
    owner.set_name(ent.meta.owner_display_name);
    uploads.push_back(this->get_multipart_upload(
        name, mp.upload_id, std::move(owner), ent.meta.mtime));
  }

  // Sort uploads
  std::sort(uploads.begin(), uploads.end(), compare_multipart_upload);

  return ret;
}

int DaosBucket::abort_multiparts(const DoutPrefixProvider* dpp,
                                 CephContext* cct) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::finalize(void) {
  ldout(cctx, 20) << "DEBUG: finalize" << dendl;
  int ret;

  ret = ds3_disconnect(ds3, nullptr);
  if (ret != 0) {
    ldout(cctx, 0) << "ERROR: ds3_disconnect() failed: " << ret << dendl;
  }
  ds3 = nullptr;

  ret = ds3_fini();
  if (ret != 0) {
    ldout(cctx, 0) << "ERROR: daos_fini() failed: " << ret << dendl;
  }
}

int DaosStore::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: initialize" << dendl;
  int ret = ds3_init();

  // DS3 init failed, allow the case where init is already done
  if (ret != 0 && ret != DER_ALREADY) {
    ldout(cct, 0) << "ERROR: ds3_init() failed: " << ret << dendl;
    return ret;
  }

  // XXX: these params should be taken from config settings and
  // cct somehow?
  const auto& daos_pool = cct->_conf.get_val<std::string>("daos_pool");
  ldout(cct, 20) << "INFO: daos pool: " << daos_pool << dendl;

  ret = ds3_connect(daos_pool.c_str(), nullptr, &ds3, nullptr);

  if (ret != 0) {
    ldout(cct, 0) << "ERROR: ds3_connect() failed: " << ret << dendl;
    ds3_fini();
  }

  return ret;
}

const std::string& DaosZoneGroup::get_endpoint() const {
  if (!group.endpoints.empty()) {
    return group.endpoints.front();
  } else {
    // use zonegroup's master zone endpoints
    auto z = group.zones.find(group.master_zone);
    if (z != group.zones.end() && !z->second.endpoints.empty()) {
      return z->second.endpoints.front();
    }
  }
  return empty;
}

bool DaosZoneGroup::placement_target_exists(std::string& target) const {
  return !!group.placement_targets.count(target);
}

void DaosZoneGroup::get_placement_target_names(
    std::set<std::string>& names) const {
  for (const auto& target : group.placement_targets) {
    names.emplace(target.second.name);
  }
}

int DaosZoneGroup::get_placement_tier(const rgw_placement_rule& rule,
                                      std::unique_ptr<PlacementTier>* tier) {
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;
  titer = group.placement_targets.find(rule.name);
  if (titer == group.placement_targets.end()) {
    return -ENOENT;
  }

  const auto& target_rule = titer->second;
  std::map<std::string, RGWZoneGroupPlacementTier>::const_iterator ttier;
  ttier = target_rule.tier_targets.find(rule.storage_class);
  if (ttier == target_rule.tier_targets.end()) {
    // not found
    return -ENOENT;
  }

  PlacementTier* t;
  t = new DaosPlacementTier(store, ttier->second);
  if (!t) return -ENOMEM;

  tier->reset(t);
  return 0;
}

ZoneGroup& DaosZone::get_zonegroup() { return zonegroup; }

int DaosZone::get_zonegroup(const std::string& id,
                            std::unique_ptr<ZoneGroup>* group) {
  /* XXX: for now only one zonegroup supported */
  ZoneGroup* zg;
  zg = new DaosZoneGroup(store, zonegroup.get_group());

  group->reset(zg);
  return 0;
}

const rgw_zone_id& DaosZone::get_id() { return cur_zone_id; }

const std::string& DaosZone::get_name() const {
  return zone_params->get_name();
}

bool DaosZone::is_writeable() { return true; }

bool DaosZone::get_redirect_endpoint(std::string* endpoint) { return false; }

bool DaosZone::has_zonegroup_api(const std::string& api) const { return false; }

const std::string& DaosZone::get_current_period_id() {
  return current_period->get_id();
}

std::unique_ptr<LuaManager> DaosStore::get_lua_manager() {
  return std::make_unique<DaosLuaManager>(this);
}

int DaosObject::get_obj_state(const DoutPrefixProvider* dpp,
                              RGWObjState** _state, optional_yield y,
                              bool follow_olh) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  ldpp_dout(dpp, 20) << "DEBUG: get_obj_state" << dendl;
  rgw_bucket_dir_entry ent;
  *_state = &state;  // state is required even if a failure occurs

  int ret = get_dir_entry_attrs(dpp, &ent);
  if (ret != 0) {
    return ret;
  }

  // Set object state.
  state.exists = true;
  state.size = ent.meta.size;
  state.accounted_size = ent.meta.size;
  state.mtime = ent.meta.mtime;

  state.has_attrs = true;
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) << __func__ << ": object's etag:  " << ent.meta.etag
                     << dendl;
  etag_bl.append(etag);
  state.attrset[RGW_ATTR_ETAG] = etag_bl;
  return 0;
}

DaosObject::~DaosObject() { close(nullptr); }

int DaosObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                              Attrs* delattrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: DaosObject::set_obj_attrs()" << dendl;
  // TODO handle target_obj
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent);
  if (ret != 0) {
    return ret;
  }

  // Update object metadata
  Attrs updateattrs = setattrs == nullptr ? attrs : *setattrs;
  if (delattrs) {
    for (auto const& [attr, attrval] : *delattrs) {
      updateattrs.erase(attr);
    }
  }

  ret = set_dir_entry_attrs(dpp, &ent, &updateattrs);
  return ret;
}

int DaosObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                              rgw_obj* target_obj) {
  ldpp_dout(dpp, 20) << "DEBUG: DaosObject::get_obj_attrs()" << dendl;
  // TODO handle target_obj
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent, &attrs);
  return ret;
}

int DaosObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  ldpp_dout(dpp, 20) << "DEBUG: modify_obj_attrs" << dendl;
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent, &attrs);
  if (ret != 0) {
    return ret;
  }

  // Update object attrs
  set_atomic();
  attrs[attr_name] = attr_val;

  ret = set_dir_entry_attrs(dpp, &ent, &attrs);
  return ret;
}

int DaosObject::delete_obj_attrs(const DoutPrefixProvider* dpp,
                                 const char* attr_name, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: delete_obj_attrs" << dendl;
  rgw_obj target = get_obj();
  Attrs rmattr;
  bufferlist bl;

  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, nullptr, &rmattr, y);
}

bool DaosObject::is_expired() {
  auto iter = attrs.find(RGW_ATTR_DELETE_AT);
  if (iter != attrs.end()) {
    utime_t delete_at;
    try {
      auto bufit = iter->second.cbegin();
      decode(delete_at, bufit);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0)
          << "ERROR: " << __func__
          << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
      return true;
    }
  }

  return false;
}

// Taken from rgw_rados.cc
void DaosObject::gen_rand_obj_instance_name() {
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(store->ctx(), buf, OBJ_INSTANCE_LEN);
  state.obj.key.set_instance(buf);
}

int DaosObject::omap_get_vals(const DoutPrefixProvider* dpp,
                              const std::string& marker, uint64_t count,
                              std::map<std::string, bufferlist>* m, bool* pmore,
                              optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_get_all(const DoutPrefixProvider* dpp,
                             std::map<std::string, bufferlist>* m,
                             optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_get_vals_by_keys(const DoutPrefixProvider* dpp,
                                      const std::string& oid,
                                      const std::set<std::string>& keys,
                                      Attrs* vals) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_set_val_by_key(const DoutPrefixProvider* dpp,
                                    const std::string& key, bufferlist& val,
                                    bool must_exist, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

std::unique_ptr<MPSerializer> DaosObject::get_serializer(
    const DoutPrefixProvider* dpp, const std::string& lock_name) {
  return std::make_unique<MPDaosSerializer>(dpp, store, this, lock_name);
}

int DaosObject::transition(Bucket* bucket,
                           const rgw_placement_rule& placement_rule,
                           const real_time& mtime, uint64_t olh_epoch,
                           const DoutPrefixProvider* dpp, optional_yield y,
                           uint32_t flags) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::transition_to_cloud(
    Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
    std::set<std::string>& cloud_targets, CephContext* cct, bool update_object,
    const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

bool DaosObject::placement_rules_match(rgw_placement_rule& r1,
                                       rgw_placement_rule& r2) {
  /* XXX: support single default zone and zonegroup for now */
  return true;
}

int DaosObject::dump_obj_layout(const DoutPrefixProvider* dpp, optional_yield y,
                                Formatter* f) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<Object::ReadOp> DaosObject::get_read_op() {
  return std::make_unique<DaosObject::DaosReadOp>(this);
}

DaosObject::DaosReadOp::DaosReadOp(DaosObject* _source) : source(_source) {}

int DaosObject::DaosReadOp::prepare(optional_yield y,
                                    const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << __func__
                     << ": bucket=" << source->get_bucket()->get_name()
                     << dendl;

  if (source->get_bucket()->versioned() && !source->have_instance()) {
    // If the bucket is versioned and no version is specified, get the latest
    // version
    source->set_instance(DS3_LATEST_INSTANCE);
  }

  rgw_bucket_dir_entry ent;
  int ret = source->get_dir_entry_attrs(dpp, &ent);

  // Set source object's attrs. The attrs is key/value map and is used
  // in send_response_data() to set attributes, including etag.
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) << __func__ << ": object's etag: " << ent.meta.etag
                     << dendl;
  etag_bl.append(etag.c_str(), etag.size());
  source->get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));

  source->set_key(ent.key);
  source->set_obj_size(ent.meta.size);
  ldpp_dout(dpp, 20) << __func__ << ": object's size: " << ent.meta.size
                     << dendl;

  return ret;
}

int DaosObject::DaosReadOp::read(int64_t off, int64_t end, bufferlist& bl,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << __func__ << ": off=" << off << " end=" << end << dendl;
  int ret = source->lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Calculate size, end is inclusive
  uint64_t size = end - off + 1;

  // Read
  ret = source->read(dpp, bl, off, size);
  if (ret != 0) {
    return ret;
  }

  return ret;
}

// RGWGetObj::execute() calls ReadOp::iterate() to read object from 'off' to
// 'end'. The returned data is processed in 'cb' which is a chain of
// post-processing filters such as decompression, de-encryption and sending back
// data to client (RGWGetObj_CB::handle_dta which in turn calls
// RGWGetObj::get_data_cb() to send data back.).
//
// POC implements a simple sync version of iterate() function in which it reads
// a block of data each time and call 'cb' for post-processing.
int DaosObject::DaosReadOp::iterate(const DoutPrefixProvider* dpp, int64_t off,
                                    int64_t end, RGWGetDataCB* cb,
                                    optional_yield y) {
  ldpp_dout(dpp, 20) << __func__ << ": off=" << off << " end=" << end << dendl;
  int ret = source->lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Calculate size, end is inclusive
  uint64_t size = end - off + 1;

  // Reserve buffers and read
  bufferlist bl;
  ret = source->read(dpp, bl, off, size);
  if (ret != 0) {
    return ret;
  }

  // Call cb to process returned data.
  ldpp_dout(dpp, 20) << __func__ << ": call cb to process data, actual=" << size
                     << dendl;
  cb->handle_data(bl, off, size);
  return ret;
}

int DaosObject::DaosReadOp::get_attr(const DoutPrefixProvider* dpp,
                                     const char* name, bufferlist& dest,
                                     optional_yield y) {
  Attrs attrs;
  int ret = source->get_dir_entry_attrs(dpp, nullptr, &attrs);
  if (!ret) {
    return -ENODATA;
  }

  auto search = attrs.find(name);
  if (search == attrs.end()) {
    return -ENODATA;
  }

  dest = search->second;
  return 0;
}

std::unique_ptr<Object::DeleteOp> DaosObject::get_delete_op() {
  return std::make_unique<DaosObject::DaosDeleteOp>(this);
}

DaosObject::DaosDeleteOp::DaosDeleteOp(DaosObject* _source) : source(_source) {}

// Implementation of DELETE OBJ also requires DaosObject::get_obj_state()
// to retrieve and set object's state from object's metadata.
//
// TODO:
// 1. The POC only deletes the Daos objects. It doesn't handle the
// DeleteOp::params. Delete::delete_obj() in rgw_rados.cc shows how rados
// backend process the params.
// 2. Delete an object when its versioning is turned on.
// 3. Handle empty directories
// 4. Fail when file doesn't exist
int DaosObject::DaosDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                         optional_yield y, uint32_t flags) {
  ldpp_dout(dpp, 20) << "DaosDeleteOp::delete_obj "
                     << source->get_key().get_oid() << " from "
                     << source->get_bucket()->get_name() << dendl;
  if (source->get_instance() == "null") {
    source->clear_instance();
  }

  // Open bucket
  int ret = 0;
  std::string key = source->get_key().get_oid();
  DaosBucket* daos_bucket = source->get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  // Remove the daos object
  ret = ds3_obj_destroy(key.c_str(), daos_bucket->ds3b);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_destroy key=" << key << " ret=" << ret
                     << dendl;

  // result.delete_marker = parent_op.result.delete_marker;
  // result.version_id = parent_op.result.version_id;

  return ret;
}

int DaosObject::delete_object(const DoutPrefixProvider* dpp, optional_yield y,
                              uint32_t flags) {
  ldpp_dout(dpp, 20) << "DEBUG: delete_object" << dendl;
  DaosObject::DaosDeleteOp del_op(this);
  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = bucket->get_info().versioning_status();

  return del_op.delete_obj(dpp, y, flags);
}

int DaosObject::delete_obj_aio(const DoutPrefixProvider* dpp,
                               RGWObjState* astate, Completions* aio,
                               bool keep_index_consistent, optional_yield y) {
  /* XXX: Make it async */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::copy_object(
    User* user, req_info* info, const rgw_zone_id& source_zone,
    rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
    rgw::sal::Bucket* src_bucket, const rgw_placement_rule& dest_placement,
    ceph::real_time* src_mtime, ceph::real_time* mtime,
    const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
    bool high_precision_time, const char* if_match, const char* if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string* version_id,
    std::string* tag, std::string* etag, void (*progress_cb)(off_t, void*),
    void* progress_data, const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::swift_versioning_restore(bool& restored,
                                         const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::lookup(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: lookup" << dendl;
  if (is_open()) {
    return 0;
  }

  if (get_instance() == "null") {
    clear_instance();
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  ret = ds3_obj_open(get_key().get_oid().c_str(), &ds3o, daos_bucket->ds3b);

  if (ret == -ENOENT) {
    ldpp_dout(dpp, 20) << "DEBUG: daos object (" << get_bucket()->get_name()
                       << ", " << get_key().get_oid()
                       << ") does not exist: ret=" << ret << dendl;
  } else if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::create(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: create" << dendl;
  if (is_open()) {
    return 0;
  }

  if (get_instance() == "null") {
    clear_instance();
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  ret = ds3_obj_create(get_key().get_oid().c_str(), &ds3o, daos_bucket->ds3b);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::close(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: close" << dendl;
  if (!is_open()) {
    return 0;
  }

  int ret = ds3_obj_close(ds3o);
  ds3o = nullptr;
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_close ret=" << ret << dendl;
  return ret;
}

int DaosObject::write(const DoutPrefixProvider* dpp, bufferlist&& data,
                      uint64_t offset) {
  ldpp_dout(dpp, 20) << "DEBUG: write" << dendl;
  uint64_t size = data.length();
  int ret = ds3_obj_write(data.c_str(), offset, &size, get_daos_bucket()->ds3b,
                          ds3o, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to write into daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::read(const DoutPrefixProvider* dpp, bufferlist& data,
                     uint64_t offset, uint64_t& size) {
  ldpp_dout(dpp, 20) << "DEBUG: read" << dendl;
  int ret = ds3_obj_read(data.append_hole(size).c_str(), offset, &size,
                         get_daos_bucket()->ds3b, ds3o, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read from daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

// Get the object's dirent and attrs
int DaosObject::get_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent,
                                    Attrs* getattrs) {
  ldpp_dout(dpp, 20) << "DEBUG: get_dir_entry_attrs" << dendl;
  int ret = 0;
  vector<uint8_t> value(DS3_MAX_ENCODED_LEN);
  uint32_t size = value.size();

  if (get_key().ns == RGW_OBJ_NS_MULTIPART) {
    struct ds3_multipart_upload_info ui = {.encoded = value.data(),
                                           .encoded_length = size};
    ret = ds3_upload_get_info(&ui, bucket->get_name().c_str(),
                              get_key().get_oid().c_str(), store->ds3);
  } else {
    ret = lookup(dpp);
    if (ret != 0) {
      return ret;
    }

    auto object_info = std::make_unique<struct ds3_object_info>();
    object_info->encoded = value.data();
    object_info->encoded_length = size;
    ret = ds3_obj_get_info(object_info.get(), get_daos_bucket()->ds3b, ds3o);
    size = object_info->encoded_length;
  }

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get info of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket_dir_entry dummy_ent;
  if (!ent) {
    // if ent is not passed, use a dummy ent
    ent = &dummy_ent;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<char*>(value.data()), size);
  auto iter = bl.cbegin();
  ent->decode(iter);
  if (getattrs) {
    decode(*getattrs, iter);
  }

  return ret;
}
// Set the object's dirent and attrs
int DaosObject::set_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent,
                                    Attrs* setattrs) {
  ldpp_dout(dpp, 20) << "DEBUG: set_dir_entry_attrs" << dendl;
  int ret = lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Set defaults
  if (!ent) {
    // if ent is not passed, return an error
    return -EINVAL;
  }

  if (!setattrs) {
    // if setattrs is not passed, use object attrs
    setattrs = &attrs;
  }

  bufferlist wbl;
  ent->encode(wbl);
  encode(*setattrs, wbl);

  // Write rgw_bucket_dir_entry into object xattr
  auto object_info = std::make_unique<struct ds3_object_info>();
  object_info->encoded = wbl.c_str();
  object_info->encoded_length = wbl.length();
  ret = ds3_obj_set_info(object_info.get(), get_daos_bucket()->ds3b, ds3o);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set info of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::mark_as_latest(const DoutPrefixProvider* dpp,
                               ceph::real_time set_mtime) {
  // TODO handle deletion
  // TODO understand race conditions
  ldpp_dout(dpp, 20) << "DEBUG: mark_as_latest" << dendl;

  // Get latest version so far
  std::unique_ptr<DaosObject> latest_object = std::make_unique<DaosObject>(
      store, rgw_obj_key(get_name(), DS3_LATEST_INSTANCE), get_bucket());

  ldpp_dout(dpp, 20) << __func__ << ": key=" << get_key().get_oid()
                     << " latest_object_key= "
                     << latest_object->get_key().get_oid() << dendl;

  int ret = latest_object->lookup(dpp);
  if (ret == 0) {
    // Get metadata only if file exists
    rgw_bucket_dir_entry latest_ent;
    Attrs latest_attrs;
    ret = latest_object->get_dir_entry_attrs(dpp, &latest_ent, &latest_attrs);
    if (ret != 0) {
      return ret;
    }

    // Update flags
    latest_ent.flags = rgw_bucket_dir_entry::FLAG_VER;
    latest_ent.meta.mtime = set_mtime;
    ret = latest_object->set_dir_entry_attrs(dpp, &latest_ent, &latest_attrs);
    if (ret != 0) {
      return ret;
    }
  }

  // Get or create the link [latest], make it link to the current latest
  // version.
  ret =
      ds3_obj_mark_latest(get_key().get_oid().c_str(), get_daos_bucket()->ds3b);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_mark_latest ret=" << ret << dendl;
  return ret;
}

DaosAtomicWriter::DaosAtomicWriter(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, DaosStore* _store,
    const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
    uint64_t _olh_epoch, const std::string& _unique_tag)
    : StoreWriter(dpp, y),
      store(_store),
      owner(_owner),
      ptail_placement_rule(_ptail_placement_rule),
      olh_epoch(_olh_epoch),
      unique_tag(_unique_tag),
      obj(_store, obj->get_key(), obj->get_bucket()) {}

int DaosAtomicWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: prepare" << dendl;
  int ret = obj.create(dpp);
  return ret;
}

// TODO: Handle concurrent writes, a unique object id is a possible solution, or
// use DAOS transactions
// XXX: Do we need to accumulate writes as motr does?
int DaosAtomicWriter::process(bufferlist&& data, uint64_t offset) {
  ldpp_dout(dpp, 20) << "DEBUG: process" << dendl;
  if (data.length() == 0) {
    return 0;
  }

  int ret = 0;
  if (!obj.is_open()) {
    ret = obj.lookup(dpp);
    if (ret != 0) {
      return ret;
    }
  }

  // XXX: Combine multiple streams into one as motr does
  uint64_t data_size = data.length();
  ret = obj.write(dpp, std::move(data), offset);
  if (ret == 0) {
    total_data_size += data_size;
  }
  return ret;
}

int DaosAtomicWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y, uint32_t flags) {
  ldpp_dout(dpp, 20) << "DEBUG: complete" << dendl;
  bufferlist bl;
  rgw_bucket_dir_entry ent;
  int ret;

  // Set rgw_bucet_dir_entry. Some of the members of this structure may not
  // apply to daos.
  //
  // Checkout AtomicObjectProcessor::complete() in rgw_putobj_processor.cc
  // and RGWRados::Object::Write::write_meta() in rgw_rados.cc for what and
  // how to set the dir entry. Only set the basic ones for POC, no ACLs and
  // other attrs.
  obj.get_key().get_index_key(&ent.key);
  ent.meta.size = total_data_size;
  ent.meta.accounted_size = accounted_size;
  ent.meta.mtime =
      real_clock::is_zero(set_mtime) ? ceph::real_clock::now() : set_mtime;
  ent.meta.etag = etag;
  ent.meta.owner = owner.to_str();
  ent.meta.owner_display_name =
      obj.get_bucket()->get_owner()->get_display_name();
  bool is_versioned = obj.get_bucket()->versioned();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ldpp_dout(dpp, 20) << __func__ << ": key=" << obj.get_key().get_oid()
                     << " etag: " << etag << dendl;
  if (user_data) ent.meta.user_data = *user_data;

  RGWBucketInfo& info = obj.get_bucket()->get_info();
  if (info.obj_lock_enabled() && info.obj_lock.has_rule()) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date =
          info.obj_lock.get_lock_until_date(ent.meta.mtime);
      string mode = info.obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist retention_bl;
      obj_retention.encode(retention_bl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = retention_bl;
    }
  }

  ret = obj.set_dir_entry_attrs(dpp, &ent, &attrs);

  if (is_versioned) {
    ret = obj.mark_as_latest(dpp, set_mtime);
    if (ret != 0) {
      return ret;
    }
  }

  return ret;
}

int DaosMultipartUpload::abort(const DoutPrefixProvider* dpp,
                               CephContext* cct) {
  // Remove upload from bucket multipart index
  ldpp_dout(dpp, 20) << "DEBUG: abort" << dendl;
  return ds3_upload_remove(bucket->get_name().c_str(), get_upload_id().c_str(),
                           store->ds3);
}

std::unique_ptr<rgw::sal::Object> DaosMultipartUpload::get_meta_obj() {
  return bucket->get_object(
      rgw_obj_key(get_upload_id(), string(), RGW_OBJ_NS_MULTIPART));
}

int DaosMultipartUpload::init(const DoutPrefixProvider* dpp, optional_yield y,
                              ACLOwner& _owner,
                              rgw_placement_rule& dest_placement,
                              rgw::sal::Attrs& attrs) {
  ldpp_dout(dpp, 20) << "DEBUG: init" << dendl;
  int ret;
  std::string oid = mp_obj.get_key();

  // Create an initial entry in the bucket. The entry will be
  // updated when multipart upload is completed, for example,
  // size, etag etc.
  bufferlist bl;
  rgw_bucket_dir_entry ent;
  ent.key.name = oid;
  ent.meta.owner = owner.get_id().to_str();
  ent.meta.category = RGWObjCategory::MultiMeta;
  ent.meta.mtime = ceph::real_clock::now();

  multipart_upload_info upload_info;
  upload_info.dest_placement = dest_placement;

  ent.encode(bl);
  encode(attrs, bl);
  encode(upload_info, bl);

  struct ds3_multipart_upload_info ui;
  std::strcpy(ui.upload_id, MULTIPART_UPLOAD_ID_PREFIX);
  std::strncpy(ui.key, oid.c_str(), sizeof(ui.key));
  ui.encoded = bl.c_str();
  ui.encoded_length = bl.length();
  int prefix_length = strlen(ui.upload_id);

  do {
    gen_rand_alphanumeric(store->ctx(), ui.upload_id + prefix_length,
                          sizeof(ui.upload_id) - 1 - prefix_length);
    mp_obj.init(oid, ui.upload_id);
    ret = ds3_upload_init(&ui, bucket->get_name().c_str(), store->ds3);
  } while (ret == -EEXIST);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create multipart upload dir ("
                      << bucket->get_name() << "/" << get_upload_id()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosMultipartUpload::list_parts(const DoutPrefixProvider* dpp,
                                    CephContext* cct, int num_parts, int marker,
                                    int* next_marker, bool* truncated,
                                    bool assume_unsorted) {
  ldpp_dout(dpp, 20) << "DEBUG: list_parts" << dendl;
  // Init needed structures
  vector<struct ds3_multipart_part_info> multipart_part_infos(num_parts);
  uint32_t npart = multipart_part_infos.size();
  vector<vector<uint8_t>> values(npart, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < npart; i++) {
    multipart_part_infos[i].encoded = values[i].data();
    multipart_part_infos[i].encoded_length = values[i].size();
  }

  uint32_t daos_marker = marker;
  int ret = ds3_upload_list_parts(
      bucket->get_name().c_str(), get_upload_id().c_str(), &npart,
      multipart_part_infos.data(), &daos_marker, truncated, store->ds3);

  if (ret != 0) {
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  multipart_part_infos.resize(npart);
  values.resize(npart);
  parts.clear();

  for (auto const& pi : multipart_part_infos) {
    bufferlist bl;
    bl.append(reinterpret_cast<char*>(pi.encoded), pi.encoded_length);

    std::unique_ptr<DaosMultipartPart> part =
        std::make_unique<DaosMultipartPart>();
    auto iter = bl.cbegin();
    decode(part->info, iter);
    parts[pi.part_num] = std::move(part);
  }

  if (next_marker) {
    *next_marker = daos_marker;
  }
  return ret;
}

// Heavily copied from rgw_sal_rados.cc
int DaosMultipartUpload::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    map<int, string>& part_etags, list<rgw_obj_index_key>& remove_objs,
    uint64_t& accounted_size, bool& compressed, RGWCompressionInfo& cs_info,
    off_t& off, std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
    rgw::sal::Object* target_obj) {
  ldpp_dout(dpp, 20) << "DEBUG: complete" << dendl;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): enter" << dendl;
  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs attrs = target_obj->get_attrs();

  do {
    ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): list_parts()"
                       << dendl;
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret != 0) return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
                        << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }
    ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): parts.size()="
                       << parts.size() << dendl;

    for (auto obj_iter = parts.begin();
         etags_iter != part_etags.end() && obj_iter != parts.end();
         ++etags_iter, ++obj_iter, ++handled_parts) {
      DaosMultipartPart* part =
          dynamic_cast<rgw::sal::DaosMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): part_size="
                         << part_size << dendl;
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
                          << etags_iter->first
                          << " next uploaded: " << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: "
                          << etags_iter->first
                          << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      hex_to_buf(part->get_etag().c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char*)petag, sizeof(petag));
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): calc etag "
                         << dendl;

      RGWUploadPartInfo& obj_part = part->info;
      string oid = mp_obj.get_part(obj_part.num);
      rgw_obj src_obj;
      src_obj.init_ns(bucket->get_key(), oid, RGW_OBJ_NS_MULTIPART);

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
           (cs_info.compression_type != obj_part.cs_info.compression_type))) {
        ldpp_dout(dpp, 0)
            << "ERROR: compression type was changed during multipart upload ("
            << cs_info.compression_type << ">>"
            << obj_part.cs_info.compression_type << ")" << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): part compression"
                         << dendl;
      if (part_compressed) {
        int64_t new_ofs;  // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }

      // We may not need to do the following as remove_objs are those
      // don't show when listing a bucket. As we store in-progress uploaded
      // object's metadata in a separate index, they are not shown when
      // listing a bucket.
      rgw_obj_index_key remove_key;
      src_obj.key.get_index_key(&remove_key);

      remove_objs.push_back(remove_key);

      off += obj_part.size;
      accounted_size += obj_part.accounted_size;
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): off=" << off
                         << ", accounted_size = " << accounted_size << dendl;
    }
  } while (truncated);
  hash.Final((unsigned char*)final_etag);

  buf_to_hex((unsigned char*)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
           sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2, "-%lld",
           (long long)part_etags.size());
  etag = final_etag_str;
  ldpp_dout(dpp, 10) << "calculated etag: " << etag << dendl;

  etag_bl.append(etag);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  // Different from rgw_sal_rados.cc starts here
  // Read the object's multipart info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_multipart_upload_info ui = {
      .encoded = bl.append_hole(size).c_str(), .encoded_length = size};
  ret = ds3_upload_get_info(&ui, bucket->get_name().c_str(),
                            get_upload_id().c_str(), store->ds3);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_upload_get_info entry="
                     << bucket->get_name() << "/" << get_upload_id() << dendl;
  if (ret != 0) {
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  rgw_bucket_dir_entry ent;
  auto iter = bl.cbegin();
  ent.decode(iter);

  // Update entry data and name
  target_obj->get_key().get_index_key(&ent.key);
  ent.meta.size = off;
  ent.meta.accounted_size = accounted_size;
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): obj size="
                     << ent.meta.size
                     << " obj accounted size=" << ent.meta.accounted_size
                     << dendl;
  ent.meta.category = RGWObjCategory::Main;
  ent.meta.mtime = ceph::real_clock::now();
  bool is_versioned = target_obj->get_bucket()->versioned();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ent.meta.etag = etag;

  // Open object
  DaosObject* obj = static_cast<DaosObject*>(target_obj);
  ret = obj->create(dpp);
  if (ret != 0) {
    return ret;
  }

  // Copy data from parts to object
  uint64_t write_off = 0;
  for (auto const& [part_num, part] : get_parts()) {
    ds3_part_t* ds3p;
    ret = ds3_part_open(get_bucket_name().c_str(), get_upload_id().c_str(),
                        part_num, false, &ds3p, store->ds3);
    if (ret != 0) {
      return ret;
    }

    // Reserve buffers and read
    uint64_t size = part->get_size();
    bufferlist bl;
    ret = ds3_part_read(bl.append_hole(size).c_str(), 0, &size, ds3p,
                        store->ds3, nullptr);
    if (ret != 0) {
      ds3_part_close(ds3p);
      return ret;
    }

    ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): part " << part_num
                       << " size is " << size << dendl;

    // write to obj
    obj->write(dpp, std::move(bl), write_off);
    ds3_part_close(ds3p);
    write_off += part->get_size();
  }

  // Set attributes
  ret = obj->set_dir_entry_attrs(dpp, &ent, &attrs);

  if (is_versioned) {
    ret = obj->mark_as_latest(dpp, ent.meta.mtime);
    if (ret != 0) {
      return ret;
    }
  }

  // Remove upload from bucket multipart index
  ret = ds3_upload_remove(get_bucket_name().c_str(), get_upload_id().c_str(),
                          store->ds3);
  return ret;
}

int DaosMultipartUpload::get_info(const DoutPrefixProvider* dpp,
                                  optional_yield y, rgw_placement_rule** rule,
                                  rgw::sal::Attrs* attrs) {
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::get_info(): enter" << dendl;
  if (!rule && !attrs) {
    return 0;
  }

  if (rule) {
    if (!placement.empty()) {
      *rule = &placement;
      if (!attrs) {
        // Don't need attrs, done
        return 0;
      }
    } else {
      *rule = nullptr;
    }
  }

  // Read the multipart upload dirent from index
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_multipart_upload_info ui = {
      .encoded = bl.append_hole(size).c_str(), .encoded_length = size};
  int ret = ds3_upload_get_info(&ui, bucket->get_name().c_str(),
                                get_upload_id().c_str(), store->ds3);

  if (ret != 0) {
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  multipart_upload_info upload_info;
  rgw_bucket_dir_entry ent;
  Attrs decoded_attrs;
  auto iter = bl.cbegin();
  ent.decode(iter);
  decode(decoded_attrs, iter);
  ldpp_dout(dpp, 20) << "DEBUG: decoded_attrs=" << attrs << dendl;

  if (attrs) {
    *attrs = decoded_attrs;
    if (!rule || *rule != nullptr) {
      // placement was cached; don't actually read
      return 0;
    }
  }

  // Now decode the placement rule
  decode(upload_info, iter);
  placement = upload_info.dest_placement;
  *rule = &placement;

  return 0;
}

std::unique_ptr<Writer> DaosMultipartUpload::get_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
    const std::string& part_num_str) {
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::get_writer(): enter part="
                     << part_num << " head_obj=" << _head_obj << dendl;
  return std::make_unique<DaosMultipartWriter>(
      dpp, y, this, obj, store, owner, ptail_placement_rule,
      part_num, part_num_str);
}

DaosMultipartWriter::~DaosMultipartWriter() {
  if (is_open()) ds3_part_close(ds3p);
}

int DaosMultipartWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::prepare(): enter part="
                     << part_num_str << dendl;
  int ret = ds3_part_open(get_bucket_name().c_str(), upload_id.c_str(),
                          part_num, true, &ds3p, store->ds3);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_UPLOAD;
  }
  return ret;
}

const std::string& DaosMultipartWriter::get_bucket_name() {
  return static_cast<DaosMultipartUpload*>(upload)->get_bucket_name();
}

int DaosMultipartWriter::process(bufferlist&& data, uint64_t offset) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::process(): enter part="
                     << part_num_str << " offset=" << offset << dendl;
  if (data.length() == 0) {
    return 0;
  }

  uint64_t size = data.length();
  int ret =
      ds3_part_write(data.c_str(), offset, &size, ds3p, store->ds3, nullptr);
  if (ret == 0) {
    // XXX: Combine multiple streams into one as motr does
    actual_part_size += size;
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to write into part ("
                      << get_bucket_name() << ", " << upload_id << ", "
                      << part_num << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosMultipartWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y, uint32_t flags) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): enter part="
                     << part_num_str << dendl;

  // Add an entry into part index
  bufferlist bl;
  RGWUploadPartInfo info;
  info.num = part_num;
  info.etag = etag;
  info.size = actual_part_size;
  info.accounted_size = accounted_size;
  info.modified = real_clock::now();

  bool compressed;
  int ret = rgw_compression_info_from_attrset(attrs, compressed, info.cs_info);
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): compression ret="
                     << ret << dendl;
  if (ret != 0) {
    ldpp_dout(dpp, 1) << "cannot get compression info" << dendl;
    return ret;
  }
  encode(info, bl);
  encode(attrs, bl);
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): entry size"
                     << bl.length() << dendl;

  struct ds3_multipart_part_info part_info = {.part_num = part_num,
                                              .encoded = bl.c_str(),
                                              .encoded_length = bl.length()};

  ret = ds3_part_set_info(&part_info, ds3p, store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set part info (" << get_bucket_name()
                      << ", " << upload_id << ", " << part_num
                      << "): ret=" << ret << dendl;
    if (ret == ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
  }

  return ret;
}

std::unique_ptr<RGWRole> DaosStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> DaosStore::get_role(const RGWRoleInfo& info) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> DaosStore::get_role(std::string id) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

int DaosStore::get_roles(const DoutPrefixProvider* dpp, optional_yield y,
                         const std::string& path_prefix,
                         const std::string& tenant,
                         vector<std::unique_ptr<RGWRole>>& roles) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<RGWOIDCProvider> DaosStore::get_oidc_provider() {
  RGWOIDCProvider* p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int DaosStore::get_oidc_providers(
    const DoutPrefixProvider* dpp, const std::string& tenant,
    vector<std::unique_ptr<RGWOIDCProvider>>& providers) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<MultipartUpload> DaosBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime) {
  return std::make_unique<DaosMultipartUpload>(store, this, oid, upload_id,
                                               owner, mtime);
}

std::unique_ptr<Writer> DaosStore::get_append_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag, uint64_t position,
    uint64_t* cur_accounted_size) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return nullptr;
}

std::unique_ptr<Writer> DaosStore::get_atomic_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
    const std::string& unique_tag) {
  ldpp_dout(dpp, 20) << "get_atomic_writer" << dendl;
  return std::make_unique<DaosAtomicWriter>(dpp, y, obj, this,
                                            owner, ptail_placement_rule,
                                            olh_epoch, unique_tag);
}

const std::string& DaosStore::get_compression_type(
    const rgw_placement_rule& rule) {
  return zone.zone_params->get_compression_type(rule);
}

bool DaosStore::valid_placement(const rgw_placement_rule& rule) {
  return zone.zone_params->valid_placement(rule);
}

std::unique_ptr<User> DaosStore::get_user(const rgw_user& u) {
  ldout(cctx, 20) << "DEBUG: bucket's user:  " << u.to_str() << dendl;
  return std::make_unique<DaosUser>(this, u);
}

int DaosStore::get_user_by_access_key(const DoutPrefixProvider* dpp,
                                      const std::string& key, optional_yield y,
                                      std::unique_ptr<User>* user) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get_by_key(key.c_str(), &user_info, ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get_by_key failed, key=" << key
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  DaosUserInfo duinfo;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo.decode(iter);

  User* u = new DaosUser(this, duinfo.info);
  if (!u) {
    return -ENOMEM;
  }

  user->reset(u);
  return 0;
}

int DaosStore::get_user_by_email(const DoutPrefixProvider* dpp,
                                 const std::string& email, optional_yield y,
                                 std::unique_ptr<User>* user) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get_by_email(email.c_str(), &user_info, ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get_by_email failed, email=" << email
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  DaosUserInfo duinfo;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo.decode(iter);

  User* u = new DaosUser(this, duinfo.info);
  if (!u) {
    return -ENOMEM;
  }

  user->reset(u);
  return 0;
}

int DaosStore::get_user_by_swift(const DoutPrefixProvider* dpp,
                                 const std::string& user_str, optional_yield y,
                                 std::unique_ptr<User>* user) {
  /* Swift keys and subusers are not supported for now */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<Object> DaosStore::get_object(const rgw_obj_key& k) {
  return std::make_unique<DaosObject>(this, k);
}

inline std::ostream& operator<<(std::ostream& out, const rgw_user* u) {
  std::string s;
  if (u != nullptr)
    u->to_str(s);
  else
    s = "(nullptr)";
  return out << s;
}

int DaosStore::get_bucket(const DoutPrefixProvider* dpp, User* u,
                          const rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
                          optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: get_bucket1: User: " << u << dendl;
  int ret;
  Bucket* bp;

  bp = new DaosBucket(this, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret != 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int DaosStore::get_bucket(User* u, const RGWBucketInfo& i,
                          std::unique_ptr<Bucket>* bucket) {
  DaosBucket* bp;

  bp = new DaosBucket(this, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int DaosStore::get_bucket(const DoutPrefixProvider* dpp, User* u,
                          const std::string& tenant, const std::string& name,
                          std::unique_ptr<Bucket>* bucket, optional_yield y) {
  ldpp_dout(dpp, 20) << "get_bucket" << dendl;
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

bool DaosStore::is_meta_master() { return true; }

int DaosStore::forward_request_to_master(const DoutPrefixProvider* dpp,
                                         User* user, obj_version* objv,
                                         bufferlist& in_data, JSONParser* jp,
                                         req_info& info, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::forward_iam_request_to_master(const DoutPrefixProvider* dpp,
                                             const RGWAccessKey& key,
                                             obj_version* objv,
                                             bufferlist& in_data,
                                             RGWXMLDecoder::XMLParser* parser,
                                             req_info& info, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::string DaosStore::zone_unique_id(uint64_t unique_num) { return ""; }

std::string DaosStore::zone_unique_trans_id(const uint64_t unique_num) {
  return "";
}

int DaosStore::cluster_stat(RGWClusterStat& stats) {
  return DAOS_NOT_IMPLEMENTED_LOG(nullptr);
}

std::unique_ptr<Lifecycle> DaosStore::get_lifecycle(void) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

std::unique_ptr<Completions> DaosStore::get_completions(void) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

std::unique_ptr<Notification> DaosStore::get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
    rgw::notify::EventType event_type, const std::string* object_name) {
  return std::make_unique<DaosNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification> DaosStore::get_notification(
    const DoutPrefixProvider* dpp, Object* obj, Object* src_obj,
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket,
    std::string& _user_id, std::string& _user_tenant, std::string& _req_id,
    optional_yield y) {
  ldpp_dout(dpp, 20) << "get_notification" << dendl;
  return std::make_unique<DaosNotification>(obj, src_obj, event_type);
}

int DaosStore::log_usage(const DoutPrefixProvider* dpp,
                         map<rgw_user_bucket, RGWUsageBatch>& usage_info) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return 0;
}

int DaosStore::log_op(const DoutPrefixProvider* dpp, string& oid,
                      bufferlist& bl) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::register_to_service_map(const DoutPrefixProvider* dpp,
                                       const string& daemon_type,
                                       const map<string, string>& meta) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::get_quota(RGWQuota& quota) {
  // XXX: Not handled for the first pass
  return;
}

void DaosStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                              RGWRateLimitInfo& user_ratelimit,
                              RGWRateLimitInfo& anon_ratelimit) {
  return;
}

int DaosStore::set_buckets_enabled(const DoutPrefixProvider* dpp,
                                   std::vector<rgw_bucket>& buckets,
                                   bool enabled) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::get_sync_policy_handler(const DoutPrefixProvider* dpp,
                                       std::optional<rgw_zone_id> zone,
                                       std::optional<rgw_bucket> bucket,
                                       RGWBucketSyncPolicyHandlerRef* phandler,
                                       optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

RGWDataSyncStatusManager* DaosStore::get_data_sync_manager(
    const rgw_zone_id& source_zone) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

int DaosStore::read_all_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::trim_all_usage(const DoutPrefixProvider* dpp,
                              uint64_t start_epoch, uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::get_config_key_val(string name, bufferlist* bl) {
  return DAOS_NOT_IMPLEMENTED_LOG(nullptr);
}

int DaosStore::meta_list_keys_init(const DoutPrefixProvider* dpp,
                                   const string& section, const string& marker,
                                   void** phandle) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::meta_list_keys_next(const DoutPrefixProvider* dpp, void* handle,
                                   int max, list<string>& keys,
                                   bool* truncated) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::meta_list_keys_complete(void* handle) { return; }

std::string DaosStore::meta_get_marker(void* handle) { return ""; }

int DaosStore::meta_remove(const DoutPrefixProvider* dpp, string& metadata_key,
                           optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::string DaosStore::get_cluster_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return "";
}

}  // namespace rgw::sal

extern "C" {

void* newDaosStore(CephContext* cct) {
  return new rgw::sal::DaosStore(cct);
}
}
