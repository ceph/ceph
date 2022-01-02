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

#define RGW_BUCKET_RGW_INFO "rgw_info"
#define RGW_DIR_ENTRY_XATTR "rgw_entry"
#define RGW_PART_XATTR "rgw_part"
#define METADATA_BUCKET "_METADATA"
#define USERS_DIR "users"
#define EMAILS_DIR "emails"
#define ACCESS_KEYS_DIR "access_keys"
#define MULTIPART_DIR "multipart"
#define MULTIPART_MAX_PARTS 10000
#define LATEST_INSTANCE "latest"
#define LATEST_INSTANCE_SUFFIX "[latest]"

static const std::string METADATA_DIRS[] = {USERS_DIR, EMAILS_DIR,
                                            ACCESS_KEYS_DIR, MULTIPART_DIR};

/**
 * Combines the vector to return a path
 */
fs::path make_path(std::vector<std::string>&& components) {
  fs::path p;
  for (auto comp : components) {
    p /= comp;
  }
  return p;
}

int DaosUser::list_buckets(const DoutPrefixProvider* dpp, const string& marker,
                           const string& end_marker, uint64_t max,
                           bool need_stats, BucketList& buckets,
                           optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: list_user_buckets: marker=" << marker
                     << " end_marker=" << end_marker << " max=" << max << dendl;
  int ret = 0;
  bool is_truncated = false;
  buckets.clear();
  daos_size_t bcount = max;
  vector<struct daos_pool_cont_info> daos_buckets(max);

  // XXX: Somehow handle markers and other bucket info
  ret = daos_pool_list_cont(store->poh, &bcount, daos_buckets.data(), nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: daos_pool_list_cont: bcount=" << bcount
                     << " ret=" << ret << dendl;
  if (ret == -DER_TRUNC) {
    is_truncated = true;
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: daos_pool_list_cont failed!" << ret << dendl;
    return ret;
  }

  if (!is_truncated) {
    daos_buckets.resize(bcount);
  }

  for (const auto& db : daos_buckets) {
    string name = db.pci_label;
    if (name == METADATA_BUCKET) {
      // Skip metadata bucket
      continue;
    }

    RGWBucketEnt ent = {};
    ent.bucket.name = name;
    DaosBucket* daos_bucket = new DaosBucket(this->store, ent, this);
    daos_bucket->load_bucket(dpp, y);
    buckets.add(std::unique_ptr<DaosBucket>(daos_bucket));
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
  if (ret < 0 && ret != -ENOENT) {
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
    ret = dfs_cont_create_with_label(store->poh, bucket->get_name().c_str(),
                                     nullptr, nullptr, nullptr, nullptr);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_cont_create_with_label ret=" << ret
                       << " name=" << bucket->get_name() << dendl;
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: dfs_cont_create_with_label failed! ret="
                        << ret << dendl;
      return ret;
    }
    ret = daos_bucket->put_info(dpp, y, ceph::real_time());
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put bucket info! ret=" << ret
                        << dendl;
      return ret;
    }

    // Create multipart index
    ret = dfs_mkdir(store->meta_dfs, store->dirs[MULTIPART_DIR],
                    bucket->get_name().c_str(), DEFFILEMODE, 0);
    ldpp_dout(dpp, 20) << "DEBUG: multipart index dfs_mkdir bucket="
                       << bucket->get_name() << " ret=" << ret << dendl;
    if (ret != 0 && ret != EEXIST) {
      ldpp_dout(dpp, 20)
          << "ERROR: multipart index creation failed! dfs_mkdir ret=" << ret
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
  return 0;
}

int DaosUser::read_stats(const DoutPrefixProvider* dpp, optional_yield y,
                         RGWStorageStats* stats,
                         ceph::real_time* last_stats_sync,
                         ceph::real_time* last_stats_update) {
  return 0;
}

/* stats - Not for first pass */
int DaosUser::read_stats_async(const DoutPrefixProvider* dpp,
                               RGWGetUserStats_CB* cb) {
  return 0;
}

int DaosUser::complete_flush_stats(const DoutPrefixProvider* dpp,
                                   optional_yield y) {
  return 0;
}

int DaosUser::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch, uint32_t max_entries,
                         bool* is_truncated, RGWUsageIter& usage_iter,
                         map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return 0;
}

int DaosUser::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch) {
  return 0;
}

int DaosUser::load_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();
  ldpp_dout(dpp, 20) << "DEBUG: load_user, name=" << name << dendl;

  DaosUserInfo duinfo;
  int ret = store->read_user(dpp, USERS_DIR, name, &duinfo);
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
  ret = store->read_user(dpp, USERS_DIR, name, &duinfo);
  obj_version obj_ver = duinfo.user_version;

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
  } else {
    obj_ver.ver = 1;
    obj_ver.tag = "UserTAG";
  }

  // Encode user data
  bufferlist bl;
  duinfo.info = info;
  duinfo.attrs = attrs;
  duinfo.user_version = obj_ver;
  duinfo.encode(bl);

  // Open user file
  dfs_obj_t* user_obj;
  mode_t mode = DEFFILEMODE;
  ret = dfs_open(store->meta_dfs, store->dirs[USERS_DIR], name.c_str(),
                 S_IFREG | mode, O_RDWR | O_CREAT | O_TRUNC, 0, 0, nullptr,
                 &user_obj);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open user file, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Write user data
  d_sg_list_t wsgl;
  d_iov_t iov;
  d_iov_set(&iov, bl.c_str(), bl.length());
  wsgl.sg_nr = 1;
  wsgl.sg_iovs = &iov;
  ret = dfs_write(store->meta_dfs, user_obj, &wsgl, 0, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to write to user file, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Close file
  ret = dfs_release(user_obj);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: dfs_release failed, user name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  if (ret == 0) {
    objv_tracker.read_version = obj_ver;
    objv_tracker.write_version = obj_ver;
  }

  fs::path user_path = make_path({"..", USERS_DIR, name});

  // Store access key in access key index
  if (!info.access_keys.empty()) {
    for (auto const& [id, key] : info.access_keys) {
      ret = dfs_open(store->meta_dfs, store->dirs[ACCESS_KEYS_DIR], id.c_str(),
                     S_IFLNK | mode, O_RDWR | O_CREAT | O_TRUNC, 0, 0,
                     user_path.c_str(), &user_obj);
      if (ret != 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to create user file, id=" << id
                          << " ret=" << ret << dendl;
        return ret;
      }

      ret = dfs_release(user_obj);
      if (ret != 0) {
        ldpp_dout(dpp, 0) << "ERROR: dfs_release failed, user file, id=" << id
                          << " ret=" << ret << dendl;
        return ret;
      }
    }
  }

  // Store email in email index
  if (!info.user_email.empty()) {
    string& email = info.user_email;
    ret = dfs_open(store->meta_dfs, store->dirs[EMAILS_DIR], email.c_str(),
                   S_IFLNK | mode, O_RDWR | O_CREAT | O_TRUNC, 0, 0,
                   user_path.c_str(), &user_obj);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to create user file, email=" << email
                        << " ret=" << ret << dendl;
      return ret;
    }

    ret = dfs_release(user_obj);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: dfs_release failed, user file, email="
                        << email << " ret=" << ret << dendl;
      return ret;
    }
  }

  return 0;
}

int DaosUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();

  // TODO: the expectation is that the object version needs to be passed in as a
  // method arg see int DB::remove_user(const DoutPrefixProvider *dpp,
  // RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv)

  // Open user file
  dfs_obj_t* user_obj;
  mode_t mode = DEFFILEMODE;
  int ret = dfs_open(store->meta_dfs, store->dirs[USERS_DIR], name.c_str(),
                     S_IFREG | mode, O_RDWR, 0, 0, nullptr, &user_obj);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open user file, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // make sure each access_key is removed
  for (auto it : info.access_keys) {
    if (dfs_access(store->meta_dfs, store->dirs[ACCESS_KEYS_DIR],
                   it.first.c_str(), W_OK) == 0) {
      ret = dfs_remove(store->meta_dfs, store->dirs[ACCESS_KEYS_DIR],
                       it.first.c_str(), true, nullptr);
      if (ret != 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to remove access_keys file="
                          << it.first.c_str() << " ret=" << ret << dendl;
        return ret;
      }
    }
  }

  // email should be removed if it exists
  const string& email = info.user_email;
  if (email.length() > 0) {
    if (dfs_access(store->meta_dfs, store->dirs[EMAILS_DIR], email.c_str(),
                   W_OK) == 0) {
      ret = dfs_remove(store->meta_dfs, store->dirs[EMAILS_DIR], email.c_str(),
                       true, nullptr);
      if (ret != 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to remove email file, email="
                          << email << " ret=" << ret << dendl;
        return ret;
      }
    }
  }

  // and remove the user object
  if (dfs_access(store->meta_dfs, store->dirs[USERS_DIR], name.c_str(), W_OK) ==
      0) {
    ret = dfs_remove(store->meta_dfs, store->dirs[USERS_DIR], name.c_str(),
                     true, nullptr);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to remove user file, name=" << name
                        << " ret=" << ret << dendl;
      return ret;
    }
  }

  // Close file
  ret = dfs_release(user_obj);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: dfs_release failed, user name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

DaosBucket::~DaosBucket() { close(nullptr); }

int DaosBucket::open(const DoutPrefixProvider* dpp) {
  // Idempotent
  if (_is_open) {
    return 0;
  }

  int ret;
  daos_cont_info_t cont_info;
  // TODO: We need to cache open container handles
  ret = daos_cont_open(store->poh, info.bucket.name.c_str(), DAOS_COO_RW, &coh,
                       &cont_info, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: daos_cont_open, name=" << info.bucket.name
                     << ", ret=" << ret << dendl;

  if (ret != 0) {
    return -ENOENT;
  }

  uuid_copy(cont_uuid, cont_info.ci_uuid);

  ret = dfs_mount(store->poh, coh, O_RDWR, &dfs);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_mount ret=" << ret << dendl;

  if (ret != 0) {
    daos_cont_close(coh, nullptr);
    return -ENOENT;
  }
  _is_open = true;
  return 0;
}

int DaosBucket::close(const DoutPrefixProvider* dpp) {
  // Idempotent
  if (!_is_open) {
    return 0;
  }

  int ret = 0;
  ret = dfs_umount(dfs);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_umount ret=" << ret << dendl;

  if (ret < 0) {
    return ret;
  }

  ret = daos_cont_close(coh, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: daos_cont_close ret=" << ret << dendl;
  if (ret < 0) {
    return ret;
  }
  _is_open = false;
  return 0;
}

std::unique_ptr<DaosObject> DaosBucket::get_part_object(std::string upload_id,
                                                        uint64_t part_num) {
  // XXX: create a util for path build
  fs::path part_path = make_path(
      {MULTIPART_DIR, get_name(), upload_id, std::to_string(part_num)});
  rgw_obj_key k;
  k.name = part_path.string();
  return std::make_unique<DaosObject>(store, k, store->get_metadata_bucket());
}

int DaosBucket::remove_bucket(const DoutPrefixProvider* dpp,
                              bool delete_children, bool forward_to_master,
                              req_info* req_info, optional_yield y) {
  // TODO implement

  return 0;
}

int DaosBucket::remove_bucket_bypass_gc(int concurrent_max,
                                        bool keep_index_consistent,
                                        optional_yield y,
                                        const DoutPrefixProvider* dpp) {
  // TODO implement
  return 0;
}

int DaosBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive,
                         ceph::real_time _mtime) {
  ldpp_dout(dpp, 20) << "DEBUG: put_info(): bucket name=" << info.bucket.name
                     << dendl;

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  DaosBucketInfo dbinfo;
  dbinfo.info = info;
  dbinfo.bucket_attrs = attrs;
  dbinfo.mtime = _mtime;
  dbinfo.bucket_version = bucket_version;
  dbinfo.encode(bl);

  char const* const names[] = {RGW_BUCKET_RGW_INFO};
  void const* const values[] = {bl.c_str()};
  size_t const sizes[] = {bl.length()};
  // TODO: separate attributes
  ret = daos_cont_set_attr(coh, 1, names, values, sizes, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: daos_cont_set_attr failed: " << ret << dendl;
  }
  ret = close(dpp);
  return ret;
}

int DaosBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                            bool get_stats) {
  ldpp_dout(dpp, 20) << "DEBUG: load_bucket(): bucket name=" << get_name()
                     << dendl;

  // Prevent attempting to load metadata bucket
  if (get_name() == METADATA_BUCKET) {
    ldpp_dout(dpp, 0) << "ERROR: Cannot load metadata bucket" << dendl;
    return -ENOENT;
  }

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  DaosBucketInfo dbinfo;
  vector<uint8_t> value(DFS_MAX_XATTR_LEN);
  char const* const names[] = {RGW_BUCKET_RGW_INFO};
  void* const values[] = {value.data()};
  size_t sizes[] = {value.size()};

  ret = daos_cont_get_attr(coh, 1, names, values, sizes, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: daos_cont_get_attr failed: " << ret << dendl;
    return ret;
  }

  bl.append(reinterpret_cast<char*>(value.data()), sizes[0]);
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

  ret = close(dpp);
  return ret;
}

/* stats - Not for first pass */
int DaosBucket::read_stats(const DoutPrefixProvider* dpp, int shard_id,
                           std::string* bucket_ver, std::string* master_ver,
                           std::map<RGWObjCategory, RGWStorageStats>& stats,
                           std::string* max_marker, bool* syncstopped) {
  return 0;
}

int DaosBucket::read_stats_async(const DoutPrefixProvider* dpp, int shard_id,
                                 RGWGetBucketStats_CB* ctx) {
  return 0;
}

int DaosBucket::sync_user_stats(const DoutPrefixProvider* dpp,
                                optional_yield y) {
  return 0;
}

int DaosBucket::update_container_stats(const DoutPrefixProvider* dpp) {
  return 0;
}

int DaosBucket::check_bucket_shards(const DoutPrefixProvider* dpp) { return 0; }

int DaosBucket::chown(const DoutPrefixProvider* dpp, User* new_user,
                      User* old_user, optional_yield y,
                      const std::string* marker) {
  /* XXX: Update policies of all the bucket->objects with new user */
  return 0;
}

/* Make sure to call load_bucket() if you need it first */
bool DaosBucket::is_owner(User* user) {
  return (info.owner.compare(user->get_id()) == 0);
}

int DaosBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y) {
  /* XXX: Check if bucket contains any objects */
  return 0;
}

int DaosBucket::check_quota(const DoutPrefixProvider* dpp,
                            RGWQuotaInfo& user_quota,
                            RGWQuotaInfo& bucket_quota, uint64_t obj_size,
                            optional_yield y, bool check_size_only) {
  /* Not Handled in the first pass as stats are also needed */
  return 0;
}

int DaosBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                      Attrs& new_attrs, optional_yield y) {
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;
  }

  return put_info(dpp, y, ceph::real_time());
}

int DaosBucket::try_refresh_info(const DoutPrefixProvider* dpp,
                                 ceph::real_time* pmtime) {
  return 0;
}

/* XXX: usage and stats not supported in the first pass */
int DaosBucket::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch, uint32_t max_entries,
                           bool* is_truncated, RGWUsageIter& usage_iter,
                           map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return 0;
}

int DaosBucket::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch) {
  return 0;
}

int DaosBucket::remove_objs_from_index(
    const DoutPrefixProvider* dpp,
    std::list<rgw_obj_index_key>& objs_to_unlink) {
  /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
   * Delete all the object in the list from the object table of this
   * bucket
   */
  return 0;
}

int DaosBucket::check_index(
    const DoutPrefixProvider* dpp,
    std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
    std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) {
  /* XXX: stats not supported yet */
  return 0;
}

int DaosBucket::rebuild_index(const DoutPrefixProvider* dpp) {
  /* there is no index table in DAOS. Not applicable */
  return 0;
}

int DaosBucket::set_tag_timeout(const DoutPrefixProvider* dpp,
                                uint64_t timeout) {
  /* XXX: CHECK: set tag timeout for all the bucket objects? */
  return 0;
}

int DaosBucket::purge_instance(const DoutPrefixProvider* dpp) {
  /* XXX: CHECK: for DAOS only single instance supported.
   * Remove all the objects for that instance? Anything extra needed?
   */
  return 0;
}

int DaosBucket::set_acl(const DoutPrefixProvider* dpp,
                        RGWAccessControlPolicy& acl, optional_yield y) {
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
  ldpp_dout(dpp, 20) << "DEBUG: list bucket=" << info.bucket.name
                     << " max=" << max << " params=" << params << dendl;

  // TODO: support the case when delim is not /
  if (params.delim != "/") {
    return -EINVAL;
  }

  // End
  if (max == 0) {
    return 0;
  }

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  size_t file_start = params.prefix.rfind(params.delim);
  string path = "";
  string prefix_rest = params.prefix;

  if (file_start != std::string::npos) {
    path = params.prefix.substr(0, file_start);
    prefix_rest = params.prefix.substr(file_start + params.delim.length());
  }

  dfs_obj_t* dir_obj;

  string lookup_path = "/" + path;
  ret =
      dfs_lookup(dfs, lookup_path.c_str(), O_RDWR, &dir_obj, nullptr, nullptr);

  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup lookup_path=" << lookup_path
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    return ret;
  }

  vector<struct dirent> dirents(max);
  // TODO handle bigger directories
  // TODO handle ordering
  daos_anchor_t anchor;
  daos_anchor_init(&anchor, 0);

  uint32_t nr = dirents.size();
  ret = dfs_readdir(dfs, dir_obj, &anchor, &nr, dirents.data());
  ldpp_dout(dpp, 20) << "DEBUG: dfs_readdir path=" << path << " nr=" << nr
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    dfs_release(dir_obj);
    return ret;
  }

  if (!daos_anchor_is_eof(&anchor)) {
    results.is_truncated = true;
  }

  for (uint32_t i = 0; i < nr; i++) {
    const auto& name = dirents[i].d_name;

    // Skip entries that do not start with prefix_rest
    // TODO handle how this affects max
    if (string(name).compare(0, prefix_rest.length(), prefix_rest) != 0) {
      continue;
    }

    dfs_obj_t* entry_obj;
    mode_t mode;
    ret = dfs_lookup_rel(dfs, dir_obj, name, O_RDWR | O_NOFOLLOW, &entry_obj,
                         &mode, nullptr);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel i=" << i << " entry=" << name
                       << " ret=" << ret << dendl;
    if (ret != 0) {
      dfs_release(dir_obj);
      return ret;
    }

    if (S_ISDIR(mode)) {
      // The entry is a directory, add to common prefix
      string key = path.empty() ? "" : path + params.delim;
      key += name + params.delim;
      results.common_prefixes[key] = true;

    } else if (S_ISREG(mode)) {
      // The entry is a regular file, read the xattr and add to objs
      vector<uint8_t> value(DFS_MAX_XATTR_LEN);
      size_t size = value.size();
      ret = dfs_getxattr(dfs, entry_obj, RGW_DIR_ENTRY_XATTR, value.data(),
                         &size);
      ldpp_dout(dpp, 20) << "DEBUG: dfs_getxattr entry=" << name
                         << " xattr=" << RGW_DIR_ENTRY_XATTR << dendl;
      // Skip if file has no dirent
      if (ret != 0) {
        ldpp_dout(dpp, 0) << "ERROR: no dirent, skipping entry=" << name
                          << dendl;
        dfs_release(entry_obj);
        continue;
      }

      bufferlist bl;
      rgw_bucket_dir_entry ent;
      bl.append(reinterpret_cast<char*>(value.data()), size);
      auto iter = bl.cbegin();
      ent.decode(iter);
      if (params.list_versions || ent.is_visible()) {
        results.objs.emplace_back(std::move(ent));
      }
    } else {
      // Skip other types
      ldpp_dout(dpp, 20) << "DEBUG: skipping entry=" << name << dendl;
    }

    // Close handles
    ret = dfs_release(entry_obj);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_release entry_obj ret=" << ret << dendl;
  }

  if (!params.allow_unordered) {
    std::sort(results.objs.begin(), results.objs.end(),
              compare_rgw_bucket_dir_entry);
  }

  ret = dfs_release(dir_obj);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_release dir_obj ret=" << ret << dendl;

  ret = close(dpp);

  return ret;
}

int DaosBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const string& prefix, string& marker,
    const string& delim, const int& max_uploads,
    vector<std::unique_ptr<MultipartUpload>>& uploads,
    map<string, bool>* common_prefixes, bool* is_truncated) {
  // TODO handle markers
  // TODO: handle more than max

  // End of uploading
  if (max_uploads == 0) {
    return 0;
  }

  int ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  dfs_obj_t* multipart_dir;
  ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                       get_name().c_str(), O_RDWR, &multipart_dir, nullptr,
                       nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel bucket=" << get_name()
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    dfs_release(multipart_dir);
    return ret;
  }

  vector<struct dirent> dirents(max_uploads);
  daos_anchor_t anchor;
  daos_anchor_init(&anchor, 0);

  uint32_t nr = dirents.size();
  ret =
      dfs_readdir(store->meta_dfs, multipart_dir, &anchor, &nr, dirents.data());
  ldpp_dout(dpp, 20) << "DEBUG: dfs_readdir bucket=" << get_name()
                     << " nr=" << nr << " ret=" << ret << dendl;
  if (ret != 0) {
    return ret;
  }

  if (is_truncated && !daos_anchor_is_eof(&anchor)) {
    *is_truncated = true;
  }

  for (uint32_t i = 0; i < nr; i++) {
    const auto& upload_id = dirents[i].d_name;

    // Open upload dir
    dfs_obj_t* upload_dir;
    ret = dfs_lookup_rel(store->meta_dfs, multipart_dir, upload_id, O_RDWR,
                         &upload_dir, nullptr, nullptr);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel i=" << i
                       << " upload_id=" << upload_id << " ret=" << ret << dendl;
    if (ret != 0) {
      return ret;
    }

    // Read the xattr
    vector<uint8_t> value(DFS_MAX_XATTR_LEN);
    size_t size = value.size();
    ret = dfs_getxattr(store->meta_dfs, upload_dir, RGW_DIR_ENTRY_XATTR,
                       value.data(), &size);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_getxattr upload_id=" << upload_id
                       << " xattr=" << RGW_DIR_ENTRY_XATTR << dendl;

    // Skip if file has no dirent
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: no dirent, skipping upload_id=" << upload_id
                        << dendl;
      ret = dfs_release(upload_dir);
      continue;
    }

    // Decode the xattr
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    bl.append(reinterpret_cast<char*>(value.data()), size);
    auto iter = bl.cbegin();
    ent.decode(iter);
    string name = ent.key.name;

    // Only add entries that start with prefix
    // TODO handle how this affects max
    if (name.compare(0, prefix.length(), prefix) == 0) {
      ACLOwner owner(rgw_user(ent.meta.owner));
      owner.set_name(ent.meta.owner_display_name);
      uploads.push_back(this->get_multipart_upload(
          name, upload_id, std::move(owner), ent.meta.mtime));

      // Add common prefixes
      if (common_prefixes && !delim.empty()) {
        // Name key has delim after the prefix
        const size_t delim_pos = name.find(delim, prefix.size());
        if (delim_pos != std::string::npos) {
          string prefix_key = name.substr(0, delim_pos + delim.length());
          (*common_prefixes)[prefix_key] = true;
        }
      }
    }

    // Close handles
    ret = dfs_release(upload_dir);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_release upload_dir ret=" << ret << dendl;
  }

  // Sort uploads
  std::sort(uploads.begin(), uploads.end(), compare_multipart_upload);

  ret = dfs_release(multipart_dir);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_release multipart_dir ret=" << ret << dendl;

  ret = close(dpp);

  return ret;
}

int DaosBucket::abort_multiparts(const DoutPrefixProvider* dpp,
                                 CephContext* cct) {
  return 0;
}

void DaosStore::finalize(void) {
  int ret;

  for (auto const& [dir_name, dir_obj] : dirs) {
    ret = dfs_release(dir_obj);
    ldout(cctx, 20) << "DEBUG: dfs_release name=" << dir_name << ", ret=" << ret
                    << dendl;
  }

  if (meta_dfs != nullptr) {
    ret = dfs_umount(meta_dfs);
    if (ret < 0) {
      ldout(cctx, 0) << "ERROR: dfs_umount() failed: " << ret << dendl;
    }
    meta_dfs = nullptr;
  }

  if (daos_handle_is_valid(meta_coh)) {
    ret = daos_cont_close(meta_coh, nullptr);
    if (ret < 0) {
      ldout(cctx, 0) << "ERROR: daos_cont_close() failed: " << ret << dendl;
    }
    meta_coh = DAOS_HDL_INVAL;
  }

  if (daos_handle_is_valid(poh)) {
    ret = daos_pool_disconnect(poh, nullptr);
    if (ret != 0) {
      ldout(cctx, 0) << "ERROR: daos_pool_disconnect() failed: " << ret
                     << dendl;
    }
    poh = DAOS_HDL_INVAL;
  }

  ret = daos_fini();
  if (ret != 0) {
    ldout(cctx, 0) << "ERROR: daos_fini() failed: " << ret << dendl;
  }
}

int DaosStore::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  int ret = daos_init();

  // DAOS init failed, allow the case where init is already done
  if (ret != 0 && ret != DER_ALREADY) {
    ldout(cct, 0) << "ERROR: daos_init() failed: " << ret << dendl;
    return ret;
  }

  // XXX: these params should be taken from config settings and
  // cct somehow?
  const auto& daos_pool = g_conf().get_val<std::string>("daos_pool");
  ldout(cct, 20) << "INFO: daos pool: " << daos_pool << dendl;
  daos_pool_info_t pool_info = {};
  ret = daos_pool_connect(daos_pool.c_str(), nullptr, DAOS_PC_RW, &poh,
                          &pool_info, nullptr);

  if (ret != 0) {
    ldout(cct, 0) << "ERROR: daos_pool_connect() failed: " << ret << dendl;
    return ret;
  }

  uuid_copy(pool, pool_info.pi_uuid);

  // Connect to metadata container
  // TODO use dfs_connect
  // Attempt to create
  ret = dfs_cont_create_with_label(poh, METADATA_BUCKET, nullptr, nullptr,
                                   &meta_coh, &meta_dfs);

  if (ret == 0) {
    // Create inner directories
    mode_t mode = DEFFILEMODE;
    for (auto& dir : METADATA_DIRS) {
      ret = dfs_mkdir(meta_dfs, nullptr, dir.c_str(), mode, 0);
      ldout(cct, 20) << "DEBUG: dfs_mkdir dir=" << dir << " ret=" << ret
                     << dendl;
      if (ret != 0 && ret != EEXIST) {
        ldout(cct, 0) << "ERROR: dfs_mkdir failed! ret=" << ret << dendl;
        return ret;
      }
    }
  } else if (ret == EEXIST) {
    // Metadata container exists, mount it
    ret = daos_cont_open(poh, METADATA_BUCKET, DAOS_COO_RW, &meta_coh, nullptr,
                         nullptr);

    if (ret != 0) {
      ldout(cct, 0) << "ERROR: daos_cont_open failed! ret=" << ret << dendl;
      return ret;
    }

    ret = dfs_mount(poh, meta_coh, O_RDWR, &meta_dfs);

    if (ret != 0) {
      ldout(cct, 0) << "ERROR: dfs_mount failed! ret=" << ret << dendl;
      return ret;
    }

  } else {
    ldout(cct, 0) << "ERROR: dfs_cont_create_with_label failed! ret=" << ret
                  << dendl;
    return ret;
  }

  // XXX: if we do this, do we need to keep everything else open?
  rgw_bucket b;
  b.name = METADATA_BUCKET;
  metadata_bucket = std::make_unique<DaosBucket>(this, b);

  // Open metadata dirs
  for (auto& dir : METADATA_DIRS) {
    dirs[dir] = nullptr;
    ret = dfs_lookup_rel(meta_dfs, nullptr, dir.c_str(), O_RDWR, &dirs[dir],
                         nullptr, nullptr);
    ldout(cct, 20) << "DEBUG: dfs_lookup_rel dir=" << dir << " ret=" << ret
                   << dendl;
    if (ret != 0) {
      ldout(cct, 0) << "ERROR: dfs_lookup_rel failed! ret=" << ret << dendl;
      return ret;
    }
  }

  return 0;
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

int DaosZoneGroup::get_placement_target_names(
    std::set<std::string>& names) const {
  for (const auto& target : group.placement_targets) {
    names.emplace(target.second.name);
  }

  return 0;
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

bool DaosZone::has_zonegroup_api(const std::string& api) const {
  return false;
}

const std::string& DaosZone::get_current_period_id() {
  return current_period->get_id();
}

std::unique_ptr<LuaScriptManager> DaosStore::get_lua_script_manager() {
  return std::make_unique<DaosLuaScriptManager>(this);
}

int DaosObject::get_obj_state(const DoutPrefixProvider* dpp,
                              RGWObjState** _state, optional_yield y,
                              bool follow_olh) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  rgw_bucket_dir_entry ent;
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

DaosObject::~DaosObject() {
  close(nullptr);
}

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
  Attrs updateattrs = setattrs ? attrs : *setattrs;
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
  if (ret != 0) {
    return ret;
  }
  return ret;
}

int DaosObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
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
  return 0;
}

int DaosObject::omap_get_all(const DoutPrefixProvider* dpp,
                             std::map<std::string, bufferlist>* m,
                             optional_yield y) {
  return 0;
}

int DaosObject::omap_get_vals_by_keys(const DoutPrefixProvider* dpp,
                                      const std::string& oid,
                                      const std::set<std::string>& keys,
                                      Attrs* vals) {
  return 0;
}

int DaosObject::omap_set_val_by_key(const DoutPrefixProvider* dpp,
                                    const std::string& key, bufferlist& val,
                                    bool must_exist, optional_yield y) {
  return 0;
}

MPSerializer* DaosObject::get_serializer(const DoutPrefixProvider* dpp,
                                         const std::string& lock_name) {
  return new MPDaosSerializer(dpp, store, this, lock_name);
}

int DaosObject::transition(Bucket* bucket,
                           const rgw_placement_rule& placement_rule,
                           const real_time& mtime, uint64_t olh_epoch,
                           const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

int DaosObject::transition_to_cloud(
    Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
    std::set<std::string>& cloud_targets, CephContext* cct, bool update_object,
    const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

bool DaosObject::placement_rules_match(rgw_placement_rule& r1,
                                       rgw_placement_rule& r2) {
  /* XXX: support single default zone and zonegroup for now */
  return true;
}

int DaosObject::dump_obj_layout(const DoutPrefixProvider* dpp, optional_yield y,
                                Formatter* f) {
  return 0;
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
    source->set_instance(LATEST_INSTANCE);
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
  ldpp_dout(dpp, 20) << "DaosReadOp::read(): sync read." << dendl;
  return 0;
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
                                         optional_yield y) {
  ldpp_dout(dpp, 20) << "DaosDeleteOp::delete_obj "
                     << source->get_key().to_str() << " from "
                     << source->get_bucket()->get_name() << dendl;
  // Open bucket
  int ret = 0;
  std::string path = source->get_key().to_str();
  DaosBucket* daos_bucket = source->get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  // Remove the daos object
  dfs_obj_t* parent = nullptr;
  std::string file_name = path;

  size_t file_start = path.rfind("/");

  if (file_start != std::string::npos) {
    // Open parent dir
    std::string parent_path = "/" + path.substr(0, file_start);
    file_name = path.substr(file_start + 1);
    ret = dfs_lookup(daos_bucket->dfs, parent_path.c_str(), O_RDWR, &parent,
                     nullptr, nullptr);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup parent_path=" << parent_path
                       << " ret=" << ret << dendl;
    if (ret != 0) {
      return ret;
    }
  }

  ret = dfs_remove(daos_bucket->dfs, parent, file_name.c_str(), false, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_remove file_name=" << file_name
                     << " ret=" << ret << dendl;

  // result.delete_marker = parent_op.result.delete_marker;
  // result.version_id = parent_op.result.version_id;

  // Finalize
  dfs_release(parent);
  daos_bucket->close(dpp);
  return ret;
}

int DaosObject::delete_object(const DoutPrefixProvider* dpp, optional_yield y,
                              bool prevent_versioning) {
  DaosObject::DaosDeleteOp del_op(this);
  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = bucket->get_info().versioning_status();

  return del_op.delete_obj(dpp, y);
}

int DaosObject::delete_obj_aio(const DoutPrefixProvider* dpp,
                               RGWObjState* astate, Completions* aio,
                               bool keep_index_consistent, optional_yield y) {
  /* XXX: Make it async */
  return 0;
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
  return 0;
}

int DaosObject::swift_versioning_restore(bool& restored,
                                         const DoutPrefixProvider* dpp) {
  return 0;
}

int DaosObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  return 0;
}

int DaosObject::lookup(const DoutPrefixProvider* dpp, mode_t* mode) {
  if (is_open()) {
    return 0;
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  // TODO: cache open file handles
  std::string path = get_key().to_str();
  if (path.front() != '/') path = "/" + path;
  ret = dfs_lookup(daos_bucket->dfs, path.c_str(), O_RDWR, &dfs_obj, mode,
                   nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup path=" << path << " ret=" << ret
                     << dendl;

  if (ret != 0) {
    size_t suffix_start = path.rfind(LATEST_INSTANCE_SUFFIX);
    if (suffix_start != std::string::npos) {
      // If we are trying to access the latest version, try accessing key with
      // null instance since it is likely that the bucket did not have
      // versioning before
      path = path.substr(0, suffix_start);
      ret = dfs_lookup(daos_bucket->dfs, path.c_str(), O_RDWR, &dfs_obj, mode,
                       nullptr);
      ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup path=" << path << " ret=" << ret
                         << dendl;
    }
  }

  if (ret == 0) {
    _is_open = true;
  } else if (ret == ENOENT) {
    ldpp_dout(dpp, 20) << "DEBUG: daos object (" << get_bucket()->get_name()
                       << ", " << get_key().to_str()
                       << ") does not exist: ret=" << ret << dendl;
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to open daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::create(const DoutPrefixProvider* dpp, const bool create_parents,
                       const string link_to) {
  if (is_open()) {
    return 0;
  }

  // TODO: cache open file handles
  dfs_obj_t* parent = nullptr;
  fs::path path = get_key().to_str();
  fs::path file_name = path.filename();
  fs::path parent_path = path.parent_path();
  mode_t mode = DEFFILEMODE;

  // Disallow creating a file with the instance = latest, since it is supposed
  // to be a link, not a writeable file
  size_t suffix_pos = path.string().rfind(LATEST_INSTANCE_SUFFIX);
  if (suffix_pos != std::string::npos && link_to.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: creating an object that ends with "
                      << LATEST_INSTANCE_SUFFIX
                      << " is not allowed unless it is a link" << dendl;
    return -EINVAL;
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  if (!parent_path.empty()) {
    // Recursively open parent directories
    vector<string> dirs;
    dfs_obj_t* dir_obj;

    for (const auto& dir : parent_path) {
      if (create_parents) {
        // Create directory
        ret = dfs_mkdir(daos_bucket->dfs, parent, dir.c_str(), mode, 0);
        ldpp_dout(dpp, 20) << "DEBUG: dfs_mkdir dir=" << dir << " ret=" << ret
                           << dendl;
        if (ret != 0 && ret != EEXIST) {
          if (parent) {
            dfs_release(parent);
          }
          return ret;
        }
      }

      // Open directory
      ret = dfs_lookup_rel(daos_bucket->dfs, parent, dir.c_str(), O_RDWR,
                           &dir_obj, nullptr, nullptr);
      ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel dir=" << dir
                         << " ret=" << ret << dendl;
      if (parent) {
        dfs_release(parent);
      }
      if (ret != 0) {
        return ret;
      }
      parent = dir_obj;
    }
  }

  // Create links if requested
  const char* link_to_c = nullptr;
  if (!link_to.empty()) {
    mode |= S_IFLNK;
    link_to_c = link_to.c_str();

    // Remove any existing symlinks since O_TRUNC is not handled
    ret =
        dfs_remove(daos_bucket->dfs, parent, file_name.c_str(), false, nullptr);
  } else {
    mode |= S_IFREG;
  }

  // Finally create the file
  ret = dfs_open(daos_bucket->dfs, parent, file_name.c_str(), mode,
                 O_RDWR | O_CREAT | O_TRUNC, 0, 0, link_to_c, &dfs_obj);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_open file_name=" << file_name
                     << " ret=" << ret << dendl;
  if (parent) {
    ret = dfs_release(parent);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_release ret=" << ret << dendl;
  }

  if (ret == 0 || ret == EEXIST) {
    ret = 0;
    _is_open = true;
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to open daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::close(const DoutPrefixProvider* dpp) {
  if (!is_open()) {
    return 0;
  }

  int ret = dfs_release(dfs_obj);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_release ret=" << ret << dendl;

  if (ret == 0) {
    _is_open = false;
  }
  return ret;
}

int DaosObject::write(const DoutPrefixProvider* dpp, bufferlist&& data,
                      uint64_t offset) {
  d_sg_list_t wsgl;
  d_iov_t iov;
  d_iov_set(&iov, data.c_str(), data.length());
  wsgl.sg_nr = 1;
  wsgl.sg_iovs = &iov;
  int ret = dfs_write(get_daos_bucket()->dfs, dfs_obj, &wsgl, offset, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to write into daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::read(const DoutPrefixProvider* dpp, bufferlist& data,
                     uint64_t offset, uint64_t& size) {
  d_iov_t iov;
  d_iov_set(&iov, data.append_hole(size).c_str(), size);

  d_sg_list_t rsgl;
  rsgl.sg_nr = 1;
  rsgl.sg_iovs = &iov;
  rsgl.sg_nr_out = 1;
  int ret =
      dfs_read(get_daos_bucket()->dfs, dfs_obj, &rsgl, offset, &size, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read from daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

// Get the object's dirent and attrs
int DaosObject::get_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent, Attrs* getattrs,
                                    multipart_upload_info* upload_info) {
  int ret = lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  vector<uint8_t> value(DFS_MAX_XATTR_LEN);
  size_t size = value.size();
  ret = dfs_getxattr(get_daos_bucket()->dfs, dfs_obj, RGW_DIR_ENTRY_XATTR,
                     value.data(), &size);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get dirent of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket_dir_entry dummy_ent;
  if (!ent) {
    // if ent is not passed, use a dummy ent
    ent = &dummy_ent;
  }

  Attrs dummy_attrs;
  if (!getattrs) {
    // if ent is not passed, use a dummy ent
    getattrs = &dummy_attrs;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<char*>(value.data()), size);
  auto iter = bl.cbegin();
  ent->decode(iter);
  decode(*getattrs, iter);
  if (upload_info) {
    decode(*upload_info, iter);
  }

  return ret;
}
// Set the object's dirent and attrs
int DaosObject::set_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent, Attrs* setattrs,
                                    multipart_upload_info* upload_info) {
  int ret = lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Set defaults
  rgw_bucket_dir_entry dummy_ent;
  if (!ent) {
    // if ent is not passed, return an error
    return -EINVAL;
  }

  if (!setattrs) {
    // if setattrs is not passed, use object attrs
    setattrs = &attrs;
  }

  multipart_upload_info dummy_upload_info;
  if (!upload_info) {
    // if upload_info is not passed, use dummy
    upload_info = &dummy_upload_info;
  }

  bufferlist wbl;
  ent->encode(wbl);
  encode(*setattrs, wbl);
  encode(*upload_info, wbl);

  // Write rgw_bucket_dir_entry into object xattr
  ret = dfs_setxattr(get_daos_bucket()->dfs, dfs_obj, RGW_DIR_ENTRY_XATTR,
                     wbl.c_str(), wbl.length(), 0);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set dirent of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().to_str()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::mark_as_latest(const DoutPrefixProvider* dpp,
                               ceph::real_time set_mtime) {
  // TODO handle deletion
  // TODO understand race conditions

  // Get latest version so far
  std::unique_ptr<DaosObject> latest_object = std::make_unique<DaosObject>(
      store, rgw_obj_key(get_name(), LATEST_INSTANCE), get_bucket());

  ldpp_dout(dpp, 20) << __func__ << ": key=" << get_key().to_str()
                     << " latest_object_key= "
                     << latest_object->get_key().to_str() << dendl;

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
  std::unique_ptr<DaosObject> latest_link = std::make_unique<DaosObject>(
      store, rgw_obj_key(get_name(), LATEST_INSTANCE), get_bucket());
  fs::path path = get_key().to_str();
  ret = latest_link->create(dpp, false, path.filename().string());

  // TODO Update an xattr with a list to all the version ids, ordered by
  // creation to handle deletion
  return ret;
}

DaosAtomicWriter::DaosAtomicWriter(
    const DoutPrefixProvider* dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, DaosStore* _store,
    const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
    uint64_t _olh_epoch, const std::string& _unique_tag)
    : Writer(dpp, y),
      store(_store),
      owner(_owner),
      ptail_placement_rule(_ptail_placement_rule),
      olh_epoch(_olh_epoch),
      unique_tag(_unique_tag),
      obj(_store, _head_obj->get_key(), _head_obj->get_bucket()) {}

int DaosAtomicWriter::prepare(optional_yield y) {
  int ret = obj.create(dpp);
  return ret;
}

// TODO: Handle concurrent writes, a unique object id is a possible solution, or
// use DAOS transactions
// XXX: Do we need to accumulate writes as motr does?
int DaosAtomicWriter::process(bufferlist&& data, uint64_t offset) {
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
    optional_yield y) {
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
  bool is_versioned = obj.have_instance();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ldpp_dout(dpp, 20) << __func__ << ": key=" << obj.get_key().to_str()
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

  obj.close(dpp);
  return ret;
}

int DaosMultipartUpload::abort(const DoutPrefixProvider* dpp,
                               CephContext* cct) {
  // Remove upload from bucket multipart index
  dfs_obj_t* multipart_dir;
  int ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                           bucket->get_name().c_str(), O_RDWR, &multipart_dir,
                           nullptr, nullptr);
  ret = dfs_remove(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                   true, nullptr);
  dfs_release(multipart_dir);
  return ret;
}

static string mp_ns = RGW_OBJ_NS_MULTIPART;

std::unique_ptr<rgw::sal::Object> DaosMultipartUpload::get_meta_obj() {
  // TODO fix this so it refers to the upload_dir
  return bucket->get_object(rgw_obj_key(get_meta(), string(), mp_ns));
}

int DaosMultipartUpload::init(const DoutPrefixProvider* dpp, optional_yield y,
                              ACLOwner& _owner,
                              rgw_placement_rule& dest_placement,
                              rgw::sal::Attrs& attrs) {
  int ret;
  std::string oid = mp_obj.get_key();
  dfs_obj_t* multipart_dir;
  ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                       bucket->get_name().c_str(), O_RDWR, &multipart_dir,
                       nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel multipart dir for bucket="
                     << bucket->get_name() << " ret=" << ret << dendl;

  do {
    char buf[33];
    gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
    std::string upload_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
    upload_id.append(buf);

    mp_obj.init(oid, upload_id);

    // Create meta index
    ret = dfs_mkdir(store->meta_dfs, multipart_dir, upload_id.c_str(),
                    DEFFILEMODE, 0);
  } while (ret == EEXIST);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create multipart upload dir ("
                      << bucket->get_name() << "/" << get_upload_id()
                      << "): ret=" << ret << dendl;
    dfs_release(multipart_dir);
    return ret;
  }

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

  // Insert an entry into bucket multipart index
  dfs_obj_t* upload_dir;
  ret = dfs_lookup_rel(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                       O_RDWR, &upload_dir, nullptr, nullptr);

  ret = dfs_setxattr(store->meta_dfs, upload_dir, RGW_DIR_ENTRY_XATTR,
                     bl.c_str(), bl.length(), 0);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set xattr of multipart upload dir ("
                      << bucket->get_name() << "/" << get_upload_id()
                      << "): ret=" << ret << dendl;
    dfs_release(multipart_dir);
    return ret;
  }

  dfs_release(upload_dir);
  dfs_release(multipart_dir);
  return ret;
}

int DaosMultipartUpload::list_parts(const DoutPrefixProvider* dpp,
                                    CephContext* cct, int num_parts, int marker,
                                    int* next_marker, bool* truncated,
                                    bool assume_unsorted) {
  dfs_obj_t* multipart_dir;
  dfs_obj_t* upload_dir;
  int ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                           bucket->get_name().c_str(), O_RDWR, &multipart_dir,
                           nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << bucket->get_name()
                     << " ret=" << ret << dendl;
  ret = dfs_lookup_rel(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                       O_RDWR, &upload_dir, nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << get_upload_id()
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    if (ret == ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    dfs_release(multipart_dir);
    return ret;
  }

  vector<struct dirent> dirents(MULTIPART_MAX_PARTS);
  daos_anchor_t anchor;
  daos_anchor_init(&anchor, 0);

  uint32_t nr = dirents.size();
  ret = dfs_readdir(store->meta_dfs, upload_dir, &anchor, &nr, dirents.data());
  ldpp_dout(dpp, 20) << "DEBUG: dfs_readdir name=" << get_upload_id()
                     << " nr=" << nr << " ret=" << ret << dendl;
  if (ret != 0) {
    dfs_release(upload_dir);
    dfs_release(multipart_dir);
    return ret;
  }

  for (uint32_t i = 0; i < nr; i++) {
    const auto& part_name = dirents[i].d_name;
    std::string err;
    const int part_num = strict_strtol(part_name, 10, &err);
    if (!err.empty()) {
      ldpp_dout(dpp, 10) << "bad part number: " << part_name << ": " << err
                         << dendl;
      dfs_release(upload_dir);
      dfs_release(multipart_dir);
      return -EINVAL;
    }

    // Skip entries that are not larger than marker
    if (part_num <= marker) {
      continue;
    }

    dfs_obj_t* part_obj;
    ret = dfs_lookup_rel(store->meta_dfs, upload_dir, part_name, O_RDWR,
                         &part_obj, nullptr, nullptr);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel i=" << i
                       << " entry=" << part_name << " ret=" << ret << dendl;
    if (ret != 0) {
      return ret;
    }

    // The entry is a regular file, read the xattr and add to objs
    vector<uint8_t> value(DFS_MAX_XATTR_LEN);
    size_t size = value.size();
    ret = dfs_getxattr(store->meta_dfs, part_obj, RGW_PART_XATTR, value.data(),
                       &size);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_getxattr entry=" << part_name
                       << " xattr=" << RGW_PART_XATTR << dendl;
    // Skip if the part has no info
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: no part info, skipping part=" << part_name
                        << dendl;
      ret = dfs_release(part_obj);
      continue;
    }

    bufferlist bl;
    bl.append(reinterpret_cast<char*>(value.data()), size);

    std::unique_ptr<DaosMultipartPart> part =
        std::make_unique<DaosMultipartPart>();
    auto iter = bl.cbegin();
    decode(part->info, iter);
    parts[part->info.num] = std::move(part);

    // Close handles
    ret = dfs_release(part_obj);
    ldpp_dout(dpp, 20) << "DEBUG: dfs_release part_obj ret=" << ret << dendl;
  }

  // rebuild a map with only num_parts entries
  int last_num = 0;
  std::map<uint32_t, std::unique_ptr<MultipartPart>> new_parts;
  std::map<uint32_t, std::unique_ptr<MultipartPart>>::iterator piter;
  int i;
  for (i = 0, piter = parts.begin(); i < num_parts && piter != parts.end();
       ++i, ++piter) {
    last_num = piter->first;
    new_parts[piter->first] = std::move(piter->second);
  }

  if (truncated) {
    *truncated = (piter != parts.end());
  }

  parts.swap(new_parts);

  if (next_marker) {
    *next_marker = last_num;
  }

  ret = dfs_release(upload_dir);
  ret = dfs_release(multipart_dir);
  return ret;
}

// Heavily copied from rgw_sal_rados.cc
int DaosMultipartUpload::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    map<int, string>& part_etags, list<rgw_obj_index_key>& remove_objs,
    uint64_t& accounted_size, bool& compressed, RGWCompressionInfo& cs_info,
    off_t& off, std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
    rgw::sal::Object* target_obj) {
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
    if (ret < 0) return ret;

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
      src_obj.init_ns(bucket->get_key(), oid, mp_ns);

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
  // TODO reduce redundant code
  // TODO handle errors
  // Read the object's multipart info
  dfs_obj_t* multipart_dir;
  dfs_obj_t* upload_dir;
  ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                       bucket->get_name().c_str(), O_RDWR, &multipart_dir,
                       nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << bucket->get_name()
                     << " ret=" << ret << dendl;
  ret = dfs_lookup_rel(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                       O_RDWR, &upload_dir, nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << get_upload_id()
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    if (ret == ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    dfs_release(multipart_dir);
    return ret;
  }

  vector<uint8_t> value(DFS_MAX_XATTR_LEN);
  size_t size = value.size();
  ret = dfs_getxattr(store->meta_dfs, upload_dir, RGW_DIR_ENTRY_XATTR,
                     value.data(), &size);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_getxattr entry=" << bucket->get_name()
                     << "/" << get_upload_id()
                     << " xattr=" << RGW_DIR_ENTRY_XATTR << dendl;
  dfs_release(upload_dir);

  rgw_bucket_dir_entry ent;
  bufferlist bl;
  bl.append(reinterpret_cast<char*>(value.data()), size);
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
  bool is_versioned = target_obj->have_instance();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ent.meta.etag = etag;

  // Open object
  DaosObject* obj = static_cast<DaosObject*>(target_obj);
  ret = obj->create(dpp);
  if (ret != 0) {
    dfs_release(multipart_dir);
    return ret;
  }

  // Copy data from parts to object
  uint64_t write_off = 0;
  for (auto const& [part_num, part] : get_parts()) {
    // TODO DRY
    std::unique_ptr<DaosObject> part_obj =
        get_daos_bucket()->get_part_object(get_upload_id(), part_num);
    ret = part_obj->lookup(dpp);
    if (ret != 0) {
      obj->close(dpp);
      dfs_release(multipart_dir);
      return ret;
    }

    // Reserve buffers and read
    uint64_t size = part->get_size();
    bufferlist bl;
    ret = part_obj->read(dpp, bl, 0, size);
    if (ret != 0) {
      part_obj->close(dpp);
      obj->close(dpp);
      dfs_release(multipart_dir);
      return ret;
    }

    // write to obj
    obj->write(dpp, std::move(bl), write_off);
    part_obj->close(dpp);
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

  obj->close(dpp);

  // Remove upload from bucket multipart index
  ret = dfs_remove(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                   true, nullptr);
  dfs_release(multipart_dir);
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

  dfs_obj_t* multipart_dir;
  dfs_obj_t* upload_dir;
  int ret = dfs_lookup_rel(store->meta_dfs, store->dirs[MULTIPART_DIR],
                           bucket->get_name().c_str(), O_RDWR, &multipart_dir,
                           nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << bucket->get_name()
                     << " ret=" << ret << dendl;
  ret = dfs_lookup_rel(store->meta_dfs, multipart_dir, get_upload_id().c_str(),
                       O_RDWR, &upload_dir, nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel entry=" << get_upload_id()
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    if (ret == ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    dfs_release(multipart_dir);
    return ret;
  }

  vector<uint8_t> value(DFS_MAX_XATTR_LEN);
  size_t size = value.size();
  ret = dfs_getxattr(store->meta_dfs, upload_dir, RGW_DIR_ENTRY_XATTR,
                     value.data(), &size);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_getxattr entry=" << bucket->get_name()
                     << "/" << get_upload_id()
                     << " xattr=" << RGW_DIR_ENTRY_XATTR << " ret=" << ret
                     << dendl;
  dfs_release(upload_dir);
  dfs_release(multipart_dir);

  multipart_upload_info upload_info;
  rgw_bucket_dir_entry ent;
  Attrs decoded_attrs;
  bufferlist bl;
  bl.append(reinterpret_cast<char*>(value.data()), size);
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
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
    const std::string& part_num_str) {
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::get_writer(): enter part="
                     << part_num << " head_obj=" << _head_obj << dendl;
  return std::make_unique<DaosMultipartWriter>(
      dpp, y, this, std::move(_head_obj), store, owner, ptail_placement_rule,
      part_num, part_num_str);
}

int DaosMultipartWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::prepare(): enter part="
                     << part_num_str << dendl;

  part_obj = get_daos_bucket()->get_part_object(upload_id, part_num);
  // XXX: we should just create the file, and not the whole path
  int ret = part_obj->create(dpp, false);
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_UPLOAD;
  }
  return ret;
}

DaosBucket* DaosMultipartWriter::get_daos_bucket() {
  return static_cast<DaosMultipartUpload*>(upload)->get_daos_bucket();
}

int DaosMultipartWriter::process(bufferlist&& data, uint64_t offset) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::process(): enter part="
                     << part_num_str << " offset=" << offset << dendl;
  if (data.length() == 0) {
    return 0;
  }

  int ret = 0;
  if (!part_obj->is_open()) {
    ret = part_obj->lookup(dpp);
    if (ret != 0) {
      return ret;
    }
  }

  // XXX: Combine multiple streams into one as motr does
  uint64_t data_size = data.length();
  ret = part_obj->write(dpp, std::move(data), offset);
  if (ret == 0) {
    actual_part_size += data_size;
  }
  return ret;
}

int DaosMultipartWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y) {
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
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "cannot get compression info" << dendl;
    part_obj->close(dpp);
    return ret;
  }
  encode(info, bl);
  encode(attrs, bl);
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): entry size"
                     << bl.length() << dendl;

  ret = dfs_setxattr(store->meta_dfs, part_obj->dfs_obj, RGW_PART_XATTR,
                     bl.c_str(), bl.length(), 0);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set xattr part (" << upload_id << "/"
                      << part_num_str << "): ret=" << ret << dendl;
    if (ret == ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
  }

  part_obj->close(dpp);
  return ret;
}

std::unique_ptr<RGWRole> DaosStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags) {
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
  return 0;
}

std::unique_ptr<RGWOIDCProvider> DaosStore::get_oidc_provider() {
  RGWOIDCProvider* p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int DaosStore::get_oidc_providers(
    const DoutPrefixProvider* dpp, const std::string& tenant,
    vector<std::unique_ptr<RGWOIDCProvider>>& providers) {
  return 0;
}

std::unique_ptr<MultipartUpload> DaosBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime) {
  return std::make_unique<DaosMultipartUpload>(store, this, oid, upload_id,
                                               owner, mtime);
}

std::unique_ptr<Writer> DaosStore::get_append_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag, uint64_t position,
    uint64_t* cur_accounted_size) {
  return nullptr;
}

std::unique_ptr<Writer> DaosStore::get_atomic_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
    const std::string& unique_tag) {
  return std::make_unique<DaosAtomicWriter>(dpp, y, std::move(_head_obj), this,
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

int DaosStore::read_user(const DoutPrefixProvider* dpp, std::string parent,
                         std::string name, DaosUserInfo* duinfo) {
  // Open file
  dfs_obj_t* user_obj;
  int ret = dfs_lookup_rel(meta_dfs, dirs[parent], name.c_str(), O_RDWR,
                           &user_obj, nullptr, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: dfs_lookup_rel parent=" << parent
                     << " name=" << name << " ret=" << ret << dendl;
  if (ret != 0) {
    return -ENOENT;
  }

  // Reserve buffers
  uint64_t size = DFS_MAX_XATTR_LEN;
  bufferlist bl;
  d_iov_t iov;
  d_sg_list_t rsgl;
  d_iov_set(&iov, bl.append_hole(size).c_str(), size);
  rsgl.sg_nr = 1;
  rsgl.sg_iovs = &iov;
  rsgl.sg_nr_out = 1;

  // Read file
  uint64_t actual;
  ret = dfs_read(meta_dfs, user_obj, &rsgl, 0, &actual, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read user file, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Close file
  ret = dfs_release(user_obj);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: dfs_release failed, user name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo->decode(iter);
  return 0;
}

std::unique_ptr<User> DaosStore::get_user(const rgw_user& u) {
  ldout(cctx, 20) << "DEBUG: bucket's user:  " << u.to_str() << dendl;
  return std::make_unique<DaosUser>(this, u);
}

int DaosStore::get_user_by_access_key(const DoutPrefixProvider* dpp,
                                      const std::string& key, optional_yield y,
                                      std::unique_ptr<User>* user) {
  DaosUserInfo duinfo;
  int ret = read_user(dpp, ACCESS_KEYS_DIR, key, &duinfo);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: get_user_by_access_key failed, key=" << key
                      << dendl;
    return ret;
  }

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
  DaosUserInfo duinfo;
  int ret = read_user(dpp, EMAILS_DIR, email, &duinfo);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: get_user_by_email failed, email=" << email
                      << dendl;
    return ret;
  }

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
  return 0;
}

std::unique_ptr<Object> DaosStore::get_object(const rgw_obj_key& k) {
  return std::make_unique<DaosObject>(this, k);
}

int DaosStore::get_bucket(const DoutPrefixProvider* dpp, User* u,
                          const rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
                          optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: get_bucket1: User" << u->get_id() << dendl;
  int ret;
  Bucket* bp;

  bp = new DaosBucket(this, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret < 0) {
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
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

DaosBucket* DaosStore::get_metadata_bucket() { return metadata_bucket.get(); }

bool DaosStore::is_meta_master() { return true; }

int DaosStore::forward_request_to_master(const DoutPrefixProvider* dpp,
                                         User* user, obj_version* objv,
                                         bufferlist& in_data, JSONParser* jp,
                                         req_info& info, optional_yield y) {
  return 0;
}

std::string DaosStore::zone_unique_id(uint64_t unique_num) { return ""; }

std::string DaosStore::zone_unique_trans_id(const uint64_t unique_num) {
  return "";
}

int DaosStore::cluster_stat(RGWClusterStat& stats) { return 0; }

std::unique_ptr<Lifecycle> DaosStore::get_lifecycle(void) { return 0; }

std::unique_ptr<Completions> DaosStore::get_completions(void) { return 0; }

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
  return std::make_unique<DaosNotification>(obj, src_obj, event_type);
}

int DaosStore::log_usage(const DoutPrefixProvider* dpp,
                         map<rgw_user_bucket, RGWUsageBatch>& usage_info) {
  return 0;
}

int DaosStore::log_op(const DoutPrefixProvider* dpp, string& oid,
                      bufferlist& bl) {
  return 0;
}

int DaosStore::register_to_service_map(const DoutPrefixProvider* dpp,
                                       const string& daemon_type,
                                       const map<string, string>& meta) {
  return 0;
}

void DaosStore::get_quota(RGWQuotaInfo& bucket_quota,
                          RGWQuotaInfo& user_quota) {
  // XXX: Not handled for the first pass
  return;
}

void DaosStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                              RGWRateLimitInfo& user_ratelimit,
                              RGWRateLimitInfo& anon_ratelimit) {
  return;
}

int DaosStore::set_buckets_enabled(const DoutPrefixProvider* dpp,
                                   vector<rgw_bucket>& buckets, bool enabled) {
  return 0;
}

int DaosStore::get_sync_policy_handler(const DoutPrefixProvider* dpp,
                                       std::optional<rgw_zone_id> zone,
                                       std::optional<rgw_bucket> bucket,
                                       RGWBucketSyncPolicyHandlerRef* phandler,
                                       optional_yield y) {
  return 0;
}

RGWDataSyncStatusManager* DaosStore::get_data_sync_manager(
    const rgw_zone_id& source_zone) {
  return 0;
}

int DaosStore::read_all_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return 0;
}

int DaosStore::trim_all_usage(const DoutPrefixProvider* dpp,
                              uint64_t start_epoch, uint64_t end_epoch) {
  return 0;
}

int DaosStore::get_config_key_val(string name, bufferlist* bl) { return 0; }

int DaosStore::meta_list_keys_init(const DoutPrefixProvider* dpp,
                                   const string& section, const string& marker,
                                   void** phandle) {
  return 0;
}

int DaosStore::meta_list_keys_next(const DoutPrefixProvider* dpp, void* handle,
                                   int max, list<string>& keys,
                                   bool* truncated) {
  return 0;
}

void DaosStore::meta_list_keys_complete(void* handle) { return; }

std::string DaosStore::meta_get_marker(void* handle) { return ""; }

int DaosStore::meta_remove(const DoutPrefixProvider* dpp, string& metadata_key,
                           optional_yield y) {
  return 0;
}

std::string DaosStore::get_cluster_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  return "";
}

}  // namespace rgw::sal

extern "C" {

void* newDaosStore(CephContext* cct) {
  int ret = -1;
  rgw::sal::DaosStore* store = new rgw::sal::DaosStore(cct);

  if (store) {
    ret = store->initialize(cct, nullptr);
    if (ret != 0) {
      ldout(cct, 0) << "ERROR: store->initialize() failed: " << ret << dendl;
      store->finalize();
      delete store;
      return nullptr;
    }
  }
  return store;
}
}
