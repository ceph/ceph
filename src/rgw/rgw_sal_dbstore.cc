// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_dbstore.h"
#include "rgw_bucket.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

  int DBUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
      const string& end_marker, uint64_t max, bool need_stats,
      BucketList &buckets, optional_yield y)
  {
    RGWUserBuckets ulist;
    bool is_truncated = false;
    int ret;

    buckets.clear();
    ret = store->getDBStore()->list_buckets(info.user_id, marker, end_marker, max,
        need_stats, &ulist, &is_truncated);
    if (ret < 0)
      return ret;

    buckets.set_truncated(is_truncated);
    for (const auto& ent : ulist.get_buckets()) {
      buckets.add(std::unique_ptr<Bucket>(new DBBucket(this->store, ent.second, this)));
    }

    return 0;
  }

  Bucket* DBUser::create_bucket(rgw_bucket& bucket,
      ceph::real_time creation_time)
  {
    return NULL;
  }

  int DBUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
  {
    int ret;
    ret = store->getDBStore()->get_user(string("user_id"), "", info, &attrs,
        &objv_tracker);
    return ret;
  }

  int DBUser::read_stats(const DoutPrefixProvider *dpp,
      optional_yield y, RGWStorageStats* stats,
      ceph::real_time *last_stats_sync,
      ceph::real_time *last_stats_update)
  {
    return 0;
  }

  /* stats - Not for first pass */
  int DBUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB *cb)
  {
    return 0;
  }

  int DBUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int DBUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
      bool *is_truncated, RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int DBUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int DBUser::load_user(const DoutPrefixProvider *dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDBStore()->get_user(string("user_id"), "", info, &attrs,
        &objv_tracker);

    return ret;
  }

  int DBUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
  {
    int ret = 0;

    ret = store->getDBStore()->store_user(info, exclusive, &attrs, &objv_tracker, old_info);

    return ret;
  }

  int DBUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDBStore()->remove_user(info, &objv_tracker);

    return ret;
  }

  Object *DBBucket::create_object(const rgw_obj_key &key)
  {
    return nullptr;
  }

  int DBBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y)
  {
    int ret;

    ret = get_bucket_info(dpp, y);
    if (ret < 0)
      return ret;

    /* XXX: handle delete_children */

    ret = store->getDBStore()->remove_bucket(info);

    return ret;
  }

  int DBBucket::get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDBStore()->get_bucket_info(string("name"), "", info, &attrs,
        &mtime, &bucket_version);

    return ret;
  }

  /* stats - Not for first pass */
  int DBBucket::get_bucket_stats(const DoutPrefixProvider *dpp, int shard_id,
      std::string *bucket_ver, std::string *master_ver,
      std::map<RGWObjCategory, RGWStorageStats>& stats,
      std::string *max_marker, bool *syncstopped)
  {
    return 0;
  }

  int DBBucket::get_bucket_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB *ctx)
  {
    return 0;
  }

  int DBBucket::read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int DBBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int DBBucket::update_container_stats(const DoutPrefixProvider *dpp)
  {
    return 0;
  }

  int DBBucket::check_bucket_shards(const DoutPrefixProvider *dpp)
  {
    return 0;
  }

  int DBBucket::chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker)
  {
    int ret;

    ret = store->getDBStore()->update_bucket("owner", info, false, &(new_user->get_id()), nullptr, nullptr, nullptr);

    /* XXX: Update policies of all the bucket->objects with new user */
    return ret;
  }

  int DBBucket::put_instance_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
  {
    int ret;

    ret = store->getDBStore()->update_bucket("info", info, exclusive, nullptr, nullptr, &_mtime, &info.objv_tracker);

    return ret;

  }

  int DBBucket::remove_metadata(const DoutPrefixProvider* dpp, RGWObjVersionTracker* objv, optional_yield y)
  {
    /* XXX: same as DBBUcket::remove_bucket() but should return error if there are objects
     * in that bucket. */

    int ret = store->getDBStore()->remove_bucket(info);

    return ret;
  }

  /* Make sure to call get_bucket_info() if you need it first */
  bool DBBucket::is_owner(User* user)
  {
    return (info.owner.compare(user->get_id()) == 0);
  }

  int DBBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* XXX: Check if bucket contains any objects */
    return 0;
  }

  int DBBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
      optional_yield y, bool check_size_only)
  {
    /* Not Handled in the first pass as stats are also needed */
    return 0;
  }

  int DBBucket::set_instance_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y)
  {
    int ret = 0;

    /* XXX: handle has_instance_obj like in set_bucket_instance_attrs() */

    ret = store->getDBStore()->update_bucket("attrs", info, false, nullptr, &attrs, nullptr, &get_info().objv_tracker);

    return ret;
  }

  int DBBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime)
  {
    int ret = 0;

    ret = store->getDBStore()->get_bucket_info(string("name"), "", info, &attrs,
        pmtime, &bucket_version);

    return ret;
  }

  /* XXX: usage and stats not supported in the first pass */
  int DBBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int DBBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int DBBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
  {
    /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
     * Delete all the object in the list from the object table of this
     * bucket
     */
    return 0;
  }

  int DBBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
  {
    /* XXX: stats not supported yet */
    return 0;
  }

  int DBBucket::rebuild_index(const DoutPrefixProvider *dpp)
  {
    /* there is no index table in dbstore. Not applicable */
    return 0;
  }

  int DBBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
  {
    /* XXX: CHECK: set tag timeout for all the bucket objects? */
    return 0;
  }

  int DBBucket::purge_instance(const DoutPrefixProvider *dpp)
  {
    /* XXX: CHECK: for dbstore only single instance supported.
     * Remove all the objects for that instance? Anything extra needed?
     */
    return 0;
  }

  int DBBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
  {
    int ret = 0;
    bufferlist aclbl;

    acls = acl;
    acl.encode(aclbl);

    Attrs attrs = get_attrs();
    attrs[RGW_ATTR_ACL] = aclbl;

    ret = store->getDBStore()->update_bucket("attrs", info, false, &(acl.get_owner().get_id()), &attrs, nullptr, nullptr);

    return ret;
  }

  std::unique_ptr<Object> DBBucket::get_object(const rgw_obj_key& k)
  {
    return nullptr;
  }

  int DBBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
  {
    /* XXX: Objects */
    return 0;
  }

  void RGWDBStore::finalize(void)
  {
    if (dbsm)
      dbsm->destroyAllHandles();
  }

  const RGWZoneGroup& DBZone::get_zonegroup()
  {
    return *zonegroup;
  }

  int DBZone::get_zonegroup(const std::string& id, RGWZoneGroup& zg)
  {
    /* XXX: for now only one zonegroup supported */
    zg = *zonegroup;
    return 0;
  }

  const RGWZoneParams& DBZone::get_params()
  {
    return *zone_params;
  }

  const rgw_zone_id& DBZone::get_id()
  {
    return cur_zone_id;
  }

  const RGWRealm& DBZone::get_realm()
  {
    return *realm;
  }

  const std::string& DBZone::get_name() const
  {
    return zone_params->get_name();
  }

  bool DBZone::is_writeable()
  {
    return true;
  }

  bool DBZone::get_redirect_endpoint(std::string* endpoint)
  {
    return false;
  }

  bool DBZone::has_zonegroup_api(const std::string& api) const
  {
    return false;
  }

  const std::string& DBZone::get_current_period_id()
  {
    return current_period->get_id();
  }

  std::unique_ptr<LuaScriptManager> RGWDBStore::get_lua_script_manager()
  {
    return std::unique_ptr<LuaScriptManager>(new DBLuaScriptManager(this));
  }


  std::unique_ptr<RGWRole> RGWDBStore::get_role(std::string name,
      std::string tenant,
      std::string path,
      std::string trust_policy,
      std::string max_session_duration_str)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  std::unique_ptr<RGWRole> RGWDBStore::get_role(std::string id)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  int RGWDBStore::get_roles(const DoutPrefixProvider *dpp,
      optional_yield y,
      const std::string& path_prefix,
      const std::string& tenant,
      vector<std::unique_ptr<RGWRole>>& roles)
  {
    return 0;
  }

  std::unique_ptr<RGWOIDCProvider> RGWDBStore::get_oidc_provider()
  {
    RGWOIDCProvider* p = nullptr;
    return std::unique_ptr<RGWOIDCProvider>(p);
  }

  int RGWDBStore::get_oidc_providers(const DoutPrefixProvider *dpp,
      const std::string& tenant,
      vector<std::unique_ptr<RGWOIDCProvider>>& providers)
  {
    return 0;
  }

  std::unique_ptr<User> RGWDBStore::get_user(const rgw_user &u)
  {
    return std::unique_ptr<User>(new DBUser(this, u));
  }

  int RGWDBStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
  {
    RGWUserInfo uinfo;
    User *u;
    int ret = 0;
    RGWObjVersionTracker objv_tracker;

    ret = getDBStore()->get_user(string("access_key"), key, uinfo, nullptr,
        &objv_tracker);

    if (ret < 0)
      return ret;

    u = new DBUser(this, uinfo);

    if (!u)
      return -ENOMEM;

    u->get_version_tracker() = objv_tracker;
    user->reset(u);

    return 0;
  }

  int RGWDBStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
  {
    RGWUserInfo uinfo;
    User *u;
    int ret = 0;
    RGWObjVersionTracker objv_tracker;

    ret = getDBStore()->get_user(string("email"), email, uinfo, nullptr,
        &objv_tracker);

    if (ret < 0)
      return ret;

    u = new DBUser(this, uinfo);

    if (!u)
      return -ENOMEM;

    u->get_version_tracker() = objv_tracker;
    user->reset(u);

    return ret;
  }

  int RGWDBStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
  {
    /* Swift keys and subusers are not supported for now */
    return 0;
  }

  std::unique_ptr<Object> RGWDBStore::get_object(const rgw_obj_key& k)
  {
    return NULL;
  }


  int RGWDBStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    int ret;
    Bucket* bp;

    bp = new DBBucket(this, b, u);
    ret = bp->get_bucket_info(dpp, y);
    if (ret < 0) {
      delete bp;
      return ret;
    }

    bucket->reset(bp);
    return 0;
  }

  int RGWDBStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
  {
    Bucket* bp;

    bp = new DBBucket(this, i, u);
    /* Don't need to fetch the bucket info, use the provided one */

    bucket->reset(bp);
    return 0;
  }

  int RGWDBStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    rgw_bucket b;

    b.tenant = tenant;
    b.name = name;

    return get_bucket(dpp, u, b, bucket, y);
  }

  int RGWDBStore::create_bucket(const DoutPrefixProvider *dpp,
      User* u, const rgw_bucket& b,
      const string& zonegroup_id,
      rgw_placement_rule& placement_rule,
      string& swift_ver_location,
      const RGWQuotaInfo * pquota_info,
      const RGWAccessControlPolicy& policy,
      Attrs& attrs,
      RGWBucketInfo& info,
      obj_version& ep_objv,
      bool exclusive,
      bool obj_lock_enabled,
      bool *existed,
      req_info& req_info,
      std::unique_ptr<Bucket>* bucket_out,
      optional_yield y)
  {
    int ret;
    bufferlist in_data;
    RGWBucketInfo master_info;
    rgw_bucket *pmaster_bucket = nullptr;
    uint32_t *pmaster_num_shards = nullptr;
    real_time creation_time;
    std::unique_ptr<Bucket> bucket;
    obj_version objv, *pobjv = NULL;

    /* If it exists, look it up; otherwise create it */
    ret = get_bucket(dpp, u, b, &bucket, y);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    if (ret != -ENOENT) {
      RGWAccessControlPolicy old_policy(ctx());
      *existed = true;
      if (swift_ver_location.empty()) {
        swift_ver_location = bucket->get_info().swift_ver_location;
      }
      placement_rule.inherit_from(bucket->get_info().placement_rule);

      // don't allow changes to the acl policy
      /*    int r = rgw_op_get_bucket_policy_from_attr(dpp, this, u, bucket->get_attrs(),
            &old_policy, y);
            if (r >= 0 && old_policy != policy) {
            bucket_out->swap(bucket);
            return -EEXIST;
            }*/
    } else {
      bucket = std::unique_ptr<Bucket>(new DBBucket(this, b, u));
      *existed = false;
      bucket->set_attrs(attrs);
      // XXX: For now single default zone and STANDARD storage class
      // supported.
      placement_rule.name = "default";
      placement_rule.storage_class = "STANDARD";
    }

    /*
     * XXX: If not master zone, fwd the request to master zone.
     * For now DBStore has single zone.
     */
    std::string zid = zonegroup_id;
    /* if (zid.empty()) {
       zid = svc()->zone->get_zonegroup().get_id();
       } */

    if (*existed) {
      rgw_placement_rule selected_placement_rule;
      /* XXX: Handle this when zone is implemented
         ret = svc()->zone->select_bucket_placement(u.get_info(),
         zid, placement_rule,
         &selected_placement_rule, nullptr, y);
         if (selected_placement_rule != info.placement_rule) {
         ret = -EEXIST;
         bucket_out->swap(bucket);
         return ret;
         } */
    } else {

      /* XXX: We may not need to send all these params. Cleanup the unused ones */
      ret = getDBStore()->create_bucket(u->get_info(), bucket->get_key(),
          zid, placement_rule, swift_ver_location, pquota_info,
          attrs, info, pobjv, &ep_objv, creation_time,
          pmaster_bucket, pmaster_num_shards, y, dpp,
          exclusive);
      if (ret == -EEXIST) {
        *existed = true;
        ret = 0;
      } else if (ret != 0) {
        return ret;
      }
    }

    bucket->set_version(ep_objv);
    bucket->get_info() = info;

    bucket_out->swap(bucket);

    return ret;
  }

  bool RGWDBStore::is_meta_master()
  {
    return true;
  }

  int RGWDBStore::forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version *objv,
      bufferlist& in_data,
      JSONParser *jp, req_info& info,
      optional_yield y)
  {
    return 0;
  }

  int RGWDBStore::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, Bucket* bucket, Object* obj, optional_yield y)
  {
    return 0;
  }

  std::string RGWDBStore::zone_unique_id(uint64_t unique_num)
  {
    return "";
  }

  std::string RGWDBStore::zone_unique_trans_id(const uint64_t unique_num)
  {
    return "";
  }

  int RGWDBStore::cluster_stat(RGWClusterStat& stats)
  {
    return 0;
  }

  std::unique_ptr<Lifecycle> RGWDBStore::get_lifecycle(void)
  {
    return 0;
  }

  std::unique_ptr<Completions> RGWDBStore::get_completions(void)
  {
    return 0;
  }

  std::unique_ptr<Notification> RGWDBStore::get_notification(rgw::sal::Object* obj,
      struct req_state* s,
      rgw::notify::EventType event_type)
  {
    return 0;
  }

  std::unique_ptr<GCChain> RGWDBStore::get_gc_chain(rgw::sal::Object* obj)
  {
    return 0;
  }

  std::unique_ptr<Writer> RGWDBStore::get_writer(Aio *aio, rgw::sal::Bucket* bucket,
      RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::Object> _head_obj,
      const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int RGWDBStore::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj)
  {
    return 0;
  }

  int RGWDBStore::delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, Completions* aio)
  {
    return 0;
  }

  void RGWDBStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
  {
    return;
  }

  int RGWDBStore::get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
  {
    return 0;
  }

  int RGWDBStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info)
  {
    return 0;
  }

  int RGWDBStore::log_op(const DoutPrefixProvider *dpp, string& oid, bufferlist& bl)
  {
    return 0;
  }

  int RGWDBStore::register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
      const map<string, string>& meta)
  {
    return 0;
  }

  void RGWDBStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
  {
    // XXX: Not handled for the first pass 
    return;
  }

  int RGWDBStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled)
  {
    int ret = 0;

    vector<rgw_bucket>::iterator iter;

    for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
      rgw_bucket& bucket = *iter;
      if (enabled) {
        ldpp_dout(dpp, 20) << "enabling bucket name=" << bucket.name << dendl;
      } else {
        ldpp_dout(dpp, 20) << "disabling bucket name=" << bucket.name << dendl;
      }

      RGWBucketInfo info;
      map<string, bufferlist> attrs;
      int r = getDBStore()->get_bucket_info(string("name"), "", info, &attrs,
          nullptr, nullptr);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
        ret = r;
        continue;
      }
      if (enabled) {
        info.flags &= ~BUCKET_SUSPENDED;
      } else {
        info.flags |= BUCKET_SUSPENDED;
      }

      r = getDBStore()->update_bucket("info", info, false, nullptr, &attrs, nullptr, &info.objv_tracker);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
        ret = r;
        continue;
      }
    }
    return ret;
  }

  int RGWDBStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
      std::optional<rgw_zone_id> zone,
      std::optional<rgw_bucket> bucket,
      RGWBucketSyncPolicyHandlerRef *phandler,
      optional_yield y)
  {
    return 0;
  }

  RGWDataSyncStatusManager* RGWDBStore::get_data_sync_manager(const rgw_zone_id& source_zone)
  {
    return 0;
  }

  int RGWDBStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, 
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int RGWDBStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int RGWDBStore::get_config_key_val(string name, bufferlist *bl)
  {
    return 0;
  }

  int RGWDBStore::meta_list_keys_init(const DoutPrefixProvider *dpp, const string& section, const string& marker, void** phandle)
  {
    return 0;
  }

  int RGWDBStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<string>& keys, bool* truncated)
  {
    return 0;
  }

  void RGWDBStore::meta_list_keys_complete(void* handle)
  {
    return;
  }

  std::string RGWDBStore::meta_get_marker(void* handle)
  {
    return "";
  }

  int RGWDBStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
  {
    return 0;
  }

} // namespace rgw::sal

extern "C" {

  void *newRGWDBStore(void)
  {
    rgw::sal::RGWDBStore *store = new rgw::sal::RGWDBStore();
    if (store) {
      DBStoreManager *dbsm = new DBStoreManager();

      if (!dbsm ) {
        delete store; store = nullptr;
      }

      DBStore *dbstore = dbsm->getDBStore();
      if (!dbstore ) {
        delete dbsm;
        delete store; store = nullptr;
      }

      store->setDBStoreManager(dbsm);
      store->setDBStore(dbstore);
    }

    return store;
  }

}
