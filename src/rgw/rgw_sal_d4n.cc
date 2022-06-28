// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

int D4NFilterZoneGroup::get_placement_tier(const rgw_placement_rule& rule,
					std::unique_ptr<PlacementTier>* tier)
{
  std::unique_ptr<PlacementTier> nt;
  int ret;

  ret = next->get_placement_tier(rule, &nt);
  if (ret != 0)
    return ret;

  PlacementTier* t = new D4NFilterPlacementTier(std::move(nt));
  if (!t)
    return -ENOMEM;

  tier->reset(t);
  return 0;
}

int D4NFilterZone::get_zonegroup(const std::string& id,
			      std::unique_ptr<ZoneGroup>* zonegroup)
{
  std::unique_ptr<ZoneGroup> ngz;
  int ret;

  ret = next->get_zonegroup(id, &ngz);
  if (ret != 0)
    return ret;

  ZoneGroup* zg = new D4NFilterZoneGroup(std::move(ngz));
  if (!zg)
    return -ENOMEM;

  zonegroup->reset(zg);
  return 0;
}

int D4NFilterStore::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  zone = std::make_unique<D4NFilterZone>(next->get_zone()->clone());

  return 0;
}

const std::string D4NFilterStore::get_name() const
{
  std::string name = "filter<" + next->get_name() + ">";
  return name;
}

std::string D4NFilterStore::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
{
  return next->get_cluster_id(dpp, y);
}

std::unique_ptr<User> D4NFilterStore::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);
  return std::make_unique<D4NFilterUser>(std::move(user));
}

int D4NFilterStore::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new D4NFilterUser(std::move(nu));
  if (!u)
    return -ENOMEM;

  user->reset(u);
  return 0;
}

int D4NFilterStore::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new D4NFilterUser(std::move(nu));
  if (!u)
    return -ENOMEM;

  user->reset(u);
  return 0;
}

int D4NFilterStore::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new D4NFilterUser(std::move(nu));
  if (!u)
    return -ENOMEM;

  user->reset(u);
  return 0;
}

std::unique_ptr<Object> D4NFilterStore::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);
  return std::make_unique<D4NFilterObject>(std::move(o));
}

int D4NFilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu{nullptr};

  if (u)
    nu = dynamic_cast<D4NFilterUser*>(u)->get_next();

  ret = next->get_bucket(dpp, nu, b, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new D4NFilterBucket(std::move(nb), u);
  if (!fb)
    return -ENOMEM;

  bucket->reset(fb);
  return 0;
}

int D4NFilterStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu{nullptr};

  if (u)
    nu = dynamic_cast<D4NFilterUser*>(u)->get_next();

  ret = next->get_bucket(nu, i, &nb);
  if (ret != 0)
    return ret;

  Bucket* fb = new D4NFilterBucket(std::move(nb), u);
  if (!fb)
    return -ENOMEM;

  bucket->reset(fb);
  return 0;
}

int D4NFilterStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  int ret;
  User* nu{nullptr};

  if (u)
    nu = dynamic_cast<D4NFilterUser*>(u)->get_next();

  ret = next->get_bucket(dpp, nu, tenant, name, &nb, y);
  if (ret != 0)
    return ret;

  Bucket* fb = new D4NFilterBucket(std::move(nb), u);
  if (!fb)
    return -ENOMEM;

  bucket->reset(fb);
  return 0;
}

bool D4NFilterStore::is_meta_master()
{
  return next->is_meta_master();
}

int D4NFilterStore::forward_request_to_master(const DoutPrefixProvider *dpp,
					   User* user, obj_version* objv,
					   bufferlist& in_data,
					   JSONParser* jp, req_info& info,
					   optional_yield y)
{
  return next->forward_request_to_master(dpp, user, objv, in_data, jp, info, y);
}

int D4NFilterStore::forward_iam_request_to_master(const DoutPrefixProvider *dpp,
					       const RGWAccessKey& key,
					       obj_version* objv,
					       bufferlist& in_data,
					       RGWXMLDecoder::XMLParser* parser,
					       req_info& info,
					       optional_yield y)
{
  return next->forward_iam_request_to_master(dpp, key, objv, in_data, parser, info, y);
}

std::string D4NFilterStore::zone_unique_id(uint64_t unique_num)
{
  return next->zone_unique_id(unique_num);
}

std::string D4NFilterStore::zone_unique_trans_id(uint64_t unique_num)
{
  return next->zone_unique_trans_id(unique_num);
}

int D4NFilterStore::cluster_stat(RGWClusterStat& stats)
{
  return next->cluster_stat(stats);
}

std::unique_ptr<Lifecycle> D4NFilterStore::get_lifecycle(void)
{
  std::unique_ptr<Lifecycle> lc = next->get_lifecycle();
  return std::make_unique<D4NFilterLifecycle>(std::move(lc));
}

std::unique_ptr<Completions> D4NFilterStore::get_completions(void)
{
  std::unique_ptr<Completions> c = next->get_completions();
  return std::make_unique<D4NFilterCompletions>(std::move(c));
}

std::unique_ptr<Notification> D4NFilterStore::get_notification(rgw::sal::Object* obj,
				rgw::sal::Object* src_obj, req_state* s,
				rgw::notify::EventType event_type,
				const std::string* object_name)
{
  std::unique_ptr<Notification> n = next->get_notification(obj, src_obj, s,
							   event_type,
							   object_name);
  return std::make_unique<D4NFilterNotification>(std::move(n));
}

std::unique_ptr<Notification> D4NFilterStore::get_notification(const DoutPrefixProvider* dpp,
				rgw::sal::Object* obj, rgw::sal::Object* src_obj,
				rgw::notify::EventType event_type,
				rgw::sal::Bucket* _bucket, std::string& _user_id,
				std::string& _user_tenant, std::string& _req_id,
				optional_yield y)
{
  std::unique_ptr<Notification> n = next->get_notification(dpp, obj, src_obj, event_type,
							   _bucket, _user_id,
							   _user_tenant, _req_id, y);
  return std::make_unique<D4NFilterNotification>(std::move(n));
}

RGWLC* D4NFilterStore::get_rgwlc()
{
  return next->get_rgwlc();
}

RGWCoroutinesManagerRegistry* D4NFilterStore::get_cr_registry()
{
  return next->get_cr_registry();
}

int D4NFilterStore::log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
    return next->log_usage(dpp, usage_info);
}

int D4NFilterStore::log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl)
{
    return next->log_op(dpp, oid, bl);
}

int D4NFilterStore::register_to_service_map(const DoutPrefixProvider *dpp,
					 const std::string& daemon_type,
					 const std::map<std::string, std::string>& meta)
{
  return next->register_to_service_map(dpp, daemon_type, meta);
}

void D4NFilterStore::get_quota(RGWQuota& quota)
{
  return next->get_quota(quota);
}

void D4NFilterStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
				RGWRateLimitInfo& user_ratelimit,
				RGWRateLimitInfo& anon_ratelimit)
{
  return next->get_ratelimit(bucket_ratelimit, user_ratelimit, anon_ratelimit);
}

int D4NFilterStore::set_buckets_enabled(const DoutPrefixProvider* dpp,
				     std::vector<rgw_bucket>& buckets, bool enabled)
{
    return next->set_buckets_enabled(dpp, buckets, enabled);
}

uint64_t D4NFilterStore::get_new_req_id()
{
    return next->get_new_req_id();
}

int D4NFilterStore::get_sync_policy_handler(const DoutPrefixProvider* dpp,
					 std::optional<rgw_zone_id> zone,
					 std::optional<rgw_bucket> bucket,
					 RGWBucketSyncPolicyHandlerRef* phandler,
					 optional_yield y)
{
  return next->get_sync_policy_handler(dpp, zone, bucket, phandler, y);
}

RGWDataSyncStatusManager* D4NFilterStore::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  return next->get_data_sync_manager(source_zone);
}

void D4NFilterStore::wakeup_meta_sync_shards(std::set<int>& shard_ids)
{
  return next->wakeup_meta_sync_shards(shard_ids);
}

void D4NFilterStore::wakeup_data_sync_shards(const DoutPrefixProvider *dpp,
					  const rgw_zone_id& source_zone,
					  boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids)
{
  return next->wakeup_data_sync_shards(dpp, source_zone, shard_ids);
}

int D4NFilterStore::clear_usage(const DoutPrefixProvider *dpp)
{
  return next->clear_usage(dpp);
}

int D4NFilterStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
				uint64_t end_epoch, uint32_t max_entries,
				bool* is_truncated, RGWUsageIter& usage_iter,
				std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_all_usage(dpp, start_epoch, end_epoch, max_entries,
			      is_truncated, usage_iter, usage);
}

int D4NFilterStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
				uint64_t end_epoch)
{
  return next->trim_all_usage(dpp, start_epoch, end_epoch);
}

int D4NFilterStore::get_config_key_val(std::string name, bufferlist* bl)
{
  return next->get_config_key_val(name, bl);
}

int D4NFilterStore::meta_list_keys_init(const DoutPrefixProvider *dpp,
				     const std::string& section,
				     const std::string& marker, void** phandle)
{
  return next->meta_list_keys_init(dpp, section, marker, phandle);
}

int D4NFilterStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
				     int max, std::list<std::string>& keys,
				     bool* truncated)
{
  return next->meta_list_keys_next(dpp, handle, max, keys, truncated);
}

void D4NFilterStore::meta_list_keys_complete(void* handle)
{
  next->meta_list_keys_complete(handle);
}

std::string D4NFilterStore::meta_get_marker(void* handle)
{
  return next->meta_get_marker(handle);
}

int D4NFilterStore::meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key,
			     optional_yield y)
{
  return next->meta_remove(dpp, metadata_key, y);
}

const RGWSyncModuleInstanceRef& D4NFilterStore::get_sync_module()
{
  return next->get_sync_module();
}

std::unique_ptr<LuaScriptManager> D4NFilterStore::get_lua_script_manager()
{
  std::unique_ptr<LuaScriptManager> nm = next->get_lua_script_manager();

  return std::make_unique<D4NFilterLuaScriptManager>(std::move(nm));
}

std::unique_ptr<RGWRole> D4NFilterStore::get_role(std::string name,
					      std::string tenant,
					      std::string path,
					      std::string trust_policy,
					      std::string max_session_duration_str,
                std::multimap<std::string,std::string> tags)
{
  return next->get_role(name, tenant, path, trust_policy, max_session_duration_str, tags);
}

std::unique_ptr<RGWRole> D4NFilterStore::get_role(std::string id)
{
  return next->get_role(id);
}

std::unique_ptr<RGWRole> D4NFilterStore::get_role(const RGWRoleInfo& info)
{
  return next->get_role(info);
}

int D4NFilterStore::get_roles(const DoutPrefixProvider *dpp,
			   optional_yield y,
			   const std::string& path_prefix,
			   const std::string& tenant,
			   std::vector<std::unique_ptr<RGWRole>>& roles)
{
  return next->get_roles(dpp, y, path_prefix, tenant, roles);
}

std::unique_ptr<RGWOIDCProvider> D4NFilterStore::get_oidc_provider()
{
  return next->get_oidc_provider();
}

int D4NFilterStore::get_oidc_providers(const DoutPrefixProvider *dpp,
				    const std::string& tenant,
				    std::vector<std::unique_ptr<RGWOIDCProvider>>& providers)
{
  return next->get_oidc_providers(dpp, tenant, providers);
}

std::unique_ptr<Writer> D4NFilterStore::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  /* We're going to lose _head_obj here, but we don't use it, so I think it's
   * okay */
  std::unique_ptr<Object> no =
    dynamic_cast<D4NFilterObject*>(_head_obj.get())->get_next()->clone();

  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   unique_tag, position,
							   cur_accounted_size);

  return std::make_unique<D4NFilterWriter>(std::move(writer));
}

std::unique_ptr<Writer> D4NFilterStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  /* We're going to lose _head_obj here, but we don't use it, so I think it's
   * okay */
  std::unique_ptr<Object> no =
    dynamic_cast<D4NFilterObject*>(_head_obj.get())->get_next()->clone();

  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, std::move(no),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<D4NFilterWriter>(std::move(writer), this, std::move(_head_obj));
}

const std::string& D4NFilterStore::get_compression_type(const rgw_placement_rule& rule)
{
  return next->get_compression_type(rule);
}

bool D4NFilterStore::valid_placement(const rgw_placement_rule& rule)
{
  return next->valid_placement(rule);
}

void D4NFilterStore::finalize(void)
{
  next->finalize();
}

CephContext* D4NFilterStore::ctx(void)
{
  return next->ctx();
}

const std::string& D4NFilterStore::get_luarocks_path() const
{
  return next->get_luarocks_path();
}

void D4NFilterStore::set_luarocks_path(const std::string& path)
{
  next->set_luarocks_path(path);
}

int D4NFilterUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &buckets, optional_yield y)
{
  BucketList bl;
  int ret;

  buckets.clear();
  ret = next->list_buckets(dpp, marker, end_marker, max, need_stats, bl, y);
  if (ret < 0)
    return ret;

  buckets.set_truncated(bl.is_truncated());
  for (auto& ent : bl.get_buckets()) {
    buckets.add(std::make_unique<D4NFilterBucket>(std::move(ent.second), this));
  }

  return 0;
}

int D4NFilterUser::create_bucket(const DoutPrefixProvider* dpp,
			      const rgw_bucket& b,
			      const std::string& zonegroup_id,
			      rgw_placement_rule& placement_rule,
			      std::string& swift_ver_location,
			      const RGWQuotaInfo * pquota_info,
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
  std::unique_ptr<Bucket> nb;
  int ret;

  ret = next->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy, attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, &nb, y);
  if (ret < 0)
    return ret;

  Bucket* fb = new D4NFilterBucket(std::move(nb), this);
  if (!fb)
    return -ENOMEM;

  bucket_out->reset(fb);
  return 0;
}

int D4NFilterUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->read_attrs(dpp, y);
}

int D4NFilterUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int D4NFilterUser::read_stats(const DoutPrefixProvider *dpp,
			   optional_yield y, RGWStorageStats* stats,
			   ceph::real_time* last_stats_sync,
			   ceph::real_time* last_stats_update)
{
  return next->read_stats(dpp, y, stats, last_stats_sync, last_stats_update);
}

int D4NFilterUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb)
{
  return next->read_stats_async(dpp, cb);
}

int D4NFilterUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return next->complete_flush_stats(dpp, y);
}

int D4NFilterUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_usage(dpp, start_epoch, end_epoch, max_entries,
			  is_truncated, usage_iter, usage);
}

int D4NFilterUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch)
{
  return next->trim_usage(dpp, start_epoch, end_epoch);
}

int D4NFilterUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_user(dpp, y);
}

int D4NFilterUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  return next->store_user(dpp, y, exclusive, old_info);
}

int D4NFilterUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->remove_user(dpp, y);
}

std::unique_ptr<Object> D4NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<D4NFilterObject>(std::move(o), this);
}

int D4NFilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		       ListResults& results, optional_yield y)
{
  return next->list(dpp, params, max, results, y);
}

int D4NFilterBucket::remove_bucket(const DoutPrefixProvider* dpp,
				bool delete_children,
				bool forward_to_master,
				req_info* req_info,
				optional_yield y)
{
  return next->remove_bucket(dpp, delete_children, forward_to_master, req_info, y);
}

int D4NFilterBucket::remove_bucket_bypass_gc(int concurrent_max,
					  bool keep_index_consistent,
					  optional_yield y,
					  const DoutPrefixProvider *dpp)
{
  return next->remove_bucket_bypass_gc(concurrent_max, keep_index_consistent, y, dpp);
}

int D4NFilterBucket::set_acl(const DoutPrefixProvider* dpp,
			  RGWAccessControlPolicy &acl, optional_yield y)
{
  return next->set_acl(dpp, acl, y);
}

int D4NFilterBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			      bool get_stats)
{
  return next->load_bucket(dpp, y, get_stats);
}

int D4NFilterBucket::read_stats(const DoutPrefixProvider *dpp,
			     const bucket_index_layout_generation& idx_layout,
			     int shard_id, std::string* bucket_ver,
			     std::string* master_ver,
			     std::map<RGWObjCategory, RGWStorageStats>& stats,
			     std::string* max_marker, bool* syncstopped)
{
  return next->read_stats(dpp, idx_layout, shard_id, bucket_ver, master_ver,
			  stats, max_marker, syncstopped);
}

int D4NFilterBucket::read_stats_async(const DoutPrefixProvider *dpp,
				   const bucket_index_layout_generation& idx_layout,
				   int shard_id, RGWGetBucketStats_CB* ctx)
{
  return next->read_stats_async(dpp, idx_layout, shard_id, ctx);
}

int D4NFilterBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return next->sync_user_stats(dpp, y);
}

int D4NFilterBucket::update_container_stats(const DoutPrefixProvider* dpp)
{
  return next->update_container_stats(dpp);
}

int D4NFilterBucket::check_bucket_shards(const DoutPrefixProvider* dpp)
{
  return next->check_bucket_shards(dpp);
}

int D4NFilterBucket::chown(const DoutPrefixProvider* dpp, User* new_user,
			User* old_user, optional_yield y,
			const std::string* marker)
{
  return next->chown(dpp, new_user, old_user, y, marker);
}

int D4NFilterBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive,
			   ceph::real_time _mtime)
{
  return next->put_info(dpp, exclusive, _mtime);
}

bool D4NFilterBucket::is_owner(User* user)
{
  D4NFilterUser* fu = dynamic_cast<D4NFilterUser*>(user);

  return next->is_owner(fu->get_next());
}

int D4NFilterBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->check_empty(dpp, y);
}

int D4NFilterBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota,
			      uint64_t obj_size, optional_yield y,
			      bool check_size_only)
{
  return next->check_quota(dpp, quota, obj_size, y, check_size_only);
}

int D4NFilterBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int D4NFilterBucket::try_refresh_info(const DoutPrefixProvider* dpp,
				   ceph::real_time* pmtime)
{
  return next->try_refresh_info(dpp, pmtime);
}

int D4NFilterBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			     uint64_t end_epoch, uint32_t max_entries,
			     bool* is_truncated, RGWUsageIter& usage_iter,
			     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_usage(dpp, start_epoch, end_epoch, max_entries,
			  is_truncated, usage_iter, usage);
}

int D4NFilterBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			     uint64_t end_epoch)
{
  return next->trim_usage(dpp, start_epoch, end_epoch);
}

int D4NFilterBucket::remove_objs_from_index(const DoutPrefixProvider *dpp,
					 std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return next->remove_objs_from_index(dpp, objs_to_unlink);
}

int D4NFilterBucket::check_index(const DoutPrefixProvider *dpp,
			      std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
			      std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return next->check_index(dpp, existing_stats, calculated_stats);
}

int D4NFilterBucket::rebuild_index(const DoutPrefixProvider *dpp)
{
  return next->rebuild_index(dpp);
}

int D4NFilterBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
{
  return next->set_tag_timeout(dpp, timeout);
}

int D4NFilterBucket::purge_instance(const DoutPrefixProvider* dpp)
{
  return next->purge_instance(dpp);
}

std::unique_ptr<MultipartUpload> D4NFilterBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu =
    next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<D4NFilterMultipartUpload>(std::move(nmu), this);
}

int D4NFilterBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated)
{
  std::vector<std::unique_ptr<MultipartUpload>> nup;
  int ret;

  ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads, nup,
			      common_prefixes, is_truncated);
  if (ret < 0)
    return ret;

  for (auto& ent : nup) {
    uploads.emplace_back(std::make_unique<D4NFilterMultipartUpload>(std::move(ent), this));
  }

  return 0;
}

int D4NFilterBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct)
{
  return next->abort_multiparts(dpp, cct);
}

int D4NFilterObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				bool prevent_versioning)
{
  return next->delete_object(dpp, y, prevent_versioning);
}

int D4NFilterObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
				 Completions* aio, bool keep_index_consistent,
				 optional_yield y)
{
  return next->delete_obj_aio(dpp, astate, aio, keep_index_consistent, y);
}

int D4NFilterObject::copy_object(User* user,
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
  return next->copy_object(user, info, source_zone, dest_object, dest_bucket,
			   src_bucket, dest_placement, src_mtime, mtime,
			   mod_ptr, unmod_ptr, high_precision_time, if_match,
			   if_nomatch, attrs_mod, copy_if_newer, attrs,
			   category, olh_epoch, delete_at, version_id, tag,
			   etag, progress_cb, progress_data, dpp, y);
}

RGWAccessControlPolicy& D4NFilterObject::get_acl()
{
  return next->get_acl();
}

int D4NFilterObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **pstate,
				optional_yield y, bool follow_olh)
{
  return next->get_obj_state(dpp, pstate, y, follow_olh);
}

int D4NFilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
				Attrs* delattrs, optional_yield y)
{
  return next->set_obj_attrs(dpp, setattrs, delattrs, y);
}

int D4NFilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
				rgw_obj* target_obj)
{
  return next->get_obj_attrs(y, dpp, target_obj);
}

int D4NFilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
				   optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);
}

int D4NFilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp,
				   const char* attr_name, optional_yield y)
{
  return next->delete_obj_attrs(dpp, attr_name, y);
}

bool D4NFilterObject::is_expired()
{
  return next->is_expired();
}

void D4NFilterObject::gen_rand_obj_instance_name()
{
  return next->gen_rand_obj_instance_name();
}

std::unique_ptr<MPSerializer> D4NFilterObject::get_serializer(const DoutPrefixProvider *dpp,
							   const std::string& lock_name)
{
  std::unique_ptr<MPSerializer> s = next->get_serializer(dpp, lock_name);
  return std::make_unique<D4NFilterMPSerializer>(std::move(s));
}

int D4NFilterObject::transition(Bucket* bucket,
			     const rgw_placement_rule& placement_rule,
			     const real_time& mtime,
			     uint64_t olh_epoch,
			     const DoutPrefixProvider* dpp,
			     optional_yield y)
{
  return next->transition(bucket, placement_rule, mtime, olh_epoch, dpp, y);
}

int D4NFilterObject::transition_to_cloud(Bucket* bucket,
				      rgw::sal::PlacementTier* tier,
				      rgw_bucket_dir_entry& o,
				      std::set<std::string>& cloud_targets,
				      CephContext* cct,
				      bool update_object,
				      const DoutPrefixProvider* dpp,
				      optional_yield y)
{
  return next->transition_to_cloud(bucket, tier, o, cloud_targets, cct,
				   update_object, dpp, y);
}

bool D4NFilterObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return next->placement_rules_match(r1, r2);
}

int D4NFilterObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y,
				  Formatter* f)
{
  return next->dump_obj_layout(dpp, y, f);
}

void D4NFilterObject::set_bucket(Bucket* b)
{
  bucket = b;
  next->set_bucket(dynamic_cast<D4NFilterBucket*>(b)->get_next());
};

int D4NFilterObject::swift_versioning_restore(bool& restored,
					   const DoutPrefixProvider* dpp)
{
  return next->swift_versioning_restore(restored, dpp);
}

int D4NFilterObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
					optional_yield y)
{
  return next->swift_versioning_copy(dpp, y);
}

std::unique_ptr<Object::ReadOp> D4NFilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<D4NFilterReadOp>(std::move(r), this);
}

std::unique_ptr<Object::DeleteOp> D4NFilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<D4NFilterDeleteOp>(std::move(d), this);
}

int D4NFilterObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker,
				uint64_t count, std::map<std::string, bufferlist> *m,
				bool* pmore, optional_yield y)
{
  return next->omap_get_vals(dpp, marker, count, m, pmore, y);
}

int D4NFilterObject::omap_get_all(const DoutPrefixProvider *dpp,
			       std::map<std::string, bufferlist> *m,
			       optional_yield y)
{
  return next->omap_get_all(dpp, m, y);
}

int D4NFilterObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
					const std::string& oid,
					const std::set<std::string>& keys,
					Attrs* vals)
{
  return next->omap_get_vals_by_keys(dpp, oid, keys, vals);
}

int D4NFilterObject::omap_set_val_by_key(const DoutPrefixProvider *dpp,
				      const std::string& key, bufferlist& val,
				      bool must_exist, optional_yield y)
{
  return next->omap_set_val_by_key(dpp, key, val, must_exist, y);
}

int D4NFilterObject::D4NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  if (source->trace->blk_dir->existKey(source->get_name())) { // Checks if object is in D4N
    int getReturn = source->trace->blk_dir->getValue(source->trace->c_blk);

    if (getReturn < 0) {
      dout(0) << "D4N Filter: Directory get operation failed." << dendl;
    } else {
      dout(0) << "D4N Filter: Directory get operation succeeded." << dendl;
    }
  }

  return next->prepare(y, dpp);
}

int D4NFilterObject::D4NFilterReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->read(ofs, end, bl, y, dpp);
}

int D4NFilterObject::D4NFilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  return next->get_attr(dpp, name, dest, y);
}

int D4NFilterObject::D4NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  return next->iterate(dpp, ofs, end, cb, y);
}

int D4NFilterObject::D4NFilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y)
{
  if (source->trace->blk_dir->existKey(source->get_name())) { // Checks if object is in D4N
    int delReturn = source->trace->blk_dir->delValue(source->trace->c_blk);

    if (delReturn < 0) {
      dout(0) << "D4N Filter: Directory delete operation failed." << dendl;
    } else {
      dout(0) << "D4N Filter: Directory delete operation succeeded." << dendl;
    }
  }

  return next->delete_obj(dpp, y);
}

std::unique_ptr<rgw::sal::Object> D4NFilterMultipartUpload::get_meta_obj()
{
  std::unique_ptr<Object> no = next->get_meta_obj();

  return std::make_unique<D4NFilterObject>(std::move(no), bucket);
}

int D4NFilterMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  return next->init(dpp, y, owner, dest_placement, attrs);
}

int D4NFilterMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated,
				      bool assume_unsorted)
{
  int ret;

  ret = next->list_parts(dpp, cct, num_parts, marker, next_marker, truncated,
			 assume_unsorted);
  if (ret < 0)
    return ret;

  parts.clear();

  for (auto& ent : next->get_parts()) {
    parts.emplace(ent.first, std::make_unique<D4NFilterMultipartPart>(std::move(ent.second)));
  }

  return 0;
}

int D4NFilterMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct)
{
  return next->abort(dpp, cct);
}

int D4NFilterMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj)
{
  return next->complete(dpp, y, cct, part_etags, remove_objs, accounted_size,
			compressed, cs_info, ofs, tag, owner, olh_epoch,
			dynamic_cast<D4NFilterObject*>(target_obj)->get_next());
}

int D4NFilterMultipartUpload::get_info(const DoutPrefixProvider *dpp,
				    optional_yield y, rgw_placement_rule** rule,
				    rgw::sal::Attrs* attrs)
{
  return next->get_info(dpp, y, rule, attrs);
}

std::unique_ptr<Writer> D4NFilterMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  /* We're going to lose _head_obj here, but we don't use it, so I think it's
   * okay */
  std::unique_ptr<Object> no =
    dynamic_cast<D4NFilterObject*>(_head_obj.get())->get_next()->clone();

  std::unique_ptr<Writer> writer;
  writer = next->get_writer(dpp, y, std::move(no), owner,
			    ptail_placement_rule, part_num, part_num_str);

  return std::make_unique<D4NFilterWriter>(std::move(writer));
}

int D4NFilterMPSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur,
				 optional_yield y)
{
  return next->try_lock(dpp, dur, y);
}

int D4NFilterLCSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur,
				 optional_yield y)
{
  return next->try_lock(dpp, dur, y);
}

std::unique_ptr<Lifecycle::LCEntry> D4NFilterLifecycle::get_entry()
{
  return std::make_unique<D4NFilterLCEntry>(std::move(next->get_entry()));
}

int D4NFilterLifecycle::get_entry(const std::string& oid, const std::string& marker,
			       std::unique_ptr<LCEntry>* entry)
{
  std::unique_ptr<LCEntry> ne;
  int ret;

  ret = next->get_entry(oid, marker, &ne);
  if (ret < 0)
    return ret;

  LCEntry* e = new D4NFilterLCEntry(std::move(ne));
  entry->reset(e);

  return 0;
}

int D4NFilterLifecycle::get_next_entry(const std::string& oid, const std::string& marker,
				    std::unique_ptr<LCEntry>* entry)
{
  std::unique_ptr<LCEntry> ne;
  int ret;

  ret = next->get_next_entry(oid, marker, &ne);
  if (ret < 0)
    return ret;

  LCEntry* e = new D4NFilterLCEntry(std::move(ne));
  entry->reset(e);

  return 0;
}

int D4NFilterLifecycle::set_entry(const std::string& oid, LCEntry& entry)
{
  return next->set_entry(oid, entry);
}

int D4NFilterLifecycle::list_entries(const std::string& oid, const std::string& marker,
				  uint32_t max_entries,
				  std::vector<std::unique_ptr<LCEntry>>& entries)
{
  std::vector<std::unique_ptr<LCEntry>> ne;
  int ret;

  ret = next->list_entries(oid, marker, max_entries, ne);
  if (ret < 0)
    return ret;

  for (auto& ent : ne) {
    entries.emplace_back(std::make_unique<D4NFilterLCEntry>(std::move(ent)));
  }

  return 0;
}

int D4NFilterLifecycle::rm_entry(const std::string& oid, LCEntry& entry)
{
  return next->rm_entry(oid, entry);
}

int D4NFilterLifecycle::get_head(const std::string& oid, std::unique_ptr<LCHead>* head)
{
  std::unique_ptr<LCHead> nh;
  int ret;

  ret = next->get_head(oid, &nh);
  if (ret < 0)
    return ret;

  LCHead* h = new D4NFilterLCHead(std::move(nh));
  head->reset(h);

  return 0;
}

int D4NFilterLifecycle::put_head(const std::string& oid, LCHead& head)
{
  return next->put_head(oid, *(dynamic_cast<D4NFilterLCHead&>(head).next.get()));
}

std::unique_ptr<LCSerializer> D4NFilterLifecycle::get_serializer(
					      const std::string& lock_name,
					      const std::string& oid,
					      const std::string& cookie)
{
  std::unique_ptr<LCSerializer> ns;
  ns = next->get_serializer(lock_name, oid, cookie);

  return std::make_unique<D4NFilterLCSerializer>(std::move(ns));
}

int D4NFilterNotification::publish_reserve(const DoutPrefixProvider *dpp,
					RGWObjTags* obj_tags)
{
  return next->publish_reserve(dpp, obj_tags);
}

int D4NFilterNotification::publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
				       const ceph::real_time& mtime, const
				       std::string& etag, const std::string& version)
{
  return next->publish_commit(dpp, size, mtime, etag, version);
}

int D4NFilterWriter::process(bufferlist&& data, uint64_t offset)
{
  return next->process(std::move(data), offset);
}

int D4NFilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  trace->c_blk->hosts_list.push_back("127.0.0.1:6379"); // Hardcoded until cct is added
  trace->c_blk->size_in_bytes = accounted_size;
  trace->c_blk->c_obj.bucket_name = head_obj->get_bucket()->get_name();
  trace->c_blk->c_obj.obj_name = head_obj->get_name();

  int setReturn = trace->blk_dir->setValue(trace->c_blk);

  if (setReturn < 0) {
    dout(0) << "D4N Filter: Directory set operation failed." << dendl;
  } else {
    dout(0) << "D4N Filter: Directory set operation succeeded." << dendl;
  }

  return next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, y);
}

int D4NFilterLuaScriptManager::get(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key, std::string& script)
{
  return next->get(dpp, y, key, script);
}

int D4NFilterLuaScriptManager::put(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key, const std::string& script)
{
  return next->put(dpp, y, key, script);
}

int D4NFilterLuaScriptManager::del(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key)
{
  return next->del(dpp, y, key);
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Store* newBaseD4NFilter(rgw::sal::Store* next)
{
  rgw::sal::D4NFilterStore* store = new rgw::sal::D4NFilterStore(next);

  return store;
}

}
