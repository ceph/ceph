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

#include "rgw_sal_filter.h"

namespace rgw { namespace sal {

/* These are helpers for getting 'next' out of an object, handling nullptr */
static inline PlacementTier* nextPlacementTier(PlacementTier* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterPlacementTier*>(t)->get_next();
}

static inline User* nextUser(User* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterUser*>(t)->get_next();
}

static inline Bucket* nextBucket(Bucket* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterBucket*>(t)->get_next();
}

static inline Object* nextObject(Object* t)
{
  if (!t)
    return nullptr;

  return dynamic_cast<FilterObject*>(t)->get_next();
}

int FilterZoneGroup::get_placement_tier(const rgw_placement_rule& rule,
					std::unique_ptr<PlacementTier>* tier)
{
  std::unique_ptr<PlacementTier> nt;
  int ret;

  ret = next->get_placement_tier(rule, &nt);
  if (ret != 0)
    return ret;

  PlacementTier* t = new FilterPlacementTier(std::move(nt));
  tier->reset(t);
  return 0;
}

int FilterZoneGroup::get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone)
{
  std::unique_ptr<Zone> nz;
  int ret = next->get_zone_by_id(id, &nz);
  if (ret < 0)
    return ret;
  Zone *z = new FilterZone(std::move(nz));

  zone->reset(z);
  return 0;
}

int FilterZoneGroup::get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone)
{
  std::unique_ptr<Zone> nz;
  int ret = next->get_zone_by_name(name, &nz);
  if (ret < 0)
    return ret;
  Zone *z = new FilterZone(std::move(nz));

  zone->reset(z);
  return 0;
}

int FilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  zone = std::make_unique<FilterZone>(next->get_zone()->clone());

  return 0;
}

const std::string FilterDriver::get_name() const
{
  std::string name = "filter<" + next->get_name() + ">";
  return name;
}

std::string FilterDriver::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
{
  return next->get_cluster_id(dpp, y);
}

std::unique_ptr<User> FilterDriver::get_user(const rgw_user &u)
{
  std::unique_ptr<User> user = next->get_user(u);
  return std::make_unique<FilterUser>(std::move(user));
}

int FilterDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_access_key(dpp, key, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

int FilterDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_email(dpp, email, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

int FilterDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret;

  ret = next->get_user_by_swift(dpp, user_str, y, &nu);
  if (ret != 0)
    return ret;

  User* u = new FilterUser(std::move(nu));
  user->reset(u);
  return 0;
}

int FilterDriver::load_account_by_id(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view id,
                                     RGWAccountInfo& info,
                                     Attrs& attrs,
                                     RGWObjVersionTracker& objv)
{
  return next->load_account_by_id(dpp, y, id, info, attrs, objv);
}

int FilterDriver::load_account_by_name(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view tenant,
                                       std::string_view name,
                                       RGWAccountInfo& info,
                                       Attrs& attrs,
                                       RGWObjVersionTracker& objv)
{
  return next->load_account_by_name(dpp, y, tenant, name, info, attrs, objv);
}

int FilterDriver::load_account_by_email(const DoutPrefixProvider* dpp,
                                        optional_yield y,
                                        std::string_view email,
                                        RGWAccountInfo& info,
                                        Attrs& attrs,
                                        RGWObjVersionTracker& objv)
{
  return next->load_account_by_email(dpp, y, email, info, attrs, objv);
}

int FilterDriver::store_account(const DoutPrefixProvider* dpp,
                                optional_yield y, bool exclusive,
                                const RGWAccountInfo& info,
                                const RGWAccountInfo* old_info,
                                const Attrs& attrs,
                                RGWObjVersionTracker& objv)
{
  return next->store_account(dpp, y, exclusive, info, old_info, attrs, objv);
}

int FilterDriver::delete_account(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 const RGWAccountInfo& info,
                                 RGWObjVersionTracker& objv)
{
  return next->delete_account(dpp, y, info, objv);
}

int FilterDriver::load_stats(const DoutPrefixProvider* dpp,
                             optional_yield y,
                             const rgw_owner& owner,
                             RGWStorageStats& stats,
                             ceph::real_time& last_synced,
                             ceph::real_time& last_updated)
{
  return next->load_stats(dpp, y, owner, stats, last_synced, last_updated);
}

int FilterDriver::load_stats_async(const DoutPrefixProvider* dpp,
                                   const rgw_owner& owner,
                                   boost::intrusive_ptr<ReadStatsCB> cb)
{
  return next->load_stats_async(dpp, owner, std::move(cb));
}

int FilterDriver::reset_stats(const DoutPrefixProvider *dpp,
                              optional_yield y,
                              const rgw_owner& owner)
{
  return next->reset_stats(dpp, y, owner);
}

int FilterDriver::complete_flush_stats(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const rgw_owner& owner)
{
  return next->complete_flush_stats(dpp, y, owner);
}

int FilterDriver::load_owner_by_email(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      std::string_view email,
                                      rgw_owner& owner)
{
  return next->load_owner_by_email(dpp, y, email, owner);
}

int FilterDriver::count_account_roles(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      std::string_view account_id,
                                      uint32_t& count)
{
  return next->count_account_roles(dpp, y, account_id, count);
}

int FilterDriver::list_account_roles(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view account_id,
                                     std::string_view path_prefix,
                                     std::string_view marker,
                                     uint32_t max_items,
                                     RoleList& listing)
{
  return next->list_account_roles(dpp, y, account_id, path_prefix,
                                  marker, max_items, listing);
}

int FilterDriver::load_account_user_by_name(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            std::string_view account_id,
                                            std::string_view tenant,
                                            std::string_view username,
                                            std::unique_ptr<User>* user)
{
  std::unique_ptr<User> nu;
  int ret = next->load_account_user_by_name(dpp, y, account_id, tenant,
                                            username, &nu);
  if (ret >= 0) {
    *user = std::make_unique<FilterUser>(std::move(nu));
  }
  return ret;
}

int FilterDriver::count_account_users(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      std::string_view account_id,
                                      uint32_t& count)
{
  return next->count_account_users(dpp, y, account_id, count);
}

int FilterDriver::list_account_users(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view account_id,
                                     std::string_view tenant,
                                     std::string_view path_prefix,
                                     std::string_view marker,
                                     uint32_t max_items,
                                     UserList& listing)
{
  return next->list_account_users(dpp, y, account_id, tenant, path_prefix,
                                  marker, max_items, listing);
}

int FilterDriver::load_group_by_id(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view id,
                                   RGWGroupInfo& info, Attrs& attrs,
                                   RGWObjVersionTracker& objv)
{
  return next->load_group_by_id(dpp, y, id, info, attrs, objv);
}

int FilterDriver::load_group_by_name(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view account_id,
                                     std::string_view name,
                                     RGWGroupInfo& info, Attrs& attrs,
                                     RGWObjVersionTracker& objv)
{
  return next->load_group_by_name(dpp, y, account_id, name, info, attrs, objv);
}

int FilterDriver::store_group(const DoutPrefixProvider* dpp, optional_yield y,
                              const RGWGroupInfo& info, const Attrs& attrs,
                              RGWObjVersionTracker& objv, bool exclusive,
                              const RGWGroupInfo* old_info)
{
  return next->store_group(dpp, y, info, attrs, objv, exclusive, old_info);
}

int FilterDriver::remove_group(const DoutPrefixProvider* dpp, optional_yield y,
                               const RGWGroupInfo& info,
                               RGWObjVersionTracker& objv)
{
  return next->remove_group(dpp, y, info, objv);
}

int FilterDriver::list_group_users(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   std::string_view tenant,
                                   std::string_view id,
                                   std::string_view marker,
                                   uint32_t max_items,
                                   UserList& listing)
{
  return next->list_group_users(dpp, y, tenant, id, marker, max_items, listing);
}

int FilterDriver::count_account_groups(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view account_id,
                                       uint32_t& count)
{
  return next->count_account_groups(dpp, y, account_id, count);
}

int FilterDriver::list_account_groups(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      std::string_view account_id,
                                      std::string_view path_prefix,
                                      std::string_view marker,
                                      uint32_t max_items,
                                      GroupList& listing)
{
  return next->list_account_groups(dpp, y, account_id, path_prefix,
                                   marker, max_items, listing);
}

std::unique_ptr<Object> FilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);
  return std::make_unique<FilterObject>(std::move(o));
}

std::unique_ptr<Bucket> FilterDriver::get_bucket(const RGWBucketInfo& i)
{
  return std::make_unique<FilterBucket>(next->get_bucket(i));
}

int FilterDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                              std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  std::unique_ptr<Bucket> nb;
  const int ret = next->load_bucket(dpp, b, &nb, y);
  *bucket = std::make_unique<FilterBucket>(std::move(nb));
  return ret;
}

int FilterDriver::list_buckets(const DoutPrefixProvider* dpp,
                               const rgw_owner& owner, const std::string& tenant,
                               const std::string& marker, const std::string& end_marker,
                               uint64_t max, bool need_stats, BucketList &buckets, optional_yield y)
{
  return next->list_buckets(dpp, owner, tenant, marker, end_marker,
                            max, need_stats, buckets, y);
}

bool FilterDriver::is_meta_master()
{
  return next->is_meta_master();
}

std::string FilterDriver::zone_unique_id(uint64_t unique_num)
{
  return next->zone_unique_id(unique_num);
}

std::string FilterDriver::zone_unique_trans_id(uint64_t unique_num)
{
  return next->zone_unique_trans_id(unique_num);
}

int FilterDriver::get_zonegroup(const std::string& id,
			       std::unique_ptr<ZoneGroup>* zonegroup)
{
  std::unique_ptr<ZoneGroup> ngz;
  int ret;

  ret = next->get_zonegroup(id, &ngz);
  if (ret != 0)
    return ret;

  ZoneGroup* zg = new FilterZoneGroup(std::move(ngz));
  zonegroup->reset(zg);
  return 0;
}

int FilterDriver::cluster_stat(RGWClusterStat& stats)
{
  return next->cluster_stat(stats);
}

std::unique_ptr<Lifecycle> FilterDriver::get_lifecycle(void)
{
  std::unique_ptr<Lifecycle> lc = next->get_lifecycle();
  return std::make_unique<FilterLifecycle>(std::move(lc));
}

std::unique_ptr<Notification> FilterDriver::get_notification(rgw::sal::Object* obj,
				rgw::sal::Object* src_obj, req_state* s,
				rgw::notify::EventType event_type, optional_yield y,
				const std::string* object_name)
{
  std::unique_ptr<Notification> n = next->get_notification(nextObject(obj),
							   nextObject(src_obj),
							   s, event_type, y,
							   object_name);
  return std::make_unique<FilterNotification>(std::move(n));
}

std::unique_ptr<Notification> FilterDriver::get_notification(
    const DoutPrefixProvider* dpp,
    rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    const rgw::notify::EventTypeList& event_types,
    rgw::sal::Bucket* _bucket,
    std::string& _user_id,
    std::string& _user_tenant,
    std::string& _req_id,
    optional_yield y) {
  std::unique_ptr<Notification> n = next->get_notification(
      dpp, nextObject(obj), nextObject(src_obj), event_types,
      nextBucket(_bucket), _user_id, _user_tenant, _req_id, y);
  return std::make_unique<FilterNotification>(std::move(n));
}

int FilterDriver::add_persistent_topic(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       const std::string& topic_queue)
{
  return next->add_persistent_topic(dpp, y, topic_queue);
}

int FilterDriver::remove_persistent_topic(const DoutPrefixProvider* dpp,
                                          optional_yield y,
                                          const std::string& topic_queue)
{
  return next->remove_persistent_topic(dpp, y, topic_queue);
}

RGWLC* FilterDriver::get_rgwlc()
{
  return next->get_rgwlc();
}

RGWCoroutinesManagerRegistry* FilterDriver::get_cr_registry()
{
  return next->get_cr_registry();
}

int FilterDriver::log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y)
{
    return next->log_usage(dpp, usage_info, y);
}

int FilterDriver::log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl)
{
    return next->log_op(dpp, oid, bl);
}

int FilterDriver::register_to_service_map(const DoutPrefixProvider *dpp,
					 const std::string& daemon_type,
					 const std::map<std::string, std::string>& meta)
{
  return next->register_to_service_map(dpp, daemon_type, meta);
}

void FilterDriver::get_quota(RGWQuota& quota)
{
  return next->get_quota(quota);
}

void FilterDriver::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
				RGWRateLimitInfo& user_ratelimit,
				RGWRateLimitInfo& anon_ratelimit)
{
  return next->get_ratelimit(bucket_ratelimit, user_ratelimit, anon_ratelimit);
}

int FilterDriver::set_buckets_enabled(const DoutPrefixProvider* dpp,
				     std::vector<rgw_bucket>& buckets, bool enabled, optional_yield y)
{
    return next->set_buckets_enabled(dpp, buckets, enabled, y);
}

uint64_t FilterDriver::get_new_req_id()
{
    return next->get_new_req_id();
}

int FilterDriver::get_sync_policy_handler(const DoutPrefixProvider* dpp,
					 std::optional<rgw_zone_id> zone,
					 std::optional<rgw_bucket> bucket,
					 RGWBucketSyncPolicyHandlerRef* phandler,
					 optional_yield y)
{
  return next->get_sync_policy_handler(dpp, zone, bucket, phandler, y);
}

RGWDataSyncStatusManager* FilterDriver::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  return next->get_data_sync_manager(source_zone);
}

void FilterDriver::wakeup_meta_sync_shards(std::set<int>& shard_ids)
{
  return next->wakeup_meta_sync_shards(shard_ids);
}

void FilterDriver::wakeup_data_sync_shards(const DoutPrefixProvider *dpp,
					  const rgw_zone_id& source_zone,
					  boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids)
{
  return next->wakeup_data_sync_shards(dpp, source_zone, shard_ids);
}

int FilterDriver::clear_usage(const DoutPrefixProvider *dpp, optional_yield y)
{
  return next->clear_usage(dpp, y);
}

int FilterDriver::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
				uint64_t end_epoch, uint32_t max_entries,
				bool* is_truncated, RGWUsageIter& usage_iter,
				std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_all_usage(dpp, start_epoch, end_epoch, max_entries,
			      is_truncated, usage_iter, usage);
}

int FilterDriver::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
				uint64_t end_epoch, optional_yield y)
{
  return next->trim_all_usage(dpp, start_epoch, end_epoch, y);
}

int FilterDriver::get_config_key_val(std::string name, bufferlist* bl)
{
  return next->get_config_key_val(name, bl);
}

int FilterDriver::meta_list_keys_init(const DoutPrefixProvider *dpp,
				     const std::string& section,
				     const std::string& marker, void** phandle)
{
  return next->meta_list_keys_init(dpp, section, marker, phandle);
}

int FilterDriver::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
				     int max, std::list<std::string>& keys,
				     bool* truncated)
{
  return next->meta_list_keys_next(dpp, handle, max, keys, truncated);
}

void FilterDriver::meta_list_keys_complete(void* handle)
{
  next->meta_list_keys_complete(handle);
}

std::string FilterDriver::meta_get_marker(void* handle)
{
  return next->meta_get_marker(handle);
}

int FilterDriver::meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key,
			     optional_yield y)
{
  return next->meta_remove(dpp, metadata_key, y);
}

const RGWSyncModuleInstanceRef& FilterDriver::get_sync_module()
{
  return next->get_sync_module();
}

std::unique_ptr<LuaManager> FilterDriver::get_lua_manager(const std::string& luarocks_path)
{
  std::unique_ptr<LuaManager> nm = next->get_lua_manager(luarocks_path);

  return std::make_unique<FilterLuaManager>(std::move(nm));
}

std::unique_ptr<RGWRole> FilterDriver::get_role(std::string name,
					      std::string tenant,
					      rgw_account_id account_id,
					      std::string path,
					      std::string trust_policy,
					      std::string description,
					      std::string max_session_duration_str,
                std::multimap<std::string,std::string> tags)
{
  return next->get_role(name, tenant, std::move(account_id), path, trust_policy, std::move(description), max_session_duration_str, tags);
}

std::unique_ptr<RGWRole> FilterDriver::get_role(std::string id)
{
  return next->get_role(id);
}

std::unique_ptr<RGWRole> FilterDriver::get_role(const RGWRoleInfo& info)
{
  return next->get_role(info);
}

int FilterDriver::list_roles(const DoutPrefixProvider *dpp,
			     optional_yield y,
			     const std::string& tenant,
			     const std::string& path_prefix,
			     const std::string& marker,
			     uint32_t max_items,
			     RoleList& listing)
{
  return next->list_roles(dpp, y, tenant, path_prefix,
                          marker, max_items, listing);
}

int FilterDriver::store_oidc_provider(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      const RGWOIDCProviderInfo& info,
                                      bool exclusive)
{
  return next->store_oidc_provider(dpp, y, info, exclusive);
}

int FilterDriver::load_oidc_provider(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view tenant,
                                     std::string_view url,
                                     RGWOIDCProviderInfo& info)
{
  return next->load_oidc_provider(dpp, y, tenant, url, info);
}

int FilterDriver::delete_oidc_provider(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       std::string_view tenant,
                                       std::string_view url)
{
  return next->delete_oidc_provider(dpp, y, tenant, url);
}

int FilterDriver::get_oidc_providers(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     std::string_view tenant,
                                     std::vector<RGWOIDCProviderInfo>& providers)
{
  return next->get_oidc_providers(dpp, y, tenant, providers);
}

std::unique_ptr<Writer> FilterDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  std::unique_ptr<Writer> writer = next->get_append_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   unique_tag, position,
							   cur_accounted_size);

  return std::make_unique<FilterWriter>(std::move(writer), obj);
}

std::unique_ptr<Writer> FilterDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  std::unique_ptr<Writer> writer = next->get_atomic_writer(dpp, y, nextObject(obj),
							   owner, ptail_placement_rule,
							   olh_epoch, unique_tag);

  return std::make_unique<FilterWriter>(std::move(writer), obj);
}

const std::string& FilterDriver::get_compression_type(const rgw_placement_rule& rule)
{
  return next->get_compression_type(rule);
}

bool FilterDriver::valid_placement(const rgw_placement_rule& rule)
{
  return next->valid_placement(rule);
}

void FilterDriver::finalize(void)
{
  next->finalize();
}

CephContext* FilterDriver::ctx(void)
{
  return next->ctx();
}

int FilterUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->read_attrs(dpp, y);
}

int FilterUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int FilterUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, uint32_t max_entries,
			   bool* is_truncated, RGWUsageIter& usage_iter,
			   std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_usage(dpp, start_epoch, end_epoch, max_entries,
			  is_truncated, usage_iter, usage);
}

int FilterUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			   uint64_t end_epoch, optional_yield y)
{
  return next->trim_usage(dpp, start_epoch, end_epoch, y);
}

int FilterUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_user(dpp, y);
}

int FilterUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  return next->store_user(dpp, y, exclusive, old_info);
}

int FilterUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->remove_user(dpp, y);
}

int FilterUser::verify_mfa(const std::string& mfa_str, bool* verified,
			   const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->verify_mfa(mfa_str, verified, dpp, y);
}

int FilterUser::list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                            std::string_view marker, uint32_t max_items,
                            GroupList& listing)
{
  return next->list_groups(dpp, y, marker, max_items, listing);
}

std::unique_ptr<Object> FilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> o = next->get_object(k);

  return std::make_unique<FilterObject>(std::move(o), this);
}

int FilterBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
		       ListResults& results, optional_yield y)
{
  return next->list(dpp, params, max, results, y);
}

int FilterBucket::remove(const DoutPrefixProvider* dpp,
			 bool delete_children,
			 optional_yield y)
{
  return next->remove(dpp, delete_children, y);
}

int FilterBucket::remove_bypass_gc(int concurrent_max,
				   bool keep_index_consistent,
				   optional_yield y,
				   const DoutPrefixProvider *dpp)
{
  return next->remove_bypass_gc(concurrent_max, keep_index_consistent, y, dpp);
}

int FilterBucket::set_acl(const DoutPrefixProvider* dpp,
			  RGWAccessControlPolicy &acl, optional_yield y)
{
  return next->set_acl(dpp, acl, y);
}

int FilterBucket::create(const DoutPrefixProvider* dpp,
                         const CreateParams& params,
                         optional_yield y)
{
  return next->create(dpp, params, y);
}

int FilterBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->load_bucket(dpp, y);
}

int FilterBucket::read_stats(const DoutPrefixProvider *dpp,
			     const bucket_index_layout_generation& idx_layout,
			     int shard_id, std::string* bucket_ver,
			     std::string* master_ver,
			     std::map<RGWObjCategory, RGWStorageStats>& stats,
			     std::string* max_marker, bool* syncstopped)
{
  return next->read_stats(dpp, idx_layout, shard_id, bucket_ver, master_ver,
			  stats, max_marker, syncstopped);
}

int FilterBucket::read_stats_async(const DoutPrefixProvider *dpp,
				   const bucket_index_layout_generation& idx_layout,
				   int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return next->read_stats_async(dpp, idx_layout, shard_id, ctx);
}

int FilterBucket::sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                   RGWBucketEnt* ent)
{
  return next->sync_owner_stats(dpp, y, ent);
}

int FilterBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                      uint64_t num_objs, optional_yield y)
{
  return next->check_bucket_shards(dpp, num_objs, y);
}

int FilterBucket::chown(const DoutPrefixProvider* dpp, const rgw_owner& new_owner, optional_yield y)
{
  return next->chown(dpp, new_owner, y);
}

int FilterBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive,
			   ceph::real_time _mtime, optional_yield y)
{
  return next->put_info(dpp, exclusive, _mtime, y);
}

const rgw_owner& FilterBucket::get_owner() const
{
  return next->get_owner();
}

int FilterBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->check_empty(dpp, y);
}

int FilterBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota,
			      uint64_t obj_size, optional_yield y,
			      bool check_size_only)
{
  return next->check_quota(dpp, quota, obj_size, y, check_size_only);
}

int FilterBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  return next->merge_and_store_attrs(dpp, new_attrs, y);
}

int FilterBucket::try_refresh_info(const DoutPrefixProvider* dpp,
				   ceph::real_time* pmtime, optional_yield y)
{
  return next->try_refresh_info(dpp, pmtime, y);
}

int FilterBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			     uint64_t end_epoch, uint32_t max_entries,
			     bool* is_truncated, RGWUsageIter& usage_iter,
			     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return next->read_usage(dpp, start_epoch, end_epoch, max_entries,
			  is_truncated, usage_iter, usage);
}

int FilterBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			     uint64_t end_epoch, optional_yield y)
{
  return next->trim_usage(dpp, start_epoch, end_epoch, y);
}

int FilterBucket::remove_objs_from_index(const DoutPrefixProvider *dpp,
					 std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return next->remove_objs_from_index(dpp, objs_to_unlink);
}

int FilterBucket::check_index(const DoutPrefixProvider *dpp,
			      std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
			      std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return next->check_index(dpp, existing_stats, calculated_stats);
}

int FilterBucket::rebuild_index(const DoutPrefixProvider *dpp)
{
  return next->rebuild_index(dpp);
}

int FilterBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
{
  return next->set_tag_timeout(dpp, timeout);
}

int FilterBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->purge_instance(dpp, y);
}

std::unique_ptr<MultipartUpload> FilterBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  std::unique_ptr<MultipartUpload> nmu =
    next->get_multipart_upload(oid, upload_id, owner, mtime);

  return std::make_unique<FilterMultipartUpload>(std::move(nmu), this);
}

int FilterBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated, optional_yield y)
{
  std::vector<std::unique_ptr<MultipartUpload>> nup;
  int ret;

  ret = next->list_multiparts(dpp, prefix, marker, delim, max_uploads, nup,
			      common_prefixes, is_truncated, y);
  if (ret < 0)
    return ret;

  for (auto& ent : nup) {
    uploads.emplace_back(std::make_unique<FilterMultipartUpload>(std::move(ent), this));
  }

  return 0;
}

int FilterBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y)
{
  return next->abort_multiparts(dpp, cct, y);
}

int FilterObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				uint32_t flags)
{
  return next->delete_object(dpp, y, flags);
}

int FilterObject::copy_object(const ACLOwner& owner,
			      const rgw_user& remote_user,
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
  return next->copy_object(owner, remote_user, info, source_zone,
			   nextObject(dest_object),
			   nextBucket(dest_bucket),
			   nextBucket(src_bucket),
			   dest_placement, src_mtime, mtime,
			   mod_ptr, unmod_ptr, high_precision_time, if_match,
			   if_nomatch, attrs_mod, copy_if_newer, attrs,
			   category, olh_epoch, delete_at, version_id, tag,
			   etag, progress_cb, progress_data, dpp, y);
}

RGWAccessControlPolicy& FilterObject::get_acl()
{
  return next->get_acl();
}

int FilterObject::load_obj_state(const DoutPrefixProvider *dpp,
                                 optional_yield y, bool follow_olh) {
  return next->load_obj_state(dpp, y, follow_olh);
}

int FilterObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
				Attrs* delattrs, optional_yield y)
{
  return next->set_obj_attrs(dpp, setattrs, delattrs, y);
}

int FilterObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
				rgw_obj* target_obj)
{
  return next->get_obj_attrs(y, dpp, target_obj);
}

int FilterObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
				   optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->modify_obj_attrs(attr_name, attr_val, y, dpp);
}

int FilterObject::delete_obj_attrs(const DoutPrefixProvider* dpp,
				   const char* attr_name, optional_yield y)
{
  return next->delete_obj_attrs(dpp, attr_name, y);
}

bool FilterObject::is_expired()
{
  return next->is_expired();
}

void FilterObject::gen_rand_obj_instance_name()
{
  return next->gen_rand_obj_instance_name();
}

std::unique_ptr<MPSerializer> FilterObject::get_serializer(const DoutPrefixProvider *dpp,
							   const std::string& lock_name)
{
  std::unique_ptr<MPSerializer> s = next->get_serializer(dpp, lock_name);
  return std::make_unique<FilterMPSerializer>(std::move(s));
}

int FilterObject::transition(Bucket* bucket,
			     const rgw_placement_rule& placement_rule,
			     const real_time& mtime,
			     uint64_t olh_epoch,
			     const DoutPrefixProvider* dpp,
			     optional_yield y,
                             uint32_t flags)
{
  return next->transition(nextBucket(bucket), placement_rule, mtime, olh_epoch,
			  dpp, y, flags);
}

int FilterObject::transition_to_cloud(Bucket* bucket,
				      rgw::sal::PlacementTier* tier,
				      rgw_bucket_dir_entry& o,
				      std::set<std::string>& cloud_targets,
				      CephContext* cct,
				      bool update_object,
				      const DoutPrefixProvider* dpp,
				      optional_yield y)
{
  return next->transition_to_cloud(nextBucket(bucket), nextPlacementTier(tier),
				   o, cloud_targets, cct, update_object, dpp, y);
}

bool FilterObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return next->placement_rules_match(r1, r2);
}

int FilterObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y,
				  Formatter* f)
{
  return next->dump_obj_layout(dpp, y, f);
}

void FilterObject::set_bucket(Bucket* b)
{
  bucket = b;
  next->set_bucket(nextBucket(b));
};

int FilterObject::swift_versioning_restore(const ACLOwner& owner,
                                           const rgw_user& remote_user,
                                           bool& restored,
                                           const DoutPrefixProvider* dpp,
                                           optional_yield y)
{
  return next->swift_versioning_restore(owner, remote_user, restored, dpp, y);
}

int FilterObject::swift_versioning_copy(const ACLOwner& owner,
                                        const rgw_user& remote_user,
                                        const DoutPrefixProvider* dpp,
                                        optional_yield y)
{
  return next->swift_versioning_copy(owner, remote_user, dpp, y);
}

std::unique_ptr<Object::ReadOp> FilterObject::get_read_op()
{
  std::unique_ptr<ReadOp> r = next->get_read_op();
  return std::make_unique<FilterReadOp>(std::move(r));
}

std::unique_ptr<Object::DeleteOp> FilterObject::get_delete_op()
{
  std::unique_ptr<DeleteOp> d = next->get_delete_op();
  return std::make_unique<FilterDeleteOp>(std::move(d));
}

int FilterObject::get_torrent_info(const DoutPrefixProvider* dpp,
                                   optional_yield y, bufferlist& bl)
{
  return next->get_torrent_info(dpp, y, bl);
}

int FilterObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
					const std::string& oid,
					const std::set<std::string>& keys,
					Attrs* vals)
{
  return next->omap_get_vals_by_keys(dpp, oid, keys, vals);
}

int FilterObject::omap_set_val_by_key(const DoutPrefixProvider *dpp,
				      const std::string& key, bufferlist& val,
				      bool must_exist, optional_yield y)
{
  return next->omap_set_val_by_key(dpp, key, val, must_exist, y);
}

int FilterObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->chown(new_user, dpp, y);
}

int FilterObject::FilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  /* Copy params into next */
  next->params = params;
  return next->prepare(y, dpp);
}

int FilterObject::FilterReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  int ret = next->read(ofs, end, bl, y, dpp);
  if (ret < 0)
    return ret;

  /* Copy params out of next */
  params = next->params;
  return ret;
}

int FilterObject::FilterReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  return next->get_attr(dpp, name, dest, y);
}

int FilterObject::FilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  int ret = next->iterate(dpp, ofs, end, cb, y);
  if (ret < 0)
    return ret;

  /* Copy params out of next */
  params = next->params;
  return ret;
}

int FilterObject::FilterDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y, uint32_t flags)
{
  /* Copy params into next */
  next->params = params;
  int ret = next->delete_obj(dpp, y, flags);
  /* Copy result back */
  result = next->result;
  return ret;
}

std::unique_ptr<rgw::sal::Object> FilterMultipartUpload::get_meta_obj()
{
  std::unique_ptr<Object> no = next->get_meta_obj();

  return std::make_unique<FilterObject>(std::move(no), bucket);
}

int FilterMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  return next->init(dpp, y, owner, dest_placement, attrs);
}

int FilterMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated, optional_yield y,
				      bool assume_unsorted)
{
  int ret;

  ret = next->list_parts(dpp, cct, num_parts, marker, next_marker, truncated, y,
			 assume_unsorted);
  if (ret < 0)
    return ret;

  parts.clear();

  for (auto& ent : next->get_parts()) {
    parts.emplace(ent.first, std::make_unique<FilterMultipartPart>(std::move(ent.second)));
  }

  return 0;
}

int FilterMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
{
  return next->abort(dpp, cct, y);
}

int FilterMultipartUpload::complete(const DoutPrefixProvider *dpp,
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
			nextObject(target_obj));
}

int FilterMultipartUpload::get_info(const DoutPrefixProvider *dpp,
				    optional_yield y, rgw_placement_rule** rule,
				    rgw::sal::Attrs* attrs)
{
  return next->get_info(dpp, y, rule, attrs);
}

std::unique_ptr<Writer> FilterMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  std::unique_ptr<Writer> writer;
  writer = next->get_writer(dpp, y, nextObject(obj), owner,
			    ptail_placement_rule, part_num, part_num_str);

  return std::make_unique<FilterWriter>(std::move(writer), obj);
}

int FilterMPSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur,
				 optional_yield y)
{
  return next->try_lock(dpp, dur, y);
}

int FilterLCSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur,
				 optional_yield y)
{
  return next->try_lock(dpp, dur, y);
}

std::unique_ptr<Lifecycle::LCEntry> FilterLifecycle::get_entry()
{
  std::unique_ptr<Lifecycle::LCEntry> e = next->get_entry();
  return std::make_unique<FilterLCEntry>(std::move(e));
}

int FilterLifecycle::get_entry(const std::string& oid, const std::string& marker,
			       std::unique_ptr<LCEntry>* entry)
{
  std::unique_ptr<LCEntry> ne;
  int ret;

  ret = next->get_entry(oid, marker, &ne);
  if (ret < 0)
    return ret;

  LCEntry* e = new FilterLCEntry(std::move(ne));
  entry->reset(e);

  return 0;
}

int FilterLifecycle::get_next_entry(const std::string& oid, const std::string& marker,
				    std::unique_ptr<LCEntry>* entry)
{
  std::unique_ptr<LCEntry> ne;
  int ret;

  ret = next->get_next_entry(oid, marker, &ne);
  if (ret < 0)
    return ret;

  LCEntry* e = new FilterLCEntry(std::move(ne));
  entry->reset(e);

  return 0;
}

int FilterLifecycle::set_entry(const std::string& oid, LCEntry& entry)
{
  return next->set_entry(oid, entry);
}

int FilterLifecycle::list_entries(const std::string& oid, const std::string& marker,
				  uint32_t max_entries,
				  std::vector<std::unique_ptr<LCEntry>>& entries)
{
  std::vector<std::unique_ptr<LCEntry>> ne;
  int ret;

  ret = next->list_entries(oid, marker, max_entries, ne);
  if (ret < 0)
    return ret;

  for (auto& ent : ne) {
    entries.emplace_back(std::make_unique<FilterLCEntry>(std::move(ent)));
  }

  return 0;
}

int FilterLifecycle::rm_entry(const std::string& oid, LCEntry& entry)
{
  return next->rm_entry(oid, entry);
}

int FilterLifecycle::get_head(const std::string& oid, std::unique_ptr<LCHead>* head)
{
  std::unique_ptr<LCHead> nh;
  int ret;

  ret = next->get_head(oid, &nh);
  if (ret < 0)
    return ret;

  LCHead* h = new FilterLCHead(std::move(nh));
  head->reset(h);

  return 0;
}

int FilterLifecycle::put_head(const std::string& oid, LCHead& head)
{
  return next->put_head(oid, *(dynamic_cast<FilterLCHead&>(head).next.get()));
}

std::unique_ptr<LCSerializer> FilterLifecycle::get_serializer(
					      const std::string& lock_name,
					      const std::string& oid,
					      const std::string& cookie)
{
  std::unique_ptr<LCSerializer> ns;
  ns = next->get_serializer(lock_name, oid, cookie);

  return std::make_unique<FilterLCSerializer>(std::move(ns));
}

int FilterNotification::publish_reserve(const DoutPrefixProvider *dpp,
					RGWObjTags* obj_tags)
{
  return next->publish_reserve(dpp, obj_tags);
}

int FilterNotification::publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
				       const ceph::real_time& mtime, const
				       std::string& etag, const std::string& version)
{
  return next->publish_commit(dpp, size, mtime, etag, version);
}

int FilterWriter::process(bufferlist&& data, uint64_t offset)
{
  return next->process(std::move(data), offset);
}

int FilterWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  return next->complete(accounted_size, etag, mtime, set_mtime, attrs,
			delete_at, if_match, if_nomatch, user_data, zones_trace,
			canceled, rctx, flags);
}

int FilterLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key, std::string& script)
{
  return next->get_script(dpp, y, key, script);
}

int FilterLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key, const std::string& script)
{
  return next->put_script(dpp, y, key, script);
}

int FilterLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& key)
{
  return next->del_script(dpp, y, key);
}

int FilterLuaManager::add_package(const DoutPrefixProvider* dpp, optional_yield y,
                                 const std::string& package_name)
{
  return next->add_package(dpp, y, package_name);
}

int FilterLuaManager::remove_package(const DoutPrefixProvider* dpp, optional_yield y, 
                                    const std::string& package_name)
{
  return next->remove_package(dpp, y, package_name);
}

int FilterLuaManager::list_packages(const DoutPrefixProvider* dpp, optional_yield y,
                                   rgw::lua::packages_t& packages)
{
  return next->list_packages(dpp, y, packages);
}

int FilterLuaManager::reload_packages(const DoutPrefixProvider* dpp, optional_yield y)
{
  return next->reload_packages(dpp, y);
}

const std::string& FilterLuaManager::luarocks_path() const {
  return next->luarocks_path();
}

void FilterLuaManager::set_luarocks_path(const std::string& path) {
  next->set_luarocks_path(path);
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newBaseFilter(rgw::sal::Driver* next)
{
  rgw::sal::FilterDriver* driver = new rgw::sal::FilterDriver(next);

  return driver;
}

}
